from kafka import KafkaConsumer, KafkaProducer
import json
from os import environ
from shapely.geometry import shape, Polygon, MultiPolygon, Point, mapping
from shapely.ops import transform
from pyproj import CRS, Transformer

# Kafka setup
bootstrap_servers = environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
CONSUMER_TOPIC = "adsb.live"
PRODUCER_TOPIC = "adsb.alerts"

consumer = KafkaConsumer(
    CONSUMER_TOPIC,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Airports and buffer radius (meters)
airports = {
    "SFO": (37.6191, -122.3816),
    # "SJC": (37.3635, -121.9286),
}
radius = 10_000  # in meters

# Geometry containers
polygonsGeneral = []
polygonsLaser = []
airportShapes = []

def setup():
    global polygonsGeneral, polygonsLaser, airportShapes
    polygonsGeneral.clear()
    polygonsLaser.clear()
    airportShapes.clear()

    # 1) Load your flight envelopes
    with open("flight_envelopes.geojson", "r") as f:
        geojson_data = json.load(f)

    for feature in geojson_data["features"]:
        geom = shape(feature["geometry"])
        is_precision = feature.get("properties", {}).get("type") == "precision"

        if isinstance(geom, MultiPolygon):
            geoms = list(geom.geoms)
        elif isinstance(geom, Polygon):
            geoms = [geom]
        else:
            continue

        if is_precision:
            polygonsLaser.extend(geoms)
        else:
            polygonsGeneral.extend(geoms)

    # 2) Build airport buffers and collect GeoJSON features
    wgs84 = CRS.from_epsg(4326)
    features = []

    for code, (lat, lon) in airports.items():
        # Azimuthal equidistant CRS centered on the airport
        aeqd_crs = CRS.from_proj4(
            f"+proj=aeqd +R=6371000 +units=m +lat_0={lat} +lon_0={lon}"
        )
        to_aeqd = Transformer.from_crs(wgs84, aeqd_crs, always_xy=True).transform
        to_wgs84 = Transformer.from_crs(aeqd_crs, wgs84, always_xy=True).transform

        # Create, project, buffer, reproject
        center = Point(lon, lat)
        pt_aeqd = transform(to_aeqd, center)
        buf_aeqd = pt_aeqd.buffer(radius)          # buffer in meters
        circle_poly = transform(to_wgs84, buf_aeqd)

        airportShapes.append(circle_poly)

        # prepare GeoJSON feature
        features.append({
            "type": "Feature",
            "geometry": mapping(circle_poly),
            "properties": {
                "airport": code,
                "center": {"lat": lat, "lon": lon},
                "radius_m": radius
            }
        })

    # 3) Debug: print loaded buffers
    print(f"Loaded {len(airportShapes)} airport buffer(s):")
    for code, poly in zip(airports.keys(), airportShapes):
        print(f"  {code} bounds: {poly.bounds}")

    # 4) Print out the GeoJSON FeatureCollection
    feature_collection = {
        "type": "FeatureCollection",
        "features": features
    }
    print("\n--- Airport Buffers GeoJSON ---")
    print(json.dumps(feature_collection, indent=2))

def main():
    for message in consumer:
        value = message.value  # already a dict
        value = json.loads(value)

        # skip invalid or ground messages
        if not value.get("icao24") or value["icao24"] == "-1":
            continue
        if value.get("lat") is None or value.get("lon") is None:
            continue

        lon = value["lon"]
        lat = value["lat"]
        point = Point(lon, lat)

        kafkaMessage = {"icao24": value["icao24"], "rating": 0, "message": ""}

        # Check if inside the airport buffer at all
        inside_any = any(point.within(poly) for poly in airportShapes)
        inside_precision = any(point.within(poly) for poly in polygonsLaser)
        inside_wider = any(point.within(poly) for poly in polygonsGeneral)

        if inside_any:
            # If inside general buffer, see if outside the laser-thin precision zone

            if not (inside_precision or inside_wider):

                kafkaMessage["rating"] = 4
                kafkaMessage["message"] = "URGENT URGENT URGENT"
            elif not(inside_precision):
                kafkaMessage["rating"] = 3
                kafkaMessage["message"] = "Outside laser-thin final approach"

        else:
            # Outside airport buffer altogether: check both general + precision zones
            if not (inside_wider or inside_precision):
                kafkaMessage["rating"] = 2
                kafkaMessage["message"] = "Outside wider transition and laser-thin areas outside final approach"
            elif not (inside_precision):
                kafkaMessage["rating"] = 1
                kafkaMessage["message"] = "Inside wider transition areas outside final approach"
            else:
                kafkaMessage["rating"] = 0
                kafkaMessage["message"] = "Inside wider transition and laser-thin areas outside final approach"

        # Send alert if needed
        print("Aircraft: "+value["icao24"] + ", Alarm(0-4): "+ kafkaMessage['rating'],)
        print("Message: "+kafkaMessage['message'])
        print('\n')
        producer.send(PRODUCER_TOPIC, value=kafkaMessage)

    producer.flush()

if __name__ == "__main__":
    setup()
    main()
