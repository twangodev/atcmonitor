from kafka import KafkaConsumer, KafkaProducer
import json
from os import environ
from shapely.geometry import shape, Polygon, MultiPolygon, Point, mapping
from shapely.ops import transform
from pyproj import CRS, Transformer
from typing import List, Tuple, Optional


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
height = 1000 # in feet
# Geometry containers
polygonsGeneral = []
polygonsLaser = []
polygonsGeneral2d = []
polygonsLaser2d = []
airportShapes = []
airportShapes2d = []

def setup():
    global polygonsGeneral, polygonsLaser, airportShapes, airportShapes2d, polygonsGeneral2d, polygonsLaser2d
    polygonsGeneral.clear()
    polygonsLaser.clear()
    airportShapes.clear()
    polygonsLaser2d.clear()
    airportShapes2d.clear()

    # 1) Load your flight envelopes
    with open("flight_envelopes_3d.geojson", "r") as f:
        geojson_data = json.load(f)

    for feature in geojson_data["features"]:
        geom = shape(feature["geometry"])
        if not isinstance(geom, (Polygon, MultiPolygon)) or geom.is_empty:
            continue
        props = feature.get("properties", {})
        is_precision = props.get("type") == "precision"
        alt = props.get("geoaltitude")

        if geom.is_empty or alt is None:
            continue
        if is_precision:
            polygonsLaser.append((alt, geom))
        else:
            polygonsGeneral.append((alt, geom))



    #2D
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
            polygonsLaser2d.extend(geoms)
        else:
            polygonsGeneral2d.extend(geoms)



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

        airportShapes2d.append(circle_poly)

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
    """    print(f"Loaded {len(airportShapes)} airport buffer(s):")
        for code, poly in zip(airports.keys(), airportShapes):
            print(f"  {code} bounds: {poly.bounds}")
    """
    # 4) Print out the GeoJSON FeatureCollection
    feature_collection = {
        "type": "FeatureCollection",
        "features": features
    }
"""    print("\n--- Airport Buffers GeoJSON ---")
    print(json.dumps(feature_collection, indent=2))
"""


def load_3d_envelopes(path):
    prec_env = []
    trans_env = []

    with open(path, "r") as f:
        geojson_data = json.load(f)

    for feature in geojson_data["features"]:
        props = feature.get("properties", {})
        altitude = float(props.get("geoaltitude", 0))
        polygon = shape(feature["geometry"])
        if not polygon.is_valid or polygon.is_empty:
            continue

        if props.get("type") == "precision":
            prec_env.append((altitude, polygon))
        elif props.get("type") == "transition":
            trans_env.append((altitude, polygon))

    return prec_env, trans_env





def check_3d_slice(point: Point, alt: float, slices: List[Tuple[float, Polygon]], tolerance: float = 500) -> Tuple[bool, Optional[float]]:
    best_diff = None
    for slice_alt, geom in slices:
        diff = abs(alt - slice_alt)
        if diff <= tolerance and geom.contains(point):
            if best_diff is None or diff < best_diff:
                best_diff = diff
    return (best_diff is not None, best_diff)

def main():

    prec_env,trans_env = load_3d_envelopes('flight_envelopes_3d.geojson')

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

        if value.get("geoaltitude") is None:
            value["geoaltitude"] = ''
            
        alt = value["geoaltitude"]

        point = Point(lon, lat)

        kafkaMessage = {"icao24": value.get("icao24", "UNKNOWN"), "rating": 0, "message": "", "altitude": False}
        lon, lat = value.get("lon"), value.get("lat")
        if lon is None or lat is None:
            return None  # skip incomplete data

        point = Point(lon, lat)


        if alt is not None and alt is not '' and float(alt) <= 10000:
            alt = float(alt)
            kafkaMessage['altitude'] = True
            # 3D logic
            inside_precision, diff_prec = check_3d_slice(point, alt, prec_env)
            inside_wider, diff_wide = check_3d_slice(point, alt, trans_env)
            inside_any = any(point.within(poly) for poly in airportShapes2d)

            if alt < 1000 and inside_any:
                if inside_precision:
                    """                    kafkaMessage["rating"] = 0  
                                        kafkaMessage["message"] = f"Inside precision 3D envelope (\u0394alt~{diff_prec:.0f}ft)"
                    """                
                    pass
                elif inside_wider:
                    kafkaMessage["rating"] = 3
                    kafkaMessage["message"] = f"Outside 3D laser-thin final approach"
                else:
                    kafkaMessage["rating"] = 4
                    kafkaMessage["message"] = "URGENT ALERT URGENT ALERT"
            else:
                if inside_precision:
                    kafkaMessage["rating"] = 0  
                    kafkaMessage["message"] = f"Inside 3D wider transition and laser-thin areas outside final approach"
                elif inside_wider:
                    kafkaMessage["rating"] = 1
                    kafkaMessage["message"] = f"Inside 3D wider transition areas outside final approach"
                else:
                    kafkaMessage["rating"] = 2
                    kafkaMessage["message"] = f"Outside 3D wider transition and laser-thin areas outside final approach"


        else:
            # Fallback to 2D
            inside_any = any(point.within(poly) for poly in airportShapes2d)
            inside_precision = any(point.within(poly) for poly in polygonsLaser2d)
            inside_wider = any(point.within(poly) for poly in polygonsGeneral2d)

            if inside_any:
                # If inside general buffer, see if outside the laser-thin precision zone

                if not (inside_precision or inside_wider):

                    kafkaMessage["rating"] = 4
                    kafkaMessage["message"] = "URGENT ALERT URGENT ALERT"
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
        print("Aircraft: "+value["icao24"] + ", Alarm(0-4): "+ str(kafkaMessage['rating'])+',')
        print("Message: "+kafkaMessage['message'] + '.')
        print('\n')
        producer.send(PRODUCER_TOPIC, value=kafkaMessage)

    producer.flush()

if __name__ == "__main__":
    setup()
    main()
