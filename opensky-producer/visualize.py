import itertools
from datetime import datetime

import folium
from folium.plugins import TimestampedGeoJson
from pandas import DataFrame

from main import main, center


def timeline(map_center, df: DataFrame):
    colors = itertools.cycle([
        '#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd',
        '#8c564b','#e377c2','#7f7f7f','#bcbd22','#17becf'
    ])
    icao_colors = {icao: next(colors) for icao in df['icao24'].unique()}

    features = []
    for icao, grp in df.groupby('icao24'):
        # ISO-format timestamps with Z (UTC) suffix:
        times = [
            datetime.fromtimestamp(ts).isoformat() + 'Z'
            for ts in grp['time']
        ]
        coords = list(zip(grp['lon'], grp['lat']))  # GeoJSON is [lon,lat]

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": coords
            },
            "properties": {
                "times": times,
                "style": {"color": icao_colors[icao], "weight": 2},
                "popup": icao
            }
        })

    m = folium.Map(location=map_center, zoom_start=10, tiles='CartoDB positron')

    TimestampedGeoJson(
        {"type": "FeatureCollection", "features": features},
        period="PT1S",             # time resolution: 1 second
        add_last_point=True,       # show the aircraft’s “current” point
        auto_play=False,
        loop=False,
        max_speed=1,
        loop_button=True,
        date_options='YYYY/MM/DD HH:mm:ss',
        time_slider_drag_update=True
    ).add_to(m)

    # 4) Save & view
    m.save('sfo_tracks_timeslider.html')

def tracks(map_center, df: DataFrame):
    m = folium.Map(location=map_center, zoom_start=10, tiles='CartoDB positron')

    # 3. Draw each aircraft’s raw track
    for icao, grp in df.groupby('icao24'):
        coords = list(zip(grp['lat'], grp['lon']))
        folium.PolyLine(
            coords,
            weight=1,
            opacity=0.1,
            tooltip=icao
        ).add_to(m)


    # 5. Save to an HTML file and open in your browser
    m.save('sfo_aircraft_tracks.html')


if __name__ == "__main__":
    df = main(
        should_sum_dfs=True,
        should_send_to_kafka=False,
    )

    timeline(center, df)
    tracks(center, df)



