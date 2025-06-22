#!/usr/bin/env python3
import json
from pathlib import Path

# Paths
GEOJSON_PATH = Path("flight_envelopes.geojson")   # now plural
HTML_PATH    = Path("viewer.html")

# Load the GeoJSON (expects a FeatureCollection with two Features,
# each having properties.type = "precision" or "transition")
with GEOJSON_PATH.open() as f:
    geojson = json.load(f)

# Build the HTML with the GeoJSON embedded
html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Flight Envelopes Viewer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <!-- Leaflet CSS -->
  <link
    rel="stylesheet"
    href="https://unpkg.com/leaflet@1.9.3/dist/leaflet.css"
  />
  <style>
    html, body, #map {{ height: 100%; margin: 0; padding: 0; }}
    .legend {{ background: white; padding: 6px; font-family: sans-serif; }}
    .legend-item {{ display: flex; align-items: center; margin-bottom: 4px; }}
    .legend-color {{ width: 16px; height: 16px; margin-right: 6px; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <!-- Leaflet JS -->
  <script src="https://unpkg.com/leaflet@1.9.3/dist/leaflet.js"></script>
  <script>
    // 1) Initialize map
    var map = L.map('map').setView([0,0], 2);
    L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 19, attribution: '© OpenStreetMap'
    }}).addTo(map);

    // 2) Embedded GeoJSON
    var data = {json.dumps(geojson)};

    // 3) Define styles
    function styleByType(feature) {{
      if (feature.properties.type === "precision") {{
        return {{ color: '#3388ff', weight: 2, opacity: 0.8, fillOpacity: 0.1 }};
      }} else if (feature.properties.type === "transition") {{
        return {{ color: '#ff0000', weight: 2, opacity: 0.6, fillOpacity: 0.15 }};
      }} else {{
        return {{ color: '#888888', weight: 1, opacity: 0.5 }};
      }}
    }}

    // 4) Add GeoJSON layer
    var layer = L.geoJSON(data, {{
      style: styleByType
    }}).addTo(map);

    // 5) Fit to bounds
    map.fitBounds(layer.getBounds());

    // 6) Add legend
    var legend = L.control({{position: 'bottomright'}});
    legend.onAdd = function(map) {{
      var div = L.DomUtil.create('div', 'legend');
      var items = [
        ['precision', '#3388ff', 'Laser-thin final approach'],
        ['transition', '#ff0000', 'Wider transition areas']
      ];
      items.forEach(function(it) {{
        var color = it[1], label = it[2];
        div.innerHTML +=
          '<div class="legend-item">'
          + '<span class="legend-color" style="background:'+color+'"></span>'
          + label
          + '</div>';
      }});
      return div;
    }};
    legend.addTo(map);
  </script>
</body>
</html>
"""

# Write out
HTML_PATH.write_text(html)
print(f"Wrote viewer to {HTML_PATH}")
