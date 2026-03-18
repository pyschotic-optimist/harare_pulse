
HARARE PULSE — Power BI Dashboard Setup Guide
==============================================
Generated: 2026-03-17 15:05

FILES IN THIS FOLDER
--------------------
map_roads.csv
  Use for: Map visual (Latitude = mid_lat, Longitude = mid_lon)
  Colour by: congestion_band or band_colour
  Tooltip: road_name, avg_congestion, avg_speed_kmh

map_incidents.csv
  Use for: Incident heatmap / scatter map
  Latitude: lat  |  Longitude: lon
  Colour by: severity or incident_type
  Filter by: status (Active / Resolved)

map_nodes.csv
  Use for: Intersection density map
  Latitude: lat  |  Longitude: lon
  Filter by: is_major (1 = 4+ roads meeting)

dashboard_kpis.csv
  Use for: KPI card visuals (single row of summary stats)
  Fields: peak_avg_congestion, peak_avg_speed_kmh,
          active_incidents, total_road_km, kombi_routes

hourly_congestion_profile.csv
  Use for: Line chart — congestion by hour, split by day_name
  X-axis: hour_of_day  |  Y-axis: avg_congestion
  Legend: day_name

peak_vs_offpeak.csv
  Use for: Clustered bar chart — peak vs off-peak by artery
  X-axis: artery
  Y-axis: peak_congestion and offpeak_congestion (two series)

artery_comparison.csv
  Use for: Table or bar chart comparing major arteries

RECOMMENDED DASHBOARD LAYOUT
-----------------------------
Page 1 — Overview
  [ KPI Cards: Congestion | Speed | Incidents | Road km ]
  [ Map: road segments coloured by congestion band       ]
  [ Line chart: hourly congestion profile by day         ]

Page 2 — Arteries
  [ Clustered bar: peak vs off-peak by artery            ]
  [ Table: artery stats with conditional formatting      ]
  [ Slicer: filter by day of week / peak hour            ]

Page 3 — Incidents
  [ Map: incident pins coloured by severity              ]
  [ Bar chart: incidents by type                         ]
  [ Table: active incidents sorted by severity           ]

CONGESTION BAND COLOURS (for manual conditional formatting)
-----------------------------------------------------------
  Free flow  #1D9E75  (green)
  Moderate   #BA7517  (amber)
  Heavy      #D85A30  (orange-red)
  Gridlock   #A32D2D  (red)
  No data    #888780  (gray)

NEXT STEPS
----------
  Phase 2 will replace synthetic data with live Google Maps /
  Waze API feeds — the schema and all Power BI connections
  stay identical, so the dashboard upgrades automatically.
