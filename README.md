# Harare Pulse 🚦

A Digital Twin and traffic intelligence platform for the **Harare Central Business District (CBD)**.

Built to visualise congestion patterns, model infrastructure interventions, and power smarter urban planning decisions through data.

---

## Project Overview

| Component | Detail |
|---|---|
| **City map** | 3,753 road segments · 1,482 intersections · 521 km of network |
| **Traffic data** | 756,000 readings across 7 days at 15-min intervals |
| **Incidents** | 120 incidents at 8 CBD hotspots |
| **Routes** | 7 kombi / bus routes seeded |
| **Dashboard** | Power BI — congestion, speed, incidents, artery comparison |

---

## Repository Structure

```
harare-pulse/
│
├── step1_harare_street_network.py   # Download CBD street network from OSM
├── step2_data_schema.py             # Create SQLite database schema
├── step3_synthetic_data.py          # Generate synthetic traffic data
├── step4_powerbi_export.py          # Export Power BI data package
│
├── harare_pulse_data/               # Generated data folder (gitignored in part)
│   ├── road_segments.csv            ← tracked (small, useful reference)
│   ├── intersections.csv            ← tracked
│   ├── incidents.csv                ← tracked
│   ├── hourly_congestion_profile.csv← tracked
│   ├── peak_vs_offpeak.csv          ← tracked
│   ├── artery_comparison.csv        ← tracked
│   ├── dashboard_kpis.csv           ← tracked
│   └── README_PowerBI.txt           ← tracked
│
├── .gitignore
└── README.md
```

---

## Quickstart

### Requirements
```bash
pip install osmnx networkx geopandas matplotlib folium pandas numpy
```

### Run in order
```bash
python step1_harare_street_network.py   # ~2 min — downloads live OSM data
python step2_data_schema.py             # ~5 sec — creates database
python step3_synthetic_data.py          # ~2 min — generates 756K readings
python step4_powerbi_export.py          # ~30 sec — exports Power BI CSVs
```

Output lands in `harare_pulse_data\`. Load all CSVs into Power BI Desktop to build the dashboard (see `README_PowerBI.txt`).

---

## Traffic Patterns Modelled

| Period | Congestion level |
|---|---|
| Morning peak (7–9am) | ~62–90% |
| Lunch shoulder (12–1pm) | ~55% |
| Evening peak (5–7pm) | ~85–90% |
| Off-peak | ~20–38% |
| Saturday | ~40% lighter than weekday |
| Sunday | ~65% lighter than weekday |

**Major arteries tracked:** Samora Machel Ave · Julius Nyerere Way · Kwame Nkrumah Ave · Robert Mugabe Rd · First Street · Second Street

---

## Roadmap

- [x] Phase 1 — Data & Map Foundation
- [ ] Phase 2 — Live Google Maps / Waze API feeds
- [ ] Phase 3 — Power BI dashboard (arteries + incidents pages)
- [ ] Phase 4 — Impact simulation engine (road closures, Pedestrian-Only Sunday)
- [ ] Phase 5 — Validation & deployment

---

## Data Sources

- **Street network:** [OpenStreetMap](https://www.openstreetmap.org/) via [OSMnx](https://github.com/gboeing/osmnx)
- **Traffic data:** Synthetic (Phase 1) — to be replaced with Google Maps / Waze API in Phase 2
- **Incidents:** Synthetic, modelled on known Harare CBD hotspot intersections

---

## Tech Stack

| Layer | Tools |
|---|---|
| Data ingestion | Python · OSMnx · GeoPandas |
| Storage | SQLite |
| Analytics | Pandas · NumPy |
| Visualisation | Power BI · Folium · Matplotlib |
| Phase 2+ | Google Maps API · Waze · R |

---

*Built as part of the Harare Pulse Digital Twin project.*
