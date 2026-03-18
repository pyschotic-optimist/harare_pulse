"""
Harare Pulse — Phase 1, Step 2
================================
Creates the SQLite database schema for the Harare Pulse digital twin.
Tables:
  - road_segments     → the CBD street network (from Step 1)
  - traffic_readings  → timestamped congestion readings per segment
  - incidents         → accidents, breakdowns, roadworks
  - transport_routes  → public transport / kombi routes
  - simulations       → logged what-if scenario runs

Run this ONCE to initialise the database. Safe to re-run — uses
CREATE TABLE IF NOT EXISTS throughout.
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR = "harare_pulse_data"
DB_PATH  = os.path.join(DATA_DIR, "harare_pulse.db")
os.makedirs(DATA_DIR, exist_ok=True)

print("=" * 55)
print("  Harare Pulse · Step 2: Data Schema Setup")
print("=" * 55)

conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

# ── Enable foreign keys & WAL mode (faster writes) ───────────────────────────
cur.execute("PRAGMA foreign_keys = ON")
cur.execute("PRAGMA journal_mode = WAL")

# ── Table 1: road_segments ────────────────────────────────────────────────────
print("\n[1/6] Creating table: road_segments ...")
cur.execute("""
CREATE TABLE IF NOT EXISTS road_segments (
    segment_id      TEXT PRIMARY KEY,   -- OSM edge ID (u_v_key)
    osm_id          INTEGER,            -- OpenStreetMap way ID
    name            TEXT,               -- Road name (e.g. Samora Machel Ave)
    artery          TEXT,               -- Major artery label or 'Other'
    highway         TEXT,               -- OSM highway type (primary, secondary...)
    length_m        REAL,               -- Segment length in metres
    max_speed_kmh   INTEGER,            -- Speed limit (NULL if unknown)
    oneway          INTEGER DEFAULT 0,  -- 1 = one-way, 0 = two-way
    lanes           INTEGER,            -- Number of lanes (NULL if unknown)
    from_node       INTEGER,            -- OSM start node ID
    to_node         INTEGER,            -- OSM end node ID
    geometry_wkt    TEXT,               -- WKT linestring for mapping
    created_at      TEXT DEFAULT (datetime('now'))
)
""")

# ── Table 2: traffic_readings ─────────────────────────────────────────────────
print("[2/6] Creating table: traffic_readings ...")
cur.execute("""
CREATE TABLE IF NOT EXISTS traffic_readings (
    reading_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_id          TEXT NOT NULL,
    recorded_at         TEXT NOT NULL,              -- ISO timestamp
    hour_of_day         INTEGER,                    -- 0–23 (derived, for fast queries)
    day_of_week         INTEGER,                    -- 0=Mon … 6=Sun
    is_peak_hour        INTEGER DEFAULT 0,          -- 1 if 7-9am or 5-7pm
    congestion_level    REAL,                       -- 0.0 (free flow) → 1.0 (gridlock)
    avg_speed_kmh       REAL,                       -- Average vehicle speed
    travel_time_s       REAL,                       -- Estimated traversal time (seconds)
    vehicle_count       INTEGER,                    -- Vehicles observed / estimated
    source              TEXT DEFAULT 'synthetic',   -- 'synthetic','gps','waze','sensor'
    FOREIGN KEY (segment_id) REFERENCES road_segments(segment_id)
)
""")

# ── Table 3: incidents ────────────────────────────────────────────────────────
print("[3/6] Creating table: incidents ...")
cur.execute("""
CREATE TABLE IF NOT EXISTS incidents (
    incident_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_id      TEXT,
    incident_type   TEXT NOT NULL,      -- 'accident','breakdown','roadwork','flooding'
    severity        TEXT DEFAULT 'low', -- 'low','medium','high','critical'
    reported_at     TEXT NOT NULL,
    resolved_at     TEXT,               -- NULL = still active
    lat             REAL,
    lon             REAL,
    description     TEXT,
    source          TEXT DEFAULT 'synthetic',
    FOREIGN KEY (segment_id) REFERENCES road_segments(segment_id)
)
""")

# ── Table 4: transport_routes ─────────────────────────────────────────────────
print("[4/6] Creating table: transport_routes ...")
cur.execute("""
CREATE TABLE IF NOT EXISTS transport_routes (
    route_id        TEXT PRIMARY KEY,
    route_name      TEXT NOT NULL,      -- e.g. 'Kombi Route 4 — CBD to Mbare'
    mode            TEXT DEFAULT 'kombi', -- 'kombi','bus','emergency'
    frequency_min   INTEGER,            -- Headway in minutes (peak)
    active          INTEGER DEFAULT 1,
    notes           TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS route_segments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    route_id        TEXT NOT NULL,
    segment_id      TEXT NOT NULL,
    stop_sequence   INTEGER,
    FOREIGN KEY (route_id)  REFERENCES transport_routes(route_id),
    FOREIGN KEY (segment_id) REFERENCES road_segments(segment_id)
)
""")

# ── Table 5: simulations ──────────────────────────────────────────────────────
print("[5/6] Creating table: simulations ...")
cur.execute("""
CREATE TABLE IF NOT EXISTS simulations (
    sim_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,      -- e.g. 'Close Samora Machel — Sunday'
    scenario_type   TEXT,               -- 'road_closure','pedestrian_day','new_route'
    closed_segments TEXT,               -- JSON list of closed segment_ids
    run_at          TEXT DEFAULT (datetime('now')),
    duration_min    INTEGER,            -- Simulated duration in minutes
    notes           TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS simulation_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sim_id          INTEGER NOT NULL,
    segment_id      TEXT NOT NULL,
    congestion_delta REAL,             -- Change vs baseline (+ve = worse)
    travel_time_delta_s REAL,
    rerouted_volume INTEGER,
    FOREIGN KEY (sim_id) REFERENCES simulations(sim_id)
)
""")

# ── Indexes for fast dashboard queries ────────────────────────────────────────
print("[6/6] Creating indexes ...")
cur.executescript("""
CREATE INDEX IF NOT EXISTS idx_readings_segment   ON traffic_readings(segment_id);
CREATE INDEX IF NOT EXISTS idx_readings_time      ON traffic_readings(recorded_at);
CREATE INDEX IF NOT EXISTS idx_readings_peak      ON traffic_readings(is_peak_hour);
CREATE INDEX IF NOT EXISTS idx_readings_hour      ON traffic_readings(hour_of_day);
CREATE INDEX IF NOT EXISTS idx_incidents_segment  ON incidents(segment_id);
CREATE INDEX IF NOT EXISTS idx_incidents_type     ON incidents(incident_type);
CREATE INDEX IF NOT EXISTS idx_incidents_active   ON incidents(resolved_at);
""")

conn.commit()

# ── Seed reference data: known Harare kombi routes ───────────────────────────
print("\n  Seeding reference data (kombi routes)...")
routes = [
    ("RT01", "CBD → Mbare via Remembrance Drive",      "kombi", 8),
    ("RT02", "CBD → Highfield via Beatrice Rd",        "kombi", 10),
    ("RT03", "CBD → Borrowdale via Samora Machel Ave", "kombi", 12),
    ("RT04", "CBD → Msasa via Robert Mugabe Rd",       "kombi", 15),
    ("RT05", "CBD → Glen Norah via Nyerere Way",       "kombi", 10),
    ("RT06", "CBD → Kuwadzana via Lytton Rd",          "kombi", 20),
    ("RT07", "CBD → Epworth",                          "bus",   25),
]
cur.executemany("""
    INSERT OR IGNORE INTO transport_routes
        (route_id, route_name, mode, frequency_min)
    VALUES (?, ?, ?, ?)
""", routes)
conn.commit()

# ── Load road_segments from Step 1 CSV (if available) ─────────────────────────
csv_path = os.path.join(DATA_DIR, "road_segments.csv")
if os.path.exists(csv_path):
    print("  Loading road segments from Step 1 CSV...")
    df = pd.read_csv(csv_path)

    # Build segment_id from index (u, v, key) if available
    if "u" in df.columns and "v" in df.columns:
        df["segment_id"] = df["u"].astype(str) + "_" + df["v"].astype(str)
    elif df.index.name:
        df = df.reset_index()
        df["segment_id"] = df.index.astype(str)
    else:
        df["segment_id"] = df.index.astype(str)

    df["name"]   = df["name"].apply(lambda n: str(n) if pd.notna(n) else None)
    df["oneway"] = df["oneway"].apply(lambda x: 1 if str(x).lower() == "true" else 0)

    rows = []
    for _, r in df.iterrows():
        rows.append((
            str(r.get("segment_id", "")),
            int(r["osmid"])    if pd.notna(r.get("osmid"))    else None,
            r.get("name"),
            r.get("artery",   "Other"),
            r.get("highway"),
            float(r["length"]) if pd.notna(r.get("length"))   else None,
            None,   # max_speed — not always in OSM data
            int(r.get("oneway", 0)),
            None,   # lanes
            None,   # from_node
            None,   # to_node
            None,   # geometry_wkt (GeoJSON file holds this)
        ))

    cur.executemany("""
        INSERT OR IGNORE INTO road_segments
            (segment_id, osm_id, name, artery, highway,
             length_m, max_speed_kmh, oneway, lanes,
             from_node, to_node, geometry_wkt)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    seg_count = cur.execute("SELECT COUNT(*) FROM road_segments").fetchone()[0]
    print(f"      ✓ {seg_count:,} road segments loaded into database")
else:
    print("  ⚠  road_segments.csv not found — run Step 1 first, then re-run this script.")
    print("     Schema created successfully; segments can be loaded later.")

# ── Verification ──────────────────────────────────────────────────────────────
print("\n  Database verification:")
tables = cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()
for (tbl,) in tables:
    count = cur.execute(f"SELECT COUNT(*) FROM [{tbl}]").fetchone()[0]
    print(f"    {tbl:<28} {count:>6} rows")

conn.close()

print("\n" + "=" * 55)
print("  Step 2 Complete!")
print("=" * 55)
print(f"""
  Database: {DB_PATH}

  Tables created:
    road_segments          ← CBD street network
    traffic_readings       ← congestion per segment/time
    incidents              ← accidents, breakdowns, roadworks
    transport_routes       ← kombi & bus routes (7 seeded)
    route_segments         ← route ↔ segment mapping
    simulations            ← what-if scenario log
    simulation_results     ← per-segment simulation output

  Next: Run step3_synthetic_data.py to populate traffic_readings
        with realistic Harare CBD traffic patterns.
""")
