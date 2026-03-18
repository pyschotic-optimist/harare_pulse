"""
Harare Pulse — Phase 1, Step 3
================================
Generates realistic synthetic traffic data for the Harare CBD and loads
it into the SQLite database created in Step 2.

Patterns modelled:
  - Morning peak  : 07:00 – 09:00 (high congestion inbound)
  - Evening peak  : 17:00 – 19:00 (high congestion outbound)
  - Lunch shoulder: 12:00 – 13:00 (moderate)
  - Off-peak      : remaining hours (low)
  - Weekend drop  : Saturday 40% lighter, Sunday 65% lighter
  - Artery weights: major roads carry more volume and show higher congestion
  - Incidents     : accidents and breakdowns seeded at realistic hotspot locations

Generates 7 days of readings at 15-minute intervals = 672 timestamps × 3,753 segments
(subset sampled for performance — configurable via SAMPLE_RATE below).
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR    = "harare_pulse_data"
DB_PATH     = os.path.join(DATA_DIR, "harare_pulse.db")
SAMPLE_RATE = 0.30          # fraction of segments per timestamp (0.3 = 30%)
DAYS        = 7             # days of history to generate
INTERVAL    = 15            # minutes between readings
SEED        = 42
np.random.seed(SEED)

print("=" * 55)
print("  Harare Pulse · Step 3: Synthetic Traffic Data")
print("=" * 55)

conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

# ── Load segments from DB ─────────────────────────────────────────────────────
print("\n[1/5] Loading road segments from database...")
segments = pd.read_sql("SELECT segment_id, artery, length_m FROM road_segments", conn)
total_segs = len(segments)
print(f"      ✓ {total_segs:,} segments loaded")

# ── Congestion model ──────────────────────────────────────────────────────────
# Base congestion by hour (0–23), weekday
HOUR_PROFILE = {
     0: 0.05,  1: 0.04,  2: 0.03,  3: 0.03,  4: 0.04,  5: 0.08,
     6: 0.20,  7: 0.72,  8: 0.88,  9: 0.55, 10: 0.38, 11: 0.42,
    12: 0.58, 13: 0.52, 14: 0.40, 15: 0.45, 16: 0.62, 17: 0.85,
    18: 0.90, 19: 0.65, 20: 0.40, 21: 0.25, 22: 0.15, 23: 0.08,
}

ARTERY_MULTIPLIER = {
    "Samora Machel Ave"  : 1.30,
    "Julius Nyerere Way" : 1.25,
    "Robert Mugabe Rd"   : 1.20,
    "Kwame Nkrumah Ave"  : 1.15,
    "First Street"       : 1.10,
    "Second Street"      : 1.05,
    "Other"              : 0.85,
}

SPEED_LIMIT = 60  # CBD default km/h

def congestion_to_speed(cong):
    """Convert congestion level to average speed."""
    # Free flow at cong=0 → SPEED_LIMIT; gridlock at cong=1 → 5 km/h
    return max(5.0, SPEED_LIMIT * (1 - cong * 0.92))

def travel_time(length_m, speed_kmh):
    return (length_m / 1000) / speed_kmh * 3600  # seconds

def is_peak(hour):
    return 1 if (7 <= hour <= 8) or (17 <= hour <= 18) else 0

# ── Generate timestamps ───────────────────────────────────────────────────────
print("\n[2/5] Building timestamp series...")
start_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) \
           - timedelta(days=DAYS - 1)
timestamps = []
t = start_dt
while t < start_dt + timedelta(days=DAYS):
    timestamps.append(t)
    t += timedelta(minutes=INTERVAL)
print(f"      ✓ {len(timestamps):,} timestamps over {DAYS} days")

# ── Generate traffic readings ─────────────────────────────────────────────────
print("\n[3/5] Generating traffic readings...")
print(f"      Sample rate: {int(SAMPLE_RATE*100)}% of segments per timestamp")

seg_ids      = segments["segment_id"].values
seg_arteries = dict(zip(segments["segment_id"], segments["artery"].fillna("Other")))
seg_lengths  = dict(zip(segments["segment_id"], segments["length_m"].fillna(138)))

batch     = []
batch_size = 50_000
total_rows = 0

for ts in timestamps:
    hour   = ts.hour
    dow    = ts.weekday()           # 0=Mon … 6=Sun
    is_sat = dow == 5
    is_sun = dow == 6

    base_cong = HOUR_PROFILE[hour]

    # Weekend dampening
    if is_sun:
        base_cong *= 0.35
    elif is_sat:
        base_cong *= 0.60

    ts_str  = ts.isoformat()
    sample  = np.random.choice(seg_ids,
                               size=int(len(seg_ids) * SAMPLE_RATE),
                               replace=False)

    for seg_id in sample:
        artery  = seg_arteries.get(seg_id, "Other")
        mult    = ARTERY_MULTIPLIER.get(artery, 0.85)
        length  = seg_lengths.get(seg_id, 138)

        # Congestion: base × artery weight + noise
        cong = np.clip(
            base_cong * mult + np.random.normal(0, 0.07),
            0.0, 1.0
        )
        speed = congestion_to_speed(cong)
        tt    = travel_time(length, speed)

        # Vehicle count: rough proxy
        vehicles = int(np.clip(
            cong * 120 * mult + np.random.normal(0, 8),
            0, 200
        ))

        batch.append((
            seg_id, ts_str, hour, dow, is_peak(hour),
            round(cong, 4), round(speed, 2), round(tt, 2),
            vehicles, "synthetic"
        ))

        if len(batch) >= batch_size:
            cur.executemany("""
                INSERT INTO traffic_readings
                    (segment_id, recorded_at, hour_of_day, day_of_week,
                     is_peak_hour, congestion_level, avg_speed_kmh,
                     travel_time_s, vehicle_count, source)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, batch)
            conn.commit()
            total_rows += len(batch)
            batch = []
            print(f"      ... {total_rows:,} rows written", end="\r")

# Final flush
if batch:
    cur.executemany("""
        INSERT INTO traffic_readings
            (segment_id, recorded_at, hour_of_day, day_of_week,
             is_peak_hour, congestion_level, avg_speed_kmh,
             travel_time_s, vehicle_count, source)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, batch)
    conn.commit()
    total_rows += len(batch)

print(f"      ✓ {total_rows:,} traffic readings generated          ")

# ── Generate incidents ────────────────────────────────────────────────────────
print("\n[4/5] Seeding incident data...")

# Known hotspot intersections in Harare CBD (lat, lon, description)
HOTSPOTS = [
    (-17.8292, 31.0522, "Samora Machel Ave / Julius Nyerere Way junction"),
    (-17.8310, 31.0489, "Julius Nyerere Way / Kwame Nkrumah Ave"),
    (-17.8278, 31.0501, "Samora Machel Ave / First Street"),
    (-17.8350, 31.0470, "Robert Mugabe Rd / Second Street"),
    (-17.8320, 31.0540, "Samora Machel Ave near Town House"),
    (-17.8265, 31.0458, "Kwame Nkrumah Ave / Leopold Takawira"),
    (-17.8340, 31.0510, "Julius Nyerere Way southbound"),
    (-17.8295, 31.0478, "First Street Mall approach"),
]

INCIDENT_TYPES = ["accident", "breakdown", "roadwork", "flooding"]
SEVERITIES     = ["low", "low", "medium", "medium", "high", "critical"]

incidents_batch = []
for i in range(120):   # 120 incidents over 7 days
    hotspot = HOTSPOTS[i % len(HOTSPOTS)]
    lat     = hotspot[0] + np.random.normal(0, 0.001)
    lon     = hotspot[1] + np.random.normal(0, 0.001)
    desc    = hotspot[2]

    reported_offset = timedelta(
        days=np.random.randint(0, DAYS),
        hours=np.random.randint(6, 21),
        minutes=np.random.randint(0, 59)
    )
    reported_at = (start_dt + reported_offset).isoformat()

    # Most incidents resolve within 1–3 hours; ~20% stay unresolved
    if np.random.random() > 0.20:
        resolved_at = (start_dt + reported_offset +
                       timedelta(hours=np.random.uniform(0.5, 3.0))).isoformat()
    else:
        resolved_at = None

    inc_type = np.random.choice(INCIDENT_TYPES, p=[0.30, 0.35, 0.25, 0.10])
    severity = np.random.choice(SEVERITIES)

    incidents_batch.append((
        None,               # segment_id — could be matched later
        inc_type, severity, reported_at, resolved_at,
        round(lat, 6), round(lon, 6), desc, "synthetic"
    ))

cur.executemany("""
    INSERT INTO incidents
        (segment_id, incident_type, severity, reported_at,
         resolved_at, lat, lon, description, source)
    VALUES (?,?,?,?,?,?,?,?,?)
""", incidents_batch)
conn.commit()
print(f"      ✓ {len(incidents_batch)} incidents seeded across {len(HOTSPOTS)} hotspots")

# ── Export CSVs for Power BI ──────────────────────────────────────────────────
print("\n[5/5] Exporting Power BI CSVs...")

# traffic summary — avg congestion per segment per hour (manageable size)
summary = pd.read_sql("""
    SELECT
        r.segment_id,
        r.artery,
        t.hour_of_day,
        t.day_of_week,
        t.is_peak_hour,
        ROUND(AVG(t.congestion_level), 4)  AS avg_congestion,
        ROUND(AVG(t.avg_speed_kmh),   2)   AS avg_speed_kmh,
        ROUND(AVG(t.travel_time_s),   2)   AS avg_travel_time_s,
        ROUND(AVG(t.vehicle_count),   0)   AS avg_vehicles
    FROM traffic_readings t
    JOIN road_segments r ON t.segment_id = r.segment_id
    GROUP BY r.segment_id, t.hour_of_day, t.day_of_week
""", conn)
summary_path = os.path.join(DATA_DIR, "traffic_summary.csv")
summary.to_csv(summary_path, index=False)
print(f"      ✓ traffic_summary.csv  ({len(summary):,} rows)")

# incidents CSV
incidents_df = pd.read_sql("SELECT * FROM incidents", conn)
inc_path = os.path.join(DATA_DIR, "incidents.csv")
incidents_df.to_csv(inc_path, index=False)
print(f"      ✓ incidents.csv        ({len(incidents_df)} rows)")

# peak hour profile CSV (for line chart in Power BI)
peak_profile = pd.read_sql("""
    SELECT
        hour_of_day,
        CASE day_of_week
            WHEN 0 THEN 'Monday'    WHEN 1 THEN 'Tuesday'
            WHEN 2 THEN 'Wednesday' WHEN 3 THEN 'Thursday'
            WHEN 4 THEN 'Friday'    WHEN 5 THEN 'Saturday'
            WHEN 6 THEN 'Sunday'
        END AS day_name,
        ROUND(AVG(congestion_level), 4) AS avg_congestion,
        ROUND(AVG(avg_speed_kmh),    2) AS avg_speed_kmh
    FROM traffic_readings
    GROUP BY hour_of_day, day_of_week
    ORDER BY day_of_week, hour_of_day
""", conn)
peak_profile.to_csv(os.path.join(DATA_DIR, "hourly_congestion_profile.csv"), index=False)
print(f"      ✓ hourly_congestion_profile.csv  ({len(peak_profile)} rows)")

# artery comparison CSV
artery_stats = pd.read_sql("""
    SELECT
        r.artery,
        t.is_peak_hour,
        ROUND(AVG(t.congestion_level), 4) AS avg_congestion,
        ROUND(AVG(t.avg_speed_kmh),    2) AS avg_speed_kmh,
        COUNT(*) AS reading_count
    FROM traffic_readings t
    JOIN road_segments r ON t.segment_id = r.segment_id
    WHERE r.artery != 'Other'
    GROUP BY r.artery, t.is_peak_hour
    ORDER BY avg_congestion DESC
""", conn)
artery_stats.to_csv(os.path.join(DATA_DIR, "artery_comparison.csv"), index=False)
print(f"      ✓ artery_comparison.csv          ({len(artery_stats)} rows)")

conn.close()

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 55)
print("  Step 3 Complete!")
print("=" * 55)
print(f"""
  {total_rows:,} traffic readings across {DAYS} days
  {len(incidents_batch)} incidents at {len(HOTSPOTS)} CBD hotspots

  Power BI files ready in harare_pulse_data\\:
    traffic_summary.csv              ← segment-level congestion
    hourly_congestion_profile.csv    ← line chart by hour/day
    artery_comparison.csv            ← major artery benchmarks
    incidents.csv                    ← incident heatmap data

  Next: Run step4_powerbi_export.py to finalise the
        Power BI map layer and dashboard data package.
""")
