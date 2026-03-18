"""
Harare Pulse — Phase 1, Step 1
================================
Downloads the Harare CBD street network from OpenStreetMap using OSMnx,
analyses key stats, saves the data as GeoJSON and CSV (Power BI ready),
and produces two map outputs:
  - static_map.png     → quick visual check
  - interactive_map.html → zoomable map in your browser
"""

import osmnx as ox
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import folium
import os

# ── Config ────────────────────────────────────────────────────────────────────
# Harare CBD centre point (lat, lon) + radius in metres
CENTRE     = (-17.8292, 31.0522)
RADIUS_M   = 3000             # 3 km covers the CBD comfortably; increase if needed
NETWORK    = "drive"
OUTPUT_DIR = "harare_pulse_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 55)
print("  Harare Pulse · Step 1: Street Network Download")
print("=" * 55)

# ── 1. Download the street network ───────────────────────────────────────────
print("\n[1/5] Downloading street network from OSM...")
G = ox.graph_from_point(
    center_point=CENTRE,
    dist=RADIUS_M,
    network_type=NETWORK
)
nodes, edges = ox.graph_to_gdfs(G)
print(f"      Done — {len(nodes):,} intersections, {len(edges):,} road segments")

# ── 2. Basic stats ────────────────────────────────────────────────────────────
print("\n[2/5] Analysing network...")
stats = ox.basic_stats(G)

node_count      = len(nodes)
edge_count      = len(edges)
total_length_km = edges["length"].sum() / 1000

print(f"      Nodes (intersections)  : {node_count:,}")
print(f"      Edges (road segments)  : {edge_count:,}")
print(f"      Total road length      : {total_length_km:.1f} km")
print(f"      Avg segment length     : {edges['length'].mean():.0f} m")
print(f"      Intersections/km²      : {stats.get('intersection_density_km', 'n/a')}")

# ── 3. Tag segments with key arteries ────────────────────────────────────────
print("\n[3/5] Tagging major arteries...")

KEY_ARTERIES = {
    "Samora Machel Ave"  : ["samora machel"],
    "Julius Nyerere Way" : ["julius nyerere", "nyerere"],
    "Kwame Nkrumah Ave"  : ["kwame nkrumah", "nkrumah"],
    "Robert Mugabe Rd"   : ["robert mugabe", "mugabe"],
    "First Street"       : ["first street", "1st street"],
    "Second Street"      : ["second street", "2nd street"],
}

def classify_road(name):
    if pd.isna(name):
        return "Other"
    name_lower = str(name).lower()
    for label, keywords in KEY_ARTERIES.items():
        if any(kw in name_lower for kw in keywords):
            return label
    return "Other"

edges["artery"] = edges["name"].apply(
    lambda n: classify_road(n[0] if isinstance(n, list) else n)
)

artery_counts = edges[edges["artery"] != "Other"]["artery"].value_counts()
print(f"      Tagged {len(artery_counts)} major arteries:")
for artery, count in artery_counts.items():
    print(f"        · {artery}: {count} segments")

# ── 4. Save data files ────────────────────────────────────────────────────────
print("\n[4/5] Saving data files...")

# GeoJSON (for GIS tools & Power BI map visuals)
nodes_path = os.path.join(OUTPUT_DIR, "nodes.geojson")
edges_path = os.path.join(OUTPUT_DIR, "edges.geojson")
nodes.to_file(nodes_path, driver="GeoJSON")
edges.to_file(edges_path, driver="GeoJSON")
print(f"      nodes.geojson  ({node_count:,} rows)")
print(f"      edges.geojson  ({edge_count:,} rows)")

# CSV — edges flattened for Power BI
edges_csv = edges[["osmid", "name", "highway", "length",
                    "maxspeed", "oneway", "artery"]].copy()
edges_csv["name"] = edges_csv["name"].apply(
    lambda n: n[0] if isinstance(n, list) else n
)
edges_csv["osmid"] = edges_csv["osmid"].apply(
    lambda x: x[0] if isinstance(x, list) else x
)
csv_path = os.path.join(OUTPUT_DIR, "road_segments.csv")
edges_csv.to_csv(csv_path, index=True)
print(f"      road_segments.csv")

# Nodes CSV (intersections) — osmid is the index, so reset it first
nodes_reset = nodes.reset_index()
keep_cols = [c for c in ["osmid", "x", "y", "street_count"] if c in nodes_reset.columns]
nodes_csv = nodes_reset[keep_cols].copy()
nodes_csv.to_csv(os.path.join(OUTPUT_DIR, "intersections.csv"), index=False)
print(f"      intersections.csv")

# ── 5a. Static map ────────────────────────────────────────────────────────────
print("\n[5/5] Generating maps...")

ARTERY_COLOURS = {
    "Samora Machel Ave"  : "#3B8BD4",
    "Julius Nyerere Way" : "#1D9E75",
    "Kwame Nkrumah Ave"  : "#BA7517",
    "Robert Mugabe Rd"   : "#D85A30",
    "First Street"       : "#8B5CF6",
    "Second Street"      : "#EC4899",
    "Other"              : "#D3D1C7",
}

fig, ax = plt.subplots(1, 1, figsize=(14, 12), facecolor="#1a1a1a")
ax.set_facecolor("#1a1a1a")

# Draw "Other" roads first (background layer)
other = edges[edges["artery"] == "Other"]
other.plot(ax=ax, color="#D3D1C7", linewidth=0.4, alpha=0.4)

# Draw major arteries on top
for artery, colour in ARTERY_COLOURS.items():
    if artery == "Other":
        continue
    subset = edges[edges["artery"] == artery]
    if not subset.empty:
        subset.plot(ax=ax, color=colour, linewidth=2.2, alpha=0.95)

# Title & legend
ax.set_title("Harare CBD · Street Network", color="white",
             fontsize=18, fontweight="bold", pad=16)
ax.set_axis_off()

patches = [mpatches.Patch(color=c, label=a)
           for a, c in ARTERY_COLOURS.items() if a != "Other"]
patches.append(mpatches.Patch(color="#D3D1C7", label="Other roads", alpha=0.5))
ax.legend(handles=patches, loc="lower right", framealpha=0.15,
          labelcolor="white", fontsize=9, facecolor="#333")

plt.tight_layout()
static_path = os.path.join(OUTPUT_DIR, "static_map.png")
plt.savefig(static_path, dpi=180, bbox_inches="tight", facecolor="#1a1a1a")
plt.close()
print(f"      static_map.png")

# ── 5b. Interactive Folium map ────────────────────────────────────────────────
centre = [nodes["y"].mean(), nodes["x"].mean()]
fmap   = folium.Map(location=centre, zoom_start=15,
                    tiles="CartoDB dark_matter")

for _, row in edges.iterrows():
    colour = ARTERY_COLOURS.get(row["artery"], "#D3D1C7")
    weight = 4 if row["artery"] != "Other" else 1
    name   = row["name"][0] if isinstance(row["name"], list) else row["name"]
    tooltip = f"{name or 'Unnamed'} — {row['length']:.0f}m"
    if row.geometry.geom_type == "LineString":
        coords = [(lat, lon) for lon, lat in row.geometry.coords]
        folium.PolyLine(coords, color=colour, weight=weight,
                        opacity=0.8 if row["artery"] != "Other" else 0.4,
                        tooltip=tooltip).add_to(fmap)

html_path = os.path.join(OUTPUT_DIR, "interactive_map.html")
fmap.save(html_path)
print(f"      interactive_map.html")

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 55)
print("  Step 1 Complete!")
print("=" * 55)
print(f"\n  Output folder: ./{OUTPUT_DIR}/")
print("""
  Files created:
    nodes.geojson          ← intersections (GIS / Power BI)
    edges.geojson          ← road segments (GIS / Power BI)
    road_segments.csv      ← flat table for Power BI
    intersections.csv      ← node coordinates
    static_map.png         ← visual check
    interactive_map.html   ← open in browser to explore

  Next: Run step2_data_schema.py to set up the traffic database.
""")
