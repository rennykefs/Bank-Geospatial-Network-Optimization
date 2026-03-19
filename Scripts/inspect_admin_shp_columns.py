import geopandas as gpd
import os
# Base directory
BASE_DIR = r"C:\Users\Jones Mbela\Desktop\RENNY\AI AND ML\Geospatial Network Optimization"
DATA_DIR = os.path.join(BASE_DIR,"Data")
ADMIN_BOUNDARIES_PATH = os.path.join(BASE_DIR,"Data","kenya_admin_boundaries","ken_admin1.shp")

print(f"Inspecting shapefile: {ADMIN_BOUNDARIES_PATH}")

if not os.path.exists(ADMIN_BOUNDARIES_PATH):
    print(f"Error: Shapefile not found at {ADMIN_BOUNDARIES_PATH}")
else:
    try:
        admin_gdf = gpd.read_file(ADMIN_BOUNDARIES_PATH)
        print("\nColumns in the admin coundaries shapefile:")
        for col in admin_gdf.columns:
            print(f"- {col}")
    except Exception as e:
        print (f"Error readng shapefile: {e}")