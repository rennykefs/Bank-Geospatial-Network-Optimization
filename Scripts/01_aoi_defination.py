import geopandas as gpd
import os

# defining base directory
BASE_DIR = r"C:\Users\Jones Mbela\Desktop\RENNY\AI AND ML\Geospatial Network Optimization"

#defining path to admin boundaries
ADMIN_BOUNDARIES_PATH = os.path.join(BASE_DIR,"Data","kenya_admin_boundaries","ken_admin1.shp")
AOI_DIR = os.path.join(BASE_DIR, "Data","aoi_data")

os.makedirs(AOI_DIR,exist_ok=True)

def define_aois_from_local_shapefile(admin_boundaries_path,aoi_dir):
    """
    Defines Areas of Interest for Kenyan cities from local shape file
    """
    print (f"loading administrative boundaries from: {admin_boundaries_path}")
    try:
        admin_gdf = gpd.read_file(admin_boundaries_path)
    except Exception as e :
        print (f"Error loading shapefile: {e}")
        print (f"Please ensure the file exists at {admin_boundaries_path} and geopandas is installed.")
    

    #inspecting columns to check "admin1_name" is present

    cities = {
        "Nairobi": "Nairobi",
        "Mombasa": "Mombasa",
        "Kisumu": "Kisumu"
    }

    for city_name,admin_name in cities.items():
        print (f"Processing {city_name}.....")
        #filtering sepsific city or admin units using column name

        city_aoi = admin_gdf[admin_gdf["adm1_name"] == admin_name]

        if not city_aoi.empty:
            output_path = os.path.join(aoi_dir,f"{city_name.lower().replace(' ','_')}_aoi.geojson")
            city_aoi.to_file(output_path,driver="GeoJSON")
            print(f"AOI for {city_name} saved to {output_path}")
        else:
            print(f"Could not find admin boundary for {city_name} in the provided shapefile (column 'adm1_name').")

if __name__ == "__main__":
    define_aois_from_local_shapefile(ADMIN_BOUNDARIES_PATH,AOI_DIR)