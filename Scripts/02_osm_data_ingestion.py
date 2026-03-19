import os
import geopandas as gpd
import osmnx as ox
from sqlalchemy import create_engine
import urllib.parse

# Connecting to DB

DB_USER = "geospatial"
DB_PASSWORD = "RennySweetpea@1"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "geospatial_banking_kenya"

# Define base directory for your project
BASE_DIR = r"C:\Users\Jones Mbela\Desktop\RENNY\AI AND ML\Geospatial Network Optimization"

# Paths
AOI_DIR = os.path.join(BASE_DIR,"Data","aoi_data")

def ingest_osm_data(aoi_dir,db_engine):
    """
    Downloads OSM data for each AOI and loads it to DB(PostGis)
    """
    for filename in os.listdir(aoi_dir):
        if filename.endswith("_aoi.geojson"):
            city_name = filename.replace("_aoi.geojson","").replace("_"," ").title()
            aoi_path = os.path.join(aoi_dir,filename)
            print(f"Processing OSM data for {city_name} using AOI from {aoi_path}......")

            #Loading the AOI polygon
            aoi_gdf = gpd.read_file(aoi_path)
            polygon = aoi_gdf.geometry.iloc[0]

            # Download road network
            print (f"Downloading road network for {city_name}....")
            try:
                G = ox.graph_from_polygon(polygon,network_type="all")
                nodes,edges = ox.graph_to_gdfs(G)
                nodes["geometry"] = nodes["geometry"].to_crs(epsg=4326)
                edges["geometry"] = edges["geometry"].to_crs(epsg=4326) # Ensure CRS

                table_name_prefix = city_name.lower().replace(" ","_")
                nodes.to_postgis(f"{table_name_prefix}_osm_nodes",db_engine,if_exists="replace",index=True)
                edges.to_postgis(f"{table_name_prefix}_osm_edges",db_engine,if_exists="replace",index=True)
                print(f"Road network for {city_name} loaded to PostGis DB.")
            except Exception as e:
                print(f"Error downloading road netowrk for {city_name}: {e}")
            
            #Download Points of interest (poi) THE LIKES OF BANKS atms ETC
            print (f"Downloading POI for {city_name}.....")
            try:
                tags = {"amenity": ["bank","atm"],"shop": ["supermarket","mall"],"building":"commercial"}
                pois = ox.features_from_polygon(polygon,tags)
                if not pois.empty:
                    pois["geometry"] = pois["geometry"].to_crs(epsg=4326)
                    pois.to_postgis(f"{table_name_prefix}_osm_pois",db_engine,if_exists="replace",index=True)
                    print(f"POIs for {city_name} loaded into PostGis.")
                else:
                    print(f"No POIs found for {city_name} with spesific tags.")
            except Exception as e:
                print(f"Error downloading/ingesting POIs for {city_name}: {e}")


if __name__ == "__main__":
    #creating a SQLAlchemy engine for postgis db
    safe_password = urllib.parse.quote_plus(DB_PASSWORD)
    db_connection_str = f"postgresql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_connection_str)

    ingest_osm_data(AOI_DIR,engine)
    print("OSM data ingestion script finished")

