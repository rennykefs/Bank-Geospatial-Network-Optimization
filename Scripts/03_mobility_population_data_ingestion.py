import os
# Force Python to use the PROJ data inside your conda environment
conda_proj_path = r"C:\Users\Jones Mbela\.conda\envs\geospatial_env\Library\share\proj"
os.environ["PROJ_LIB"] = conda_proj_path
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.mask import mask
from sqlalchemy import create_engine
import urllib.parse
import zipfile
import osmnx as ox


#DB connections details 
DB_USER = "geospatial"
DB_PASSWORD = "RennySweetpea@1"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "geospatial_banking_kenya"

# Base directory
BASE_DIR = r"C:\Users\Jones Mbela\Desktop\RENNY\AI AND ML\Geospatial Network Optimization"

#Paths
AOI_DIR = os.path.join(BASE_DIR,"Data","aoi_data")
DATA_DIR = os.path.join(BASE_DIR,"Data")
ADMIN_BOUNDARIES_PATH = os.path.join(BASE_DIR,"Data","kenya_admin_boundaries","ken_admin1.shp")

WORLDPOP_FILE = os.path.join(DATA_DIR,"worldpop","ken_ppp_2020_UNadj_constrained.tif")
FACEBOOK_MOBILITY_ZIP = os.path.join(DATA_DIR,"facebook_mobility","movement-range-data-2022-05-22.zip")
FACEBOOK_MOBILITY_UNZIPPED_DIR = os.path.join(DATA_DIR,"facebook_mobility","movement-range-data-2022-05-22")
FACEBOOK_MOBILITY_TXT = os.path.join(FACEBOOK_MOBILITY_UNZIPPED_DIR,"movement-range-2022-05-22.txt")

def ingest_worldpop_data(aoi_dir,worldpop_file,db_engine):
    """
    Ingests woroldpop gridded population data. clipping it to city AOI then loading it postgis
    """

    print(f"\n--- Ingesting WorldPop Data ---")
    if not os.path.exists(worldpop_file):
        print(f"Error: WorldPop file not found at {worldpop_file}. Please download it first.")
        return
    
    for filename in os.listdir(aoi_dir):
        if filename.endswith("_aoi.geojson"):
            city_name = filename.replace("_aoi.geojson","").replace("_"," ").title()
            aoi_path = os.path.join(aoi_dir,filename)
            print(f"Processing Worldpop data for {city_name} using AOI from {aoi_path}......")

            aoi_gdf = gpd.read_file(aoi_path)
            geoms = aoi_gdf.geometry.values

            try:
                with rasterio.open(worldpop_file) as src:
                    out_image,out_transform = mask(src,geoms,crop=True)
                    out_meta = src.meta.copy()
                    out_meta.update({
                        "driver": "GTiff",
                        "height": out_image.shape[1],
                        "width": out_image.shape[2],
                        "transform": out_transform
                    })
                    #Creating a temporary GeoTIFF for the clipped data
                    clipped_tif_path = os.path.join(DATA_DIR,f"{city_name.lower().replace(' ','_')}_worldpop_clipped_tif")
                    with rasterio.open(clipped_tif_path,"w",**out_meta) as dest:
                        dest.write(out_image)
                    
                    #converting raster to points(centroids of pixels with population >0)

                    from rasterio.features import shapes
                    population_points = []
                    with rasterio.open(clipped_tif_path) as src_clipped:
                        image = src_clipped.read(1)
                        for geom,val in shapes(image,transform=src_clipped.transform):
                            if val >0: # Only including populated areas
                                from shapely.geometry import shape
                                pixel_shape = shape(geom)
                                population_points.append({"geometry": pixel_shape.centroid,"population":val})
                    
                    if population_points:
                        gdf_pop = gpd.GeoDataFrame(population_points,crs=src_clipped.crs)
                        gdf_pop.to_postgis(f"{city_name.lower().replace(' ', '_')}_worldpop_points", db_engine, if_exists="replace", index=False)
                        print(f"Clipped WorldPop for {city_name} converted to pints and loaded into PostGis db.")
                    else:
                        print(f"No  populated areas found in clipped Worldpop for {city_name}.")
            
            except Exception as e:
                print(f"Error processing WorldPop data for {city_name}: {e}")

def ingest_facebook_mobility_data(aoi_dir,fb_mobility_zip,fb_mobility_txt,fb_mobility_unzipped_dir,admin_boundaries_path,db_engine):
    """
    Ingests Facebook Mobility data, filters it by city AOIs and loads to DB
    """
    print(f"\n--- Ingesting Facebook Mobility Data ---")

    if not os.path.exists(fb_mobility_zip):
        print(f"Error: Facbook mobility ZIP not found at {fb_mobility_zip}. Please download it first.")
        return
    
    #Unzipping the file if not unzipped
    if not os.path.exists(fb_mobility_unzipped_dir):
        print(f"Unzipping {fb_mobility_zip}....")
        with zipfile.ZipFile(fb_mobility_zip,'r') as zip_ref:
            zip_ref.extractall(fb_mobility_unzipped_dir)
        print("Unzipping the file complete")
    if not os.path.exists(fb_mobility_txt):
        print(f"Error:Facebook Mobility TXT file not found at {fb_mobility_txt} after unzipping")
        return
    print(f"Loading Facebok mobility data from {fb_mobility_txt}....")

    try:
        # Read the tab-delimated text file
        mobility_df = pd.read_csv(fb_mobility_txt ,sep='\t',low_memory=False)
        print(f"Mobility data loaded. Columns: {mobility_df.columns.to_list()}")

        # City mapping based on constituents
        city_mapping = {
            'Nairobi': [
                'Dagoretti North', 'Dagoretti South', 'Embakasi Central', 'Embakasi East',
                'Embakasi North', 'Embakasi South', 'Embakasi West', 'Kamukunji', 'Kasarani',
                'Kibra', 'Langata', 'Mathare', 'Roysambu', 'Ruaraka', 'Starehe', 'Westlands'
            ],
            'Mombasa': [
                'Changamwe', 'Jomvu', 'Kisauni', 'Likoni', 'Mvita', 'Nyali'
            ],
            'Kisumu': [
                'Kisumu Central', 'Kisumu East', 'Kisumu West'
            ]

        }

        #Reverse the map for easier look up based on fb name - target city
        fb_to_city_map = {}
        for city,fb_names in city_mapping.items():
            for name in fb_names:
                fb_to_city_map[name] = city
        
        # Adding a target city column to mobility_df based on the mapping
        mobility_df['target_city'] = mobility_df['polygon_name'].map(fb_to_city_map)

        #Filter out rows that dont map the target cities
        mobility_df = mobility_df.dropna(subset=['target_city'])

        if mobility_df.empty:
            print("No Facebook Mobility data found for Nairobi, Mombasa, or Kisumu after mapping.")
            return
        
        # Load GADM administrative boundaries for spatial clipping
        admin_gdf = gpd.read_file(admin_boundaries_path)
        admin_gdf = admin_gdf.rename(columns={'adm1_name': 'city_name'}) # Rename for clarity
        admin_gdf = admin_gdf[admin_gdf['city_name'].isin(['Nairobi', 'Mombasa', 'Kisumu'])]
        admin_gdf = admin_gdf[['city_name', 'geometry']]
        admin_gdf = admin_gdf.to_crs(epsg=4326) # Ensure consistent CRS

        #Looping through each target city to clip and load data
        for city_name in ['Nairobi','Mombasa','Kisumu']:
            print(f"Processing Facebook Mobility data for {city_name}.....")

            #Filter mobility data for current target city
            city_mobility_filtered_df = mobility_df[mobility_df['target_city']==city_name].copy()
            if not city_mobility_filtered_df.empty:
                # Getting the AOI polygon for current city
                aoi_path = os.path.join(aoi_dir, f"{city_name.lower().replace(' ', '_')}_aoi.geojson")
                if not os.path.exists(aoi_path):
                    print(f"Warning: AOI file not found for {city_name} at {aoi_path}. Skipping.")
                    continue
                aoi_poly = gpd.read_file(aoi_path).geometry.iloc[0]


                # Using osmnx to get geometries for subcounties (polygon_name) in facebook data

                sub_county_geometries = {}
                for poly_name in city_mobility_filtered_df['polygon_name'].unique():
                    try:
                        #geocoding the subcounties within kenya
                        gdf_sub = ox.geocode_to_gdf(f"{poly_name},{city_name},Kenya") 
                        if not gdf_sub.empty:
                            sub_county_geometries[poly_name] = gdf_sub.geometry.iloc[0].centroid
                        else:
                            print(f"Could not geocode {poly_name},{city_name},Kenya.Using city centroid as a fallback")
                            sub_county_geometries[poly_name] = aoi_poly.centroid
                    except Exception as e:
                        print(f"Error geocodeing {poly_name},{city_name},Kenya: {e}. Using city centroid as fallback")
                        sub_county_geometries[poly_name] = aoi_poly.centroid

                
                # Assigning the generated centroids to the mobility data
                city_mobility_filtered_df['geometry'] = city_mobility_filtered_df['polygon_name'].map(sub_county_geometries)
                city_mobility_gdf = gpd.GeoDataFrame(city_mobility_filtered_df,geometry='geometry',crs=admin_gdf.crs)

                #Filtering rows where the centriods could not be determined
                city_mobility_gdf = city_mobility_gdf.dropna(subset=['geometry'])

                if not city_mobility_gdf.empty:
                    table_name = f"{city_name.lower().replace(' ', '_')}_fb_mobility"
                    city_mobility_gdf.to_postgis(table_name, db_engine, if_exists="replace", index=False)
                    print(f"Facebook Mobility data for {city_name} loaded into PostGIS.")
                else:
                   
                   print(f"No valid geometries found for FB mobility data in {city_name}. Table not created")

        else:
            print(f"No Facebook Mobility data found for {city_name} after initial mapping. Table not created.")

    except Exception as e:
        print(f"Error processing Facebook Mobility Data: {e}")
                  
        



            

if __name__ == "__main__":
    safe_password = urllib.parse.quote_plus(DB_PASSWORD)
    db_connection_str = f"postgresql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_connection_str)

    #Ensuring the data directory exists
    os.makedirs(DATA_DIR,exist_ok=True)

    # Ingest WorldPop data
    ingest_worldpop_data(AOI_DIR, WORLDPOP_FILE, engine)

    # Ingest Facebook Mobility data
    ingest_facebook_mobility_data(AOI_DIR, FACEBOOK_MOBILITY_ZIP, FACEBOOK_MOBILITY_TXT, FACEBOOK_MOBILITY_UNZIPPED_DIR,ADMIN_BOUNDARIES_PATH ,engine)

    print("Mobility and Population data ingestion script finished.")

                                                           
                                                           
                                                           


