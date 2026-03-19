import os 
import pandas as pd
BASE_DIR = r"C:\Users\Jones Mbela\Desktop\RENNY\AI AND ML\Geospatial Network Optimization"
DATA_DIR = os.path.join(BASE_DIR,"Data")

FACEBOOK_MOBILITY_UNZIPPED_DIR = os.path.join(DATA_DIR,"facebook_mobility","movement-range-data-2022-05-22")
FACEBOOK_MOBILITY_TXT = os.path.join(FACEBOOK_MOBILITY_UNZIPPED_DIR,"movement-range-2022-05-22.txt")

print ("Auditing FB mobility data from : {FACEBOOK_MOBILITY_TXT}")

if not os.path.exists(FACEBOOK_MOBILITY_TXT):
    print(f"Error: Facebook Mobility TXT file not found at {FACEBOOK_MOBILITY_TXT}")

else:
    try:
        mobility_df = pd.read_csv(FACEBOOK_MOBILITY_TXT ,sep='\t',low_memory=False)
        kenya_mobility_df = mobility_df[mobility_df['country']=='KEN'].copy()

        if not kenya_mobility_df.empty:
            unique_polygon_names = sorted(kenya_mobility_df['polygon_name'].unique())
            print("\n--- Unique 'polygon_name' values for Kenya ---")

            for name in unique_polygon_names:
                print(f"-{name}")

        else:
            print(" No data found for 'KEN' country code in the file" )
    except Exception as e:
        print("Error as {e}")