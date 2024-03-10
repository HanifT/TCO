import numpy as np
import requests
import json
import geopandas as gpd
import pandas as pd
from zipfile import ZipFile
from io import BytesIO

# %%
# Download the Census Tract Geometries (county)
# url = "https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_tract_500k.zip"
url = "https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_county_5m.zip"
response = requests.get(url)
print("Tracts Downloaded")

# Extract the Zip File to a BytesIO object (in memory extraction)
with ZipFile(BytesIO(response.content)) as zip_ref:
    zip_ref.extractall("Data/ACS_2021/Tract_Geometries")
print("Tracts Unzipped")

# Read the Shapefile
# tract_file = "Data/ACS_2021/Tract_Geometries/cb_2021_us_tract_500k.shp"
tract_file = "Data/ACS_2021/Tract_Geometries/cb_2021_us_county_5m.shp"
tracts = gpd.read_file(tract_file)

# Create a DataFrame with Latitude and Longitude
# Convert CRS to a projected system before calculating centroids
tracts['lon'] = tracts.geometry.centroid.x
tracts['lat'] = tracts.geometry.centroid.y
# Convert back to geographic CRS if needed for lat/lon output
tract_locations = tracts[['GEOID', 'lon', 'lat']]

# Optionally, convert the GeoDataFrame to a Pandas DataFrame if no geometry is needed
tract_locations_df = pd.DataFrame(tract_locations)
# Initialize an empty DataFrame for consolidated data
# Initialize an empty DataFrame for consolidated data with additional columns for utility name and Rate name

consolidated_data1 = pd.DataFrame(columns=['GEOID', 'lat', 'lon', 'sector', 'utility_name', 'Rate_name',
                                           "demandrate", "demandweekdayschedule", "demandweekendschedule",
                                           "energyratestructure", "energyweekdayschedule", "energyweekendschedule",
                                           "fixedmonthlycharge", "minmonthlycharge", "annualmincharge", "fixedattrs"])
# Your API key and base URL
# api_key = '9ED4OxQpOHLCajotcjhqAvvN1BebbVG4dVk7AcyU'.strip() # ucdavis.edu
# api_key = 'oPuhNmPIkLWHqF0ZyVyjCIdjdb4qABKIRxFGm3p4'.strip() # gmail.com
api_key = 'YK13Yb1bbOVF2upGFl6R4LZAorqtP2HEy9Esnkcx'.strip()  # ymail.com
base_url = 'https://api.openei.org/utility_rates?version=3&'
new_rows = []

# Define the start and end indices for the subset of data you want to process
start_index = 3000
end_index = 4000

# Create a subset of the DataFrame based on the defined indices
subset_df = tract_locations_df.iloc[start_index:end_index]
# Initialize an empty DataFrame to store the results
consolidated_data1 = pd.DataFrame()


# Loop through the subset DataFrame
for index, row in subset_df.iterrows():
# for index, row in tract_locations_df.iterrows():
    print(index)
    params = {
        'api_key': api_key,
        'lat': row['lat'],
        'lon': row['lon'],
        'format': 'json',
        'sector': 'Commercial',
        "detail": "full"  # Explicitly request JSON format
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:

        data = response.json()

        if 'items' in data:

            for item in data['items']:
                utility = item.get('utility', 'N/A')  # Retrieve the label value from the item
                # Assuming 'rate_url' is a URL parameter, you might construct a URL like so:
                rate_url = f"https://api.openei.org/utility_rates?version=3&format=json&ratesforutility={utility}&api_key={api_key}"

                new_row = {
                    'GEOID': row['GEOID'],
                    'lat': row['lat'],
                    'lon': row['lon'],
                    "sector": item.get("sector", 'N/A'),
                    "utility_name": utility,  # Default if not found
                    'Rate_name': item.get('name', 'N/A'),  # Default if not found
                    'demandrate': item.get('demandratestructure', []),
                    "demandweekdayschedule": item.get('demandweekdayschedule', []),
                    "demandweekendschedule": item.get('demandweekendschedule', []),
                    'energyratestructure': item.get('energyratestructure', []),
                    "energyweekdayschedule": item.get('energyweekdayschedule', []),
                    "energyweekendschedule": item.get('energyweekendschedule', []),
                    'fixedmonthlycharge': item.get('fixedmonthlycharge', 'N/A'),
                    "minmonthlycharge": item.get('minmonthlycharge', 'N/A'),
                    "annualmincharge": item.get('annualmincharge', 'N/A'),
                    "fixedattrs": item.get('fixedattrs', 'N/A')
                }
                # Append the new row to consolidated_data DataFrame
                consolidated_data1 = pd.concat([consolidated_data1, pd.DataFrame([new_row])], ignore_index=True)
        else:
            print(f"No 'items' key in response for region {row['lat']}, {row['lon']}")
    else:
        print(f"Failed to download data for region {row['lat']}, {row['lon']}")

# Save the consolidated DataFrame to a CSV file
