import numpy as np
import requests
import json
import geopandas as gpd
import pandas as pd
from zipfile import ZipFile
from io import BytesIO


#%%
# Download the Census Tract Geometries
url = "https://www2.census.gov/geo/tiger/GENZ2021/shp/cb_2021_us_tract_500k.zip"
response = requests.get(url)
print("Tracts Downloaded")

# Extract the Zip File to a BytesIO object (in memory extraction)
with ZipFile(BytesIO(response.content)) as zip_ref:
    zip_ref.extractall("Data/ACS_2021/Tract_Geometries")
print("Tracts Unzipped")

# Read the Shapefile
tract_file = "Data/ACS_2021/Tract_Geometries/cb_2021_us_tract_500k.shp"
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
consolidated_data = pd.DataFrame(columns=['GEOID', 'lat', 'lon', 'utility_name', 'commercial', 'industrial', 'residential'])
# Initialize an empty list to store row data
new_rows = []

api_key = '5NafpqgGlLOBTehX0g7wPnu6qxpYn35jGVjFBnmj'.strip()
base_url = 'https://developer.nrel.gov/api/census_rate/v3.json?'

# Loop through each row in the DataFrame
for index, row in tract_locations_df.iterrows():
    params = {
        'api_key': api_key,
        'region': 'tract',
        'lat': row['lat'],
        'lon': row['lon'],
    }
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()
        new_row = {
            'GEOID': row['GEOID'],
            'lat': row['lat'],
            'lon': row['lon'],
            'utility_name': data['outputs'].get('utility_name', ''),
            'commercial': data['outputs'].get('commercial', None),
            'industrial': data['outputs'].get('industrial', None),
            'residential': data['outputs'].get('residential', None)
        }
        new_rows.append(new_row)
    else:
        print(f"Failed to download data for region {row['lat']}, {row['lon']}")

# Create a new DataFrame from the list of new rows and concatenate it with the original DataFrame
new_data_df = pd.DataFrame(new_rows)
consolidated_data = pd.concat([consolidated_data, new_data_df], ignore_index=True)

print(consolidated_data.head())

# Save the consolidated DataFrame to a CSV file
consolidated_data.to_csv('consolidated_data.csv', index=False)