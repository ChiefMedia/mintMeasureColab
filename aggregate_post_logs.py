"""
Script to create a single dataset of spots from multiple post-logs. 

Data is read from /data in the folder this script runs from. 

Currently works for:

Stations (STATION/NIELSEN DMA):
KATU/820
KBNZ/821
KBOI/757
KHQ/881
KOHD/821
KOIN/820
KPTV/820
KTVM/762
KTVZ/821
KXLF/754
"""

# Module imports
import os
import pandas as pd


# First we can get the filenames from the data folder
# We can split the files into two distinct categories:
#   Single station - post log is for one station only, denoted "K___"
#   Multiple station - contains multiple stations' data for one market
all_data_filenames = []
for (dirpath, direnames, filenames) in os.walk('./data'):
    all_data_filenames.extend(filenames)
    break

print('FILENAMES')
for f in all_data_filenames:
    print(f)
print()

# The filenames are well formatted (at least in the initial data provided) so
#   let's make an assumption that if we split the filename on '_', then the 
#   second element of the resulting list is either the station or or the market
# Individual stations start with K and are no more than 4 chars long
# There are no markets that start with K and <= 4 chars, so we can use that

# Here's a list comp to get the second element and include if it meets the 
#   station criteria
station_files = [f for f in all_data_filenames if f.split('_')[1].lower().startswith('k') and len(f.split('_')[1]) <= 4] 
market_files = [f for f in all_data_filenames if f not in station_files]

# Since one group of files is logs with just one station's data, and the other 
#   is multiple stations, we essentially need to do two different data
#   transformation processes
# We'll start with the station files

# Create a pandas dataframe for each file in station_files then print the data
station_files_dfs = [pd.read_excel(os.path.join('./data', f)) for f in station_files]
for f, df in zip(station_files, station_files_dfs):
    print(f)
    print(df.info(), '\n')


### FILE HEADER CLEANING
# So there are some things standard across all files that we can do in one shot
# First clean up the column names
for df in station_files_dfs:
    # Lowercase
    df.columns = [col.lower().strip() for col in df.columns]
    # Replace blanks with underscores
    df.columns = [col.replace(' ', '_') for col in df.columns]

    # There are multiple forms of "aired_time" and "aired_date" so we'll 
    #   standardize those
    # Times
    df.columns = ['aired_time' if col in ['air_time', 'time', 'actual_time_when_spot_aired']  else col for col in df.columns]
    # Dates
    # Most files have a single date column
    df.columns = ['aired_date' if col in ['air_date', 'date']  else col for col in df.columns]
    # One file has m, d, y split out
    if 'm' in df.columns:
        df['aired_date'] = df.apply(lambda row: str(row.y) + '-' + str(row.m) + '-' + str(row.d), axis=1)

    # We'll do the same for length but this time we can just check if length is
    #   in the column name
    df.columns = ['length' if 'length' in col else col for col in df.columns]

    # And now finally for rate
    df.columns = ['rate' if 'rate' in col else col for col in df.columns]


### FILE DATA CLEANING
# Now we have standardized columns, we can do some cleaning on the data itself
# We're only concerned with 4 columns, 'aired_date', 'aired_time', 'rate', 
#   'length'
# We'll ensure those are correct

for df in station_files_dfs:
    df['aired_date'] = pd.to_datetime(df['aired_date']).dt.date
    df['aired_time'] = pd.to_datetime(df['aired_date']).dt.time
    df['rate'] = df['rate'].astype(str)
    df['length'] = df['length'].replace(':', '').astype(str)

for f, df in zip(station_files, station_files_dfs):
    print(f)
    print(df.info(), '\n')


### CREATE AGGREGATED DATASET OF STATIONS
keep_cols = ['aired_date', 'aired_time', 'rate', 'length']
all_stations_dfs = pd.concat([df[keep_cols] for df in station_files_dfs])
print(all_stations_dfs.sample(10))


