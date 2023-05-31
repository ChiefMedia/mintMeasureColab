"""
POST LOG AGGREGATION SCRIPT

Script to create a single dataset of spots from multiple post-log Excel files. 

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

Buy Markets:
Pierce, WA (Seattle Nielsen DMA)
Thurston, WA (Seattle Nielsen DMA)
Spokane, WA (Spokane Nielsen DMA)

See README for further documentation and instructions. 

Contributors:
Anthony Baum @ Chief Media

"""

# Module imports
import os
import pandas as pd
import numpy as np
import warnings

pd.options.mode.chained_assignment = None
# We're ignoring warnings as Pandas throws a deprecation warning when 
#   pd.to_datetime is passed with the infer_datetime_format parameter.
# In our case, we need that dynamic inference of the datetime format since each
# post-log file is coming through with something different.
warnings.filterwarnings("ignore", category=UserWarning)

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


###### INDIVIDUAL STATION FILES

### FILE HEADER CLEANING
# So there are some things standard across all files that we can do in one shot
# First clean up the column names
for i, df in enumerate(station_files_dfs):
    cleaned_df = df.copy()

    # Lowercase
    cleaned_df.columns = [col.lower().strip() for col in cleaned_df.columns]
    # Replace blanks with underscores
    cleaned_df.columns = [col.replace(' ', '_') for col in cleaned_df.columns]

    # There are multiple forms of "aired_time" and "aired_date" so we'll 
    #   standardize those
    # Times
    cleaned_df.columns = ['aired_time' if col in ['air_time', 'time', 'actual_time_when_spot_aired']  else col for col in cleaned_df.columns]
    # Dates
    # Most files have a single date column
    cleaned_df.columns = ['aired_date' if col in ['air_date', 'date']  else col for col in cleaned_df.columns]
    # One file has m, d, y split out
    if 'm' in cleaned_df.columns:
        cleaned_df['aired_date'] = cleaned_df.apply(lambda row: str(row.y) + '-' + str(row.m) + '-' + str(row.d), axis=1)

    # We'll do the same for length but this time we can just check if length is
    #   in the column name
    cleaned_df.columns = ['length' if 'length' in col else col for col in cleaned_df.columns]

    # And now finally for rate
    cleaned_df.columns = ['rate' if 'rate' in col else col for col in cleaned_df.columns]

    # Replace df at index i with new cleaned df
    station_files_dfs[i] = cleaned_df


### FILE DATA CLEANING
# Now we have standardized columns, we can do some cleaning on the data itself
# We're only concerned with 4 columns, 'aired_date', 'aired_time', 'rate', 
#   'length'
# We'll ensure those are correct

for i, (f, df) in enumerate(zip(station_files, station_files_dfs)):
    cleaned_df = df.copy()

    # Drop nulls based on the columns we're using
    # This should remove any totals columns
    cleaned_df = cleaned_df.replace('', None)
    cleaned_df = cleaned_df.dropna(subset=['aired_date', 'aired_time', 'rate', 'length'])

    cleaned_df['aired_date'] = pd.to_datetime(cleaned_df['aired_date'].astype(str), infer_datetime_format=True)
    cleaned_df['aired_date'] = cleaned_df['aired_date'].dt.date

    # There are two cases for time - where pandas can detect the format, and where it can't
    # We'll start by testing whether pandas can autodetect the format
    # Using errors = coerce will set test_case to NaT
    test_array = pd.to_datetime(cleaned_df['aired_time'], infer_datetime_format=True, errors='coerce') # this is a numpy array
    test_series_contains_nulls = np.isnan(test_array).any() # variable = True if nulls in series

    # If this was succesfull, translate the whole column
    if not test_series_contains_nulls:
        cleaned_df['aired_time'] = pd.to_datetime(cleaned_df['aired_time'].astype(str), infer_datetime_format=True).dt.time

    # If not sucessful, we need custom cases
    # Now we'll add all custom translations
    else:
        # Post log from KTVM
        try:
            cleaned_df['aired_time'] = pd.to_datetime(cleaned_df['aired_time'].astype(str) + 'm', format='%H%M%p').dt.time
        except Exception as e:
            print(f'EXCEPTION with file {f}')
            print(cleaned_df.head(3))
            print(e, '\n')
            break
        # Post log from KATU
    
    # Format rate and length columns
    cleaned_df['rate'] = cleaned_df['rate'].astype(str)
    cleaned_df['length'] = cleaned_df['length'].astype(str).str.replace(':', '')

    station_files_dfs[i] = cleaned_df


### FILE AUGMENTATIONS
# Things to add:
#   - Station name
#   - Datetime column
#   - Nielsen DMA code

for i, (station, df) in enumerate(zip([f.split('_')[1] for f in station_files], station_files_dfs)):
    augmented_df = df.copy()

    # Add station column to dataframe
    augmented_df['station'] = station

    # Create datetime column
    augmented_df['datetime'] = pd.to_datetime(augmented_df['aired_date'].astype(str) + ' ' + augmented_df['aired_time'].astype(str))

    # Add the Nielsen DMA code
    station_to_dma_lookup = {
        'KATU':820,
        'KBNZ':821,
        'KBOI':757,
        'KHQ':881,
        'KOHD':821,
        'KOIN':820,
        'KPTV':820,
        'KTVM':762,
        'KTVZ':821,
        'KXLF':754
    }
    augmented_df['dma_code'] = station_to_dma_lookup[station]

    station_files_dfs[i] = augmented_df

    #print(station)
    #print(augmented_df.head(3)[['station', 'aired_date', 'aired_time', 'rate', 'length']], '\n')


### CREATE AGGREGATED DATASET OF STATIONS
keep_cols = ['datetime', 'station', 'dma_code', 'rate', 'length']
all_stations_dfs = pd.concat([df[keep_cols] for df in station_files_dfs])
print('Aggregated all stations data sample:')
print(all_stations_dfs.sample((5)), '\n')


###### MARKET FILES (multiple stations, single file)
# Recall we created a market_files variable to store the filename for the 
#   market-scoped files. We need to clean those now. 
# As of now, all market files are in the same format so we can apply all
#   cleaning to each market file

market_file_dfs = [pd.read_excel(os.path.join('./data', f)) for f in market_files]

### FILE HEADER CLEANING
for i, df in enumerate(market_file_dfs):
    cleaned_df = df.copy()

    # Get all valid headers and cast to lowercase 
    valid_headers = [h for h in cleaned_df.columns if 'unnamed' not in h.lower()]
    # Constrain the dataframe to only the valid headers
    cleaned_df = cleaned_df[valid_headers]
    cleaned_df.columns = [h.lower() for h in cleaned_df.columns]

    # Now we want to rename columns 
    # This file had duplicate column names for time and date, and the ones we
    #   want are now suffixed with .1
    # We'll rename those columns
    cleaned_df = cleaned_df.rename(columns={
        'day.1': 'aired_date',
        'time.1': 'aired_time',
        'ntwk': 'station'
    })

    # Now let's constrain the dataframe to only the headers we need
    keep_headers = ['station', 'rate', 'aired_date', 'aired_time']
    cleaned_df = cleaned_df[keep_headers]

    market_file_dfs[i] = cleaned_df


### FILE DATA CLEANING
# A couple of things need cleaning
# Null rows need to be dropped as there are a lot from the file "prettifying"
# Need to format the dates + times

for i, df in enumerate(market_file_dfs):
    cleaned_df = df.copy()

    # Drop null rows
    cleaned_df = cleaned_df.dropna(subset=['station'])

    # Format date and time
    # The aired date column actually comes through with the time in place, even
    #   though it's not visible in the Excel file.
    # I'm not going to rely on that always being true, so we'll create the 
    #   formatted columns as we did for the station files
    cleaned_df['aired_date'] = pd.to_datetime(cleaned_df['aired_date'], infer_datetime_format=True)
    cleaned_df['aired_date'] = cleaned_df['aired_date'].dt.date.astype(str)

    cleaned_df['aired_time'] = cleaned_df['aired_time'].astype(str)

    market_file_dfs[i] = cleaned_df


### FILE DATA AUGMENTATION
# Because of the input file format, we're missing length for the spots
# Currently all spots have a length of 30, but this data was dropped because of
#   the file formatting. 
# We'll add that in hard-coded here but will need changing 
# Things to add:
#   - Length
#   - Datetime
#   - Nielsen DMA code

for i, (f, df) in enumerate(zip(market_files, market_file_dfs)):
    cleaned_df = df.copy()

    # Drop null rows
    cleaned_df['length'] = '30'

    # Add datetime column
    cleaned_df['datetime'] = cleaned_df['aired_date'] + ' ' + cleaned_df['aired_time']
    # Converting to datetime and back to string adds seconds for consistency, if
    #   it doesn't exist
    cleaned_df['datetime'] = pd.to_datetime(cleaned_df['datetime']).astype(str)

    # Add Nielsen DMA code
    market_name_to_dma_lookup = {
        'pierce': 819,
        'thurston': 819,
        'spokane': 881
    }
    market_name = f.split('_')[1].lower()
    cleaned_df['dma_code'] = market_name_to_dma_lookup[market_name]

    market_file_dfs[i] = cleaned_df


### AGGREGATE MARKET FILES
# keep_cols was defined under the station files editing
# Referencing that here so any change there is reflected here
# If an edit is missed in the market edits, this should mean an error is thrown
all_markets_dfs = pd.concat([df[keep_cols] for df in market_file_dfs])
print('Aggregated all stations data sample:')
print(all_markets_dfs.sample(5), '\n')


### UNION STATIONS AND MARKETS FILES
output_data = pd.concat([all_stations_dfs, all_markets_dfs])
print('Output data sample:')
print(all_markets_dfs.sample(10))

# Write file to output folder
# Will write to the output_data folder in the root of this repo
output_folder = './output_data'
output_filename = 'aggregated_spots_data.csv'
output_filepath = os.path.join(output_folder, output_filename)
output_data.to_csv(output_filepath, index=False)
print(f'Output file spot count is {len(output_data)}')
