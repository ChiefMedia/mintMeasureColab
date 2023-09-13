"""
This script formats aggregated post log files from the ./spots_aggregation_output
folder. 
"""
import os
import logging
import numpy as np
import pandas as pd
import yaml


### CLASSES



### FUNCTIONS
def create_dataframes_from_log_folder(folder_path: str) -> list:
    """
    Loads all CSV files in the passed folder path into dataframes and returns a
    list of the dataframes. 

    Args:
        folder_path: The path for the function to read files from. 

    Returns:
        List of dataframes from all CSVs in folder_path.
    """
    return [pd.read_csv(f'{folder_path}/{f}') for f in os.listdir(folder_path) if f.endswith('csv')]


def load_station_dma_lookup() -> dict:
    """
    Reads the station_dma_lookup.yaml file and flattens it. Flattening enables
    us to pass just the top level stations or markets key to various functions,
    while maintaining a readable lookup file. 

    Note that all station/market names are returned in lowercase.
    """
    with open('station_dma_lookup.yaml', 'r') as f:
        station_dmas = yaml.safe_load(f)

    flattened_station_dmas = {}

    for k0, v0 in station_dmas.items():
        # The top level is k0 (key0), representing either stations or markets in 
        #   the YAML config file
        top_level_dict = {}

        for k1, v1 in v0.items():
            # If v1 is an integer, we simply add to the flattened dict
            if isinstance(v1, int):
                top_level_dict[k1.lower().strip()] = v1

            # If v1 is a dict, we need to add the key name, as well as all the 
            #   subgeographies, with the DMA code provided in the dict
            elif isinstance(v1, dict):
                top_level_dict[k1.lower().strip()] = v1['dma_code']
                for geog in v1['subgeographies']:
                    top_level_dict[geog.lower().strip()] = v1['dma_code']

        # Add the top level dict as it would have been read from the YAML file,
        #   now with the flattened sub-level
        flattened_station_dmas[k0] = top_level_dict

    return flattened_station_dmas


def lookup_dma_code_by_station_name(station_name: str, station_dma_lookup: dict, verbose:bool=True) -> str:
    """
    Looks up the dma_code for the station_name in the passed station_dma_lookup
      with the addition of graceful handling in the case of key errors. This 
      is so we can use this functionality with pandas .apply().

    Args:
        station_name: String of the station name
        station_dma_lookup: Dict of stations to lookup. This must be a top 
          level dict of stations with DMA codes. The YAML file with the lookups
          contains multiple tops levels (stations, markets). Only pass the 
          stations.
        verbose: if True, output logging warnings when the station name isn't 
          found

    Returns: 
        String of the DMA code as looked up in the station DMA lookup
    """
    try:
        return str(int(station_dma_lookup[station_name.lower()]))
    except KeyError:
        if verbose:
            logging.warning(f'Station name {station_name} could not be found in market_dma_lookup')
    except Exception as e:
        print('Unhandled exception while attempting to lookup DMA for station')
        raise e
    

def lookup_dma_code_by_market_name(market_name: str, market_dma_lookup: dict, verbose:bool=True) -> str:
    """
    Looks up the dma_code for the market_name in the passed station_dma_lookup
      with the addition of graceful handling in the case of key errors. This 
      is so we can use this functionality with pandas .apply().

    Args:
        station_name: String of the station name
        market_dma_lookup: Dict of markets to lookup. This must be a top 
          level dict of markets with DMA codes. The YAML file with the lookups
          contains multiple tops levels (stations, markets). Only pass the 
          markets.
        verbose: if True, output logging warnings when the market name isn't 
          found

    Returns: 
        String of the DMA code as looked up in the market DMA lookup
    """
    try:
        return str(int(market_dma_lookup[market_name.lower()]))
    except KeyError:
        if pd.isnull(market_name) or market_name is None or market_name == 'nan':
            pass
        else:
            logging.warning(f'Market name {market_name, type(market_name)} could not be found in market_dma_lookup')
    except Exception as e:
        print('Unhandled exception while attempting to lookup DMA for market')
        raise e


def get_dma_code_for_stations(dma_lookup: dict, stations_and_markets: pd.DataFrame, verbose:bool=True) -> list:
    """
    Retrieves the DMA code for the stations passed in stations_and_markets. The 
    retrieved DMA code is added as a new column.  

    Args:
        dma_lookup: dict of stations and markets mapped to DMAs.There are two 
          top level keys - [stations, markets].
        stations_and_markets: dataframe containing two columns [station, market]
        verbose: if True, output logging warnings when stations remain unmapped

    Returns:
        Dataframe with columns [station, market_name, dma_code] with the Nielsen 
          DMA code added for all rows.

    Raises:
        KeyError: if station name is not in the stations lookup
    """
    stations_with_dmas = stations_and_markets.copy()
    stations_with_dmas['dma_code'] = None # add dma_code column 
        
    # Add DMA code by station
    # Note that logically this will overwrite DMA codes applied to markets, but
    #   this is fine behaviour as station is more precise
    stations_with_dmas['dma_code'] = stations_with_dmas['station'].apply(
        lambda s: lookup_dma_code_by_station_name(s, dma_lookup['stations'], verbose=verbose)
        )

    if verbose:
        unmapped_stations = sorted(list(set(stations_with_dmas.loc[stations_with_dmas['dma_code'].isnull()]['station'])))
        
        # Raise a warning if there are unmapped stations found in the input stations
        if len(unmapped_stations) > 0:
            print(f'{len(unmapped_stations)} unmapped stations identified')
            warning_string = f"""
                Attempted to reslove DMA code using station name, but {unmapped_stations} could not be resolved.
            """
            logging.warning(warning_string)
            return stations_with_dmas
        else:
            return None
        
    else:
        return stations_with_dmas
    

def get_dma_code_for_markets(dma_lookup: dict, stations_and_markets: pd.DataFrame, verbose:bool=True) -> list:
    """
    Retrieves the DMA code for the markets passed in stations_and_markets. The 
    retrieved DMA code is added as a new column.  

    Args:
        dma_lookup: dict of stations and markets mapped to DMAs.There are two 
          top level keys - [stations, markets].
        stations_and_markets: dataframe containing two columns [station, market]
        verbose: if True, output logging warnings when stations remain unmapped

    Returns:
        Dataframe with columns [station, market_name, dma_code] with the Nielsen 
          DMA code added for all rows.

    Raises:
        KeyError: if station name is not in the stations lookup
    """
    stations_with_dmas = stations_and_markets.copy()
    stations_with_dmas['dma_code'] = None # add dma_code column 
    
    # First, we'll add DMA code to any row that has a market name present
    stations_with_dmas['dma_code'] = stations_with_dmas['market_name'].apply(
        lambda m: lookup_dma_code_by_market_name(m, dma_lookup['markets'], verbose=verbose)
        )

    if verbose:
        unmapped_stations = sorted(list(set(stations_with_dmas.loc[stations_with_dmas['dma_code'].isnull()]['station'])))
        
        # Raise a warning if there are unmapped stations found in the input stations
        if len(unmapped_stations) > 0:
            print(f'{len(unmapped_stations)} unmapped stations identified')
            warning_string = f"""
                Attempted to reslove DMA code using market name, but {unmapped_stations} could not be resolved.
            """
            logging.warning(warning_string)
            return stations_with_dmas
        else:
            return None
        
    else:
        return stations_with_dmas


def remove_punctuation(series: pd.Series, punc_marks: list):
    """
    Takes a pandas series and removes all punc marks according to the passed 
      punc_marks param. Notably, this function will return a series of strings
      regardless of the input series type.

    Args:
        series: Pandas series to apply logic to 
        punc_marks: list of punctuation to remove

    Returns:
        Stringified pandas series with punctuation marks removed.
    """
    formatted_series = series.copy()

    for punc in punc_marks:
        formatted_series = formatted_series.astype(str).str.strip()
        formatted_series = formatted_series.astype(str).str.replace(punc, '')

    return formatted_series


def clean_dataframe(df):
    """
    Formats input dataframe columns to standards laid out in this function.

    Args:
        df: Dataframe to run formatting operations on. 

    Returns:
        Formatted dataframe.

    Raises:
        ValueError: if 'length' column contains unhandled punctuation
    """
    formatted_df = df.copy()

    # Column headers to lowercase
    formatted_df.columns = [col.lower() for col in formatted_df.columns]

    # Rename market if it as provided 
    if 'market (city)\r' in formatted_df.columns:
        formatted_df.rename(columns={'market (city)\r': 'market_name'}, inplace=True)
    else:
        # If there's a market column that doesn match our input file, raise exception
        if len([col for col in formatted_df.columns if 'market' in col]) > 0:
            raise Exception('Market-related column found in file but unhandled in dataframe formatting')
        else:
            # If the column, or any hint of a market column, doesn't exist then 
            #   create an empty column of nulls
            formatted_df['market_name'] = np.nan

    # Create datetime column
    if 'datetime' not in formatted_df.columns:
        formatted_df['datetime'] = formatted_df.apply(lambda row: f'{row.date} {row.time}', axis=1)
        formatted_df['datetime'] = pd.to_datetime(formatted_df['datetime'])
        formatted_df = formatted_df.drop(['date', 'time'], axis=1)
    else:
        formatted_df['datetime'] = pd.to_datetime(formatted_df['datetime'])

    # Format length columns
    # For any row where length is not know, length is set to 30
    formatted_df.loc[formatted_df['length'].isnull(), 'length'] = 30

    # Cast type into integers, ensuring punctuation is removed
    try:
        formatted_df['length'] = remove_punctuation(formatted_df['length'], [':'])
        formatted_df['length'] = formatted_df['length'].astype(int)

    # If there's punctuation we're not handling in this function, raise an
    #   exception with details on the non-numeric contents of length
    except:
        distinct_length_values = set([i for i in formatted_df['length'] if not i.isnumeric()])
        min_dt, max_dt = formatted_df['datetime'].min(), formatted_df['datetime'].max()
        error_string = f"""
            Dataframe with date range {min_dt}-{max_dt} contains unhandled punctuation in the length column.  
            Length column contains non numeric values: {distinct_length_values}
        """
        raise ValueError(error_string)
        
    # With length handled, let's apply this to other columns where we don't 
    #   need to specifically account for integer values
    for col in ['station', 'market_name', 'rate']:
        formatted_df[col] = remove_punctuation(formatted_df[col], [':', '/', '$'])

    ordered_cols = ['spot_id', 'datetime', 'station', 'dma_code', 'market_name', 'rate', 'length']
    return formatted_df[ordered_cols]


def create_final_dataframe(dfs:list):
    """
    Formats input dataframes into the format required for ingestion into the 
    attribution and concats into a single file. 

    Args:
        - dfs: list of dataframes to format and concat

    Returns
        A single pandas dataframe with all the script's input data correctly
        formatted for use in the attribution model. 
    """
    # Concat dataframes and reset index
    output_df = pd.concat(dfs)
    output_df = output_df.reset_index(drop=True)
    output_df = output_df.drop(['market_name'], axis=1)

    ordered_cols = ['spot_id', 'datetime', 'station', 'dma_code', 'rate', 'length']

    return output_df


### MAIN
def main(aggregated_logs_folder_path):

    station_dmas = load_station_dma_lookup()

    log_dfs = create_dataframes_from_log_folder(aggregated_logs_folder_path)
    
    for i, df in enumerate(log_dfs):
        # Format the dataframe
        log_dfs[i] = clean_dataframe(df)

        # If DMA code is null anywhere, attempt to find the DMA code using the 
        #   station_dma_lookup. We'll look for station first as it's most
        #   precise, then if still no luck, look for market.
        # Note that this section will not raise exceptions, only warnings, so 
        #   pay attention to the terminal output.
        if log_dfs[i].loc[log_dfs[i]['dma_code'].isnull()].size > 0:
            logging.warning(f'Null DMA code values found in log_df {i}. Attempting to resolve.')
            # Now... Add DMA codes anywhere where they are null based on station
            log_dfs[i].loc[log_dfs[i]['dma_code'].isnull(), 'dma_code'] = get_dma_code_for_stations(
                dma_lookup=station_dmas, 
                stations_and_markets=log_dfs[i].loc[log_dfs[i]['dma_code'].isnull()][['station', 'market_name']],
                verbose=False
            )['dma_code']

            # Add DMA codes anywhere where they are still null based on market
            log_dfs[i].loc[log_dfs[i]['dma_code'].isnull(), 'dma_code'] = get_dma_code_for_markets(
                dma_lookup=station_dmas, 
                stations_and_markets=log_dfs[i].loc[log_dfs[i]['dma_code'].isnull()][['station', 'market_name']]
            )['dma_code']

        else:
            pass

    # Clean up dataframes for output and concat
    output_df = create_final_dataframe(log_dfs)
    print(output_df.head())

    return None


if __name__ == '__main__':
    main(aggregated_logs_folder_path='./post_log_data/pacific_source')

