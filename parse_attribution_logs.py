"""
Script to parse and format attribution output logs. 
"""

import pandas as pd
import json
import os

### LOADS
# Load attribution log
attribution_log_filename = 'attribution_log_20230515_20230529'

# Mint File
#log_filepath = r"C:\Users\abaum\OneDrive - CHIEF MEDIA\Data Science Team Products\Dashboards\Mint Measure Attribution Analysis\Demo\Data\MINT_PACI_2023_05_15_log.json"
# Chief File
log_filepath = f'attribution_logs/{attribution_log_filename}.json'
log_file = open(log_filepath)
log = json.load(log_file)

# Load spots
# Mint File
#spots_filepath = r"C:\Users\abaum\OneDrive - CHIEF MEDIA\Data Science Team Products\Dashboards\Mint Measure Attribution Analysis\Demo\Data\MINT_PACI_2023_05_15_spots.csv"
# Chief file
spots_filepath = 'spots_aggregation_output/aggregated_spots_data_20230515_20230529.csv'
spots = pd.read_csv(spots_filepath)
spots['spot_id'] = spots['spot_id'].astype(str)
print(spots)

### LOG PROCESSING 
spot_results = []
for spot in log['spots']:
    for dma in spot['dma_data']:
        dma_result = dict(
            spot_id=[spot['spot_id']],
            dma_code=[dma['dma_code']],
            session_count=[dma['dma_session_total']]
        )
        spot_results.append(pd.DataFrame.from_dict(dma_result))
print(spot_results)

spot_dma_attribution = pd.concat(spot_results)
print(spot_dma_attribution.head(10))
print(spot_dma_attribution['session_count'].sum())

spot_dma_attribution = spots.merge(
    spot_dma_attribution[['spot_id', 'session_count']],
    on='spot_id',
    how='left'
)

spot_dma_attribution['session_count'] = spot_dma_attribution['session_count'].fillna(0)
spot_dma_attribution['session_count'] = spot_dma_attribution['session_count'].astype(int)
print(spot_dma_attribution.head(10))
print(spot_dma_attribution['session_count'].sum())

spot_attribution = spot_dma_attribution.groupby(
    'spot_id'
)['session_count'].sum().reset_index()
spot_attribution = spots.merge(
    spot_attribution,
    on='spot_id',
    how='inner'
)

# Write output
write_filepath = f'attribution_logs/{attribution_log_filename}.csv'
spot_attribution.to_csv(write_filepath, index=False)