# mintMeasureColab
Collaborative work between Chief Media and Mint Measure.

## aggregate_post_logs.py
### Running the script
1. Ensure to install requirements.txt. 
2. Ensure that the /data folder exists in the same folder as the script. 
3. Run file. 

### Editing the script for new data
There are multiple section headers in the file denoted by comments starting with 
'###', e.g. ### FILE HEADER CLEANING. 

If presented with post logs from stations 
not currently handled by this script, these sections will need editing:

#### FILE HEADER CLEANING
1. Check the new file for punctuation issues. The script handles single 
whitespace. 
2. The date header name is standardized to 'aired_date'. Check that one of 
'aired_date', 'air_date', 'date' are in the headers. If not, you'll need to 
add the header to the list comprehension handling date header names. 
3. The time header name is standardized to 'aired_time'. Check that one of 
'aired_time', 'air_time', 'time', or 'actual_time_when_spot_aired' are in the 
headers. If not, you'll need to add the header to the list comprehension 
handling date header names. 

