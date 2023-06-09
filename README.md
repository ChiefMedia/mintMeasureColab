# Chief Media x Mint Measure Scripts
Collaborative data munging work between Chief Media and Mint Measure.

## Repo Installation
Python version 3.8.x >= 3.10.x

This was originally written in a Conda environment, however the requirements.txt
file is formatted for installation using pip. Anaconda uses an Intel optimized
version of Numpy, so you'll note in the requirements that three packages are 
pulling from a pypi url instead of pulling directly. Those three packages will
add several files to your repo root folder. Those files are included in
.gitignore, and you can see which ones exactly are ignored at the top of the 
gitignore file.   

1. Clone repo. 
2. Create a virtual environment using the cloned repo root folder. Terminal 
commands to replicate:

git clone https://github.com/ChiefMedia/mintMeasureColab  
python -m venv mintMeasureColab  
cd mintMeasureColab  
source bin/activate  
pip install -r requirements.txt  

Do not run requirements installation outside of a virtual environment or
contained. 

## Scripts  

## aggregate_post_logs.py 

### Critical Info

#### Compatible Stations and Markets
Currently this script supports post logs from the following stations and
markets. If a post-log file is from a new station or market, the station and 
market lookup dictionaries in the script must be updated. Any formatting changes
must be added, instructions for which are in this documentation. 

##### Stations (Station code in filename)
KATU  
KBNZ  
KBOI  
KHQ  
KOHD  
KOIN  
KPTV  
KTVM  
KTVZ  
KXLF  

##### Markets (Geographical name in filename)
Pierce  
Spokane  
Thurston  

##### Markets (Nielsen DMA name in filename)
None  

#### File Formatting/Cleaning
All input files MUST contain only the data. This means the following may require
a couple of simple manual edits:
1. All files must be in .xlsx format.
2. Header row must be the first row in the file. 
3. All "pretty" formatting must be removed, i.e. file must contain only the 
data, with no whitespace columns on any side. 
4. POTENTIAL BUG: All national media must have a dma_code field value of '999'. 
In the current iteration of this script, no spots are national so this isn't 
accounted for anywhere. Should this become the case, this will need to be 
handled in under the appropriate data augmentation section (probably in the 
market-level files vs the individual station files).  
5. POTENTIAL BUG: For market (multiple station) files, the format is a mess. To 
make the cleaning work for the most part, the length column ends up getting 
removed. As such, the length is added with a hard-coded value of 30, having 
looked at the files and verified only 30s exist. If this were to change, this 
will require a bug fix. 

Points 1 and 2 above apply to all post-log files, single station or multiple 
station. 

The script should handle all other data cleaning and formatting including 
dropping subtotal and total rows. Totals and subtotals should, however, be a 
first point of troubleshooting if something in the file does not look correct. 

If additions are made to the station mix, 
adding to or modifying the script to address the changes should be trivial if 
the above criteria are met. 

### Running the script
1. Ensure that the /data folder exists in the same folder as the script. 
2. Place all post log files 
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

#### FILE DATA CLEANING
This section cleans up the data to remove punctuation, standardize data types, 
etc. 

#### FILE AUGMENTATIONS
Any additions necessary. Currently this includes adding datetime fields to all
files. 

For the individual stations files, station is added by parsing the file name. 

DMA codes are added to both files, both using hard-coded lookup dictionaries in
their respective sections.



