# RC-v1.0--Data-Gathering
## Intial data gathering for the project
### Import Data From Statcast and Save `pull_clean_and_pickle.py`
1. Access MLB Statcast data via the `pybaseball` package
2. Preform initial data cleaning
3. Save cleaned Statcast data as pickle files by season (see `season_pickles` folder) 

### Convert Data to CSVs `pitches_to_csv.py`
4. Convert pickle files to DataFrames
5. Export each season DataFrame as a CSV file

### AWS Sorage
6. CSVs are uploaded to an AWS S3 Bucket
7. A AWS Redshift schema is specified 
8. The data is pulled from S3 and every season is loaded into a `pitches` table in Redshift

### Query AWS Redshift Table `AWS_Redshift_query.ipynb`
9. Query Redshift `pitches` table with conditions
10. Save query as DataFrame with column names from `column_names.py`

## How to use 
### Clone the repo and check the wiki for specific instructions
