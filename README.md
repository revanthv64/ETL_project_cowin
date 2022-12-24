# Covid (Cowin) ETL Project
## Project Proposal
Extract Covid (Cowin) data from Google Drive, Transform csv files into clean dataframes, Load dataframes directly from pandas to PostgreSQL

### Tools Used:
- Pandas
  - SQL Alchemy
- PostgreSQL  

## Extract
Downloaded Covid (Cowin) files from Google Drive into the System

## Transform
- Replace **space** with **underscore(_)** in the STATE and DISTRICT column
- Made a new column combining STATE and DISTRICT and named it DEMOGRAPHY

## Load
- Connected Python with PostgreSQL using psycopg2
- Injected data into PostgreSQL
- Created tables in Python using SQLalchemy engine
