#Dependencies
from operator import concat
import os
import pandas as pd
import psycopg2
import datetime as dt
from sqlalchemy import create_engine

engine=create_engine('postgresql://postgres:vikranth10@localhost:5432/cowin')
#Postgres Credentials
def create_static_tables():
    hostname = 'localhost'
    database = 'cowin'
    username = 'postgres'
    pwd = 'vikranth10'
    port_id = 5432
    conn = None
    cur = None

    try:
        #connect to postgresdb
        conn = psycopg2.connect(
                    host = hostname,
                    dbname = database,
                    user = username,
                    password = pwd,
                    port = port_id)
        cur = conn.cursor()
        #create table
        script1 = ''' CREATE TABLE IF NOT EXISTS demography(
                    demography_id              varchar(100) NOT NULL,
                    total_population_2019      int,
                    eligible_population_2019  int,
                    age_15_17                 int,
                    age_18_60                 int,
                    age_60_plus               int,
                    PRIMARY KEY(demography_id) ) '''

        script2 = ''' CREATE TABLE IF NOT EXISTS district(
                    distrct_id              int NOT NULL,
                    state                    varchar(100),
                    district                 varchar(100),
                    demography_id            varchar(100) NOT NULL,
                    PRIMARY KEY(demography_id) ) '''

        script3 = ''' CREATE TABLE IF NOT EXISTS bcg_cowin(
                    distrct_id                                  int NOT NULL,
                    date                                        datetime NOT NULL,
                    Avg_sites_open_per_week_current_week        numeric
                    Avg_sites_open_per_week_last_week           numeric
                    total_registered_population_till_date       numeric     
                    total_first_dose_administered               numeric
                    total_second_dose_administered              numeric         
                    c45_60_cumulative_doses                     numeric
                    c60_plus_cumulative_doses                   numeric
                    males_vaccinated                            numeric 
                    females_vaccinated                          numeric
                    cit_15_17_d1                                numeric
                    cit_15_17_d2                                numeric
                    cit_18_d1                                   numeric
                    cit_18_d2                                   numeric
                    cit_18_d1_1                                 numeric
                    cit_45_d1                                   numeric
                    cit_45_d2                                   numeric
                    cit_45_d1_1                                 numeric
                    cit_60_d1                                   numeric
                    cit_60_d2                                   numeric
                    cit_60_pd                                   numeric
                    cit_60_d1_1                                 numeric
                    pd                                          numeric
                    total                                       numeric
                    total_7                                     numeric
                    total_14                                    numeric
                    c12_14_cumulative_doses                     numeric
                    c15_17_cumulative_doses                     numeric
                    c18_45_cumulative_doses                     numeric
                    cit_18_pd                                   numeric
                    cit_45_pd                                   numeric
                    total_precautionary_dose_administered       numeric
                    cit_12_14_d1                                numeric
                    cit_12_14_d2                                numeric
                    PRIMARY KEY(district_id, date) ) '''

        cur.execute(script3)
        conn.commit()

        cur.close()            
        conn.close()
    except Exception as error:
        print(error)
    finally:
        if cur is not None:       
            cur.close()   
        if conn is not None:             
            conn.close()

#Creating new column in csv file and combining state and district. Injecting data for all 3 csv files
def excel():
 #Read district file
    district = pd.read_csv(r'C:\Users\Revanth\Desktop\sqlasgn\district.csv')
#Read demography file
    demography = pd.read_csv(r'C:\Users\Revanth\Desktop\sqlasgn\demography.csv')
#Read bcgcowin file
    bcgcowin = pd.read_csv(r'C:\Users\Revanth\Desktop\sqlasgn\bcg_cowin.csv')
#Replace space with underscore in state and district column
    district['state'] = district['state'].str.replace(' ', '_')
    district['district'] = district['district'].str.replace(' ', '_')
#Make a new column with state and district together named demography
    nc = district['demography_id'] = district['state'] + '_' + district['district']
#Save to new csv file
    district.to_csv(r'C:\Users\Revanth\Desktop\sqlasgn\district1.csv')
#Injecting Data into 3 different tables
    district.to_sql('district',con = engine ,if_exists="append",index=False)
    demography.to_sql('demography',con = engine ,if_exists="append",index=False)
    bcgcowin.to_sql('bcgcowin',con = engine ,if_exists="append",index=False)
    