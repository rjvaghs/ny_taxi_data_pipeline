#Importing necessary libraries

from sqlalchemy import create_engine
import pandas as pd
import pyarrow as pa
import numpy as np
from time import time
import argparse
import os
import psycopg2

def main(params):

    #Arguments

    user=params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    url=params.url
    table_name=params.table_name


    #Downloading the data in parquet file format

    parquet="output.parquet"
    os.system(f"wget {url} -O {parquet}")

    #Creating pandas dataframe from the parquet data file

    df = pd.read_parquet(parquet)

    #Converting required columns to datetime format

    df["tpep_pickup_datetime"]=pd.to_datetime(df.tpep_pickup_datetime)
    df["tpep_dropoff_datetime"]=pd.to_datetime(df.tpep_dropoff_datetime)

    #Creating a connection to Postgres database named ny_taxi

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    #Creating iterator function that will iterator over a specific chunk of data

    n = 100000  #chunk row size
    list_df = [df[i:i+n] for i in range(0,df.shape[0],n)]

    #Using the iterator function uploading the data into PostgreSQL Database with a table name 'yellow_taxi'  

    df.head(n=0).to_sql(name=table_name,con=engine,if_exists="replace") #Creating column headers

    for i in list_df:
        t_start=time()
        i.to_sql(name=table_name,con=engine,if_exists="append")
        t_end=time()
        print('inserted another chunk, took %.3f seconds'%(t_end-t_start))


if __name__ == '__main__':

    #Parsing the arguments that are provided for the main function

    parser = argparse.ArgumentParser(description='Ingest PARQUET data')
    parser.add_argument('--user',help='Username for Postgres')
    parser.add_argument('--password',help='Password for Postgres')
    parser.add_argument('--host',help='Host for Postgres')
    parser.add_argument('--port',help='Port for Postgres')
    parser.add_argument('--db',help='Database name for Postgres')
    parser.add_argument('--table_name',help='Table name for Postgres where we will write the results to')
    parser.add_argument('--url',help='URL of the PARQUET file')

    args = parser.parse_args()

    main(args)




