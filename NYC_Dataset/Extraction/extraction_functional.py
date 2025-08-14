#Import modules
import json
import sqlite3
import certifi
import pandas as pd
import urllib3
import logging
#from pyarrow.dataset import parquet_dataset
#from urllib3 import request

logger = logging.getLogger(__name__)

def source_data_parquet(parquet_file_name):
    try:
        df_parquet = pd.read_parquet(parquet_file_name)
        logger.info(f'{parquet_file_name} : extracted {df_parquet.shape[0]} records from the parguet file')
    except Exception as e:
        logger.exception(f'{parquet_file_name} : - exception {e} encountered while extracting the parguet file')
        df_parquet = pd.DataFrame()

    return df_parquet

def source_data_csv(csv_file_name):
    try:
        df_csv = pd.read_csv(csv_file_name)
        logger.info(f'{csv_file_name} : extracted {df_csv.shape[0]} records from the csv file')
    except Exception as e:
        logger.exception(f'{csv_file_name} : exception {e} encountered while extracting the csv file')
        df_csv = pd.DataFrame()

    return df_csv

def source_data_api(api_endpoint):
    try:
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
        api_response = http.request('GET', api_endpoint)
        apt_status = api_response.status
        if apt_status == 200:
            logger.info(f'{apt_status} - ok : while invoking the api {api_endpoint}')
            data = json.loads(api_response.data.decode('utf-8'))
            df_api = pd.json_normalize(data)
            logger.info(f'{apt_status}- extracted {df_api.shape[0]} records from the csv file')
        else:
            logger.error(f'{apt_status}- error : while invoking the api {api_endpoint}')
            df_api = pd.DataFrame()
    except Exception as e:
        logger.exception(f'{apt_status} : - exception {e} encountered while reading data from the api')
        df_api = pd.DataFrame()
    return df_api

def source_data_table(db_name, table_name):
    try:
        with sqlite3.connect(db_name) as conn:
            df_table = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            logger.info(f'{db_name}- read {df_table.shape[0]} records from the table: {table_name}')
    except Exception as e:
        logger.exception(f'{db_name} : - exception {e} encountered while reading data from the table: {table_name}')
        df_table = pd.DataFrame()
    return df_table

def source_data_webpage(webpage_url, match_word):
    try:
        df_html = pd.read_html(webpage_url, match=match_word)
        df_html = df_html[0]
        logger.info(f'{webpage_url}- read {df_html.shape[0]} records from the page: {webpage_url}')
    except Exception as e:
        logger.exception(f'{webpage_url} : - exception {e} encountered while reading data from the page: {webpage_url}')
        df_html = pd.DataFrame()
    return df_html

def extracted_data():
    parquet_file_name = 'NYC_Dataset/yellow_tripdata_2022-01.parquet'
    csv_file_name = 'NYC_Dataset/h9gi-nx95.csv'
    api_endpoint = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500"
    db_name = "movies.sqlite"
    table_name = "movies"
    webpage_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
    match_word = "by country"

# Calling all the functions with their respective parameters so that they can be stored in the
# Staging tables namely Persist storage area table or Volatile Storage Area table
# Or to be consumed in tranformation pipeline

    df_parquet, df_csv, df_api, df_table, df_html = (source_data_parquet(parquet_file_name),
                                                source_data_csv(csv_file_name),
                                                source_data_api(api_endpoint),
                                                source_data_table(db_name, table_name),
                                                source_data_webpage(webpage_url, match_word)
                                                 )

    return df_parquet, df_csv, df_api, df_table, df_html


if __name__ == "__main__":
    df_parquet, df_csv, df_api, df_table, df_html = extracted_data()

    # Print summaries
    print(f"Parquet data: {df_parquet.shape}")
    print(f"CSV data: {df_csv.shape}")
    print(f"API data: {df_api.shape}")
    print(f"Database data: {df_table.shape}")
    print(f"HTML data: {df_html.shape}")

