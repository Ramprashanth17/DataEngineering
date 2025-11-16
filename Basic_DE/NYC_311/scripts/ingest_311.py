""" 
NYC 311 Service Requests Data Ingestion
Fetches data from NYC Open Data API and loads to Snowflake RAW layer
"""

import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
import requests
import os
import sys

# Adding parent directory to path to access .env 
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from dotenv import load_dotenv

#Loading env variables 
env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(env_path)

### Ingestion starts ###
def fetch_311_data(limit=1000, days_back=7):
    """
    Fetch NYC 311 service requests from NYC Open Data API

    Args:
        limit: No. of records to fetch
        days_back: How many days back to fetch data

    Returns:
        pandas Dataframe with the 311 data
    """

    print(f"Fetching {limit} records from the 311 API...")

    # NYC 311 API endpoint
    url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

    # Calculate the date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    # API parameters
    params = {
        '$limit': limit,
        '$where': f"created_date >= '{start_date.strftime('%Y-%m-%d')}' AND created_date <= '{end_date.strftime('%Y-%m-%d')}'",
        '$order': 'created_date DESC'
     }
    
    try:
        # Make the GET request to the API
        print("Sending API request with parameters...")
        response = requests.get(url, params = params)
        # Check for the status
        response.raise_for_status()
        # Load the json content, response.json() converts the json to python list/dict which pd.DataFrame reads
        df = pd.DataFrame(response.json())
        print(f"Successfully fetched {len(df)} records")
        return df
    
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP error occurred: {errh}")
        print(f"Reqeust URL: {response.url}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"General request error: {str(e)}")
        raise
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        raise

    ### Clean Dataframe with requried columns ###

def clean_data(df):
     """ Clean and standardize the raw data
      Args: df: Raw pandas DataFrame
      Returns: Cleaned pandas DataFrame
     """
     print("Cleaning data....")

     # Select relevant columns converted API lowercase cols to SF's uppercase
     column_mapping = {
        'unique_key': 'UNIQUE_KEY',
        'created_date': 'CREATED_DATE',
        'agency': 'AGENCY',
        'complaint_type': 'COMPLAINT_TYPE',
        'borough': 'BOROUGH',
        'status': 'STATUS'
    }

     # Keep the cols in the df that are present in our relevant cols
     available_col = [col for col in column_mapping.keys() if col in df.columns]
     df = df[available_col].copy()

     df = df.rename(columns=column_mapping)

     # Convert created_date to datetime
     if 'created_date' in df.columns:
         df['created_date'] = pd.to_datetime(df['created_date'], errors = 'coerce')

     if 'incident_zip' in df.columns:
         df['incident_zip'] = df['incident_zip'].fillna('UNKNOWN')

     if 'borough' in df.columns:
         df['borough'] = df['borough'].fillna('UNKNOWN')

     # Standardize the text fields 
     text_cols = ['agency', 'complaint_type', 'borough', 'status'] 
     for col in text_cols:
         if col in df.columns:
             df[col] = df[col].str.upper().str.strip()
    
    # Remove duplicates based on unique_key
     if 'unique_key' in df.columns:
         initial_count = len(df)
         df = df.drop_duplicates(subset=['unique_key'])
         duplicates_removed = initial_count - len(df)
         if duplicates_removed > 0:
             print(f" Removed {duplicates_removed} duplicate records")

    # Remove rows where critical fields are null
     if 'unique_key' in df.columns and 'created_date' in df.columns:
        df = df.dropna(subset=['unique_key', 'created_date'])
    
     print(f"Cleaned data: {len(df)} records ready to load")
     return df
    
    #### Loading to Snowflake ###
def load_to_snowflake(df):
    """
    Load data to Snowflake RAW layer
    
    Args:
        df: Cleaned pandas DataFrame
    """
    print(" Loading data to Snowflake...")
    
    # Connect to Snowflake
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            schema='RAW'
        )
        
        cursor = conn.cursor()

        ## Truncate table and load it fresh
        print("Truncating existing data....")
        cursor.execute("TRUNCATE TABLE NYC_311")

        # Load using pandas write__pandas(bulk_insert)
        from snowflake.connector.pandas_tools import write_pandas

        success, nchunks, nrows, _ = write_pandas(
            conn = conn,
            df = df,
            table_name = 'NYC_311',
            schema = 'RAW',
            database = 'DE_LEARNING',
            auto_create_table = False,
            overwrite = False
        )

        if success:
            print(f"Successfully loaded {nrows} rows to snowflake!")
        else:
            print("Falied to load data to Snowflake")

        # Validate load
        cursor.execute("SELECT COUNT(*) FROM NYC_311")
        count = cursor.fetchone()[0]
        print(f"Total records in Snowflake: {count}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error loading to Snowflake: {str(e)}")
        raise

def main():
    """Main ETL pipeline"""
    print("=" * 45)
    print("NYC 311 data ingestion pipeline")
    print("="*45)

    try:
        df = fetch_311_data(limit=1000, days_back=7)

        # Transform
        df_clean = clean_data(df)

        #Load
        load_to_snowflake(df_clean)

        print("\n"+ "=" * 45)
        print("Pipeline completed successfully!")
        print("="* 45)

    except Exception as e:
        print("\n"+ "=" * 45)
        print(f"Pipeline failed: {str(e)}")
        print("="* 45)
        sys.exit(1)


if __name__ == "__main__":
    main()