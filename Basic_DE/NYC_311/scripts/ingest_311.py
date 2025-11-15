""" 
NYC 311 Service Requests Data Ingestion
Fetches data from NYC Open Data API and loads to Snowflake RAW layer
"""

import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
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
        # Fetch data
        df = pd.read_json(url, params=params)
        print(f"Successfully fetched {len(df)} records")
        return df
    
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

     # Select relevant columns
     columns = [
         'unique_key', 'created_date', 'agency', 'complaint_type',
        'descriptor', 'location_type', 'incident_zip', 'borough',
        'latitude', 'longitude', 'status'
     ]

     # Keep the cols in the df that are present in our relevant cols
     available_col = [col for col in columns if col in df.columns]
     df = df[available_col].copy()

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
    
