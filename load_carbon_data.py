import os
import requests
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv('API_KEY')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')  
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT= os.getenv('DB_PORT', '5432')  # Default PostgreSQL port
API_URL = "https://api.ember-energy.org/v1/carbon-intensity/yearly"


def extract_carbon_data(start_year="2000"):
    """
    Extracts carbon intensity data from Ember API for the specified start year.
    """
    params = {
        'start_year': start_year,
        'api_key': API_KEY
    }
    
    response = requests.get(API_URL, params=params)
    
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data['data'])
    else:
        raise Exception(f"Error fetching data: {response.status_code} - {response.text}") 

def transform_carbon_data(df):
    """
    Transforms the carbon intensity data DataFrame.
    """
    df.dropna(subset=["entity", "entity_code", "date", "emissions_intensity_gco2_per_kwh"], inplace=True)
    df["emissions_intensity_gco2_per_kwh"] = df["emissions_intensity_gco2_per_kwh"].astype(float)
    df["is_aggregate_entity"] = df["is_aggregate_entity"].astype(bool)
    #df['year'] = pd.to_datetime(df['year'], format='%Y')
    # df.rename(columns={
    #     "entity": "country",
    #     "entity_code": "country_code",
    #     "emissions_intensity_gco2_per_kwh": "carbon_intensity"
    # }, inplace=True)
    return df

def load_carbon_data(df):
    """
    Loads the transformed carbon intensity data into PostgreSQL database.
    """
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS carbon_intensity (
            country TEXT,
            country_code TEXT,
            date INTEGER,
            carbon_intensity FLOAT,
            is_aggregate_entity BOOLEAN,
            PRIMARY KEY (country_code, date)
        )
    """)
    
    # Insert data into the table
    for _, row in df.iterrows():
        cursor.execute("""
    INSERT INTO carbon_intensity (
        entity,
        entity_code,
        is_aggregate_entity,
        date,
        emissions_intensity_gco2_per_kwh
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (entity_code, date) DO NOTHING;
""", (
    row["entity"],
    row["entity_code"],
    row["is_aggregate_entity"],
    row["date"],
    row["emissions_intensity_gco2_per_kwh"]
))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Data loaded successfully into the database.")
    
if __name__ == "__main__":
    try:
        # Extract data
        carbon_data = extract_carbon_data(start_year="2000")
        
        # Transform data
        transformed_data = transform_carbon_data(carbon_data)
        print("Clearned data:", transformed_data.head())
        
        # Load data into database
        load_carbon_data(transformed_data)
        
        
    except Exception as e:
        print(f"An error occurred: {e}")

    
    