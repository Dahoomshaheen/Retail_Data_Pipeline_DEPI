import pandas as pd
import sqlalchemy
import os
from sqlalchemy import create_engine

def ingest_stores():
    # Get settings
    DB_USER = "sa"
    DB_PASSWORD = os.getenv("MSSQL_SA_PASSWORD", "SuperStrong_SQL_Pass_2026!")
    DB_HOST = "sqlserver" 
    DB_PORT = "1433"
    DB_NAME = "master"

    # Connection String using pymssql (much simpler)
    # format: mssql+pymssql://user:password@host:port/dbname
    connection_url = f"mssql+pymssql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    try:
        engine = create_engine(connection_url)
        
        # path inside docker
        file_path = "/opt/airflow/data/raw/stores.csv"
        
        df = pd.read_csv(file_path)
        print(f"Uploading {len(df)} stores using pymssql...")
        
        df.to_sql(name="stores", con=engine, if_exists="replace", index=False)
        print("Upload completed successfully!")

    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    ingest_stores()