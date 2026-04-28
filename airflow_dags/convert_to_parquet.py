import pandas as pd
import os

def convert_to_parquet():
    
    raw_path = "/opt/airflow/data/raw"
    parquet_path = "/opt/airflow/data/parquet"
    
    # Ensure parquet directory exists
    os.makedirs(parquet_path, exist_ok=True)
    
    files = ["train.csv", "features.csv"]
    
    for file in files:
        input_file = f"{raw_path}/{file}"
        output_file = f"{parquet_path}/{file.replace('.csv', '.parquet')}"
        
        if not os.path.exists(input_file):
            print(f"File not found: {input_file}")
            continue
        
        print(f"Converting {file}...")
        df = pd.read_csv(input_file)
        df.to_parquet(output_file, index=False)
        print(f"Converted {file} -> {file.replace('.csv', '.parquet')}")
        print(f"Size before: {os.path.getsize(input_file) // 1024} KB")
        print(f"Size after: {os.path.getsize(output_file) // 1024} KB")

if __name__ == "__main__":
    convert_to_parquet()