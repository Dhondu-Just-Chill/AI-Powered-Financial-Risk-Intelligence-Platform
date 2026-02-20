from pathlib import Path
import pandas as pd
from data.parquet_maker import parquet_maker

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" 

def load_raw_data(file_name: str) -> pd.DataFrame:
    """
    Load raw data from a CSV file.

    Args:
        file_name (str): The name of the CSV file to load.
        
    Returns:
        pd.DataFrame: The loaded data as a pandas DataFrame.
    """
    file_path = RAW_DATA_PATH / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_name} not found in {file_path}")
    
    data = parquet
    
    return data

def list_raw_data_files() -> list:
    """
    List all CSV files in the raw data directory.

    Returns:
        list: A list of CSV file names in the raw data directory.
    """
    if not RAW_DATA_PATH.exists():
        raise FileNotFoundError(f"Raw data directory not found at {RAW_DATA_PATH}")
    
    return [f.name for f in RAW_DATA_PATH.iterdir() if f.is_file()]