import pandas as pd
import re
from datetime import datetime
import pytz

def get_ist_time():
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def clean_header_name(col_name):
    """Nuclear Header Cleaner: Removes '_Sort_table...'"""
    c = str(col_name)
    if "_Sort" in c:
        c = c.split("_Sort")[0]
    elif " Sort" in c:
        c = c.split(" Sort")[0]
    return c.strip("_ .")

def process_data(raw_dfs):
    """
    TRANSFORM LAYER:
    Cleans headers, adds timestamps, and handles data formatting.
    """
    print("2. [TRANSFORM] Cleaning data...")
    if not raw_dfs:
        return []

    timestamp = get_ist_time()
    clean_dfs = []

    for df in raw_dfs:
        # 1. Clean Headers
        df.columns = [clean_header_name(c) for c in df.columns]
        
        # 2. Force text format to prevent scientific notation in CSVs
        df.fillna("", inplace=True)
        df = df.astype(str)
        
        # 3. Add Metadata (Timestamp)
        # We insert it at the start so it's the first column
        df.insert(0, 'Scraped_At_IST', timestamp)
        
        clean_dfs.append(df)

    print(f"   -> Transformed {len(clean_dfs)} tables.")
    return clean_dfs
