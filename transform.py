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

def calculate_year(date_str):
    """
    Smart Year Logic:
    - If current month is Jan, and data says "Dec", assume it is Last Year.
    """
    try:
        current_date = datetime.now()
        current_year = current_date.year
        
        # Clean "29th Jan" -> "29 Jan"
        clean_d = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', str(date_str).strip())
        
        # Parse the date string with current year temp
        dt = datetime.strptime(f"{clean_d} {current_year}", "%d %b %Y")
        
        # Logic: If today is Jan, and row says "Dec", it must be (Year-1)
        if current_date.month == 1 and dt.month == 12:
            return current_year - 1
        
        return current_year
    except:
        return datetime.now().year

def process_data(raw_dfs):
    print("2. [TRANSFORM] Cleaning data & calculating Years...")
    if not raw_dfs:
        return []

    timestamp = get_ist_time()
    clean_dfs = []

    for df in raw_dfs:
        # 1. Clean Headers
        df.columns = [clean_header_name(c) for c in df.columns]
        
        # 2. Force text format
        df.fillna("", inplace=True)
        df = df.astype(str)
        
        # 3. Add Scraped Timestamp (First Column)
        df.insert(0, 'Scraped_At_IST', timestamp)
        
        # 4. Add Year Column (Last Column) based on 'Date' if it exists
        date_col = None
        for col in df.columns:
            if 'date' in col.lower():
                date_col = col
                break
        
        if date_col:
            year_values = df[date_col].apply(calculate_year)
            df['Year'] = year_values

        clean_dfs.append(df)

    print(f"   -> Transformed {len(clean_dfs)} tables.")
    return clean_dfs
