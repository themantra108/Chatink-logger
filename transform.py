import pandas as pd
import re
from datetime import datetime
import pytz

def get_ist_time():
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def clean_header_name(col_name):
    """Nuclear Header Cleaner"""
    c = str(col_name)
    if "_Sort" in c:
        c = c.split("_Sort")[0]
    elif " Sort" in c:
        c = c.split(" Sort")[0]
    return c.strip("_ .")

def calculate_year(date_val):
    """
    Lookback Year Logic:
    If today is Jan 2026, and row says '29th Dec', it treats it as 2025.
    """
    try:
        current_date = datetime.now()
        current_year = current_date.year
        
        # Clean "29th Jan" -> "29 Jan"
        s = str(date_val).strip()
        clean_d = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', s)
        
        # Parse date
        dt = datetime.strptime(f"{clean_d} {current_year}", "%d %b %Y")
        
        # Logic: If Current Month is Jan (1) and Data Month is Dec (12), subtract 1 year
        if current_date.month == 1 and dt.month == 12:
            return current_year - 1
        return current_year
    except:
        return datetime.now().year

def process_data(raw_dfs):
    print("2. [TRANSFORM] Cleaning & Adding Year...")
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
        
        # 3. Add Timestamp (First Column)
        df.insert(0, 'Scraped_At_IST', timestamp)
        
        # 4. Add Year Column (FORCE LOGIC)
        # We assume the Date is always the 2nd column (Index 1) after Timestamp
        try:
            # Apply logic to the column at index 1 (The Date Column)
            year_values = df.iloc[:, 1].apply(calculate_year)
            df['Year'] = year_values
        except Exception as e:
            print(f"   ⚠️ Year calculation failed: {e}")

        clean_dfs.append(df)

    print(f"   -> Transformed {len(clean_dfs)} tables.")
    return clean_dfs
