import asyncio
import os
import json
import pandas as pd
from datetime import datetime
import pytz
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
URL = "https://chartink.com/dashboard/208896"
SHEET_NAME = "Chartink Smart Log"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def get_ist_time():
    """Returns current time in Indian Standard Time."""
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

# ==========================================
# 1. THE SCRAPER (Fetches Raw Data)
# ==========================================
async def fetch_raw_html(url):
    print("Step 1: Launching Scraper...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            print(f"Visiting {url}...")
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state('networkidle')
            
            # Wait specifically for tables to appear
            try:
                await page.wait_for_selector("table", state="attached", timeout=15000)
            except:
                print("Warning: Timeout waiting for table selector.")

            # Extract the raw HTML string
            html_content = await page.content()
            return html_content

        except Exception as e:
            print(f"Scraper Error: {e}")
            return None
        finally:
            await browser.close()

# ==========================================
# 2. THE CLEANER (Process & Format Data)
# ==========================================
def process_and_clean_data(html_content):
    print("Step 2: Cleaning Data...")
    if not html_content:
        return []

    try:
        # Parse HTML into List of DataFrames
        dfs = pd.read_html(html_content)
    except ValueError:
        print("No tables found in HTML.")
        return []

    cleaned_dfs = []
    
    for df in dfs:
        # Basic Validation: Ignore empty or tiny tables
        if len(df) > 1 and len(df.columns) > 1:
            
            # A. Clean Headers (Remove 'Sort_table' junk)
            new_columns = []
            for c in df.columns:
                clean_c = str(c).split("_Sort_")[0].strip()
                clean_c = clean_c.replace(" ", "_").replace(".", "")
                new_columns.append(clean_c)
            df.columns = new_columns
            
            # B. Standardize Data
            df.fillna("", inplace=True) # Remove NaNs
            df = df.astype(str)         # Ensure all data is string for easy comparison
            
            cleaned_dfs.append(df)
            
    print(f"Cleaned {len(cleaned_dfs)} valid tables.")
    return cleaned_dfs

# ==========================================
# 3. THE LOADER (Google Sheets Logic)
# ==========================================
def sync_to_google_sheets(data_frames):
    print("Step 3: Syncing with Google Sheets...")
    if not data_frames:
        print("No data to sync.")
        return

    # Authenticate
    if 'GCP_SERVICE_ACCOUNT' not in os.environ:
        print("Error: GCP_SERVICE_ACCOUNT secret missing.")
        return

    try:
        json_creds = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
        creds = Credentials.from_service_account_info(json_creds, scopes=SCOPES)
        client = gspread.authorize(creds)
        sh = client.open(SHEET_NAME)
    except Exception as e:
        print(f"Google Sheets Connection Error: {e}")
        return

    # Process each table
    for i, new_df in enumerate(data_frames):
        tab_title = f"Scan_Results_{i+1}"
        
        try:
            worksheet = sh.worksheet(tab_title)
        except:
            worksheet = sh.add_worksheet(title=tab_title, rows=1000, cols=20)

        # --- A. HEADER CHECK ---
        # Ensure headers exist and are correct
        current_headers = worksheet.row_values(1)
        expected_cols = new_df.columns.values.tolist()
        
        # If headers missing or mismatch, force write them
        if not current_headers or current_headers[0] != 'Scraped_At_IST':
            print(f"[{tab_title}] Writing Headers...")
            full_header = ['Scraped_At_IST'] + expected_cols
            worksheet.update('A1', [full_header])

        # --- B. FULL HISTORY SYNC (Adjust past data) ---
        
        # 1. Map existing sheet data { "Key": Row_Index }
        all_values = worksheet.get_all_values()
        existing_row_map = {}
        
        # Determine which column is the Unique Key (Symbol or Date)
        # Sheet Col 1 (B) is usually the key (Col A is Timestamp)
        sheet_key_col_idx = 1 
        
        # Heuristic: If it looks like a stock scanner, find the symbol column
        first_col_name = new_df.columns[0].lower()
        if 'symbol' in first_col_name or 'stock' in first_col_name:
            for idx, col in enumerate(new_df.columns):
                if 'symbol' in col.lower() or 'stock' in col.lower():
                    sheet_key_col_idx = idx + 1 # +1 offset for timestamp
                    break
        
        # Build Map
        if len(all_values) > 1:
            for row_idx, row in enumerate(all_values):
                if row_idx == 0: continue # Skip header
                if len(row) > sheet_key_col_idx:
                    key_val = str(row[sheet_key_col_idx]).strip()
                    existing_row_map[key_val] = row_idx + 1 # Save 1-based index

        # 2. Compare New Data vs Old Data
        rows_to_insert = []
        timestamp = get_ist_time()
        
        for _, row_series in new_df.iterrows():
            new_row_data = row_series.values.tolist()
            # Key in DataFrame (no offset needed here)
            key_val = str(new_row_data[sheet_key_col_idx - 1]).strip()
            
            full_row_to_write = [timestamp] + new_row_data
            
            if key_val in existing_row_map:
                # Key exists -> Check for changes (Adjustments)
                row_number = existing_row_map[key_val]
                current_sheet_row = all_values[row_number - 1]
                
                # Compare data only (ignore timestamp at index 0)
                current_data_only = current_sheet_row[1:] if len(current_sheet_row) > 1 else []
                
                if current_data_only != new_row_data:
                    print(f"[{tab_title}] Updating changed data for: {key_val}")
                    worksheet.update(f"A{row_number}", [full_row_to_write])
            else:
                # Key is new -> Add to insert list
                rows_to_insert.append(full_row_to_write)
        
        # 3. Batch Insert New Rows
        if rows_to_insert:
            print(f"[{tab_title}] Inserting {len(rows_to_insert)} new rows.")
            worksheet.insert_rows(rows_to_insert, row=2)

# ==========================================
# MAIN EXECUTION FLOW
# ==========================================
if __name__ == "__main__":
    # 1. Scrape
    raw_html = asyncio.run(fetch_raw_html(URL))
    
    # 2. Clean
    clean_data = process_and_clean_data(raw_html)
    
    # 3. Load
    sync_to_google_sheets(clean_data)
