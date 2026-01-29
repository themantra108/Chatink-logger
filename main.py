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
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

async def scrape_chartink():
    print("Launching browser...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            await page.goto(URL, timeout=60000)
            await page.wait_for_load_state('networkidle')
            try:
                await page.wait_for_selector("table", state="attached", timeout=15000)
            except:
                pass

            content = await page.content()
            dfs = pd.read_html(content)
            
            valid_dfs = []
            for df in dfs:
                if len(df) > 1 and len(df.columns) > 1:
                    # Clean Headers
                    new_columns = []
                    for c in df.columns:
                        clean_c = str(c).split("_Sort_")[0].strip()
                        clean_c = clean_c.replace(" ", "_").replace(".", "")
                        new_columns.append(clean_c)
                    
                    df.columns = new_columns
                    df.fillna("", inplace=True)
                    df = df.astype(str)
                    valid_dfs.append(df)
            
            return valid_dfs

        except Exception as e:
            print(f"Error during scraping: {e}")
            return []
        finally:
            await browser.close()

def update_google_sheet(data_frames):
    if not data_frames:
        return

    if 'GCP_SERVICE_ACCOUNT' not in os.environ:
        print("Error: GCP_SERVICE_ACCOUNT secret missing.")
        return

    try:
        json_creds = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
        creds = Credentials.from_service_account_info(json_creds, scopes=SCOPES)
        client = gspread.authorize(creds)
        sh = client.open(SHEET_NAME)
    except Exception as e:
        print(f"Error connecting to Sheets: {e}")
        return

    for i, new_df in enumerate(data_frames):
        tab_title = f"Scan_Results_{i+1}"
        
        try:
            worksheet = sh.worksheet(tab_title)
        except:
            worksheet = sh.add_worksheet(title=tab_title, rows=1000, cols=20)

        # --- FIX 1: FORCE HEADERS ---
        # We read the first row. If it's empty OR doesn't match the new dataframe, we write headers.
        current_headers = worksheet.row_values(1)
        expected_cols = new_df.columns.values.tolist()
        
        # Check if headers need writing (A1 should be 'Scraped_At_IST')
        if not current_headers or current_headers[0] != 'Scraped_At_IST':
            print(f"[{tab_title}] Headers missing or incorrect. Writing now...")
            full_header = ['Scraped_At_IST'] + expected_cols
            worksheet.update('A1', [full_header])

        # --- FIX 2: FULL SYNC (Upsert Logic) ---
        
        # 1. Read all existing data to map Key -> Row Index
        # We need this to find "Past Dates" quickly.
        all_values = worksheet.get_all_values()
        
        # Create a map: { "Unique_ID_Value": Row_Index }
        # We assume Row 1 is header, so data starts at Row 2 (index 1 in python list)
        existing_row_map = {}
        
        # Identify which column is the Unique Key (Date or Symbol)
        key_col_idx = 0 # Default to first column of data (which is Col B in sheet)
        first_col_name = new_df.columns[0].lower()
        
        # Determine Key Column Index relative to the SHEET (Col A=0, B=1, etc.)
        # Sheet Col 0 is 'Scraped_At_IST', Sheet Col 1 is the first data column.
        sheet_key_col_idx = 1 
        if 'symbol' in first_col_name or 'stock' in first_col_name:
            # If it's a stock scanner, find the symbol column index
            for idx, col in enumerate(new_df.columns):
                if 'symbol' in col.lower() or 'stock' in col.lower():
                    sheet_key_col_idx = idx + 1 # +1 because of Scraped_At
                    break
        
        # Build the map
        if len(all_values) > 1:
            for row_idx, row in enumerate(all_values):
                if row_idx == 0: continue # Skip header
                if len(row) > sheet_key_col_idx:
                    key_value = str(row[sheet_key_col_idx]).strip()
                    existing_row_map[key_value] = row_idx + 1 # Store 1-based Row ID for gspread

        # 2. Iterate through New Data
        rows_to_insert = []
        
        timestamp = get_ist_time()
        
        for _, row_series in new_df.iterrows():
            new_row_data = row_series.values.tolist()
            key_val = str(new_row_data[sheet_key_col_idx - 1]).strip() # Key in DataFrame
            
            # Prepare the full row for sheet (Timestamp + Data)
            full_row_to_write = [timestamp] + new_row_data
            
            # CHECK: Does this Key exist in the sheet?
            if key_val in existing_row_map:
                # IT EXISTS -> CHECK FOR CHANGES
                row_number = existing_row_map[key_val]
                
                # Fetch current data from sheet to compare
                # (Optimization: We technically have it in 'all_values' variable)
                current_sheet_row = all_values[row_number - 1]
                
                # Compare Data (ignoring timestamp at index 0)
                # current_sheet_row[1:] compares against new_row_data
                # We handle potential length mismatches safely
                current_data_only = current_sheet_row[1:] if len(current_sheet_row) > 1 else []
                
                # Strict comparison
                if current_data_only != new_row_data:
                    print(f"[{tab_title}] Adjustment detected for {key_val}. Updating Row {row_number}.")
                    # Update the specific row
                    # Gspread range: A{row}:Z{row}
                    worksheet.update(f"A{row_number}", [full_row_to_write])
                # Else: Data is same, do nothing.
                
            else:
                # IT DOES NOT EXIST -> INSERT
                # We collect these to insert them all at once at the top (Row 2)
                rows_to_insert.append(full_row_to_write)
        
        # 3. Batch Insert New Rows (if any)
        if rows_to_insert:
            print(f"[{tab_title}] Found {len(rows_to_insert)} new entries. Inserting at top.")
            worksheet.insert_rows(rows_to_insert, row=2)

if __name__ == "__main__":
    data = asyncio.run(scrape_chartink())
    update_google_sheet(data)
