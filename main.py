import asyncio
import os
import json
import re
import pandas as pd
from datetime import datetime
import pytz
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
URL = "https://chartink.com/dashboard/208896"
SHEET_NAME = "Chartink_Multi_Log" 
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def get_ist_time():
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def clean_header(col_name):
    """Aggressive Header Cleaner"""
    c = str(col_name)
    c = re.split(r'_Sort_table', c, flags=re.IGNORECASE)[0]
    c = re.split(r' Sort', c, flags=re.IGNORECASE)[0]
    return c.strip()

async def scrape_chartink():
    print("🚀 Launching browser...")
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
                    if 'No data' in str(df.iloc[0,0]):
                        continue

                    # Clean Headers
                    clean_columns = [clean_header(c) for c in df.columns]
                    df.columns = clean_columns
                    df.fillna("", inplace=True)
                    df = df.astype(str)
                    valid_dfs.append(df)
            
            print(f"✅ Found {len(valid_dfs)} valid tables.")
            return valid_dfs

        except Exception as e:
            print(f"❌ Error during scraping: {e}")
            return []
        finally:
            await browser.close()

def update_google_sheet(data_frames):
    if not data_frames:
        print("No data to sync.")
        return

    if 'GCP_SERVICE_ACCOUNT' not in os.environ:
        print("Error: GCP_SERVICE_ACCOUNT secret missing.")
        return

    try:
        json_creds = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
        creds = Credentials.from_service_account_info(json_creds, scopes=SCOPES)
        client = gspread.authorize(creds)
        try:
            sh = client.open(SHEET_NAME)
        except:
            sh = client.create(SHEET_NAME)
            sh.share(json_creds['client_email'], perm_type='user', role='owner')
    except Exception as e:
        print(f"Error connecting to Sheets: {e}")
        return

    timestamp = get_ist_time()

    for i, new_df in enumerate(data_frames):
        tab_title = f"Table_{i+1}"
        
        try:
            worksheet = sh.worksheet(tab_title)
        except:
            worksheet = sh.add_worksheet(title=tab_title, rows=1000, cols=20)

        # --- HEADERS ---
        expected_headers = ['Scraped_At_IST'] + new_df.columns.values.tolist()
        try:
            current_headers = worksheet.row_values(1)
        except:
            current_headers = []
        
        if current_headers != expected_headers:
            print(f"[{tab_title}] Fixing Headers...")
            worksheet.update('A1', [expected_headers])

        # --- FULL SYNC LOGIC ---
        first_col_name = new_df.columns[0].lower()
        
        # Determine if this is a History Table (Date-based) or Scanner (Symbol-based)
        is_history_table = 'date' in first_col_name or 'day' in first_col_name
        
        if is_history_table:
            print(f"[{tab_title}] performing FULL SYNC (History Mode)...")
            
            # 1. Fetch ALL existing data to find matches
            # We map { "DateString": RowIndex }
            # Note: Sheet has headers at Row 1. Data starts at Row 2.
            # Col A is Timestamp, Col B is Date.
            
            all_values = worksheet.get_all_values()
            existing_map = {}
            
            # Build map from existing sheet data
            if len(all_values) > 1:
                for idx, row in enumerate(all_values):
                    if idx == 0: continue # Skip header
                    if len(row) > 1:
                        date_val = str(row[1]).strip() # Col B is Date
                        existing_map[date_val] = idx + 1 # Store 1-based row index
            
            rows_to_insert = []
            
            # 2. Iterate through EVERY row in scraped data
            for index, row in new_df.iterrows():
                row_list = row.values.tolist()
                date_key = str(row_list[0]).strip() # Date is first col in DF
                
                full_row_data = [timestamp] + row_list
                
                if date_key in existing_map:
                    # IT EXISTS -> CHECK FOR CHANGES
                    row_idx = existing_map[date_key]
                    current_sheet_row = all_values[row_idx - 1]
                    
                    # Compare Data (ignoring timestamp at index 0)
                    # We compare current_sheet_row[1:] vs row_list
                    # (Slice carefully to handle length diffs)
                    existing_data_part = current_sheet_row[1:] if len(current_sheet_row) > 1 else []
                    
                    if existing_data_part != row_list:
                        print(f"   -> Adjustment detected for {date_key}. Updating Row {row_idx}.")
                        # Update the specific row
                        worksheet.update(f"A{row_idx}", [full_row_data])
                else:
                    # IT IS NEW -> ADD TO INSERT LIST
                    print(f"   -> New missing date found: {date_key}")
                    rows_to_insert.append(full_row_data)
            
            # 3. Batch Insert New Rows (if any)
            if rows_to_insert:
                print(f"   -> Inserting {len(rows_to_insert)} new rows...")
                worksheet.insert_rows(rows_to_insert, row=2)

        else:
            # STOCK SCANNER LOGIC (Append-Only Logic)
            print(f"[{tab_title}] Processing Stock Scanner...")
            target_col_idx = 0
            for idx, col in enumerate(new_df.columns):
                if 'symbol' in col.lower() or 'stock' in col.lower():
                    target_col_idx = idx
                    break
            
            new_symbols = [str(s).strip() for s in new_df.iloc[:, target_col_idx].tolist()]
            sheet_col_index = target_col_idx + 1
            existing_rows = worksheet.get_values(f"A2:Z{len(new_df) + 1}")
            existing_symbols = []
            if existing_rows:
                for row in existing_rows:
                    if len(row) > sheet_col_index:
                        existing_symbols.append(str(row[sheet_col_index]).strip())

            if new_symbols == existing_symbols:
                print("   -> No change.")
            else:
                print("   -> Change detected. Appending.")
                new_df.insert(0, 'Scraped_At_IST', timestamp)
                worksheet.insert_rows(new_df.values.tolist(), row=2)

if __name__ == "__main__":
    data = asyncio.run(scrape_chartink())
    update_google_sheet(data)
