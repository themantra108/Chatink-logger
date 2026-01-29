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
    """Returns current time in Indian Standard Time (IST)"""
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

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
                # Basic filter: Table must have data and columns
                if len(df) > 1 and len(df.columns) > 1:
                    # Ignore tables that are just "No data found"
                    if 'No data' in str(df.iloc[0,0]):
                        continue

                    # --- CLEAN HEADERS ---
                    # Turns "Symbol_Sort_table_by..." -> "Symbol"
                    new_columns = []
                    for c in df.columns:
                        # Split by "_Sort" and take the first part
                        clean_c = str(c).split("_Sort")[0].strip()
                        # Remove extra spaces or dots
                        clean_c = clean_c.replace(" ", "_").replace(".", "")
                        new_columns.append(clean_c)
                    
                    df.columns = new_columns
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

    # LOOP THROUGH ALL TABLES FOUND
    for i, new_df in enumerate(data_frames):
        tab_title = f"Table_{i+1}"
        
        try:
            worksheet = sh.worksheet(tab_title)
        except:
            worksheet = sh.add_worksheet(title=tab_title, rows=1000, cols=20)

        # --- FIX 1: FORCE HEADERS ---
        # Construct expected header row: [Timestamp, Col1, Col2...]
        expected_headers = ['Scraped_At_IST'] + new_df.columns.values.tolist()
        
        try:
            current_headers = worksheet.row_values(1)
        except:
            current_headers = []
            
        # If headers are missing or wrong, overwrite them.
        if current_headers != expected_headers:
            print(f"[{tab_title}] Updating Headers...")
            worksheet.update('A1', [expected_headers])

        # --- FIX 2: SMART UPDATE LOGIC ---
        first_col_name = new_df.columns[0].lower()
        
        # A. History Table (First col is Date)
        if 'date' in first_col_name or 'day' in first_col_name:
            print(f"[{tab_title}] Processing History Log...")
            new_date = str(new_df.iloc[0, 0]).strip()
            
            try:
                sheet_top_date = worksheet.acell('B2').value
            except:
                sheet_top_date = ""

            timestamp = get_ist_time()
            new_df.insert(0, 'Scraped_At_IST', timestamp)
            row_data = new_df.iloc[0].values.tolist()

            if new_date == sheet_top_date:
                # SAME DATE -> OVERWRITE Row 2
                worksheet.update('A2', [row_data])
                print(f"   -> Updated existing entry for {new_date}")
            else:
                # NEW DATE -> INSERT Row 2
                worksheet.insert_rows([row_data], row=2)
                print(f"   -> Inserted new entry for {new_date}")

        # B. Stock Scanner (First col is Symbol)
        else:
            print(f"[{tab_title}] Processing Stock Scanner...")
            
            # Find Symbol Column Index
            target_col_idx = 0
            for idx, col in enumerate(new_df.columns):
                if 'symbol' in col.lower() or 'stock' in col.lower():
                    target_col_idx = idx
                    break
            
            new_symbols = [str(s).strip() for s in new_df.iloc[:, target_col_idx].tolist()]
            
            # Fetch Old Symbols from Sheet (Col index + 1 for timestamp offset)
            sheet_col_index = target_col_idx + 1
            existing_rows = worksheet.get_values(f"A2:Z{len(new_df) + 1}")
            existing_symbols = []
            if existing_rows:
                for row in existing_rows:
                    if len(row) > sheet_col_index:
                        existing_symbols.append(str(row[sheet_col_index]).strip())

            if new_symbols == existing_symbols:
                print("   -> No change in stock list. Skipping.")
                continue

            print("   -> Stock list changed. Appending.")
            timestamp = get_ist_time()
            new_df.insert(0, 'Scraped_At_IST', timestamp)
            worksheet.insert_rows(new_df.values.tolist(), row=2)

if __name__ == "__main__":
    data = asyncio.run(scrape_chartink())
    update_google_sheet(data)
