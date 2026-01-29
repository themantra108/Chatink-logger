import asyncio
import os
import json
import pandas as pd
from datetime import datetime
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
                    # --- FIX: AGGRESSIVE HEADER CLEANING ---
                    # Chartink puts sorting text in headers. We split by '_Sort_' and keep the first part.
                    new_columns = []
                    for c in df.columns:
                        clean_c = str(c).split("_Sort_")[0].strip() # Removes "Sort_table_by..."
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

def get_comparison_column_index(df):
    """
    Finds the index of the Symbol column to ensure we only compare Symbols, not prices.
    """
    cols = [c.lower() for c in df.columns]
    
    # Priority 1: Look for 'symbol' or 'stock'
    for i, col in enumerate(cols):
        if 'symbol' in col or 'stock' in col:
            return i
            
    # Priority 2: Fallback to column 0 (usually the stock name in Chartink)
    return 0

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
            headers = ['Scraped_At'] + new_df.columns.values.tolist()
            worksheet.append_row(headers)
        
        # --- SYMBOL-ONLY COMPARISON LOGIC ---
        
        # 1. Find which column has the Stock Name (Index in DataFrame)
        target_col_idx = get_comparison_column_index(new_df)
        
        # 2. Extract NEW Symbols
        # iloc[:, target_col_idx] grabs that specific column
        new_symbols = new_df.iloc[:, target_col_idx].tolist()

        # 3. Extract OLD Symbols from Sheet
        # The sheet has 'Scraped_At' at Col A (Index 0).
        # So DataFrame Col 0 is Sheet Col 1 (B). DataFrame Col 1 is Sheet Col 2 (C).
        sheet_col_index = target_col_idx + 1 
        
        # Get existing data (just enough rows)
        existing_rows = worksheet.get_values(f"A2:Z{len(new_df) + 1}")
        
        existing_symbols = []
        if existing_rows:
            for row in existing_rows:
                # Check if row has data at that column
                if len(row) > sheet_col_index:
                    existing_symbols.append(row[sheet_col_index])
                else:
                    existing_symbols.append("")

        # 4. Compare
        # We strip whitespace to be safe
        new_symbols_clean = [str(s).strip() for s in new_symbols]
        old_symbols_clean = [str(s).strip() for s in existing_symbols]

        if new_symbols_clean == old_symbols_clean:
            print(f"[{tab_title}] No change in Stock List. Skipping.")
            continue
            
        # --- SAVE IF DIFFERENT ---
        print(f"[{tab_title}] Stock list changed! Appending data...")
        
        # Add Timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_df.insert(0, 'Scraped_At', timestamp)
        
        # Insert at Top
        values = new_df.values.tolist()
        if values:
            worksheet.insert_rows(values, row=2)
            print(f"[{tab_title}] Logged {len(values)} rows.")

if __name__ == "__main__":
    data = asyncio.run(scrape_chartink())
    update_google_sheet(data)
