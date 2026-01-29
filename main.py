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
                    # --- CLEAN HEADERS ---
                    # Turns "45r_Sort_table_by..." -> "45r"
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
            headers = ['Scraped_At'] + new_df.columns.values.tolist()
            worksheet.append_row(headers)

        # --- SMART LOGIC SELECTOR ---
        
        first_col_name = new_df.columns[0].lower()
        
        # LOGIC A: HISTORY TABLE (First column is 'date' or similar)
        if 'date' in first_col_name or 'day' in first_col_name:
            print(f"[{tab_title}] Detected History Table (by Date).")
            
            # Get the Date from the new scan (Top Row)
            new_date = str(new_df.iloc[0, 0]).strip()
            
            # Get the Date from the Sheet (Row 2, Column B because Col A is Timestamp)
            try:
                # A2 is timestamp, B2 is the Date column
                sheet_top_date = worksheet.acell('B2').value
            except:
                sheet_top_date = ""

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_df.insert(0, 'Scraped_At', timestamp)
            row_data = new_df.iloc[0].values.tolist() # Get just the top row

            if new_date == sheet_top_date:
                # SAME DATE -> OVERWRITE (UPDATE)
                # This keeps data "Real Time" without adding rows
                print(f"[{tab_title}] Date ({new_date}) exists. Updating stats in place...")
                
                # Update Row 2 (A2:Z2)
                cell_list = worksheet.range(f"A2:Z2")
                for i, cell in enumerate(cell_list):
                    if i < len(row_data):
                        cell.value = row_data[i]
                worksheet.update_cells(cell_list)
                
            else:
                # NEW DATE -> INSERT (APPEND)
                print(f"[{tab_title}] New Date ({new_date}) detected. Inserting new row.")
                worksheet.insert_rows([row_data], row=2)

        # LOGIC B: STOCK SCANNER (First column is Symbol)
        else:
            print(f"[{tab_title}] Detected Stock Scanner.")
            
            # Identify Symbol Column
            target_col_idx = 0
            for idx, col in enumerate(new_df.columns):
                if 'symbol' in col.lower() or 'stock' in col.lower():
                    target_col_idx = idx
                    break
            
            # Extract Symbols
            new_symbols = [str(s).strip() for s in new_df.iloc[:, target_col_idx].tolist()]
            
            # Fetch Old Symbols
            sheet_col_index = target_col_idx + 1
            existing_rows = worksheet.get_values(f"A2:Z{len(new_df) + 1}")
            existing_symbols = []
            if existing_rows:
                for row in existing_rows:
                    if len(row) > sheet_col_index:
                        existing_symbols.append(str(row[sheet_col_index]).strip())

            if new_symbols == existing_symbols:
                print(f"[{tab_title}] Stock list unchanged. Skipping.")
                continue

            print(f"[{tab_title}] Stock list changed. Appending...")
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_df.insert(0, 'Scraped_At', timestamp)
            worksheet.insert_rows(new_df.values.tolist(), row=2)

if __name__ == "__main__":
    data = asyncio.run(scrape_chartink())
    update_google_sheet(data)
