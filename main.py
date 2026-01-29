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
SHEET_NAME = "Chartink Hourly Log" 
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
                print("Wait for table timed out, trying to parse anyway...")

            content = await page.content()
            dfs = pd.read_html(content)
            
            valid_dfs = []
            for df in dfs:
                if len(df) > 1 and len(df.columns) > 1:
                    # Clean headers
                    df.columns = [str(c).replace(" ", "_").replace(".", "") for c in df.columns]
                    df.fillna("", inplace=True)
                    
                    # Convert all data to string to ensure easy comparison with Google Sheets
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
        print("No data to save.")
        return

    # Authenticate
    json_creds = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
    creds = Credentials.from_service_account_info(json_creds, scopes=SCOPES)
    client = gspread.authorize(creds)

    try:
        sh = client.open(SHEET_NAME)
    except Exception as e:
        print(f"Error opening sheet: {e}")
        return

    for i, new_df in enumerate(data_frames):
        tab_title = f"Scan_Results_{i+1}"
        
        try:
            worksheet = sh.worksheet(tab_title)
        except:
            # Create new worksheet if missing and add headers
            worksheet = sh.add_worksheet(title=tab_title, rows=1000, cols=20)
            # Add Timestamp column to header
            headers = ['Scraped_At'] + new_df.columns.values.tolist()
            worksheet.append_row(headers)

        # --- DUPLICATE CHECK LOGIC ---
        
        # 1. Get the existing data from the top of the sheet (Row 2 onwards)
        # We fetch exactly as many rows as the new dataframe has to compare them.
        existing_data = worksheet.get_values(f"A2:Z{len(new_df) + 1}")
        
        # 2. Prepare the new data for comparison
        # We only compare the stock data (excluding the timestamp we are about to add)
        # existing_data format: [ [Timestamp, StockA, Price...], [Timestamp, StockB, Price...] ]
        
        # Remove the first column (Timestamp) from existing data for comparison
        existing_data_clean = [row[1:] for row in existing_data] if existing_data else []
        
        # Convert new dataframe to list of lists
        new_data_list = new_df.values.tolist()

        # 3. Compare
        if existing_data_clean == new_data_list:
            print(f"[{tab_title}] Data is identical to the last run. Skipping update.")
            continue
        
        # --- IF DATA IS NEW ---
        
        print(f"[{tab_title}] New data detected. Appending...")
        
        # Add Timestamp column to the left of the new data
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_df.insert(0, 'Scraped_At', timestamp)
        
        # Insert at top (Row 2) pushing old data down
        values = new_df.values.tolist()
        if values:
            worksheet.insert_rows(values, row=2)
            print(f"Added {len(values)} rows.")

if __name__ == "__main__":
    data = asyncio.run(scrape_chartink())
    update_google_sheet(data)
