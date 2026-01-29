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
SHEET_NAME = "Chartink Smart Log" # Make sure you create this Sheet & Share it with your Service Account Email!
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

async def scrape_chartink():
    print("Launching browser...")
    async with async_playwright() as p:
        # Launch browser in headless mode
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            # 1. Load the page
            print(f"Visiting {URL}...")
            await page.goto(URL, timeout=60000)
            await page.wait_for_load_state('networkidle')
            
            # 2. Wait for tables to render
            try:
                await page.wait_for_selector("table", state="attached", timeout=15000)
            except:
                print("Warning: Wait for table timed out, attempting to parse HTML anyway...")

            # 3. Extract HTML and Parse with Pandas
            content = await page.content()
            dfs = pd.read_html(content)
            
            valid_dfs = []
            for df in dfs:
                # Filter out empty/layout tables
                if len(df) > 1 and len(df.columns) > 1:
                    # Clean headers (remove spaces/dots for cleaner Sheet headers)
                    df.columns = [str(c).replace(" ", "_").replace(".", "") for c in df.columns]
                    # Fill NaN to avoid errors
                    df.fillna("", inplace=True)
                    # Convert to string to ensure consistent comparison
                    df = df.astype(str)
                    valid_dfs.append(df)
            
            print(f"Extracted {len(valid_dfs)} valid tables.")
            return valid_dfs

        except Exception as e:
            print(f"Error during scraping: {e}")
            return []
        finally:
            await browser.close()

def update_google_sheet(data_frames):
    if not data_frames:
        print("No data extracted. Exiting.")
        return

    # 1. Authenticate using GitHub Secret
    if 'GCP_SERVICE_ACCOUNT' not in os.environ:
        print("Error: GCP_SERVICE_ACCOUNT secret not found.")
        return

    try:
        json_creds = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
        creds = Credentials.from_service_account_info(json_creds, scopes=SCOPES)
        client = gspread.authorize(creds)
        sh = client.open(SHEET_NAME)
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        print("Check: 1. Did you share the sheet with the client_email? 2. Is the Sheet Name correct?")
        return

    # 2. Process each table
    for i, new_df in enumerate(data_frames):
        tab_title = f"Scan_Results_{i+1}"
        
        try:
            worksheet = sh.worksheet(tab_title)
        except:
            # Create new worksheet if it doesn't exist
            worksheet = sh.add_worksheet(title=tab_title, rows=1000, cols=20)
            # Add headers with 'Scraped_At' as the first column
            headers = ['Scraped_At'] + new_df.columns.values.tolist()
            worksheet.append_row(headers)
            print(f"Created new tab: {tab_title}")

        # --- SMART DUPLICATE CHECK ---
        
        # A. Fetch existing data from top of sheet to compare
        # We assume data starts at Row 2. We fetch enough rows to cover the new dataframe size.
        # We need +1 because row 1 is header.
        num_rows_to_check = len(new_df)
        existing_data_range = worksheet.get_values(f"A2:Z{num_rows_to_check + 1}")
        
        # B. Clean existing data for comparison (Remove the first column which is Timestamp)
        existing_data_clean = []
        if existing_data_range:
            for row in existing_data_range:
                # Slice [1:] removes the timestamp at index 0
                if len(row) > 1:
                    existing_data_clean.append(row[1:])
                else:
                    existing_data_clean.append([]) # Handle empty rows if any

        # C. Prepare new data as list of lists
        new_data_list = new_df.values.tolist()

        # D. Compare: If lists are identical, STOP.
        # Note: This checks if the market data is exactly the same as the last entry.
        if existing_data_clean == new_data_list:
            print(f"[{tab_title}] Data unchanged. Skipping update.")
            continue
        
        # --- SAVE NEW DATA ---
        
        print(f"[{tab_title}] CHANGE DETECTED. saving...")
        
        # Add Timestamp to the new data
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_df.insert(0, 'Scraped_At', timestamp)
        
        # Insert rows at index 2 (pushes old data down)
        values_to_write = new_df.values.tolist()
        if values_to_write:
            worksheet.insert_rows(values_to_write, row=2)
            print(f"[{tab_title}] Success: Inserted {len(values_to_write)} rows at top.")

if __name__ == "__main__":
    # Asyncio entry point
    data = asyncio.run(scrape_chartink())
    update_google_sheet(data)
