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
SHEET_NAME = "Chartink Hourly Log" # Ensure this Sheet exists and is shared with your Service Account Email
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
                    df['Scraped_At'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    df.columns = [str(c).replace(" ", "_").replace(".", "") for c in df.columns]
                    df.fillna("", inplace=True)
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

    # Authenticate using the Secret JSON from GitHub Environment
    json_creds = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
    creds = Credentials.from_service_account_info(json_creds, scopes=SCOPES)
    client = gspread.authorize(creds)

    try:
        sh = client.open(SHEET_NAME)
    except Exception as e:
        print(f"Error opening sheet '{SHEET_NAME}': {e}")
        print("Make sure you SHARED the Google Sheet with the Service Account Email!")
        return

    for i, df in enumerate(data_frames):
        tab_title = f"Scan_Results_{i+1}"
        
        try:
            worksheet = sh.worksheet(tab_title)
            # Insert at top (Row 2) to keep history with newest first
            values = df.values.tolist()
            if values:
                worksheet.insert_rows(values, row=2)
                print(f"Added {len(values)} rows to {tab_title}")
        except:
            # Create new worksheet if missing
            worksheet = sh.add_worksheet(title=tab_title, rows=1000, cols=20)
            worksheet.append_row(df.columns.values.tolist()) # Header
            worksheet.append_rows(df.values.tolist()) # Data
            print(f"Created new tab {tab_title}")

if __name__ == "__main__":
    data = asyncio.run(scrape_chartink())
    update_google_sheet(data)
