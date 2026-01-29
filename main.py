import asyncio
import os
import json
import pandas as pd
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
URL = "https://chartink.com/dashboard/208896" 
SHEET_NAME = "Chartink_Multi_Table_Log"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

async def run():
    print("STEP 1: Starting Scraper...")
    
    # --- 1. SCRAPE ---
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        print("   -> Browser Launched.")
        page = await browser.new_page()
        
        try:
            print(f"   -> Visiting {URL}...")
            await page.goto(URL, timeout=90000) # Increased timeout
            
            # Wait for data to load
            print("   -> Waiting for tables to render...")
            try:
                await page.wait_for_selector("table", state="attached", timeout=20000)
                print("   -> Tables detected.")
            except:
                print("   -> Warning: Tables did not appear in 20s. Attempting to parse anyway.")

            content = await page.content()
            dfs = pd.read_html(content)
            
            if not dfs:
                print("❌ ERROR: No tables found in HTML. Dashboard might be empty or blocking bot.")
                return

            print(f"✅ SUCCESS: Found {len(dfs)} raw tables.")

        except Exception as e:
            print(f"❌ CRITICAL SCRAPE ERROR: {e}")
            return
        finally:
            await browser.close()

    # --- 2. CONNECT TO GOOGLE ---
    print("\nSTEP 2: Connecting to Google Sheets...")
    
    if 'GCP_SERVICE_ACCOUNT' not in os.environ:
        print("❌ ERROR: Secret 'GCP_SERVICE_ACCOUNT' is missing in GitHub Settings.")
        return

    try:
        creds = Credentials.from_service_account_info(
            json.loads(os.environ['GCP_SERVICE_ACCOUNT']), 
            scopes=SCOPES
        )
        gc = gspread.authorize(creds)
        
        try:
            sh = gc.open(SHEET_NAME)
            print(f"   -> Found Sheet: {SHEET_NAME}")
        except:
            print(f"   -> Sheet '{SHEET_NAME}' not found. Creating it...")
            sh = gc.create(SHEET_NAME)
            # Share with your personal email so you can see it
            client_email = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])['client_email']
            sh.share(client_email, perm_type='user', role='owner')
            print(f"   -> Created & Shared with {client_email}")

    except Exception as e:
        print(f"❌ AUTH ERROR: {e}")
        return

    # --- 3. UPLOAD DATA ---
    print("\nSTEP 3: Uploading Data...")
    
    valid_table_count = 0
    
    for i, df in enumerate(dfs):
        # Basic Clean
        if len(df) > 1 and len(df.columns) > 1:
            valid_table_count += 1
            tab_name = f"Table_{valid_table_count}"
            
            # Clean Headers (Minimal)
            df.columns = [str(c).split(" Sort")[0].split("_Sort")[0].strip() for c in df.columns]
            df = df.astype(str) # Convert everything to text to avoid upload errors
            
            # Get Tab
            try: ws = sh.worksheet(tab_name)
            except: ws = sh.add_worksheet(title=tab_name, rows=1000, cols=20)
            
            # DUMP DATA (Overwriting everything for now - keeping it simple)
            ws.clear()
            # Convert DataFrame to list of lists for gspread
            data = [df.columns.values.tolist()] + df.values.tolist()
            ws.update(range_name='A1', values=data)
            print(f"   -> Uploaded {len(df)} rows to {tab_name}")

    print("\n✅ DONE. Script finished.")

if __name__ == "__main__":
    asyncio.run(run())
