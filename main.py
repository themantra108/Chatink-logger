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
from gspread_dataframe import set_with_dataframe

# ==========================================
#              CONFIGURATION
# ==========================================
URL = "https://chartink.com/dashboard/208896"
SHEET_NAME = "Chartink_Multi_Log"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ==========================================
#           HELPER FUNCTIONS
# ==========================================

def get_ist_time():
    """Returns current time in IST (Indian Standard Time)"""
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def clean_header(col_name):
    """
    Removes garbage text like '_Sort_table_by...' from column headers.
    Input: 'Symbol_Sort_table_by_Symbol...' -> Output: 'Symbol'
    """
    c = str(col_name)
    if "_Sort" in c: c = c.split("_Sort")[0]
    elif " Sort" in c: c = c.split(" Sort")[0]
    return c.strip("_ .")

def calculate_year(date_val):
    """
    Determines the correct year for a date like '29th Dec'.
    Logic: If we are in Jan 2026, but the data says 'Dec', it must be Dec 2025.
    """
    try:
        current_date = datetime.now()
        current_year = current_date.year
        
        # Remove suffixes like 'th', 'st', 'nd' (e.g., 29th -> 29)
        s = str(date_val).strip()
        clean_d = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', s)
        
        # Parse the date assuming current year
        dt = datetime.strptime(f"{clean_d} {current_year}", "%d %b %Y")
        
        # If today is Jan and the row date is Dec, subtract 1 year
        if current_date.month == 1 and dt.month == 12:
            return current_year - 1
        return current_year
    except:
        return datetime.now().year

def apply_formatting(worksheet, df):
    """
    Applies Yellow/Green/Red Conditional Formatting to the Dashboard.
    Yellow = Breakout, Green = Bullish, Red = Bearish.
    """
    print("         🎨 Applying visual formatting...")
    
    # Define soft pastel colors for better readability
    green  = {"red": 0.85, "green": 0.93, "blue": 0.82} 
    red    = {"red": 0.96, "green": 0.8,  "blue": 0.8}
    yellow = {"red": 1.0,  "green": 1.0,  "blue": 0.8}

    # Create a map of {ColumnName: Index} to find where to apply rules
    headers = df.columns.tolist()
    idx = {name: i for i, name in enumerate(headers)}
    requests = []
    
    # Helper to generate a rule request
    def add_rule(col, condition_type, val, color):
        if col in idx:
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{
                            "sheetId": worksheet.id, 
                            "startColumnIndex": idx[col], 
                            "endColumnIndex": idx[col]+1, 
                            "startRowIndex": 1, 
                            "endRowIndex": 1000
                        }],
                        "booleanRule": {
                            "condition": {"type": condition_type, "values": [{"userEnteredValue": str(val)}]},
                            "format": {"backgroundColor": color}
                        }
                    }, "index": 0
                }
            })

    # --- RULE DEFINITIONS ---
    if '4.5r' in idx: 
        add_rule('4.5r', 'NUMBER_GREATER', 400, yellow) # Super Breakout
        add_rule('4.5r', 'NUMBER_GREATER', 200, green)  # Strong
        add_rule('4.5r', 'NUMBER_LESS', 50, red)        # Weak

    # Standard RSI-like indicators
    if '20r' in idx: add_rule('20r', 'NUMBER_GREATER', 75, green); add_rule('20r', 'NUMBER_LESS', 50, red)
    if '50r' in idx: add_rule('50r', 'NUMBER_GREATER', 85, green); add_rule('50r', 'NUMBER_LESS', 60, red)

    # Percentage Changes
    for c in ['4.5chg', '20chg', '50chg', '%ch']:
        if c in idx: 
            add_rule(c, 'NUMBER_GREATER', 20, green)
            add_rule(c, 'NUMBER_LESS', -20, red)

    # Send batch update to Google Sheets (Faster than one by one)
    if requests:
        try: worksheet.spreadsheet.batch_update({"requests": requests})
        except: pass

# ==========================================
#           MAIN PIPELINE
# ==========================================

async def run_bot():
    print("🚀 Starting ETL Pipeline...")
    
    # -------------------------------------
    # STEP 1: EXTRACT (Scrape the Website)
    # -------------------------------------
    async with async_playwright() as p:
        print("   🌐 Launching browser...")
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            # Visit URL and wait for network to settle
            await page.goto(URL, timeout=60000)
            await page.wait_for_load_state('networkidle')
            
            # Wait specifically for the table to appear
            try: await page.wait_for_selector("table", state="attached", timeout=15000)
            except: pass
            
            # grab HTML and parse with Pandas
            content = await page.content()
            dfs = pd.read_html(content)
        except Exception as e:
            print(f"   ❌ Scrape Error: {e}")
            dfs = []
        finally:
            await browser.close()

    if not dfs: return print("   ❌ No tables found on the page.")
    
    # -------------------------------------
    # STEP 2: CONNECT (Google Sheets)
    # -------------------------------------
    if 'GCP_SERVICE_ACCOUNT' not in os.environ: 
        return print("   ❌ Secret 'GCP_SERVICE_ACCOUNT' missing.")
    
    print("   🔑 Authenticating with Google...")
    creds = Credentials.from_service_account_info(
        json.loads(os.environ['GCP_SERVICE_ACCOUNT']), scopes=SCOPES
    )
    client = gspread.authorize(creds)

    try:
        sh = client.open(SHEET_NAME)
    except:
        sh = client.create(SHEET_NAME)
        sh.share(json.loads(os.environ['GCP_SERVICE_ACCOUNT'])['client_email'], perm_type='user', role='owner')

    timestamp = get_ist_time()

    # -------------------------------------
    # STEP 3: PROCESS & LOAD (Loop Tables)
    # -------------------------------------
    print(f"   🔄 Processing {len(dfs)} tables...")

    for i, df in enumerate(dfs):
        # Skip empty tables or "No data" placeholders
        if len(df) <= 1 or len(df.columns) <= 1 or 'No data' in str(df.iloc[0,0]):
            continue

        print(f"   Processing Table {i+1}...")

        # -----------------------------
        # A. TRANSFORM (Clean Data)
        # -----------------------------
        # 1. Clean Headers
        df.columns = [clean_header(c) for c in df.columns]
        
        # 2. Convert text numbers to Real Numbers (Int/Float)
        # This allows you to do math in Google Sheets
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='ignore')
        
        df.fillna("", inplace=True)
        df.insert(0, 'Scraped_At_IST', timestamp)
        
        # 3. Add Year Column Logic
        # Looks for columns containing "Date" or "Day" logic
        try:
            # We assume the data key (Date/Symbol) is always at Index 1 (0 is Timestamp)
            # We check a sample value to see if it looks like a date (e.g. "29th Jan")
            sample_val = str(df.iloc[0, 1])
            if any(x in sample_val for x in ['Jan', 'Dec', 'th', 'st']):
                df['Year'] = df.iloc[:, 1].apply(calculate_year)
        except: pass

        # -----------------------------
        # B. LOAD TO SHEET (Smart Sync)
        # -----------------------------
        try: ws = sh.worksheet(f"Table_{i+1}")
        except: ws = sh.add_worksheet(f"Table_{i+1}", 1000, 25)
        
        # Always force headers to match our clean DataFrame
        ws.update('A1', [df.columns.values.tolist()]) 
        
        # Determine if this is a HISTORY table (Date-based) or SCANNER (Symbol-based)
        first_col_name = df.columns[1].lower() if len(df.columns) > 1 else ""
        
        if 'date' in first_col_name or 'day' in first_col_name:
            # --- HISTORY SYNC ---
            # 1. Read existing data to find matches
            all_val = ws.get_all_values()
            # Map { "29th Jan": RowNumber }
            existing_map = {str(r[1]).strip(): idx+1 for idx, r in enumerate(all_val) if idx > 0}
            
            new_rows_to_add = []
            
            for _, row in df.iterrows():
                row_list = row.values.tolist()
                date_key = str(row_list[1]).strip() # Date is at Index 1
                
                if date_key in existing_map:
                    # UPDATE existing row if data changed
                    row_idx = existing_map[date_key]
                    # Compare data (skipping timestamp at index 0)
                    # Note: We convert to string for comparison safety
                    sheet_data = [str(x) for x in all_val[row_idx-1][1:]]
                    new_data   = [str(x) for x in row_list[1:]]
                    
                    if sheet_data != new_data:
                        ws.update(f"A{row_idx}", [row_list])
                else:
                    # ADD new row
                    new_rows_to_add.append(row_list)
            
            if new_rows_to_add:
                ws.insert_rows(new_rows_to_add, 2)
            
            # Always apply colors to keep dashboard fresh
            apply_formatting(ws, df)
            
        else:
            # --- SCANNER SYNC ---
            # For scanners, we usually just append if the list of stocks changed
            # Get existing symbols from Column B (Index 1)
            target_idx = 1
            existing_symbols = [str(r[1]).strip() for r in ws.get_values(f"A2:Z{len(df)+50}")]
            new_symbols = [str(x).strip() for x in df.iloc[:, target_idx].tolist()]
            
            # Only update if the top results are different
            if existing_symbols[:len(new_symbols)] != new_symbols:
                print("      ✨ New scanner results found. Updating.")
                ws.insert_rows(df.values.tolist(), 2)
                apply_formatting(ws, df)
            else:
                print("      💤 No change in scanner results.")

    print("✅ ETL Pipeline Finished Successfully.")

if __name__ == "__main__":
    asyncio.run(run_bot())
