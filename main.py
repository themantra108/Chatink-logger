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
    """Returns current time in IST"""
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def clean_header(col_name):
    """
    Removes garbage text like '_Sort_table_by...' 
    """
    c = str(col_name)
    if "_Sort" in c: c = c.split("_Sort")[0]
    elif " Sort" in c: c = c.split(" Sort")[0]
    return c.strip("_ .")

def calculate_year(date_val):
    """
    Smart Year Logic:
    If today is Jan 2026, and row says '29th Dec', it interprets it as Dec 2025.
    """
    try:
        current_date = datetime.now()
        current_year = current_date.year
        
        # Clean "29th Jan" -> "29 Jan"
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
    Applies Traffic Light Formatting (Yellow/Green/Red) to the sheet.
    """
    print("      🎨 Applying Visual Rules...")
    
    # Define Colors
    green  = {"red": 0.85, "green": 0.93, "blue": 0.82} 
    red    = {"red": 0.96, "green": 0.8,  "blue": 0.8}
    yellow = {"red": 1.0,  "green": 1.0,  "blue": 0.8}

    # Map Headers to Column Index
    headers = df.columns.tolist()
    idx = {name: i for i, name in enumerate(headers)}
    
    requests = []
    
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
                            "endRowIndex": 1000 # Apply to rows 2-1000
                        }],
                        "booleanRule": {
                            "condition": {"type": condition_type, "values": [{"userEnteredValue": str(val)}]},
                            "format": {"backgroundColor": color}
                        }
                    }, "index": 0
                }
            })

    # --- SPECIFIC RULES ---
    
    # 1. 4.5r (High Momentum)
    if '4.5r' in idx: 
        add_rule('4.5r', 'NUMBER_GREATER', 400, yellow)
        add_rule('4.5r', 'NUMBER_GREATER', 200, green)
        add_rule('4.5r', 'NUMBER_LESS', 50, red)

    # 2. 20r
    if '20r' in idx: 
        add_rule('20r', 'NUMBER_GREATER', 75, green)
        add_rule('20r', 'NUMBER_LESS', 50, red)

    # 3. 50r
    if '50r' in idx: 
        add_rule('50r', 'NUMBER_GREATER', 85, green)
        add_rule('50r', 'NUMBER_LESS', 60, red)

    # 4. Percentage Changes
    for c in ['4.5chg', '20chg', '50chg', '%ch']:
        if c in idx: 
            add_rule(c, 'NUMBER_GREATER', 20, green)
            add_rule(c, 'NUMBER_LESS', -20, red)

    if requests:
        try:
            # Clear old rules first to avoid stacking
            worksheet.clear_basic_filter() 
            worksheet.spreadsheet.batch_update({"requests": requests})
        except Exception as e:
            print(f"      ⚠️ Formatting Warning: {e}")

# ==========================================
#           MAIN PIPELINE
# ==========================================

async def run_bot():
    print("🚀 Starting Bot...")
    
    # --- 1. EXTRACT ---
    async with async_playwright() as p:
        print("   🌐 Launching browser...")
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(URL, timeout=60000)
            await page.wait_for_load_state('networkidle')
            try: await page.wait_for_selector("table", state="attached", timeout=15000)
            except: pass
            
            content = await page.content()
            dfs = pd.read_html(content)
        except Exception as e:
            print(f"   ❌ Scrape Error: {e}")
            dfs = []
        finally:
            await browser.close()

    if not dfs: return print("   ❌ No data found.")

    # --- 2. TRANSFORM ---
    clean_dfs = []
    timestamp = get_ist_time()
    
    for df in dfs:
        # Filter empty tables
        if len(df) > 1 and len(df.columns) > 1 and 'No data' not in str(df.iloc[0,0]):
            
            # A. Clean Headers
            df.columns = [clean_header(c) for c in df.columns]
            
            # B. Convert to Numeric (Crucial for Colors!)
            for col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            
            # C. Basics
            df.fillna("", inplace=True)
            df.insert(0, 'Scraped_At_IST', timestamp)
            
            # D. Add Year Column
            try:
                # Check index 1 (Date column)
                sample = str(df.iloc[0, 1])
                if any(x in sample for x in ['Jan', 'Dec', 'th', 'st']):
                    df['Year'] = df.iloc[:, 1].apply(calculate_year)
            except: pass
            
            clean_dfs.append(df)

    # --- 3. LOAD ---
    if not clean_dfs: return print("   ❌ No valid tables to sync.")
    if 'GCP_SERVICE_ACCOUNT' not in os.environ: return print("   ❌ GCP Secret Missing.")
    
    print("   🔑 Connecting to Sheets...")
    creds = Credentials.from_service_account_info(
        json.loads(os.environ['GCP_SERVICE_ACCOUNT']), scopes=SCOPES
    )
    client = gspread.authorize(creds)

    try:
        sh = client.open(SHEET_NAME)
    except:
        sh = client.create(SHEET_NAME)
        sh.share(json.loads(os.environ['GCP_SERVICE_ACCOUNT'])['client_email'], perm_type='user', role='owner')

    # Sync Logic
    for i, df in enumerate(clean_dfs):
        tab_title = f"Table_{i+1}"
        print(f"   🔄 Syncing {tab_title}...")
        
        try: ws = sh.worksheet(tab_title)
        except: ws = sh.add_worksheet(tab_title, 1000, 25)
        
        # Update Header Row
        ws.update('A1', [df.columns.values.tolist()]) 
        
        first_col = df.columns[1].lower() if len(df.columns) > 1 else ""
        
        # HISTORY TABLE LOGIC
        if 'date' in first_col or 'day' in first_col:
            all_val = ws.get_all_values()
            # Map Date (col 1) -> Row Index
            exist = {str(r[1]).strip(): idx+1 for idx, r in enumerate(all_val) if idx > 0}
            
            new_rows = []
            for _, row in df.iterrows():
                row_l = row.values.tolist()
                key = str(row_l[1]).strip()
                
                if key in exist:
                    # Check for updates (skip timestamp)
                    row_idx = exist[key]
                    sheet_row_str = [str(x) for x in all_val[row_idx-1][1:]]
                    df_row_str = [str(x) for x in row_l[1:]]
                    
                    if sheet_row_str != df_row_str:
                        ws.update(f"A{row_idx}", [row_l])
                else:
                    new_rows.append(row_l)
            
            if new_rows: ws.insert_rows(new_rows, 2)
            
            # Apply Colors
            apply_formatting(ws, df)

        # SCANNER LOGIC
        else:
            target_idx = 1
            exist_sym = [str(r[1]).strip() for r in ws.get_values(f"A2:Z{len(df)+50}")]
            new_sym = [str(x).strip() for x in df.iloc[:, target_idx].tolist()]
            
            if exist_sym[:len(new_sym)] != new_sym:
                print("      ✨ Appending new data.")
                ws.insert_rows(df.values.tolist(), 2)
                apply_formatting(ws, df)
            else:
                print("      💤 No change.")

    print("✅ Done.")

if __name__ == "__main__":
    asyncio.run(run_bot())
