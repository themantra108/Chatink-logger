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

# --- HELPER FUNCTIONS ---
def get_ist_time():
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def clean_header(col_name):
    """Nuclear Header Cleaner"""
    c = str(col_name)
    if "_Sort" in c: c = c.split("_Sort")[0]
    elif " Sort" in c: c = c.split(" Sort")[0]
    return c.strip("_ .")

def calculate_year(date_val):
    """Smart Year Logic"""
    try:
        current_date = datetime.now()
        current_year = current_date.year
        s = str(date_val).strip()
        clean_d = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', s)
        dt = datetime.strptime(f"{clean_d} {current_year}", "%d %b %Y")
        if current_date.month == 1 and dt.month == 12:
            return current_year - 1
        return current_year
    except:
        return datetime.now().year

def apply_formatting(worksheet, df):
    """Apply Yellow/Green/Red Heatmaps"""
    print("      -> Applying Colors...")
    green  = {"red": 0.85, "green": 0.93, "blue": 0.82} 
    red    = {"red": 0.96, "green": 0.8,  "blue": 0.8}
    yellow = {"red": 1.0,  "green": 1.0,  "blue": 0.8}

    headers = df.columns.tolist()
    idx = {name: i for i, name in enumerate(headers)}
    requests = []
    
    def add_rule(col, condition_type, val, color):
        if col in idx:
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": worksheet.id, "startColumnIndex": idx[col], "endColumnIndex": idx[col]+1, "startRowIndex": 1, "endRowIndex": 1000}],
                        "booleanRule": {"condition": {"type": condition_type, "values": [{"userEnteredValue": str(val)}]}, "format": {"backgroundColor": color}}
                    }, "index": 0
                }
            })

    if '4.5r' in idx: 
        add_rule('4.5r', 'NUMBER_GREATER', 400, yellow)
        add_rule('4.5r', 'NUMBER_GREATER', 200, green)
        add_rule('4.5r', 'NUMBER_LESS', 50, red)

    if '20r' in idx: add_rule('20r', 'NUMBER_GREATER', 75, green); add_rule('20r', 'NUMBER_LESS', 50, red)
    if '50r' in idx: add_rule('50r', 'NUMBER_GREATER', 85, green); add_rule('50r', 'NUMBER_LESS', 60, red)

    for c in ['4.5chg', '20chg', '50chg', '%ch']:
        if c in idx: add_rule(c, 'NUMBER_GREATER', 20, green); add_rule(c, 'NUMBER_LESS', -20, red)

    if requests:
        try: worksheet.spreadsheet.batch_update({"requests": requests})
        except: pass

# --- MAIN LOGIC ---
async def run_bot():
    print("🚀 Starting Bot...")
    
    # 1. EXTRACT
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(URL, timeout=60000); await page.wait_for_load_state('networkidle')
            try: await page.wait_for_selector("table", state="attached", timeout=15000)
            except: pass
            dfs = pd.read_html(await page.content())
        except: dfs = []
        await browser.close()

    # 2. TRANSFORM
    clean_dfs = []
    timestamp = get_ist_time()
    for df in dfs:
        if len(df) > 1 and len(df.columns) > 1 and 'No data' not in str(df.iloc[0,0]):
            
            # A. Clean Headers
            df.columns = [clean_header(c) for c in df.columns]
            
            # B. Convert Numeric Columns (The Fix)
            # This loops through cols and forces numbers to be Real Numbers (floats/ints)
            for col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            
            # C. Handle Empty Cells (Google Sheets hates NaN)
            df.fillna("", inplace=True)
            
            # D. Add Metadata
            df.insert(0, 'Scraped_At_IST', timestamp)
            
            # E. Add Year Logic
            try:
                # Check 2nd column (Index 1) for Date-like strings
                if len(df.columns) > 1 and any(x in str(df.iloc[0, 1]) for x in ['Jan', 'Dec', 'th', 'st']):
                    df['Year'] = df.iloc[:, 1].apply(calculate_year)
            except: pass
            
            clean_dfs.append(df)

    # 3. LOAD
    if not clean_dfs or 'GCP_SERVICE_ACCOUNT' not in os.environ: return print("❌ Config Error or No Data")
    
    creds = Credentials.from_service_account_info(json.loads(os.environ['GCP_SERVICE_ACCOUNT']), scopes=SCOPES)
    client = gspread.authorize(creds)
    try: sh = client.open(SHEET_NAME)
    except: sh = client.create(SHEET_NAME).share(json.loads(os.environ['GCP_SERVICE_ACCOUNT'])['client_email'], perm_type='user', role='owner')

    for i, df in enumerate(clean_dfs):
        try: ws = sh.worksheet(f"Table_{i+1}")
        except: ws = sh.add_worksheet(f"Table_{i+1}", 1000, 25)
        
        ws.update('A1', [df.columns.values.tolist()]) 
        
        first_col = df.columns[1].lower() if len(df.columns)>1 else ""
        if 'date' in first_col or 'day' in first_col:
            # History Logic
            all_val = ws.get_all_values()
            exist = {str(r[1]).strip(): i+1 for i, r in enumerate(all_val) if i>0}
            new_rows = []
            for _, row in df.iterrows():
                row_l = row.values.tolist()
                key = str(row_l[1]).strip()
                
                # Check for updates (excluding timestamp)
                if key in exist:
                    row_idx = exist[key]
                    # Convert sheet values to string for comparison logic, but send NUMBERS to update
                    sheet_row_str = [str(x) for x in all_val[row_idx-1][1:]]
                    df_row_str = [str(x) for x in row_l[1:]]
                    
                    if sheet_row_str != df_row_str:
                        ws.update(f"A{row_idx}", [row_l])
                else: 
                    new_rows.append(row_l)
            
            if new_rows: ws.insert_rows(new_rows, 2)
            apply_formatting(ws, df)
        else:
            # Scanner Logic
            target_idx = 1
            exist_sym = [str(r[1]).strip() for r in ws.get_values(f"A2:Z{len(df)+50}")]
            new_sym = [str(x).strip() for x in df.iloc[:, target_idx].tolist()]
            
            if exist_sym[:len(new_sym)] != new_sym:
                ws.insert_rows(df.values.tolist(), 2)
                apply_formatting(ws, df)

if __name__ == "__main__":
    asyncio.run(run_bot())
