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

# ==========================================
#              CONFIGURATION
# ==========================================
URL = "https://chartink.com/dashboard/208896"
SHEET_NAME = "Chartink_Multi_Log"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ==========================================
#           OPTIMIZED HELPER FUNCTIONS
# ==========================================

def get_ist_time():
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def clean_header(col_name):
    c = str(col_name)
    if "_Sort" in c: c = c.split("_Sort")[0]
    elif " Sort" in c: c = c.split(" Sort")[0]
    return c.strip("_ .")

def calculate_year(date_val):
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
    # Colors
    green, red, yellow = {"red":0.85,"green":0.93,"blue":0.82}, {"red":0.96,"green":0.8,"blue":0.8}, {"red":1,"green":1,"blue":0.8}
    
    headers = df.columns.tolist()
    idx = {name: i for i, name in enumerate(headers)}
    requests = []
    
    def add_rule(col, type, val, color):
        if col in idx:
            requests.append({"addConditionalFormatRule": {"index":0, "rule": {
                "ranges": [{"sheetId": worksheet.id, "startColumnIndex": idx[col], "endColumnIndex": idx[col]+1, "startRowIndex": 1, "endRowIndex": 1000}],
                "booleanRule": {"condition": {"type": type, "values": [{"userEnteredValue": str(val)}]}, "format": {"backgroundColor": color}}}}})

    if '4.5r' in idx: 
        add_rule('4.5r', 'NUMBER_GREATER', 400, yellow)
        add_rule('4.5r', 'NUMBER_GREATER', 200, green)
        add_rule('4.5r', 'NUMBER_LESS', 50, red)

    for c in ['20r', '50r']:
        if c in idx: add_rule(c, 'NUMBER_GREATER', 75 if '20' in c else 85, green); add_rule(c, 'NUMBER_LESS', 50 if '20' in c else 60, red)

    for c in ['4.5chg', '20chg', '50chg', '%ch']:
        if c in idx: add_rule(c, 'NUMBER_GREATER', 20, green); add_rule(c, 'NUMBER_LESS', -20, red)

    if requests:
        try: worksheet.spreadsheet.batch_update({"requests": requests})
        except: pass

# ==========================================
#           MAIN PIPELINE
# ==========================================

async def run_bot():
    print("🚀 Turbo Pipeline Started...")
    
    # --- 1. EXTRACT (Optimized) ---
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Block images/fonts/css to speed up loading by ~60%
        context = await browser.new_context()
        await context.route("**/*", lambda route: route.abort() 
            if route.request.resource_type in ["image", "media", "font", "stylesheet"] 
            else route.continue_())
            
        page = await context.new_page()
        try:
            # Reduced timeout, wait for domcontentloaded (faster than networkidle)
            await page.goto(URL, timeout=45000, wait_until="domcontentloaded")
            
            try: await page.wait_for_selector("table", state="attached", timeout=10000)
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
        if len(df) > 1 and len(df.columns) > 1 and 'No data' not in str(df.iloc[0,0]):
            df.columns = [clean_header(c) for c in df.columns]
            for col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            df.fillna("", inplace=True)
            df.insert(0, 'Scraped_At_IST', timestamp)
            try:
                if any(x in str(df.iloc[0, 1]) for x in ['Jan', 'Dec', 'th', 'st']):
                    df['Year'] = df.iloc[:, 1].apply(calculate_year)
            except: pass
            clean_dfs.append(df)

    # --- 3. LOAD (Batch Optimized) ---
    if not clean_dfs: return
    
    creds = Credentials.from_service_account_info(
        json.loads(os.environ['GCP_SERVICE_ACCOUNT']), scopes=SCOPES
    )
    client = gspread.authorize(creds)
    try: sh = client.open(SHEET_NAME)
    except: sh = client.create(SHEET_NAME)

    for i, df in enumerate(clean_dfs):
        tab_title = f"Table_{i+1}"
        try: ws = sh.worksheet(tab_title)
        except: ws = sh.add_worksheet(tab_title, 1000, 25)
        
        ws.update('A1', [df.columns.values.tolist()]) 
        
        first_col = df.columns[1].lower() if len(df.columns) > 1 else ""
        
        if 'date' in first_col or 'day' in first_col:
            # --- HISTORY SYNC ---
            all_val = ws.get_all_values()
            exist = {str(r[1]).strip(): idx+1 for idx, r in enumerate(all_val) if idx > 0}
            
            new_rows = []
            updates = [] # Collect updates here
            
            for _, row in df.iterrows():
                row_l = row.values.tolist()
                key = str(row_l[1]).strip()
                
                if key in exist:
                    row_idx = exist[key]
                    sheet_row_str = [str(x) for x in all_val[row_idx-1][1:]]
                    df_row_str = [str(x) for x in row_l[1:]]
                    
                    if sheet_row_str != df_row_str:
                        # Instead of updating immediately, add to batch list
                        updates.append({
                            'range': f"A{row_idx}",
                            'values': [row_l]
                        })
                else:
                    new_rows.append(row_l)
            
            # Execute in BATCHES (Much Faster)
            if new_rows: ws.insert_rows(new_rows, 2)
            if updates: ws.batch_update(updates)
            
            apply_formatting(ws, df)
        else:
            # --- SCANNER SYNC ---
            target_idx = 1
            exist_sym = [str(r[1]).strip() for r in ws.get_values(f"A2:Z{len(df)+50}")]
            new_sym = [str(x).strip() for x in df.iloc[:, target_idx].tolist()]
            
            if exist_sym[:len(new_sym)] != new_sym:
                ws.insert_rows(df.values.tolist(), 2)
                apply_formatting(ws, df)

    print("✅ Fast Sync Done.")

if __name__ == "__main__":
    asyncio.run(run_bot())
