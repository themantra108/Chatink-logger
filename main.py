import asyncio
import os
import json
import re
import traceback
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
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def clean_header(col_name):
    c = str(col_name)
    if "_Sort" in c: c = c.split("_Sort")[0]
    elif " Sort" in c: c = c.split(" Sort")[0]
    return c.strip("_ .")

def clean_currency(val):
    try:
        s = str(val).replace(',', '').strip()
        if not s: return ""
        return float(s)
    except:
        return val

def calculate_year(date_val):
    """
    Smart Year Logic:
    If today is Jan 2026, and row says '29th Dec', it treats it as 2025.
    """
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

def identify_table_type(df):
    """
    SMART IDENTIFIER:
    Checks columns to decide where the data belongs.
    """
    cols = [str(c).lower() for c in df.columns]
    
    # If it has 'Date', 'Day', or 'Period' -> It's History
    if any(x in cols for x in ['date', 'day', 'period']):
        return "History_Log"
    
    # If it has 'Symbol', 'Stock', or 'Script' -> It's the Scanner
    if any(x in cols for x in ['symbol', 'stock', 'script']):
        return "Stock_Scanner"
    
    return f"Unknown_Table_{len(df.columns)}Cols"

def force_number_format(worksheet, df):
    """API CALL: Forces Google Sheets to treat columns as Numbers"""
    try:
        start_col = 2
        end_col = len(df.columns)
        requests = [{
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 1, "endRowIndex": 1000,
                    "startColumnIndex": start_col, "endColumnIndex": end_col
                },
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        }]
        worksheet.spreadsheet.batch_update({"requests": requests})
    except: pass

def apply_formatting(worksheet, df):
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

def safe_batch_update(worksheet, updates):
    """Chunks updates to avoid API limits"""
    CHUNK_SIZE = 500
    for i in range(0, len(updates), CHUNK_SIZE):
        chunk = updates[i:i + CHUNK_SIZE]
        try:
            worksheet.batch_update(chunk)
            print(f"      ✅ Synced batch {i}-{i+len(chunk)}")
        except Exception as e:
            print(f"      ⚠️ Batch failed: {e}")

# ==========================================
#           MAIN PIPELINE
# ==========================================

async def run_bot():
    print("🚀 Robust-Bot Started...")
    
    try:
        # --- 1. EXTRACT ---
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            await context.route("**/*", lambda route: route.abort() 
                if route.request.resource_type in ["image", "media", "font"] 
                else route.continue_())
                
            page = await context.new_page()
            try:
                print("   🌐 Loading URL...")
                await page.goto(URL, timeout=60000)
                try: await page.wait_for_selector("table tbody tr", state="attached", timeout=20000)
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
        processed_data = [] # Store tuple (df, type_name)
        timestamp = get_ist_time()
        
        for df in dfs:
            if len(df) > 1 and len(df.columns) > 1 and 'No data' not in str(df.iloc[0,0]):
                # Clean Headers
                df.columns = [clean_header(c) for c in df.columns]
                
                # Clean Numbers
                for col in df.columns:
                    df[col] = df[col].apply(clean_currency)
                
                df.fillna("", inplace=True)
                df.insert(0, 'Scraped_At_IST', timestamp)
                
                # Year Logic
                try:
                    if any(x in str(df.iloc[0, 1]) for x in ['Jan', 'Dec', 'th', 'st']):
                        df['Year'] = df.iloc[:, 1].apply(calculate_year)
                except: pass
                
                # Identity Logic
                t_type = identify_table_type(df)
                processed_data.append({'df': df, 'type': t_type})

        # --- 3. LOAD ---
        if not processed_data: return print("   ❌ No valid tables processed.")
        
        if 'GCP_SERVICE_ACCOUNT' not in os.environ:
            print("   ❌ Secret 'GCP_SERVICE_ACCOUNT' is missing.")
            return
        
        creds = Credentials.from_service_account_info(
            json.loads(os.environ['GCP_SERVICE_ACCOUNT']), scopes=SCOPES
        )
        client = gspread.authorize(creds)
        try: sh = client.open(SHEET_NAME)
        except: sh = client.create(SHEET_NAME)

        for item in processed_data:
            df = item['df']
            tab_title = item['type'] # e.g. "History_Log" or "Stock_Scanner"
            
            print(f"   Syncing {tab_title}...")
            
            try: ws = sh.worksheet(tab_title)
            except: ws = sh.add_worksheet(tab_title, 1000, 25)
            
            # FRESH START Check
            all_val = ws.get_all_values()
            if len(all_val) <= 1:
                print("      ✨ Sheet empty. Dumping data...")
                ws.clear()
                set_with_dataframe(ws, df, include_column_header=True)
                force_number_format(ws, df)
                apply_formatting(ws, df)
                continue
                
            # HISTORY SYNC
            if tab_title == "History_Log":
                # Only check Date column (Index 1) for duplicates
                exist = {str(r[1]).strip(): idx+1 for idx, r in enumerate(all_val) if idx > 0}
                new_rows, updates = [], []
                
                for _, row in df.iterrows():
                    row_l = row.values.tolist()
                    key = str(row_l[1]).strip()
                    
                    if key in exist:
                        row_idx = exist[key]
                        s_row = [str(x) for x in all_val[row_idx-1][1:]]
                        d_row = [str(x) for x in row_l[1:]]
                        
                        if s_row != d_row:
                            updates.append({'range': f"A{row_idx}", 'values': [row_l]})
                    else:
                        new_rows.append(row_l)
                
                if new_rows: ws.insert_rows(new_rows, 2)
                if updates: safe_batch_update(ws, updates) # Safe Batching
                
                force_number_format(ws, df)
                apply_formatting(ws, df)

            # SCANNER SYNC
            elif tab_title == "Stock_Scanner":
                target_idx = 1
                # Safely get existing symbols
                existing_rows = ws.get_values(f"A2:B{len(df)+50}")
                exist_sym = [str(r[1]).strip() for r in existing_rows if len(r) > 1]
                
                new_sym = [str(x).strip() for x in df.iloc[:, target_idx].tolist()]
                
                if exist_sym[:len(new_sym)] != new_sym:
                    ws.insert_rows(df.values.tolist(), 2)
                    force_number_format(ws, df)
                    apply_formatting(ws, df)
            
            # GENERIC SYNC
            else:
                ws.clear()
                set_with_dataframe(ws, df, include_column_header=True)

        print("✅ Smart Sync Done.")

    except Exception as e:
        print(f"❌ CRITICAL ERROR: {str(e)}")
        traceback.print_exc()
        raise e 

if __name__ == "__main__":
    asyncio.run(run_bot())
