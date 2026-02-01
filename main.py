import asyncio
import os
import json
import re
import traceback
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# ==========================================
#              CONFIGURATION
# ==========================================
URL = "https://chartink.com/dashboard/419640"
SHEET_RAW = "Chartink Smart Log"
SHEET_DASH = "Chartink Dashboard"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ==========================================
#           HELPER FUNCTIONS
# ==========================================
def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")

def clean_date_str(x):
    try:
        s = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', str(x).strip())
        current_year = datetime.now().year
        dt = datetime.strptime(f"{s} {current_year}", "%d %b %Y")
        if dt > datetime.now() + timedelta(days=30): 
            dt = dt.replace(year=dt.year - 1)
        return dt.strftime("%d/%m/%Y")
    except: return str(x)

def calc_score(r):
    try:
        def f(v): 
            clean_v = str(v).replace(',', '').replace('%', '')
            return float(clean_v) if clean_v.replace('.', '').replace('-', '').isdigit() else 0.0
        s = 0
        if '4.5r' in r: s += (1 if f(r['4.5r']) > 200 else 0) - (1 if f(r['4.5r']) < 50 else 0)
        if '4.5chg' in r: s += (1 if f(r['4.5chg']) > 20 else 0) - (1 if f(r['4.5chg']) < -20 else 0)
        if '20r' in r: s += (1 if f(r['20r']) > 75 else 0) - (1 if f(r['20r']) < 50 else 0)
        if '50r' in r: s += (1 if f(r['50r']) > 85 else 0) - (1 if f(r['50r']) < 60 else 0)
        return s
    except: return 0

def normalize_signature(df):
    return df.astype(str).apply(
        lambda x: x.str.strip().str.lower().str.replace(r'\.0$', '', regex=True)
    ).agg(''.join, axis=1)

# ==========================================
#           MAIN PIPELINE
# ==========================================
async def run_bot():
    print("🚀 Starting Debug-Ready Bot...")
    
    # --- 1. SCRAPE ---
    print("   🌐 Launching Browser...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(URL, timeout=60000)
            print("      Page loaded. Waiting for table...")
            
            # Wait for table OR take screenshot if fails
            try:
                await page.wait_for_selector("table tbody tr", state="attached", timeout=15000)
            except:
                print("      ⚠️ Timeout waiting for table! Saving screenshot...")
                await page.screenshot(path="error_screenshot.png")
            
            content = await page.content()
            dfs = pd.read_html(content)
            print(f"      Found {len(dfs)} tables.")
        except Exception as e:
            print(f"   ❌ Scrape Error: {e}")
            dfs = []
        finally:
            await browser.close()

    if not dfs: return print("   ❌ No data found in HTML.")

    # Prepare Data
    df = max(dfs, key=len).dropna(how='all').astype(str)
    df.columns = [c.split(" Sort")[0].strip() for c in df.columns]
    
    raw_date_col = df.columns[0]
    df = df[~df[raw_date_col].str.contains('No data', na=False, case=False)]
    
    df['Full_Date'] = df[raw_date_col].apply(clean_date_str)
    df['Score'] = df.apply(calc_score, axis=1)
    df['Scraped_At'] = get_ist_time()
    
    base_cols = [c for c in df.columns if c not in ['Full_Date', 'Score', 'Scraped_At']]
    final_df = df[base_cols + ['Full_Date', 'Score', 'Scraped_At']]
    
    # CRITICAL FIX: Ensure all data is JSON compliant
    final_df = final_df.fillna("")

    # --- 2. AUTH ---
    print("   🔑 Authenticating Google Sheets...")
    creds = Credentials.from_service_account_info(json.loads(os.environ['GCP_SERVICE_ACCOUNT']), scopes=SCOPES)
    client = gspread.authorize(creds)

    # --- 3. UPDATE LOG (Append) ---
    print(f"   💾 Updating {SHEET_RAW}...")
    try: sh_raw = client.open(SHEET_RAW)
    except: sh_raw = client.create(SHEET_RAW)
    ws_raw = sh_raw.sheet1
    
    try: old_log = get_as_dataframe(ws_raw).dropna(how='all')
    except: old_log = pd.DataFrame()

    if old_log.empty:
        new_rows = final_df
    else:
        compare_cols = [c for c in final_df.columns if c != 'Scraped_At' and c in old_log.columns]
        if not compare_cols: new_rows = final_df
        else:
            sig_old = normalize_signature(old_log[compare_cols])
            sig_new = normalize_signature(final_df[compare_cols])
            new_rows = final_df[~sig_new.isin(sig_old)]

    if not new_rows.empty:
        # Convert to strict list of lists for Gspread
        data_to_append = new_rows.values.tolist()
        if ws_raw.acell('A1').value: ws_raw.append_rows(data_to_append)
        else: set_with_dataframe(ws_raw, new_rows, include_column_header=True)
        print(f"      -> Appended {len(new_rows)} rows.")
    else:
        print("      -> No new history.")

    # --- 4. UPDATE DASHBOARD (Insert Top) ---
    print(f"   🎨 Updating {SHEET_DASH}...")
    try: sh_dash = client.open(SHEET_DASH)
    except: sh_dash = client.create(SHEET_DASH)
    ws_dash = sh_dash.sheet1
    
    # Get current top row to see if we need headers
    has_headers = bool(ws_dash.acell('A1').value)
    
    try: old_dash = get_as_dataframe(ws_dash).dropna(how='all')
    except: old_dash = pd.DataFrame()

    to_insert = pd.DataFrame()
    if old_dash.empty:
        to_insert = final_df
        print("      -> Dashboard empty. Filling all...")
        set_with_dataframe(ws_dash, to_insert, include_column_header=True)
    else:
        # Compare to find missing rows
        compare_cols_dash = [c for c in final_df.columns if c != 'Scraped_At' and c in old_dash.columns]
        if compare_cols_dash:
            sig_dash_old = normalize_signature(old_dash[compare_cols_dash])
            sig_dash_new = normalize_signature(final_df[compare_cols_dash])
            to_insert = final_df[~sig_dash_new.isin(sig_dash_old)]
        else:
            to_insert = final_df

        if not to_insert.empty:
            # Prepare data: Drop Score for display, Keep Scraped_At
            display_data = to_insert.drop(columns=['Score'], errors='ignore')
            
            print(f"      -> Inserting {len(display_data)} rows at Top...")
            # CRITICAL: Convert to Python native types to avoid JSON errors
            clean_values = display_data.fillna("").values.tolist()
            
            # Insert at Row 2 (pushing everything down)
            ws_dash.insert_rows(clean_values, 2)

    # --- 5. VISUALS ---
    print("      -> Refreshing Styles...")
    try:
        cols_ref = final_df.drop(columns=['Score'], errors='ignore')
        idx = {c: cols_ref.columns.get_loc(c) for c in cols_ref.columns}
        reqs = []
        
        # 1. Number Format
        reqs.append({"repeatCell": {"range": {"sheetId": ws_dash.id, "startRowIndex": 1, "startColumnIndex": 1, "endColumnIndex": 10},
                     "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}}}, "fields": "userEnteredFormat.numberFormat"}})

        # 2. Traffic Lights
        green, red, yellow = {"red":0.85,"green":0.93,"blue":0.82}, {"red":0.96,"green":0.8,"blue":0.8}, {"red":1,"green":1,"blue":0.8}
        def add_rule(col, type, val, color):
            if col in idx:
                reqs.append({"addConditionalFormatRule": {"index":0, "rule": {
                    "ranges": [{"sheetId": ws_dash.id, "startColumnIndex": idx[col], "endColumnIndex": idx[col]+1, "startRowIndex": 1}],
                    "booleanRule": {"condition": {"type": type, "values": [{"userEnteredValue": str(val)}]}, "format": {"backgroundColor": color}}}}})

        if '4.5r' in idx: add_rule('4.5r', 'NUMBER_LESS', 50, red); add_rule('4.5r', 'NUMBER_GREATER', 200, green); add_rule('4.5r', 'NUMBER_GREATER', 400, yellow)
        for c in ['4.5chg','20chg','50chg','20r','50r']:
            if c in idx:
                if 'chg' in c: add_rule(c, 'NUMBER_LESS', -20, red); add_rule(c, 'NUMBER_GREATER', 20, green)
                if '20r' in c: add_rule(c, 'NUMBER_LESS', 50, red); add_rule(c, 'NUMBER_GREATER', 75, green)
                if '50r' in c: add_rule(c, 'NUMBER_LESS', 60, red); add_rule(c, 'NUMBER_GREATER', 85, green)

        ws_dash.spreadsheet.batch_update({"requests": reqs})
    except Exception as e:
        print(f"      ⚠️ Formatting Warning: {e}")

    print("✅ DONE! Sync Complete.")

if __name__ == "__main__":
    asyncio.run(run_bot())
