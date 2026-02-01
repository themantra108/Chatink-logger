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
SHEET_RAW = "Chartink Smart Log"      # Database (Appends to Bottom)
SHEET_DASH = "Chartink Dashboard"     # Dashboard (Inserts at Top)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ==========================================
#           CORE LOGIC FUNCTIONS
# ==========================================

def get_ist_time():
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

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
    """Create unique signature for deduping"""
    return df.astype(str).apply(
        lambda x: x.str.strip().str.lower().str.replace(r'\.0$', '', regex=True)
    ).agg(''.join, axis=1)

# ==========================================
#           MAIN PIPELINE
# ==========================================

async def run_bot():
    print("🚀 Starting Smart-Insert Bot...")
    
    # --- 1. SCRAPE ---
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await context.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media", "font"] else route.continue_())
        page = await context.new_page()
        try:
            print("   🌐 Loading Dashboard...")
            await page.goto(URL, timeout=60000)
            try: await page.wait_for_selector("table tbody tr", state="attached", timeout=20000)
            except: pass
            content = await page.content()
            dfs = pd.read_html(content)
        except Exception as e:
            print(f"   ❌ Scrape Error: {e}")
            dfs = []
        finally: await browser.close()

    if not dfs: return print("   ❌ No data found.")

    # Process Data
    df = max(dfs, key=len).dropna(how='all').astype(str)
    df.columns = [c.split(" Sort")[0].strip() for c in df.columns]
    raw_date_col = df.columns[0]
    df = df[~df[raw_date_col].str.contains('No data', na=False, case=False)]
    
    # Clean & Score
    df['Full_Date'] = df[raw_date_col].apply(clean_date_str)
    df['Score'] = df.apply(calc_score, axis=1)
    df['Scraped_At'] = get_ist_time()
    
    base_cols = [c for c in df.columns if c not in ['Full_Date', 'Score', 'Scraped_At']]
    final_df = df[base_cols + ['Full_Date', 'Score', 'Scraped_At']]

    # --- 2. AUTH ---
    if 'GCP_SERVICE_ACCOUNT' not in os.environ: return print("   ❌ Secret Missing.")
    creds = Credentials.from_service_account_info(json.loads(os.environ['GCP_SERVICE_ACCOUNT']), scopes=SCOPES)
    client = gspread.authorize(creds)

    # --- 3. UPDATE LOG (Append Bottom) ---
    print(f"   💾 Checking Log ({SHEET_RAW})...")
    try: sh_raw = client.open(SHEET_RAW)
    except: sh_raw = client.create(SHEET_RAW)
    ws_raw = sh_raw.sheet1
    
    try: old_log = get_as_dataframe(ws_raw).dropna(how='all')
    except: old_log = pd.DataFrame()

    new_log_rows = pd.DataFrame()
    if old_log.empty:
        new_log_rows = final_df
    else:
        # Dedup against Log
        compare_cols = [c for c in final_df.columns if c != 'Scraped_At' and c in old_log.columns]
        if not compare_cols: new_log_rows = final_df
        else:
            sig_old = normalize_signature(old_log[compare_cols])
            sig_new = normalize_signature(final_df[compare_cols])
            new_log_rows = final_df[~sig_new.isin(sig_old)]

    if not new_log_rows.empty:
        if ws_raw.acell('A1').value: ws_raw.append_rows(new_log_rows.values.tolist())
        else: set_with_dataframe(ws_raw, new_log_rows, include_column_header=True)
        print(f"      -> Appended {len(new_log_rows)} rows to Log.")

    # --- 4. UPDATE DASHBOARD (Insert Top) ---
    print(f"   🎨 Checking Dashboard ({SHEET_DASH})...")
    try: sh_dash = client.open(SHEET_DASH)
    except: sh_dash = client.create(SHEET_DASH)
    ws_dash = sh_dash.sheet1
    
    # 4a. Get Existing Dashboard Data
    try: old_dash = get_as_dataframe(ws_dash).dropna(how='all')
    except: old_dash = pd.DataFrame()
    
    # 4b. Find Rows NOT in Dashboard (using Log Logic logic)
    # We use the FRESH scraped data 'final_df' to compare against Dashboard
    rows_to_insert = pd.DataFrame()
    
    if old_dash.empty:
        # If empty, just write everything
        rows_to_insert = final_df
        print("      -> Dashboard empty. Writing all data...")
        set_with_dataframe(ws_dash, rows_to_insert, include_column_header=True)
    else:
        # If exists, find ONLY what is missing
        # We assume Dashboard might be sorted differently, so we use Signature Check
        compare_cols_dash = [c for c in final_df.columns if c != 'Scraped_At' and c in old_dash.columns]
        if compare_cols_dash:
            sig_dash_old = normalize_signature(old_dash[compare_cols_dash])
            sig_dash_new = normalize_signature(final_df[compare_cols_dash])
            rows_to_insert = final_df[~sig_dash_new.isin(sig_dash_old)]
        else:
            rows_to_insert = final_df

        if not rows_to_insert.empty:
            # 4c. INSERT AT TOP (Row 2)
            # Remove helper columns before display
            display_data = rows_to_insert.drop(columns=['Score'], errors='ignore') # Keep Scraped_At? Yes.
            
            print(f"      -> Inserting {len(display_data)} new rows at TOP...")
            ws_dash.insert_rows(display_data.values.tolist(), 2)

    # --- 5. RE-APPLY VISUALS (NON-DESTRUCTIVE) ---
    # We simply re-broadcast the rules. Google Sheets is smart enough to merge them.
    print("      -> Refreshing Styles...")
    
    # Use the column structure of the final dataframe to map indices
    cols_ref = final_df.drop(columns=['Score'], errors='ignore')
    idx = {c: cols_ref.columns.get_loc(c) for c in cols_ref.columns}
    reqs = []
    
    # Number Format
    reqs.append({"repeatCell": {"range": {"sheetId": ws_dash.id, "startRowIndex": 1, "startColumnIndex": 1, "endColumnIndex": 10},
                 "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}}}, "fields": "userEnteredFormat.numberFormat"}})

    # Traffic Lights
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

    # Sidebar Trend Logic (Column A)
    # Since we aren't wiping, we calculate trend for the NEW rows we inserted
    # But to be safe and consistent, let's just recolor the top 50 rows?
    # Or rely on the 'Score' we calculated.
    # Actually, simpler: Recalculate Score for the TOP N rows currently in the sheet to keep Sidebar updated.
    
    # For now, let's just apply to the inserted rows to be fast.
    if not rows_to_insert.empty:
        scores = rows_to_insert['Score'] # This list matches the inserted rows order (Newest)
        trend, t_g, t_r, t_b = 0, {"red":0,"green":0.8,"blue":0}, {"red":1,"green":0,"blue":0}, {"red":0,"green":0,"blue":0}
        bg_b, bg_w = {"red":0,"green":0,"blue":0}, {"red":1,"green":1,"blue":1}
        formats = []
        
        for s in scores:
            v = float(s) if pd.notnull(s) else 0
            sig = 1 if v >= 3 else (-1 if v <= -3 else 0)
            curr_txt, curr_bg = (t_g if trend==1 else (t_r if trend==-1 else t_b)), bg_w
            if sig == 1: curr_txt, curr_bg, trend = t_g, (bg_b if trend==-1 else bg_w), 1
            elif sig == -1: curr_txt, curr_bg, trend = t_r, (bg_b if trend==1 else bg_w), -1
            formats.append((curr_txt, curr_bg))

        rows = [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": c[0], "bold": True}, "backgroundColor": c[1]}}]} for c in formats]
        reqs.append({"updateCells": {"range": {"sheetId": ws_dash.id, "startRowIndex": 1, "endRowIndex": 1+len(rows), 
                     "startColumnIndex": 0, "endColumnIndex": 1}, "rows": rows, "fields": "userEnteredFormat"}})

    try: ws_dash.spreadsheet.batch_update({"requests": reqs})
    except: pass

    print("✅ DONE! Sync Complete.")

if __name__ == "__main__":
    asyncio.run(run_bot())
