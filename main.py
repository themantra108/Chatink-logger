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
SHEET_RAW = "Chartink_Daily_Data"       # History Storage
SHEET_CLEAN = "Chartink_Unique_EOD"     # Final Dashboard
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# ==========================================
#           CORE LOGIC FUNCTIONS
# ==========================================

def get_ist_time():
    IST = pytz.timezone('Asia/Kolkata')
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def clean_date_str(x):
    """Standardize date format to dd/mm/yyyy handling year rollover"""
    try:
        s = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', str(x).strip())
        current_year = datetime.now().year
        # Parse assuming current year
        dt = datetime.strptime(f"{s} {current_year}", "%d %b %Y")
        
        # If we are in Jan 2026 but scrape "Dec", it must be Dec 2025
        if dt > datetime.now() + timedelta(days=30): 
            dt = dt.replace(year=dt.year - 1)
            
        return dt.strftime("%d/%m/%Y")
    except: return str(x)

def calc_score(r):
    """Calculate Trend Score based on strict thresholds"""
    try:
        def f(v): 
            clean_v = str(v).replace(',', '').replace('%', '')
            return float(clean_v) if clean_v.replace('.', '').replace('-', '').isdigit() else 0.0
        
        s = 0
        # 4.5r Rules
        if '4.5r' in r: s += (1 if f(r['4.5r']) > 200 else 0) - (1 if f(r['4.5r']) < 50 else 0)
        if '4.5chg' in r: s += (1 if f(r['4.5chg']) > 20 else 0) - (1 if f(r['4.5chg']) < -20 else 0)
        
        # 20r Rules
        if '20r' in r: s += (1 if f(r['20r']) > 75 else 0) - (1 if f(r['20r']) < 50 else 0)
        if '20chg' in r: s += (1 if f(r['20chg']) > 20 else 0) - (1 if f(r['20chg']) < -20 else 0)
        
        # 50r Rules
        if '50r' in r: s += (1 if f(r['50r']) > 85 else 0) - (1 if f(r['50r']) < 60 else 0)
        if '50chg' in r: s += (1 if f(r['50chg']) > 20 else 0) - (1 if f(r['50chg']) < -20 else 0)
        
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
    print("🚀 Starting Playwright Master Bot...")
    
    # --- 1. SCRAPE (EXTRACT) ---
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        # Block images/fonts for speed
        await context.route("**/*", lambda route: route.abort() 
            if route.request.resource_type in ["image", "media", "font"] 
            else route.continue_())
            
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
        finally:
            await browser.close()

    if not dfs: return print("   ❌ No data found.")

    # Select the largest table (usually the data table)
    df = max(dfs, key=len).dropna(how='all').astype(str)
    
    # --- 2. CLEAN & TRANSFORM ---
    print("   ⚙️ Transforming Data...")
    
    # Clean Headers
    df.columns = [c.split(" Sort")[0].strip() for c in df.columns]
    
    # Remove 'No data' rows
    raw_date_col = df.columns[0]
    df = df[~df[raw_date_col].str.contains('No data', na=False, case=False)]
    
    # Date & numeric cleaning
    df['Full_Date'] = df[raw_date_col].apply(clean_date_str)
    
    # Calc Score
    df['Score'] = df.apply(calc_score, axis=1)
    df['Scraped_At'] = get_ist_time()
    
    # Organize Columns
    base_cols = [c for c in df.columns if c not in ['Full_Date', 'Score', 'Scraped_At']]
    final_df = df[base_cols + ['Full_Date', 'Score', 'Scraped_At']]

    # --- 3. GOOGLE SHEETS AUTH ---
    if 'GCP_SERVICE_ACCOUNT' not in os.environ: return print("   ❌ Secret Missing.")
    
    creds = Credentials.from_service_account_info(
        json.loads(os.environ['GCP_SERVICE_ACCOUNT']), scopes=SCOPES
    )
    client = gspread.authorize(creds)

    # --- 4. UPDATE RAW HISTORY (Strict Append) ---
    print(f"   💾 Updating History ({SHEET_RAW})...")
    try: sh_raw = client.open(SHEET_RAW)
    except: sh_raw = client.create(SHEET_RAW)
    ws_raw = sh_raw.sheet1
    
    try: old = get_as_dataframe(ws_raw).dropna(how='all')
    except: old = pd.DataFrame()

    to_add = pd.DataFrame()
    if old.empty:
        to_add = final_df
    else:
        # Compare signature (exclude Scraped_At timestamp)
        compare_cols = [c for c in final_df.columns if c != 'Scraped_At' and c in old.columns]
        if not compare_cols: 
            to_add = final_df
        else:
            sig_old = normalize_signature(old[compare_cols])
            sig_new = normalize_signature(final_df[compare_cols])
            to_add = final_df[~sig_new.isin(sig_old)]

    if not to_add.empty:
        # Append rows
        if ws_raw.acell('A1').value: 
            ws_raw.append_rows(to_add.values.tolist())
        else: 
            set_with_dataframe(ws_raw, to_add, include_column_header=True)
        print(f"      -> Added {len(to_add)} new rows.")
    else:
        print("      -> No new history to add.")

    # --- 5. REGENERATE DASHBOARD ---
    print(f"   🎨 Regenerating Dashboard ({SHEET_CLEAN})...")
    
    # Fetch FRESH full history
    df_history = get_as_dataframe(ws_raw).dropna(how='all')
    if df_history.empty: return
    
    # Sort Newest First & Dedup by Date
    df_history['_d'] = pd.to_datetime(df_history['Full_Date'], format='%d/%m/%Y', errors='coerce')
    df_history = df_history.sort_values(['_d'], ascending=False)
    
    # Create clean dashboard view (Hide helpers)
    df_dash = df_history.drop_duplicates(subset=['Full_Date'], keep='first')
    df_dash = df_dash.drop(columns=['_d', 'Scraped_At'], errors='ignore')
    
    # Helper for formatting logic
    df_dash['__Score__'] = df_dash.apply(calc_score, axis=1) 
    
    # Connect to Dashboard Sheet
    try: sh_dash = client.open(SHEET_CLEAN)
    except: sh_dash = client.create(SHEET_CLEAN)
    
    # Wipe and Rewrite
    ws_dash = sh_dash.sheet1
    ws_dash.clear()
    
    # Remove hidden score col from upload, but keep in memory for coloring
    upload_df = df_dash.drop(columns=['__Score__'])
    set_with_dataframe(ws_dash, upload_df, include_column_header=True)

    # --- 6. APPLY TRAFFIC LIGHTS ---
    print("      -> Applying Visual Rules...")
    
    idx = {c: upload_df.columns.get_loc(c) for c in upload_df.columns}
    reqs = []
    
    # 1. Number Format (0.00)
    reqs.append({
        "repeatCell": {
            "range": {"sheetId": ws_dash.id, "startRowIndex": 1, "startColumnIndex": 1, "endColumnIndex": 10},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}}},
            "fields": "userEnteredFormat.numberFormat"
        }
    })

    # 2. Colors
    green, red, yellow = {"red":0.85,"green":0.93,"blue":0.82}, {"red":0.96,"green":0.8,"blue":0.8}, {"red":1,"green":1,"blue":0.8}
    
    def add_rule(col, type, val, color):
        if col in idx:
            reqs.append({"addConditionalFormatRule": {"index":0, "rule": {
                "ranges": [{"sheetId": ws_dash.id, "startColumnIndex": idx[col], "endColumnIndex": idx[col]+1, "startRowIndex": 1}],
                "booleanRule": {"condition": {"type": type, "values": [{"userEnteredValue": str(val)}]}, "format": {"backgroundColor": color}}}}})

    if '4.5r' in idx:
        add_rule('4.5r', 'NUMBER_LESS', 50, red)
        add_rule('4.5r', 'NUMBER_GREATER', 200, green)
        add_rule('4.5r', 'NUMBER_GREATER', 400, yellow)

    for c in ['4.5chg','20chg','50chg','20r','50r']:
        if c in idx:
            if 'chg' in c: add_rule(c, 'NUMBER_LESS', -20, red); add_rule(c, 'NUMBER_GREATER', 20, green)
            if '20r' in c: add_rule(c, 'NUMBER_LESS', 50, red); add_rule(c, 'NUMBER_GREATER', 75, green)
            if '50r' in c: add_rule(c, 'NUMBER_LESS', 60, red); add_rule(c, 'NUMBER_GREATER', 85, green)

    # 3. Trend Logic (Sidebar Coloring)
    scores = df_dash['__Score__'].iloc[::-1] # Oldest to Newest
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

    # Apply Sidebar Colors (Newest First)
    rows = [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": c[0], "bold": True}, "backgroundColor": c[1]}}]} for c in formats[::-1]]
    
    if rows:
        reqs.append({"updateCells": {"range": {"sheetId": ws_dash.id, "startRowIndex": 1, "endRowIndex": 1+len(rows), 
                     "startColumnIndex": 0, "endColumnIndex": 1}, "rows": rows, "fields": "userEnteredFormat"}})

    if reqs:
        try: ws_dash.spreadsheet.batch_update({"requests": reqs})
        except Exception as e: print(f"Format Warn: {e}")

    print("✅ DONE! System Synced.")

if __name__ == "__main__":
    asyncio.run(run_bot())
