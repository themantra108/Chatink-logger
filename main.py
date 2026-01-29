import asyncio, os, json, re
import pandas as pd
from datetime import datetime, timedelta
import pytz
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# --- CONFIGURATION ---
URL = "https://chartink.com/dashboard/208896" 
SHEET_NAME = "Chartink_Multi_Table_Log" # Changed name to avoid conflicts
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

# --- HELPERS ---
def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")

def clean_date(x):
    """Converts '29th Jan' -> Date Object for sorting"""
    try:
        dt = datetime.strptime(f"{re.sub(r'(\d+)(st|nd|rd|th)', r'\1', str(x).strip())} {datetime.now().year}", "%d %b %Y")
        return dt.replace(year=dt.year-1) if dt > datetime.now() + timedelta(days=30) else dt
    except: return pd.NaT

def calc_score(row):
    """Calculates Trend Score"""
    s = 0
    rules = {'4.5r':(50,200), '20r':(50,75), '50r':(60,85), '4.5chg':(-20,20), '20chg':(-20,20), '50chg':(-20,20)}
    for col, (low, high) in rules.items():
        if col in row:
            try: 
                v = float(str(row[col]).replace(',','').replace('%',''))
                s += (1 if v > high else 0) - (1 if v < low else 0)
            except: pass
    return s

# --- CORE ---
async def fetch_all_tables():
    print("🚀 Scraping all tables...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(URL, timeout=60000); await page.wait_for_load_state('networkidle')
            try: await page.wait_for_selector("table", state="attached", timeout=15000)
            except: pass
            
            # Read ALL tables
            dfs = pd.read_html(await page.content())
            if not dfs: return []
            
            clean_dfs = []
            for df in dfs:
                # Filter out tiny/empty layout tables
                if len(df) > 1 and len(df.columns) > 1:
                    df = df.dropna(how='all').astype(str)
                    # Clean Headers
                    df.columns = [str(c).split(" Sort")[0].split("_Sort")[0].strip().replace(" ", "") for c in df.columns]
                    # Filter 'No data' rows
                    df = df[~df[df.columns[0]].str.contains('No data', na=False, case=False)]
                    if not df.empty:
                        clean_dfs.append(df)
            
            print(f"✅ Found {len(clean_dfs)} valid tables.")
            return clean_dfs
            
        except Exception as e: print(f"❌ Error: {e}"); return []
        finally: await browser.close()

def main():
    # 1. Auth
    if 'GCP_SERVICE_ACCOUNT' not in os.environ: return print("❌ Missing Keys")
    creds = Credentials.from_service_account_info(json.loads(os.environ['GCP_SERVICE_ACCOUNT']), scopes=SCOPES)
    gc = gspread.authorize(creds)
    
    try: sh = gc.open(SHEET_NAME)
    except: 
        print("Creating new spreadsheet...")
        sh = gc.create(SHEET_NAME)
        sh.share(json.loads(os.environ['GCP_SERVICE_ACCOUNT'])['client_email'], perm_type='user', role='owner')

    # 2. Get Data (List of DataFrames)
    new_dfs = asyncio.run(fetch_all_tables())
    if not new_dfs: return

    # 3. Loop through EACH table and update corresponding Tab
    for i, new_df in enumerate(new_dfs):
        tab_name = f"Table_{i+1}"
        print(f"Processing {tab_name}...")
        
        try: ws = sh.worksheet(tab_name)
        except: ws = sh.add_worksheet(title=tab_name, rows=1000, cols=20)

        # Merge Logic (Per Table)
        try: old_df = get_as_dataframe(ws).dropna(how='all')
        except: old_df = pd.DataFrame()

        # Check table type (History vs Stocks)
        key_col = new_df.columns[0]
        is_history = 'date' in key_col.lower() or 'day' in key_col.lower()

        if is_history:
            # HISTORY TABLE LOGIC
            new_df['__dt__'] = new_df[key_col].apply(clean_date)
            if not old_df.empty and key_col in old_df.columns: 
                old_df['__dt__'] = old_df[key_col].apply(clean_date)
            
            full_df = pd.concat([new_df, old_df]) if not old_df.empty else new_df
            full_df = full_df.sort_values('__dt__', ascending=False)
        else:
            # STOCK SCANNER LOGIC
            full_df = new_df
            full_df.insert(0, 'Scraped_At_IST', get_ist_time())

        # Deduplicate
        cols_to_check = [c for c in full_df.columns if c not in ['__dt__', 'Scraped_At_IST']]
        full_df = full_df.drop_duplicates(subset=cols_to_check, keep='first')

        # Score & Upload
        full_df['__score__'] = full_df.apply(calc_score, axis=1)
        final_df = full_df.drop(columns=['__dt__', '__score__'], errors='ignore')
        
        ws.clear()
        set_with_dataframe(ws, final_df, include_column_header=True)
        
        # 4. Apply Formatting (Per Tab)
        print(f"   -> Formatting {tab_name}...")
        reqs = [{"repeatCell": {"range": {"sheetId": ws.id, "startRowIndex": 1, "startColumnIndex": 1}, "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}}}, "fields": "userEnteredFormat.numberFormat"}}]
        
        c_red, c_green = {"red":0.96,"green":0.8,"blue":0.8}, {"red":0.85,"green":0.93,"blue":0.82}
        idx = {c: idx for idx, c in enumerate(final_df.columns)}
        
        def rule(col, op, val, color):
            if col in idx: reqs.append({"addConditionalFormatRule": {"index": 0, "rule": {"ranges": [{"sheetId": ws.id, "startColumnIndex": idx[col], "endColumnIndex": idx[col]+1, "startRowIndex": 1}], "booleanRule": {"condition": {"type": op, "values": [{"userEnteredValue": str(val)}]}, "format": {"backgroundColor": color}}}}})

        for col in ['4.5r', '20r', '50r']: rule(col, 'NUMBER_GREATER', 200, c_green); rule(col, 'NUMBER_LESS', 50, c_red)
        for col in ['4.5chg', '20chg', '50chg']: rule(col, 'NUMBER_GREATER', 20, c_green); rule(col, 'NUMBER_LESS', -20, c_red)
        
        # Trend Color
        trend = full_df['__score__'].iloc[0] if '__score__' in full_df else 0
        t_color = {"red":0,"green":0.8,"blue":0} if trend >= 3 else ({"red":1,"green":0,"blue":0} if trend <= -3 else {"red":0,"green":0,"blue":0})
        reqs.append({"updateCells": {"range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 1}, "rows": [{"values": [{"userEnteredValue": {"stringValue": final_df.columns[0]}, "userEnteredFormat": {"backgroundColor": t_color, "textFormat": {"foregroundColor": {"red":1,"green":1,"blue":1}, "bold": True}, "horizontalAlignment": "CENTER"}}]}], "fields": "userEnteredValue,userEnteredFormat"}})

        try: ws.spreadsheet.batch_update({"requests": reqs})
        except Exception as e: print(f"⚠️ Format warning: {e}")

    print("✅ All tables processed.")

if __name__ == "__main__": main()
