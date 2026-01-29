import os
import json
import gspread
from google.oauth2.service_account import Credentials

SHEET_NAME = "Chartink_Multi_Log"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def apply_formatting(worksheet, df):
    """Applies Green/Red Conditional Formatting"""
    # Define Colors
    green = {"red": 0.85, "green": 0.93, "blue": 0.82} 
    red   = {"red": 0.96, "green": 0.8,  "blue": 0.8}
    
    headers = df.columns.tolist() # 'Scraped_At_IST' is already in df from transform
    col_map = {name: idx for idx, name in enumerate(headers)}
    
    requests = []
    def add_rule(col_name, condition_type, value, color):
        if col_name in col_map:
            col_idx = col_map[col_name]
            requests.append({
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": worksheet.id, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1, "startRowIndex": 1}],
                        "booleanRule": {
                            "condition": {"type": condition_type, "values": [{"userEnteredValue": str(value)}]},
                            "format": {"backgroundColor": color}
                        }
                    },
                    "index": 0
                }
            })

    # Apply Logic
    for c in ['4.5r', '20r', '50r', 'Rsi', 'Adx']:
        add_rule(c, 'NUMBER_GREATER', 60, green)
        add_rule(c, 'NUMBER_LESS', 50, red)
    for c in ['4.5chg', '20chg', '50chg', '%ch']:
        add_rule(c, 'NUMBER_GREATER', 0, green)
        add_rule(c, 'NUMBER_LESS', 0, red)

    if requests:
        try:
            worksheet.spreadsheet.batch_update({"requests": requests})
        except:
            pass

def sync_data(clean_dfs):
    """
    LOAD LAYER:
    Pushes data to Google Sheets using Smart Sync logic.
    """
    print("3. [LOAD] Syncing to Google Sheets...")
    
    if 'GCP_SERVICE_ACCOUNT' not in os.environ:
        print("   ❌ Error: Missing GCP_SERVICE_ACCOUNT secret.")
        return

    # Auth
    try:
        creds = Credentials.from_service_account_info(json.loads(os.environ['GCP_SERVICE_ACCOUNT']), scopes=SCOPES)
        client = gspread.authorize(creds)
        try:
            sh = client.open(SHEET_NAME)
        except:
            sh = client.create(SHEET_NAME)
            sh.share(json.loads(os.environ['GCP_SERVICE_ACCOUNT'])['client_email'], perm_type='user', role='owner')
    except Exception as e:
        print(f"   ❌ Auth Error: {e}")
        return

    for i, df in enumerate(clean_dfs):
        tab_title = f"Table_{i+1}"
        try:
            ws = sh.worksheet(tab_title)
        except:
            ws = sh.add_worksheet(title=tab_title, rows=1000, cols=20)

        # 1. Force Headers
        clean_headers = df.columns.values.tolist()
        ws.update('A1', [clean_headers])

        # 2. Identify Table Type (History vs Scanner)
        # Note: df.columns[0] is IST time. df.columns[1] is the data key (Date or Symbol)
        first_data_col = df.columns[1].lower()
        is_history_table = 'date' in first_data_col or 'day' in first_data_col

        if is_history_table:
            # --- HISTORY LOGIC ---
            print(f"   -> [{tab_title}] Syncing History...")
            all_values = ws.get_all_values()
            existing_map = {} # { "Date": RowIndex }
            
            if len(all_values) > 1:
                for idx, row in enumerate(all_values):
                    if idx == 0: continue
                    if len(row) > 1:
                        existing_map[str(row[1]).strip()] = idx + 1 # row[1] is Date
            
            rows_to_insert = []
            for index, row in df.iterrows():
                row_list = row.values.tolist()
                date_key = str(row_list[1]).strip() # Date is at index 1
                
                if date_key in existing_map:
                    row_idx = existing_map[date_key]
                    current_row = all_values[row_idx - 1]
                    # Update if values changed
                    if current_row[1:] != [str(x) for x in row_list[1:]]: # Compare data excluding timestamp
                        ws.update(f"A{row_idx}", [row_list])
                else:
                    rows_to_insert.append(row_list)
            
            if rows_to_insert:
                ws.insert_rows(rows_to_insert, row=2)

        else:
            # --- SCANNER LOGIC ---
            print(f"   -> [{tab_title}] Syncing Scanner...")
            target_col_idx = 1 # Symbol is at index 1 (0 is timestamp)
            new_symbols = [str(s).strip() for s in df.iloc[:, target_col_idx].tolist()]
            
            existing_rows = ws.get_values(f"A2:Z{len(df) + 1}")
            existing_symbols = []
            if existing_rows:
                for row in existing_rows:
                    if len(row) > target_col_idx:
                        existing_symbols.append(str(row[target_col_idx]).strip())

            if new_symbols != existing_symbols:
                print("      -> Change detected. Appending.")
                ws.insert_rows(df.values.tolist(), row=2)
                apply_formatting(ws, df)
            else:
                print("      -> No change.")
