import os
import json
import gspread
from google.oauth2.service_account import Credentials

SHEET_NAME = "Chartink_Multi_Log"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def apply_formatting(worksheet, df):
    """
    Applies Specific Green/Red/Yellow Conditional Formatting.
    """
    print("      -> Applying Visual Rules...")
    
    # 1. Define Colors
    green  = {"red": 0.85, "green": 0.93, "blue": 0.82} 
    red    = {"red": 0.96, "green": 0.8,  "blue": 0.8}
    yellow = {"red": 1.0,  "green": 1.0,  "blue": 0.8}

    # 2. Map Headers
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
                            "endColumnIndex": idx[col] + 1, 
                            "startRowIndex": 1
                        }],
                        "booleanRule": {
                            "condition": {"type": condition_type, "values": [{"userEnteredValue": str(val)}]},
                            "format": {"backgroundColor": color}
                        }
                    },
                    "index": 0
                }
            })

    # --- SPECIFIC RULES ---

    # 1. Rule for 4.5r (High Momentum)
    if '4.5r' in idx: 
        add_rule('4.5r', 'NUMBER_LESS', 50, red)
        add_rule('4.5r', 'NUMBER_GREATER', 200, green)
        add_rule('4.5r', 'NUMBER_GREATER', 400, yellow)

    # 2. Other indicators
    target_cols = ['4.5chg', '20chg', '50chg', '20r', '50r']
    
    for c in target_cols:
        if c in idx:
            if 'chg' in c: 
                add_rule(c, 'NUMBER_LESS', -20, red)
                add_rule(c, 'NUMBER_GREATER', 20, green)
            
            if '20r' in c: 
                add_rule(c, 'NUMBER_LESS', 50, red)
                add_rule(c, 'NUMBER_GREATER', 75, green)
            
            if '50r' in c: 
                add_rule(c, 'NUMBER_LESS', 60, red)
                add_rule(c, 'NUMBER_GREATER', 85, green)

    if requests:
        try:
            worksheet.spreadsheet.batch_update({"requests": requests})
        except Exception as e:
            print(f"      ⚠️ Formatting warning: {e}")

def sync_data(clean_dfs):
    print("3. [LOAD] Syncing to Google Sheets...")
    
    if 'GCP_SERVICE_ACCOUNT' not in os.environ:
        return

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

        # Force Clean Headers
        ws.update('A1', [df.columns.values.tolist()])

        # Check Table Type
        first_data_col = df.columns[1].lower() if len(df.columns) > 1 else ""
        is_history_table = 'date' in first_data_col or 'day' in first_data_col

        if is_history_table:
            print(f"   -> [{tab_title}] Syncing History...")
            all_values = ws.get_all_values()
            existing_map = {} 
            if len(all_values) > 1:
                for idx, row in enumerate(all_values):
                    if idx == 0: continue
                    if len(row) > 1:
                        existing_map[str(row[1]).strip()] = idx + 1
            
            rows_to_insert = []
            
            for index, row in df.iterrows():
                row_list = row.values.tolist()
                date_key = str(row_list[1]).strip()
                
                if date_key in existing_map:
                    row_idx = existing_map[date_key]
                    current_row = all_values[row_idx - 1]
                    if current_row[1:] != [str(x) for x in row_list[1:]]:
                        ws.update(f"A{row_idx}", [row_list])
                else:
                    rows_to_insert.append(row_list)
            
            if rows_to_insert:
                ws.insert_rows(rows_to_insert, row=2)
            
            apply_formatting(ws, df)

        else:
            print(f"   -> [{tab_title}] Syncing Scanner...")
            target_col_idx = 1
            new_symbols = [str(s).strip() for s in df.iloc[:, target_col_idx].tolist()]
            
            existing_rows = ws.get_values(f"A2:Z{len(df) + 1}")
            existing_symbols = []
            if existing_rows:
                for row in existing_rows:
                    if len(row) > target_col_idx:
                        existing_symbols.append(str(row[target_col_idx]).strip())

            if new_symbols != existing_symbols:
                print("      -> New Data. Appending.")
                ws.insert_rows(df.values.tolist(), row=2)
                apply_formatting(ws, df)
            else:
                print("      -> No change.")
