import asyncio
import os
import json
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
URL = "https://chartink.com/dashboard/208896"
SHEET_NAME = "Chartink Smart Log"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

async def scrape_chartink():
    print("Launching browser...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            await page.goto(URL, timeout=60000)
            await page.wait_for_load_state('networkidle')
            
            try:
                await page.wait_for_selector("table", state="attached", timeout=15000)
            except:
                pass

            content = await page.content()
            dfs = pd.read_html(content)
            
            valid_dfs = []
            for df in dfs:
                if len(df) > 1 and len(df.columns) > 1:
                    # --- FIX: AGGRESSIVE HEADER CLEANING ---
                    # Chartink puts sorting text in headers. We split by '_Sort_' and keep the first part.
                    new_columns = []
                    for c in df.columns:
                        clean_c = str(c).split("_Sort_")[0].strip() # Removes "Sort_table_by..."
                        clean_c = clean_c.replace(" ", "_").replace(".", "")
                        new_columns.append(clean_c)
                    
                    df.columns = new_columns
                    df.fillna("", inplace=True)
                    df = df.astype(str)
                    valid_dfs.append(df)
            
            return valid_dfs

        except Exception as e:
            print(f"Error during scraping: {e}")
            return []
        finally:
            await browser.close()

def get_comparison_column_index(df):
    """
    Finds the index of the Symbol column to ensure we only compare Symbols, not prices.
    """
    cols = [c.lower() for c in df.columns]
    
    # Priority 1: Look for 'symbol' or 'stock'
    for i, col in enumerate(cols):
        if 'symbol' in col or 'stock' in col:
            return i
            
    # Priority 2: Fallback to column 0 (usually the stock name in Chartink)
    return 0

def update_google_sheet(data_frames):
    if not data_frames:
        return

    if 'GCP_SERVICE_ACCOUNT' not in os.environ:
        print("Error: GCP_SERVICE_ACCOUNT secret missing.")
        return

    try:
        json_creds = json.loads(os.
