from playwright.async_api import async_playwright
import pandas as pd

URL = "https://chartink.com/dashboard/208896"

async def get_raw_data():
    """
    EXTRACT LAYER:
    Launches browser, scrapes the dashboard, and returns a list of raw DataFrames.
    """
    print("1. [EXTRACT] Launching Headless Browser...")
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
                    if 'No data' in str(df.iloc[0,0]):
                        continue
                    valid_dfs.append(df)
            
            print(f"   -> Extracted {len(valid_dfs)} raw tables.")
            return valid_dfs

        except Exception as e:
            print(f"   ❌ Extraction Error: {e}")
            return []
        finally:
            await browser.close()