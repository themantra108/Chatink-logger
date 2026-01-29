import asyncio
import extract
import transform
import load

# --- ORCHESTRATOR ---
if __name__ == "__main__":
    print("🚀 ETL PIPELINE STARTED")
    
    # 1. Run Extract
    raw_data = asyncio.run(extract.get_raw_data())
    
    # 2. Run Transform
    clean_data = transform.process_data(raw_data)
    
    # 3. Run Load
    load.sync_data(clean_data)
    
    print("✅ ETL PIPELINE FINISHED")
