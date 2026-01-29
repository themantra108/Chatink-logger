import asyncio
import extract
import transform
import load

if __name__ == "__main__":
    print("🚀 ETL PIPELINE STARTED")
    raw_data = asyncio.run(extract.get_raw_data())
    clean_data = transform.process_data(raw_data)
    load.sync_data(clean_data)
    print("✅ ETL PIPELINE FINISHED")
