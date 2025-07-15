# orchestration/prefect_etl_supabase.py

import os
import sys
from prefect import flow, task

# Allow imports from project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ✅ Import the correct functions from your updated pipeline scripts
from supabase_pipeline.fetch_trade_data_supabase import run as run_ingestion
from supabase_pipeline.transform_trades_silver_supabase import run_silver_transformation
from supabase_pipeline.aggregate_trade_gold_postgre import run_gold_trade_aggregation_postgre

@task
def ingest_data():
    print("📥 Starting ingestion...")
    run_ingestion()
    print("✅ Ingestion complete.")

@task
def silver_transform():
    print("🔄 Starting silver transformation...")
    run_silver_transformation()
    print("✅ Silver transformation complete.")

@task
def gold_aggregation():
    print("📊 Starting gold aggregation...")
    run_gold_trade_aggregation_postgre()
    print("✅ Gold aggregation complete.")

@flow(name="Crypto ETL Supabase")
def crypto_etl_supabase():
    print("🚀 Launching Supabase ETL pipeline...")
    ingest_data()
    silver_transform()
    gold_aggregation()
    print("✅ ETL pipeline finished successfully.")

if __name__ == "__main__":
    crypto_etl_supabase()
