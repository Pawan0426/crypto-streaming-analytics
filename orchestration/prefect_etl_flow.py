# orchestration/prefect_etl_flow.py

import os
import sys
from prefect import flow, task

# Allow imports from project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import the individual stage functions
from ingestion.fetch_coinbase_data import run_ingestion
from transformation.transform_trades_silver import run_trade_transformation
from transformation.aggregate_trade_gold import run_gold_trade_aggregation


@task
def ingest_data():
    print("📥 Starting ingestion...")
    run_ingestion()
    print("✅ Ingestion complete.")


@task
def silver_transform():
    print("🔄 Starting silver transformation...")
    run_trade_transformation()
    print("✅ Silver transformation complete.")


@task
def gold_aggregation():
    print("📊 Starting gold aggregation...")
    run_gold_trade_aggregation()
    print("✅ Gold aggregation complete.")


@flow(name="Crypto ETL Flow")
def crypto_etl():
    print("🚀 Launching ETL pipeline...")
    ingest_data()
    silver_transform()
    gold_aggregation()
    print("✅ ETL pipeline finished successfully.")


if __name__ == "__main__":
    crypto_etl()
