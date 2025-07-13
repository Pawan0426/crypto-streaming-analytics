@echo off
echo ==== Starting Hourly Crypto ETL ====

REM Set Python path
set PYTHON_PATH="C:\Program Files\Python312\python.exe"

REM Set working directory
cd /d "C:\Users\paawa\OneDrive\Desktop\crypto-streaming-analytics"
set PYTHONPATH=%cd%

echo 🟢 Fetching raw data...
%PYTHON_PATH% ingestion\fetch_coinbase_data.py

echo 🟢 Transforming to silver...
%PYTHON_PATH% transformation\transform_trades_silver.py

echo 🟢 Aggregating to gold...
%PYTHON_PATH% transformation\aggregate_trade_gold.py

echo ✅ Hourly ETL Completed.
