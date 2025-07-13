@echo off
echo ==== Starting Daily Crypto ETL ====

REM Set Python path
set PYTHON_PATH="C:\Program Files\Python312\python.exe"

REM Set working directory to your project root
cd /d "C:\Users\paawa\OneDrive\Desktop\crypto-streaming-analytics"

REM Set PYTHONPATH so Python finds your internal packages like transformation
set PYTHONPATH=%cd%

REM 1. Fetch raw data
echo 🟢 Fetching raw data...
%PYTHON_PATH% ingestion\fetch_coinbase_data.py

REM 2. Transform to silver
echo 🟢 Transforming trade data to silver...
%PYTHON_PATH% transformation\transform_trades_silver.py

echo 🟢 Transforming ticker data to silver...
%PYTHON_PATH% transformation\transform_ticker_data.py

echo 🟢 Transforming order book data to silver...
%PYTHON_PATH% transformation\transform_order_book.py

REM 3. Aggregate to gold
echo 🟢 Aggregating to gold...
%PYTHON_PATH% transformation\aggregate_to_gold.py

echo 🟢 Aggregating to gold hourly...
%PYTHON_PATH% transformation\aggregate_to_gold_hourly.py

echo 🟢 Aggregating trade data to gold...
%PYTHON_PATH% transformation\aggregate_trade_gold.py

REM 4. Validation
echo 🟢 Validating...
%PYTHON_PATH% transformation\validation.py

echo 🟢 Running validation & logging...
%PYTHON_PATH% logs\validate_and_log.py

echo ✅ ETL Completed.
pause
