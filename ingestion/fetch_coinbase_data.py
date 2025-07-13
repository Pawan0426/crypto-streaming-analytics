# ingestion/fetch_coinbase_data.py
import os
import requests
import time
import pandas as pd
from datetime import datetime, date, timezone
from concurrent.futures import ThreadPoolExecutor
import json

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

symbols = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]

def fetch_ticker(symbol):
    url = f"https://api.exchange.coinbase.com/products/{symbol}/ticker"
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "symbol": symbol,
                "price": float(data.get("price", 0)),
                "bid": float(data.get("bid", 0)),
                "ask": float(data.get("ask", 0)),
                "volume": float(data.get("volume", 0)),
                "time": datetime.fromisoformat(data.get("time").replace("Z", "+00:00")),
                "fetched_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        print(f"Error fetching ticker for {symbol}: {e}")
    return None

def fetch_order_book(symbol):
    url = f"https://api.exchange.coinbase.com/products/{symbol}/book?level=2"
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            data["fetched_at"] = datetime.now(timezone.utc).isoformat()
            return symbol, data
    except Exception as e:
        print(f"Error fetching order book for {symbol}: {e}")
    return symbol, None

def fetch_trades(symbol):
    url = f"https://api.exchange.coinbase.com/products/{symbol}/trades"
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            fetched_at = datetime.now(timezone.utc).isoformat()
            for trade in data:
                trade["fetched_at"] = fetched_at
                trade["symbol"] = symbol
            return symbol, data
    except Exception as e:
        print(f"Error fetching trades for {symbol}: {e}")
    return symbol, None

def save_order_book(symbol, data):
    if data:
        today = date.today().isoformat()
        output_dir = os.path.join(DATA_DIR, "raw", "order_books", symbol, today)
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{output_dir}/order_book_{int(time.time())}.json"
        with open(output_file, "w") as f:
            json.dump(data, f)
        print(f"✅ Saved order book for {symbol} to {output_file}")

def save_trades(symbol, data):
    if data:
        today = date.today().isoformat()
        output_dir = os.path.join(DATA_DIR, "raw", "trades", symbol, today)
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{output_dir}/trades_{int(time.time())}.json"
        with open(output_file, "w") as f:
            json.dump(data, f)
        print(f"✅ Saved trades for {symbol} to {output_file}")

def run_ingestion():
    with ThreadPoolExecutor() as executor:
        ticker_records = list(filter(None, executor.map(fetch_ticker, symbols)))
        order_books = list(executor.map(fetch_order_book, symbols))
        trades = list(executor.map(fetch_trades, symbols))

    if ticker_records:
        df = pd.DataFrame(ticker_records)
        today = date.today().isoformat()

        for symbol in df['symbol'].unique():
            symbol_data = df[df['symbol'] == symbol]
            output_dir = os.path.join(DATA_DIR, "bronze", today, symbol)
            os.makedirs(output_dir, exist_ok=True)
            output_file = f"{output_dir}/data_{int(time.time())}.parquet"
            symbol_data.to_parquet(output_file, index=False)
            print(f"✅ Saved ticker for {symbol} to {output_file}")
    else:
        print("⚠️ No ticker data fetched")

    for symbol, ob in order_books:
        save_order_book(symbol, ob)

    for symbol, tr in trades:
        save_trades(symbol, tr)

if __name__ == "__main__":
    run_ingestion()
