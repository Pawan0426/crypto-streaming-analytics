import pandas as pd

# ----------- Ticker Validation -----------
def validate_ticker_data(df: pd.DataFrame, symbol: str) -> list:
    issues = []

    required_columns = ["symbol", "price", "bid", "ask", "volume", "time", "fetched_at"]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        issues.append(f"{symbol} - Missing columns: {missing_cols}")
        return issues  # Skip further checks if basic structure is broken

    if df.isnull().any().any():
        nulls = df.isnull().sum()
        issues.append(f"{symbol} - Null values:\n{nulls}")

    if (df["price"] <= 0).any():
        issues.append(f"{symbol} - Found non-positive prices")

    if not pd.api.types.is_datetime64_any_dtype(df["fetched_at"]):
        issues.append(f"{symbol} - 'fetched_at' not datetime")

    return issues


# ----------- Trade Validation -----------
def validate_trade_data(df, symbol, level="silver"):
    issues = []

    if level == "silver":
        required_columns = ["trade_id", "price", "size", "side", "fetched_at"]
    elif level == "gold":
        required_columns = [
            "price_min", "price_max", "price_avg", "total_volume",
            "avg_trade_size", "trade_count", "buy_volume", "sell_volume",
            "symbol", "date", "bucket", "fetched_at"
        ]
    else:
        issues.append(f"{symbol} - Unknown data level: {level}")
        return issues

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        issues.append(f"{symbol} - Missing columns: {missing_cols}")

    return issues
