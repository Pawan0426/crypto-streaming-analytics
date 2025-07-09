# crypto-streaming-analytics

Ingests data from Coinbase for several crypto symbols (BTC, ETH, etc.)

Stores data in Bronze → Silver → Gold layers

Aggregates data at hourly level

Handles ticker, order book, and soon, trade data

Likely visualizes metrics like price trends, volume, market depth, etc.


Phase	What to Do Next
✅ Done	Ingestion + Aggregation
🔥 Now	Log validation issues to JSON
⏭️ Next	Rolling analytics → Dashboards
📦 Optional	Package as CLI or Docker later
🧪 Optional	Unit tests to keep pipeline stable
