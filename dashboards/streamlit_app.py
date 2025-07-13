import streamlit as st
import pandas as pd
import os
from glob import glob
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta

# ========== CONFIGURATION ==========
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "gold", "trades"))
assets = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]

# ========== PAGE SELECTION ==========
st.set_page_config(layout="wide")
st.title("📊 Crypto Trade Dashboard (Gold Layer)")

page = st.sidebar.radio("📁 Select View", ["📆 Daily Summary", "📈 Multi-Day KPIs"])
asset = st.sidebar.selectbox("Select Asset", assets)

# ========== DAILY SUMMARY DASHBOARD ==========
if page == "📆 Daily Summary":
    st.markdown("View hourly trade data from a selected day")

    try:
        available_dates = sorted(os.listdir(BASE_PATH), reverse=True)
        selected_date = st.selectbox("Select Date", available_dates)

        data_path = os.path.join(BASE_PATH, selected_date, asset)
        files = glob(f"{data_path}/trade_summary_*.parquet")

        if not files:
            st.warning("No data found for selected date and asset.")
        else:
            df = pd.concat([pd.read_parquet(f) for f in sorted(files)])
            df['timestamp'] = pd.to_datetime(df['date'] + " " + df['bucket'], errors='coerce')
            df = df.dropna(subset=['timestamp'])

            if df.empty:
                st.warning("Dataframe is empty after processing timestamps.")
            else:
                st.success(f"Loaded {len(df)} hourly data points.")

                st.subheader("📌 Summary Metrics")
                row = df.iloc[0]
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Avg Price", f"${row['price_avg']:.2f}")
                col2.metric("Total Volume", f"{row['total_volume']:,}")
                col3.metric("Trades", f"{row['trade_count']}")
                col4.metric("Buy/Sell Volume", f"{row['buy_volume']} / {row['sell_volume']}")

                st.subheader("📉 Candlestick Chart (Hourly)")
                fig = go.Figure(data=[go.Candlestick(
                    x=df['timestamp'],
                    open=df['price_avg'],
                    high=df['price_max'],
                    low=df['price_min'],
                    close=df['price_avg'],
                    increasing_line_color='green',
                    decreasing_line_color='red')])
                fig.update_layout(xaxis_title="Time", yaxis_title="Price (USD)", height=500)
                st.plotly_chart(fig, use_container_width=True)

                st.subheader("📊 Volume Over Time")
                st.bar_chart(df.set_index("timestamp")["total_volume"])

                if st.checkbox("Show raw data"):
                    st.dataframe(df)

    except Exception as e:
        st.error("❌ Failed to load or display data.")
        st.exception(e)

# ========== MULTI-DAY KPI DASHBOARD ==========
elif page == "📈 Multi-Day KPIs":
    st.markdown("Visualize key performance indicators over multiple days")
    try:
        #available_dates = sorted(os.listdir(BASE_PATH), reverse=True)[:2]  # last 2 days
        available_dates = sorted(os.listdir(BASE_PATH), reverse=True)
        dfs = []
        for date in available_dates:
            files = glob(os.path.join(BASE_PATH, date, asset, "trade_summary_*.parquet"))
            dfs.extend([pd.read_parquet(f) for f in sorted(files)])

        df = pd.concat(dfs)
        df['timestamp'] = pd.to_datetime(df['date'] + " " + df['bucket'], errors='coerce')
        df = df.dropna(subset=['timestamp']).sort_values("timestamp")

        # Calculated Metrics
        df['vwap'] = df['price_avg']
        df['buy_ratio'] = df['buy_volume'] / (df['total_volume'] + 1e-9)
        df['sell_ratio'] = df['sell_volume'] / (df['total_volume'] + 1e-9)
        df['price_change'] = df['price_avg'].diff()
        df['bullish'] = df['price_change'] > 0

        st.subheader("VWAP Over Time")
        fig1 = px.line(df, x='timestamp', y='vwap', title="VWAP", template="plotly_dark")
        st.plotly_chart(fig1, use_container_width=True)

        st.subheader("Buy vs Sell Sentiment")
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=df['timestamp'], y=df['buy_ratio'], name='Buy Ratio', line=dict(color='cyan')))
        fig2.add_trace(go.Scatter(x=df['timestamp'], y=df['sell_ratio'], name='Sell Ratio', line=dict(color='magenta')))
        fig2.update_layout(template="plotly_dark", yaxis_title="Ratio", height=400)
        st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Bullish vs Bearish Trend")
        trend_df = df.copy()
        trend_df['Trend'] = trend_df['bullish'].map({True: 'Bullish', False: 'Bearish'})
        fig3 = px.histogram(trend_df, x='timestamp', color='Trend', barmode='overlay', title="Market Trend")
        fig3.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig3, use_container_width=True)

        st.subheader("Total Volume Over Time")
        fig4 = px.area(df, x='timestamp', y='total_volume', title="Volume Flow", template="plotly_dark")
        st.plotly_chart(fig4, use_container_width=True)

    except Exception as e:
        st.error("❌ Failed to load multi-day KPI data.")
        st.exception(e)
