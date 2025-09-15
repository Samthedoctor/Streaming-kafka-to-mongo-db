# producer.py

import os
import time
import json
import yfinance as yf
import pandas as pd
import numpy as np
from confluent_kafka import Producer
import logging
from datetime import time as dt_time
import pytz

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'yfinance_features')
# Expecting a comma-separated string of tickers, e.g., "MSFT,AAPL,NVDA"
TICKER_SYMBOLS_STR = os.environ.get('TICKERS', 'MSFT,AAPL,NVDA,JPM')
TICKER_SYMBOLS = [ticker.strip().upper() for ticker in TICKER_SYMBOLS_STR.split(',')]
FETCH_INTERVAL_SECONDS = 60

# Market Hours Configuration (Eastern Time)
MARKET_TIMEZONE = pytz.timezone('America/New_York')
MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE = dt_time(16, 0)

# --- State Management ---
# Dictionary to store the last produced timestamp for each ticker to avoid duplicates
last_produced_timestamps = {ticker: None for ticker in TICKER_SYMBOLS}

# --- Kafka Producer Setup ---
try:
    producer_conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(producer_conf)
    logging.info(f"Kafka Producer initialized for broker at {KAFKA_BROKER}")
except Exception as e:
    logging.error(f"Failed to initialize Kafka Producer: {e}")
    exit(1)

# --- Delivery Report Callback ---
def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed for {msg.key().decode("utf-8")}: {err}')
    else:
        logging.info(f'Message for {msg.key().decode("utf-8")} delivered to {msg.topic()} [{msg.partition()}]')

# --- Feature Calculation Functions (No pandas_ta) ---

def calculate_rsi(series, window=14):
    """Calculate Relative Strength Index (RSI) without pandas_ta."""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/window, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/window, adjust=False).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_atr(high, low, close, window=14):
    """Calculate Average True Range (ATR) without pandas_ta."""
    high_low = high - low
    high_close = np.abs(high - close.shift())
    low_close = np.abs(low - close.shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/window, adjust=False).mean()
    return atr

def calculate_all_features(df):
    """Calculates all predictive features for the entire dataframe."""
    
    # Use .transform() for index safety
    df['returns'] = df.groupby('Ticker')['close'].transform(
        lambda x: x.pct_change(fill_method=None)
    )

    df['rolling_volatility_30m'] = df.groupby('Ticker')['returns'].transform(
        lambda x: x.rolling(window=30).std()
    )

    grouped = df.groupby('Ticker', group_keys=False)
    df['atr_14'] = grouped.apply(lambda x: calculate_atr(x['high'], x['low'], x['close']))
    df['rsi_14'] = grouped['close'].apply(calculate_rsi)

    ma_close_60 = df.groupby('Ticker')['close'].transform(lambda x: x.rolling(window=60).mean())
    ma_volume_60 = df.groupby('Ticker')['volume'].transform(lambda x: x.rolling(window=60).mean())
    df['price_vs_ma_60m'] = (df['close'] - ma_close_60) / ma_close_60
    df['volume_vs_ma_60m'] = df['volume'] / ma_volume_60

    df['turnover'] = df['close'] * df['volume']

    # Trailing Maximum Drawdown (Target Score)
    rolling_max = df.groupby('Ticker')['close'].transform(
        lambda x: x.rolling(window=30, min_periods=1).max()
    )
    df['target_score'] = (rolling_max - df['close']) / rolling_max

    return df

# --- Main Loop ---
def fetch_and_produce():
    """
    Fetches intraday data, calculates features, and produces the latest
    in-market-hours data point to Kafka, avoiding duplicates.
    """
    tickers_to_fetch = TICKER_SYMBOLS + ['SPY', '^VIX']
    logging.info(f"Starting producer for tickers: {TICKER_SYMBOLS_STR}")

    while True:
        try:
            # 1. Fetch data. no_cache=True is recommended after upgrading yfinance
            raw_df = yf.download(
                tickers=tickers_to_fetch,
                period="2d",
                interval="1m",
                prepost=True,
                auto_adjust=False,
                # This requires yfinance>=0.2.30
            )

            if raw_df.empty:
                logging.warning("No data fetched from yfinance. May be a weekend or holiday.")
                time.sleep(FETCH_INTERVAL_SECONDS)
                continue

            # 2. Pre-process and Calculate Features
            raw_df.index = raw_df.index.tz_convert('UTC')
            
            df = raw_df.stack(level=1, future_stack=True).rename_axis(['Datetime', 'Ticker']).reorder_levels(['Ticker', 'Datetime'])
            
            df.columns = df.columns.str.lower()
            
            # Merge VIX data
            if '^VIX' in df.index.get_level_values('Ticker'):
                vix_data = df.loc['^VIX'][['close']].rename(columns={'close': 'vix_value'})
                df = df.join(vix_data.ffill())

            # Calculate all features
            featured_df = calculate_all_features(df)
            featured_df.dropna(inplace=True)

            # 3. Process and produce the latest data point for each ticker
            for ticker in TICKER_SYMBOLS:
                if ticker not in featured_df.index.get_level_values('Ticker'):
                    continue

                latest_data = featured_df.loc[ticker].iloc[-1]
                latest_timestamp = latest_data.name

                market_timestamp = latest_timestamp.tz_convert(MARKET_TIMEZONE)

                is_new_data = last_produced_timestamps.get(ticker) is None or latest_timestamp > last_produced_timestamps[ticker]
                is_market_hours = MARKET_OPEN <= market_timestamp.time() < MARKET_CLOSE

                if is_new_data and is_market_hours:
                    # 4. Format and Produce Message
                    message = {
                        "ticker": ticker,
                        "timestamp_utc": latest_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        "open": latest_data.get('open'),
                        "high": latest_data.get('high'),
                        "low": latest_data.get('low'),
                        "close": latest_data.get('close'),
                        "volume": latest_data.get('volume'),
                        "returns": latest_data.get('returns'),
                        "rolling_volatility_30m": latest_data.get('rolling_volatility_30m'),
                        "atr_14": latest_data.get('atr_14'),
                        "rsi_14": latest_data.get('rsi_14'),
                        "price_vs_ma_60m": latest_data.get('price_vs_ma_60m'),
                        "volume_vs_ma_60m": latest_data.get('volume_vs_ma_60m'),
                        "turnover": latest_data.get('turnover'),
                        "vix_value": latest_data.get('vix_value'),
                        "target_score": latest_data.get('target_score')
                    }
                    
                    message_clean = {k: v for k, v in message.items() if v is not None and not np.isnan(v)}

                    producer.produce(
                        KAFKA_TOPIC,
                        key=ticker.encode('utf-8'),
                        value=json.dumps(message_clean).encode('utf-8'),
                        callback=delivery_report
                    )
                    
                    last_produced_timestamps[ticker] = latest_timestamp
                    logging.info(f"PRODUCED: New data for {ticker} at {market_timestamp.strftime('%H:%M:%S')}")
                
                elif not is_new_data:
                    logging.debug(f"SKIPPED: Data for {ticker} at {market_timestamp.strftime('%H:%M:%S')} already produced.")
                elif not is_market_hours:
                    logging.debug(f"SKIPPED: Data for {ticker} at {market_timestamp.strftime('%H:%M:%S')} is outside market hours.")

            producer.poll(0)

        except Exception as e:
            logging.error(f"An error occurred in the main loop: {e}", exc_info=True)

        finally:
            producer.flush()
            logging.info(f"Waiting for {FETCH_INTERVAL_SECONDS} seconds...")
            time.sleep(FETCH_INTERVAL_SECONDS)

if __name__ == '__main__':
    fetch_and_produce()