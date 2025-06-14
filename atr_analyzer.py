import ccxt
import pandas as pd
import numpy as np
import talib
from datetime import datetime, timedelta
import pytz

def get_atr_data(symbol='BTC/USD', timeframe='1h', limit=14):
    """
    Fetch OHLCV data and calculate ATR14 for the specified symbol and timeframe.
    
    Args:
        symbol (str): Trading pair symbol (default: 'BTC/USD')
        timeframe (str): Timeframe for the data (default: '1h')
        limit (int): Number of candles to fetch (default: 14 for ATR calculation)
    
    Returns:
        pd.DataFrame: DataFrame containing timestamp, close price, and ATR values
    """
    # Initialize exchange (using Coinbase as default)
    exchange = ccxt.coinbase()
    
    # Fetch OHLCV data
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
    
    # Convert to DataFrame
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # Calculate ATR14
    df['atr14'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
    
    return df

def main():
    # Get ATR data
    df = get_atr_data(limit=24)  # Fetch more data to ensure we have enough for ATR calculation
    
    # Get the last 10 hours of data
    last_10_hours = df.tail(10)
    
    # Print results
    print("\nATR14 (1h timeframe) for the last 10 hours:")
    print("=" * 50)
    print(f"{'Timestamp':^25} | {'Close Price':^12} | {'ATR14':^12}")
    print("-" * 50)
    
    for _, row in last_10_hours.iterrows():
        timestamp = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        close_price = f"${row['close']:.2f}"
        atr = f"${row['atr14']:.2f}" if not pd.isna(row['atr14']) else "N/A"
        print(f"{timestamp:^25} | {close_price:^12} | {atr:^12}")

if __name__ == "__main__":
    main() 