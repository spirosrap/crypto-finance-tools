import pandas as pd
import numpy as np
import talib
import ccxt
from datetime import datetime, timedelta

def check_btc_entry_conditions_last_n(n: int = 10):
    """
    Check if BTC/USD meets specific entry conditions for the last n 1h candles.
    
    Conditions:
    1. 14-period RSI < 30 (oversold condition)
    2. Current volume > 1.5x 14-period average volume
    3. Current ATR > ATR from 5 periods ago
    
    Args:
        n (int): Number of recent candles to check (default 10)
    Returns:
        pd.DataFrame: DataFrame with columns for each condition and overall result
    """
    # Initialize exchange
    exchange = ccxt.coinbase()
    
    # Get BTC/USD OHLCV data for the last 100 hours (to ensure enough data for calculations)
    timeframe = '1h'
    since = exchange.parse8601((datetime.now() - timedelta(days=5)).isoformat())
    ohlcv = exchange.fetch_ohlcv('BTC/USD', timeframe, since=since, limit=100)
    
    # Convert to DataFrame
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # Calculate indicators
    df['rsi'] = talib.RSI(df['close'].values, timeperiod=14)
    df['volume_ma'] = talib.SMA(df['volume'].values, timeperiod=14)
    df['relative_volume'] = df['volume'] / df['volume_ma']
    df['atr'] = talib.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
    
    # Prepare results for the last n candles
    results = []
    for i in range(-n, 0):
        latest_rsi = df['rsi'].iloc[i]
        latest_relative_volume = df['relative_volume'].iloc[i]
        latest_atr = df['atr'].iloc[i]
        # For ATR 5 periods ago, make sure we don't go out of bounds
        if i - 5 < -len(df):
            atr_5_periods_ago = np.nan
        else:
            atr_5_periods_ago = df['atr'].iloc[i-5]
        rsi_condition = latest_rsi < 30
        volume_condition = latest_relative_volume > 1.5
        atr_condition = latest_atr > atr_5_periods_ago if not np.isnan(atr_5_periods_ago) else False
        all_met = all([rsi_condition, volume_condition, atr_condition])
        results.append({
            'timestamp': df['timestamp'].iloc[i],
            'rsi': latest_rsi,
            'relative_volume': latest_relative_volume,
            'atr': latest_atr,
            'atr_5_periods_ago': atr_5_periods_ago,
            'rsi_condition': rsi_condition,
            'volume_condition': volume_condition,
            'atr_condition': atr_condition,
            'all_met': all_met
        })
    return pd.DataFrame(results)

# Example usage
if __name__ == "__main__":
    try:
        df_results = check_btc_entry_conditions_last_n(10)
        print(df_results[['timestamp', 'rsi', 'relative_volume', 'atr', 'atr_5_periods_ago', 'rsi_condition', 'volume_condition', 'atr_condition', 'all_met']])
    except Exception as e:
        print(f"Error occurred: {e}") 