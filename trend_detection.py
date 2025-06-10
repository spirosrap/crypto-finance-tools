import pandas as pd
import numpy as np
from typing import Tuple, Optional
from datetime import datetime, timedelta, UTC
from coinbaseservice import CoinbaseService
from config import API_KEY_PERPS, API_SECRET_PERPS
import logging
import requests
import time

# Set up logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_coinbase_data(product_id: str = 'BTC-USD', 
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch historical data from Coinbase.
    
    Args:
        product_id: Trading pair (e.g., 'BTC-USD')
        start_date: Optional start date in 'YYYY-MM-DD' format
        end_date: Optional end date in 'YYYY-MM-DD' format
        
    Returns:
        DataFrame with OHLCV data
    """
    logger.info(f"Fetching data for {product_id}")
    
    if start_date and end_date:
        start = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=UTC)
        end = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=UTC)
    else:
        # Default to last 300 5-minute candles (API limit)
        now = datetime.now(UTC)
        end = now
        start = now - timedelta(minutes=5 * 300)
    
    logger.info(f"Fetching data from {start} to {end}")
    
    # Initialize empty list to store all candles
    all_candles = []
    current_end = end
    
    # Fetch data in chunks of 300 candles (maximum allowed by API)
    while current_end > start:
        # Calculate start time for this chunk (300 candles * 5 minutes = 1500 minutes)
        chunk_start = current_end - timedelta(minutes=5 * 300)
        if chunk_start < start:
            chunk_start = start
            
        try:
            # Get raw data for this chunk using the new Exchange API endpoint
            url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"
            params = {
                "start": chunk_start.isoformat(),
                "end": current_end.isoformat(),
                "granularity": 300  # 5 minutes in seconds
            }
            logger.info(f"Making request to {url} with params: {params}")
            
            response = requests.get(url, params=params)
            
            # Check if the request was successful
            response.raise_for_status()
            
            # Parse the response
            chunk_data = response.json()
            logger.info(f"Received {len(chunk_data) if isinstance(chunk_data, list) else 0} candles")
            
            if not isinstance(chunk_data, list):
                logger.error(f"Unexpected response format: {chunk_data}")
                break
                
            # Add chunk data to our list
            all_candles.extend(chunk_data)
            logger.info(f"Total candles collected so far: {len(all_candles)}")
            
            # Update end time for next chunk
            current_end = chunk_start
            
            # If we've reached the start time, break
            if chunk_start <= start:
                break
                
            # Add a small delay to avoid rate limiting
            time.sleep(0.5)
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data chunk: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response text: {e.response.text}")
            break
        except Exception as e:
            logger.error(f"Unexpected error fetching data chunk: {str(e)}")
            break
    
    # Create DataFrame from all candles
    # First, convert the data to the correct format
    formatted_candles = []
    for candle in all_candles:
        if isinstance(candle, (list, tuple)) and len(candle) >= 6:
            formatted_candles.append({
                'start': candle[0],
                'open': candle[3],  # Coinbase returns [timestamp, low, high, open, close, volume]
                'high': candle[2],
                'low': candle[1],
                'close': candle[4],
                'volume': candle[5]
            })
    
    logger.info(f"Formatted {len(formatted_candles)} candles")
    
    # Create DataFrame from formatted candles
    df = pd.DataFrame(formatted_candles)
    
    if df.empty:
        logger.error("No valid candles found in the data")
        return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
    
    # Convert string columns to numeric
    numeric_columns = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Handle timestamp conversion
    df['start'] = pd.to_datetime(df['start'], unit='s')
    df.set_index('start', inplace=True)
    
    # Sort by timestamp to ensure correct order
    df.sort_index(inplace=True)
    
    logger.info(f"Successfully fetched {len(df)} candles")
    return df

def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate Average True Range (ATR).
    
    Args:
        high: High prices
        low: Low prices
        close: Close prices
        period: ATR period (default: 14)
        
    Returns:
        Series containing ATR values
    """
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def calculate_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate Relative Strength Index (RSI).
    
    Args:
        close: Close prices
        period: RSI period (default: 14)
        
    Returns:
        Series containing RSI values
    """
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def detect_clear_downtrend(df: pd.DataFrame) -> Tuple[bool, dict]:
    """
    Detect if there is a clear downtrend in the market.
    
    A clear downtrend is defined as:
    1. Price consistently below 50-period EMA
    2. Lower lows and lower highs over the last 5 bars
    3. ATR (14) > 0.7% of close price (volatility filter)
    
    Args:
        df: DataFrame containing OHLCV data with columns:
            - open, high, low, close, volume
            
    Returns:
        Tuple[bool, dict]: (is_downtrend, metrics)
    """
    if df.empty or len(df) < 50:
        logger.error("Not enough data points for trend detection")
        return False, {
            'ema_check': False,
            'lower_lows': False,
            'lower_highs': False,
            'atr_check': False,
            'current_atr%': 0,
            'current_close': 0,
            'current_ema50': 0
        }
    
    # Calculate technical indicators
    df['EMA50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ATR'] = calculate_atr(df['high'], df['low'], df['close'])
    df['ATR%'] = df['ATR'] / df['close'] * 100
    
    # Check if last 5 closes are below EMA50
    ema_check = (df['close'].iloc[-5:] < df['EMA50'].iloc[-5:]).all()
    
    # Check lower lows and lower highs in last 5 bars
    lows = df['low'].iloc[-5:]
    highs = df['high'].iloc[-5:]
    lower_lows = all(lows.iloc[i] < lows.iloc[i-1] for i in range(1, len(lows)))
    lower_highs = all(highs.iloc[i] < highs.iloc[i-1] for i in range(1, len(highs)))
    
    # ATR filter
    atr_check = df['ATR%'].iloc[-1] > 0.7
    
    # Collect metrics
    metrics = {
        'ema_check': ema_check,
        'lower_lows': lower_lows,
        'lower_highs': lower_highs,
        'atr_check': atr_check,
        'current_atr%': df['ATR%'].iloc[-1],
        'current_close': df['close'].iloc[-1],
        'current_ema50': df['EMA50'].iloc[-1]
    }
    
    return ema_check and lower_lows and lower_highs and atr_check, metrics

def detect_oversold_reversal(df: pd.DataFrame) -> Tuple[bool, dict]:
    """
    Detect potential oversold reversal conditions.
    
    An oversold reversal is defined as:
    1. RSI(14) < 30
    2. Price touching or under the lower Bollinger Band (20, 2)
    
    Args:
        df: DataFrame containing OHLCV data with columns:
            - open, high, low, close, volume
            
    Returns:
        Tuple[bool, dict]: (is_oversold, metrics)
    """
    if df.empty or len(df) < 20:  # Need at least 20 candles for BB
        logger.error("Not enough data points for oversold detection")
        return False, {
            'rsi_check': False,
            'bb_check': False,
            'current_rsi': 50,  # Neutral RSI value
            'current_close': 0,
            'current_bb_lower': 0
        }
    
    # Calculate technical indicators
    df['RSI'] = calculate_rsi(df['close'])
    
    # Calculate Bollinger Bands
    df['BB_middle'] = df['close'].rolling(20).mean()
    df['BB_std'] = df['close'].rolling(20).std()
    df['BB_lower'] = df['BB_middle'] - 2 * df['BB_std']
    
    # Check conditions
    rsi_check = df['RSI'].iloc[-1] < 30
    bb_check = df['close'].iloc[-1] <= df['BB_lower'].iloc[-1]
    
    # Collect metrics
    metrics = {
        'rsi_check': rsi_check,
        'bb_check': bb_check,
        'current_rsi': df['RSI'].iloc[-1],
        'current_close': df['close'].iloc[-1],
        'current_bb_lower': df['BB_lower'].iloc[-1]
    }
    
    return rsi_check and bb_check, metrics

def analyze_market_conditions(df: pd.DataFrame) -> Tuple[bool, bool, dict, dict]:
    """
    Analyze market conditions for both downtrend and oversold reversal.
    
    Args:
        df: DataFrame containing OHLCV data with columns:
            - open, high, low, close, volume
            
    Returns:
        Tuple[bool, bool, dict, dict]: (is_downtrend, is_oversold_reversal, downtrend_metrics, oversold_metrics)
    """
    is_downtrend, downtrend_metrics = detect_clear_downtrend(df)
    is_oversold_reversal, oversold_metrics = detect_oversold_reversal(df)
    return is_downtrend, is_oversold_reversal, downtrend_metrics, oversold_metrics

# Example usage:
if __name__ == "__main__":
    try:
        # Fetch data from Coinbase
        df = fetch_coinbase_data('BTC-USD')
        
        if df.empty:
            logger.error("No data available for analysis")
            exit(1)
            
        # Analyze market conditions
        is_downtrend, is_oversold_reversal, downtrend_metrics, oversold_metrics = analyze_market_conditions(df)
        
        # Print results
        logger.info("\nMarket Analysis Results:")
        logger.info("=" * 50)
        
        if is_downtrend:
            logger.info("✓ Clear downtrend detected")
            logger.info("\nDowntrend Metrics:")
            for key, value in downtrend_metrics.items():
                logger.info(f"{key}: {value}")
        else:
            logger.info("✗ No clear downtrend detected")
        
        logger.info("\n" + "=" * 50)
        
        if is_oversold_reversal:
            logger.info("✓ Potential oversold reversal detected")
            logger.info("\nOversold Metrics:")
            for key, value in oversold_metrics.items():
                logger.info(f"{key}: {value}")
        else:
            logger.info("✗ No oversold reversal detected")
            logger.info(f"Current RSI: {oversold_metrics['current_rsi']:.2f}")
            
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}")
        exit(1) 