# Crypto Finance Tools

A collection of powerful tools and utilities for cryptocurrency trading and financial analysis. This repository aims to provide practical solutions for traders and investors in the crypto space.

## Overview

This project provides a suite of tools designed to help traders make informed decisions in the cryptocurrency markets. The tools are built with a focus on technical analysis, trend identification, and automated trading strategies.

## Requirements

- pandas
- numpy
- ta-lib
- ccxt

Install the required packages using:
```bash
pip install -r requirements.txt
```

## Features

- **ATR Analysis**: Analyze the Average True Range (ATR) for market volatility insights.
- **BTC/USD Analysis**: Perform specific analysis on BTC/USD data, including ATR and entry conditions.
- **Trend Detection**: Detect market trends using various technical indicators.

## Tools

### ATR Analyzer

This tool analyzes the Average True Range (ATR) for a given dataset. It calculates the ATR and provides insights into market volatility.

#### Usage

Run the script directly:
```bash
python atr_analyzer.py
```

### BTC ATR Analysis

This tool performs ATR analysis specifically for BTC/USD data. It fetches hourly OHLCV data and calculates the ATR for the last 10 candles.

#### Usage

Run the script directly:
```bash
python btc_atr_analysis.py
```

### BTC Entry Conditions

This tool checks if BTC/USD meets specific entry conditions based on RSI, relative volume, and ATR. It fetches hourly OHLCV data from Coinbase and evaluates the following conditions for the last 10 candles:

1. **RSI Condition**: 14-period RSI < 30 (oversold condition)
2. **Relative Volume Condition**: Current volume > 1.5x 14-period average volume
3. **ATR Condition**: Current ATR > ATR from 5 periods ago

The tool returns a DataFrame with the results for each of the last 10 candles, including the indicator values and whether each condition was met.

#### Usage

Run the script directly:
```bash
python btc_entry_conditions.py
```

Or import and use the function in your own code:
```python
from btc_entry_conditions import check_btc_entry_conditions_last_n

df_results = check_btc_entry_conditions_last_n(10)
print(df_results)
```

### Trend Detection

This tool detects trends in BTC/USD data using various technical indicators. It provides insights into market trends and potential entry/exit points.

#### Usage

Run the script directly:
```bash
python trend_detection.py
```

## Getting Started

### Prerequisites
- Python 3.8+
- Conda (Miniconda or Anaconda)
- Required API keys (You'll need to create a `config.py` file. See below.)

### Environment Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/crypt-finance-tools.git
cd crypt-finance-tools
```

2. Create and activate the conda environment:
```bash
# Create the environment from environment.yml
conda env create -f environment.yml

# Activate the environment
conda activate crypto-trading
```

3. Verify the installation:
```bash
python -c "import pandas as pd; import numpy as np; import ccxt; import talib; print('Environment setup successful!')"
```

#### Troubleshooting ta-lib Installation

If you encounter issues with `ta-lib` installation, you may need to install it separately:

For macOS:
```bash
brew install ta-lib
```

For Ubuntu/Debian:
```bash
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib/
./configure --prefix=/usr
make
sudo make install
```

### Included Libraries
The environment includes essential libraries for:
- Data manipulation (pandas, numpy)
- Technical analysis (ta-lib, pandas-ta, ta)
- Cryptocurrency exchange interactions (ccxt, python-binance)
- Visualization (matplotlib, seaborn, plotly)
- Machine learning capabilities (scikit-learn)
- Environment variable management (python-dotenv)

### Configuration
Create a `config.py` file in the root directory with the following required API keys:

```python
# Coinbase API credentials
API_KEY = "your_coinbase_api_key"
API_SECRET = "your_coinbase_api_secret"

# Coinbase Perpetual Futures API credentials
API_KEY_PERPS = "your_coinbase_perps_api_key"
API_SECRET_PERPS = "your_coinbase_perps_api_secret"

# Other API keys (optional)
OPENAI_KEY = "your_openai_key"
NEWS_API_KEY = "your_news_api_key"
```

You can obtain these API keys by:
1. Creating a Coinbase account
2. Enabling API access in your account settings
3. Creating API keys with appropriate permissions

Make sure to keep your API keys secure and never commit them to version control.

## Usage

The initial tool focuses on RSI-based strategy activation:

```python
from tools.trend_analyzer import TrendAnalyzer

# Initialize the analyzer
analyzer = TrendAnalyzer()

# Analyze market trends
trend = analyzer.analyze_trend(symbol="BTC/USD")

# Check for RSI dip opportunities
opportunities = analyzer.find_rsi_dips(symbol="BTC/USD", timeframe="1h")
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This software is for educational purposes only. Do not risk money which you are afraid to lose. USE THE SOFTWARE AT YOUR OWN RISK. THE AUTHORS AND ALL AFFILIATES ASSUME NO RESPONSIBILITY FOR YOUR TRADING RESULTS. 