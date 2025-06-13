# Crypto Finance Tools

A collection of powerful tools and utilities for cryptocurrency trading and financial analysis. This repository aims to provide practical solutions for traders and investors in the crypto space.

## Overview

This project provides a suite of tools designed to help traders make informed decisions in the cryptocurrency markets. The tools are built with a focus on technical analysis, trend identification, and automated trading strategies.

## Features

### Trend Identification & RSI Strategy
- **RSI Dip Detection**: Identifies potential buying opportunities based on RSI (Relative Strength Index) dips
- **Trend Analysis**: Analyzes market trends to determine optimal entry and exit points
- **Strategy Activation**: Automatically triggers trading strategies based on identified patterns

### Volatility Analysis
- **ATR Expansion Detection**: Monitors the Average True Range (ATR) to identify periods of increasing market volatility. This tool compares the current ATR value with historical values to determine if volatility is expanding, which can be useful for:
  - Identifying potential breakout opportunities
  - Adjusting position sizes based on market volatility
  - Risk management and stop-loss placement
  - Market regime detection

## Getting Started

### Prerequisites
- Python 3.8+
- Conda (Miniconda or Anaconda)
- Required API keys (see `config.py` for details)

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