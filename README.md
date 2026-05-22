# backtesting-and-models
"""
=============================================================================
Statistical Arbitrage / Pairs Trading System — Alpaca API v2
=============================================================================
Architecture:
  1. PairsBacktester   — vectorized backtesting engine
  2. OUModelEngine     — Ornstein-Uhlenbeck calibration & signal generation
  3. AlpacaTradingBot  — async live execution via WebSocket + REST

Author:  Generated for production use
Requires: alpaca-py, pandas, numpy, statsmodels, scipy
  pip install alpaca-py pandas numpy statsmodels scipy
=============================================================================
"""
