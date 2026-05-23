"""Shared Alpaca historical data helpers for strategy modules."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit


def build_timeframe(bar_timeframe: str) -> TimeFrame:
    if bar_timeframe == "1Hour":
        return TimeFrame(1, TimeFrameUnit.Hour)
    return TimeFrame(1, TimeFrameUnit.Day)


def fetch_stock_bars_frame(
    client,
    symbol: str,
    start: datetime,
    end: datetime,
    bar_timeframe: str,
) -> pd.DataFrame:
    request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=build_timeframe(bar_timeframe),
        start=start,
        end=end,
    )

    bars = client.get_stock_bars(request)
    sym_bars = bars.data.get(symbol, [])
    if not sym_bars:
        raise ValueError(f"No data returned for {symbol}")

    records = [
        {
            "timestamp": bar.timestamp,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
        }
        for bar in sym_bars
    ]

    frame = pd.DataFrame(records).set_index("timestamp")
    frame.index = pd.DatetimeIndex(frame.index)
    return frame


def fetch_close_series(
    client,
    symbol: str,
    days_back: int,
    bar_timeframe: str,
) -> pd.Series:
    end_date = datetime.now(timezone.utc)
    start_date = end_date - pd.Timedelta(days=days_back)
    frame = fetch_stock_bars_frame(client, symbol, start_date.to_pydatetime(), end_date, bar_timeframe)
    return frame["close"].sort_index()