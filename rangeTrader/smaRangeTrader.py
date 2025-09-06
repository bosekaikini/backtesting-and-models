import time
import pandas as pd
from datetime import datetime, timedelta
import alpaca_trade_api as tradeapi
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestTradeRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.models import Clock

from datetime import datetime

class RangeTradingBot:
    def __init__(self, stock_symbol="SPY", quantity=3):
        self.stock_symbol = stock_symbol
        self.hold = False
        self.quantity = quantity

    def execute_trade(self, price, sma, highcount, lowcount, sell_price=170.00):
        if highcount > lowcount > 1:
            if price >= sma:  
                if self.hold:
                    order_data = {
                        "symbol": self.stock_symbol,
                        "side": "SELL",
                        "qty": self.quantity,
                        "sma": sma,
                        "time": datetime.now(),
                        "price": sell_price  
                    }
                    self.hold = False
                    return order_data
            else:  
                if not self.hold:
                    order_data = {
                        "symbol": self.stock_symbol,
                        "side": "BUY",
                        "qty": self.quantity,
                        "sma": sma,
                        "time": datetime.now(),
                        "price": price
                    }
                    self.hold = True
                    return order_data
        return None
