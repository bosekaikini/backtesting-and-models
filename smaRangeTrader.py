# trading_bot.py
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


class RangeTradingBot:
    def __init__(self, api_id, api_secret, stock_symbol="SPY", paper=True):
        self.api_id = api_id
        self.api_secret = api_secret
        self.stock_symbol = stock_symbol
        self.hold = False
        self.quantity = 3
        self.file = "TradeRecord.xlsx"
        self.sheet = "RangeBased"

        # Alpaca clients
        self.api = tradeapi.REST(api_id, api_secret)
        self.trading_client = TradingClient(api_id, api_secret, paper=paper)
        self.data_client = StockHistoricalDataClient(api_id, api_secret)

    def get_price(self):
        #Fetch the latest trade price for the stock.
        latest_trade_request = StockLatestTradeRequest(symbol=self.stock_symbol)
        latest_trade = self.data_client.get_stock_latest_trade(latest_trade_request)
        return latest_trade.price

    def get_sma_and_upper(self, days=10):
        #Calculate SMA and average high over n days.
        sma, upper = 0, 0
        for i in range(days):
            date = datetime.now().date() - timedelta(days=i)
            bars = self.api.get_bars(
                self.stock_symbol,
                tradeapi.TimeFrame.Day,
                date.strftime("%Y-%m-%d"),
                date.strftime("%Y-%m-%d"),
                limit=1,
            )
            if bars:
                upper += bars[0].high
                sma += bars[0].close
        return sma / days, upper / days

    def count_range_hits(self, sma, upper, days=10):
        #Count how often SMA and upper fall inside the intraday bars.
        start_time = datetime.now() - timedelta(days=days)
        request_params = StockBarsRequest(
            symbol_or_symbols=self.stock_symbol,
            timeframe=TimeFrame.Minute,
            start=start_time,
        )
        bars = self.data_client.get_stock_bars(request_params).data[self.stock_symbol]
        highcount = sum(1 for bar in bars if bar.high >= upper >= bar.low)
        lowcount = sum(1 for bar in bars if bar.high >= sma >= bar.low)
        return highcount, lowcount

    def log_trade(self, order_request):
        #Log trade details to Excel.
        new_df = pd.DataFrame([order_request.__dict__])
        try:
            existing_df = pd.read_excel(self.file, sheet_name=self.sheet)
        except FileNotFoundError:
            existing_df = pd.DataFrame()

        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
        with pd.ExcelWriter(
            self.file, engine="openpyxl", mode="a", if_sheet_exists="overlay"
        ) as writer:
            updated_df.to_excel(writer, sheet_name=self.sheet, index=False)

    def execute_trade(self, price, sma, highcount, lowcount):
        #Decide and place trades based on signals.
        if highcount > lowcount > 1:
            if price >= sma:
                if self.hold:
                    order_data = LimitOrderRequest(
                        symbol=self.stock_symbol,
                        limit_price=170.00,  # fixed target – adjust logic if needed
                        qty=self.quantity,
                        side=OrderSide.SELL,
                        time_in_force=TimeInForce.DAY,
                    )
                    self.trading_client.submit_order(order_data=order_data)
                    self.hold = False
                    self.log_trade(order_data)
            else:
                market_order_data = MarketOrderRequest(
                    symbol=self.stock_symbol,
                    qty=self.quantity,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY,
                )
                self.trading_client.submit_order(order_data=market_order_data)
                self.hold = True
                self.log_trade(market_order_data)
        else:
            print(self.stock_symbol, "not suitable for range-based trade.")

    def run(self, sleep_time=60):
        """Main loop to run the strategy continuously."""
        while True:
            clock: Clock = self.trading_client.get_clock()
            if clock.is_open:
                price = self.get_price()
                sma, upper = self.get_sma_and_upper(days=10)
                highcount, lowcount = self.count_range_hits(sma, upper, days=10)
                self.execute_trade(price, sma, highcount, lowcount)
                time.sleep(sleep_time)
            else:
                print("Market not open.")
                time.sleep(300)
