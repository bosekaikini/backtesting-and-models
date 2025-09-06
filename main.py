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

api_id = "PKF18A8UGJ5O1CU2ODQD"
api_secret = "lkAANR7xZ4Il4fFkbGCRjhTD6PdejebojeVjUMiF"
api = tradeapi.REST(api_id, api_secret)
trading_client = TradingClient(api_id, api_secret, paper=True)
client = StockHistoricalDataClient(api_id, api_secret)

stock_1 = 'SPY'
hold = False

while True:
    clock: Clock = trading_client.get_clock()

    if clock.is_open:
        latest_trade_request = StockLatestTradeRequest(symbol=stock_1)
        latest_trade = client.get_stock_latest_trade(latest_trade_request)
        price = latest_trade.price

        day = 10
        sma = 0
        upper = 0

        for i in range(day):
            date = datetime.now().date() - timedelta(days=i)
            bars = api.get_bars(stock_1, tradeapi.TimeFrame.Day, date.strftime("%Y-%m-%d"), date.strftime("%Y-%m-%d"), limit=1)
            if bars:
                upper += bars[0].high
                sma += bars[0].close

        sma /= day
        upper /= day
        file = 'TradeRecord.xlsx'
        sheet = 'RangeBased'
        quantity = 3

        start_time = datetime.now() - timedelta(days=day)
        request_params = StockBarsRequest(
            symbol_or_symbols=stock_1,
            timeframe=TimeFrame.Minute,
            start=start_time
        )

        bars = client.get_stock_bars(request_params).data[stock_1]
        highcount = sum(1 for bar in bars if bar.high >= upper >= bar.low)
        lowcount = sum(1 for bar in bars if bar.high >= sma >= bar.low)

        if highcount > lowcount > 1:
            if price >= sma:

                if hold:
                    order_data = LimitOrderRequest(
                        symbol=stock_1,
                        limit_price=170.00,
                        qty=quantity,
                        side=OrderSide.SELL,
                        time_in_force=TimeInForce.DAY
                    )
                    order = trading_client.submit_order(order_data=order_data)
                    hold = False
                    new_df = pd.DataFrame([order_data])
                    with pd.ExcelWriter(file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                        existing_df = pd.read_excel(file, sheet_name=sheet)
                        # Append new data
                        updated_df = pd.concat([existing_df, new_df], ignore_index=True)
                        updated_df.to_excel(writer, sheet_name=sheet, index=False)

            else:
                market_order_data = MarketOrderRequest(
                    symbol=stock_1,
                    qty=quantity,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                )
                market_order = trading_client.submit_order(order_data=market_order_data)
                hold = True
                new_df = pd.DataFrame([market_order_data])
                with pd.ExcelWriter(file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                    existing_df = pd.read_excel(file, sheet_name=sheet)
                    updated_df = pd.concat([existing_df, new_df], ignore_index=True)
                    updated_df.to_excel(writer, sheet_name=sheet, index=False)

        else:
            print(stock_1, 'not suitable for range-based trade.')

        time.sleep(60)

    else:
        print("Market not open.")
        time.sleep(300)
