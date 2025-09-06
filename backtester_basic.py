import pandas as pd
from rangeTrader.smaRangeTrader import RangeTradingBot

api_id = "your_api_id"
api_secret = "your_api_secret"
file="rangeTrader/Record.txt"
bot = RangeTradingBot(api_id, api_secret, stock_symbol="SPY")


price=bot.get_price()
sma, upper=bot.get_sma_and_upper()
highcount, lowcount = bot.count_range_hits(sma, upper)
order = bot.execute_trade(price, sma, highcount, lowcount)

if order:
    df=pd.DataFrame([order])
    df.to_csv(file, sep=" ",index=False, mode="a")

bot.run()
