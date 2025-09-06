import smaRangeTrader

api_id = "your_api_id"
api_secret = "your_api_secret"

bot = smaRangeTrader(api_id, api_secret, stock_symbol="SPY")
bot.run()
print("Hello World")