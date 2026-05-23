# backtesting-and-models

## Overview

This repository is a modular Alpaca-first pairs-trading workspace.

The current default strategy lives in [pairs_trading_system.py](pairs_trading_system.py), but the new orchestrator in [main.py](main.py) is the recommended entrypoint when you want to run one or more strategies at the same time.

## Entry Points

- [main.py](main.py) - orchestrator for running one or more strategy modules concurrently
- [pairs_trading_system.py](pairs_trading_system.py) - current pairs-trading strategy module and live/backtest logic
- [find_correlated_assets.py](find_correlated_assets.py) - Alpaca-based pair discovery module
- [strategy_loader.py](strategy_loader.py) - helper loader for discovery and signal plugins
- [alpaca_data_utils.py](alpaca_data_utils.py) - shared Alpaca historical data helpers

## Where To Put New Strategy Logic

If you want to implement a new strategy, create a new Python file such as `my_strategy.py` and give it one of these hooks:

- `run_strategy(config)` for a self-contained strategy module
- `discover_trade_pair(config)` for a pair-discovery module
- `create_signal_engine(cfg, asset_a, asset_b)` for a custom live signal engine

Then add the module to the list in [main.py](main.py).

## Running Multiple Strategies At Once

`main.py` can launch multiple strategy modules in parallel.

Each strategy module can run independently and can have its own config override. A portfolio allocation layer is now available on top of that, so you can assign capital weights to each strategy and have the orchestrator pass the correct `CAPITAL` value into each module.

This is the most convenient place to coordinate:

- one strategy for backtesting
- another for live paper trading
- multiple separate live strategies at the same time
- per-strategy capital weighting
- future risk budgets and portfolio overlays

If two live strategies share the same Alpaca paper account, they can both place orders, so make sure their symbols and logic do not conflict.

## More Professional Quant Setup

The cleanest next upgrades are:

1. Move every strategy into its own module with a standard `run_strategy()` entrypoint.
2. Keep a separate portfolio layer for capital allocation, drawdown limits, and strategy enable/disable control.
3. Add a proper backtest results store so you can compare strategies by Sharpe, drawdown, turnover, and live versus backtest slippage.
4. Add experiment tracking, even if it is just a CSV or SQLite log at first.
5. Split data, signal, execution, and risk into separate modules so the main file stays an orchestrator only.
6. Introduce a config file or per-strategy YAML/JSON config once you have more than a few strategies.
7. Add structured logging, trade journals, and daily reconciliation so paper/live behavior can be audited.
8. Add unit tests for signal generation and sizing logic before you add more strategies.

## Alpaca Data Usage

The code now uses Alpaca historical data as the shared source for:

- pair discovery
- warm-up data for live trading
- backtest bar loading

That logic is centralized in [alpaca_data_utils.py](alpaca_data_utils.py), so future strategies should reuse that helper instead of creating a new data path.

## After-Hours Behavior

If a valid signal appears while the market is closed, the bot queues it and executes it on the next market open instead of dropping it.

## Logs

Trade and signal events are written to `trade_activity.log` in the repository root.

## Install

```bash
pip install alpaca-py pandas numpy statsmodels scipy
```

## Run

Single strategy or default setup:

```bash
python main.py
```

If you want to keep using the pairs strategy directly:

```bash
python pairs_trading_system.py
```

## Suggested Workflow

1. Put each strategy in its own module.
2. Expose `run_strategy()` from that module.
3. Register the module in `DEFAULT_STRATEGIES` inside [main.py](main.py).
4. Assign a `capital_weight` for the strategy if you want the portfolio layer to split capital.
5. Reuse [alpaca_data_utils.py](alpaca_data_utils.py) for historical bars.
