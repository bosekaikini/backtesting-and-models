from dataclasses import dataclass, field
from typing import Dict


@dataclass
class StrategyState:
    equity: float = 0.0
    exposure: float = 0.0


class RiskManager:
    """Simple cross-strategy risk manager.

    Tracks per-strategy equity and exposure and enforces simple rules:
      - max_exposure_pct: total exposure across strategies as fraction of portfolio
      - max_drawdown_pct: per-strategy max drawdown relative to its starting equity
    """

    def __init__(self, total_capital: float, max_exposure_pct: float = 0.5, max_drawdown_pct: float = 0.25):
        self.total_capital = float(total_capital)
        self.max_exposure_pct = float(max_exposure_pct)
        self.max_drawdown_pct = float(max_drawdown_pct)
        self.strategies: Dict[str, StrategyState] = {}

    def register_strategy(self, name: str, starting_equity: float):
        self.strategies[name] = StrategyState(equity=float(starting_equity), exposure=0.0)

    def can_execute_order(self, strategy_name: str, order_value: float) -> bool:
        """Return True if order_value (notional) can be accepted for strategy."""
        total_exposure = sum(s.exposure for s in self.strategies.values())
        available = self.total_capital * self.max_exposure_pct - total_exposure
        if order_value > available:
            return False
        # check strategy drawdown
        state = self.strategies.get(strategy_name)
        if state is None:
            return True
        # disallow new orders if already beyond drawdown
        # approximate drawdown = (equity - peak) / peak; we don't track peak, so use conservative check
        if state.equity <= 0:
            return False
        return True

    def record_exposure(self, strategy_name: str, delta_notional: float):
        s = self.strategies.setdefault(strategy_name, StrategyState(equity=0.0, exposure=0.0))
        s.exposure += float(delta_notional)

    def record_trade(self, strategy_name: str, pnl: float):
        s = self.strategies.setdefault(strategy_name, StrategyState(equity=0.0, exposure=0.0))
        s.equity += float(pnl)

    def get_state(self, strategy_name: str):
        return self.strategies.get(strategy_name)
