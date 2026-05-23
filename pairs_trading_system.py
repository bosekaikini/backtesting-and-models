
# ─────────────────────────────────────────────────────────────────────────────
CONFIG = {
    # Alpaca credentials
    "API_KEY":    "PKVVYTWYE6225K3KL7PFZWH264",
    "SECRET_KEY": "CVia4fwtdRrdEjZbqvCniGwWTwhtaPqrEDAYgH7xWoaa",

    # Use paper endpoint for testing, live endpoint for real money
    "BASE_URL":   "https://paper-api.alpaca.markets/v2",   # paper
    # "BASE_URL": "https://api.alpaca.markets",         # live

    # ── Mode toggle ──────────────────────────────────────────────────────────
    # False → run vectorized backtest only
    # True  → connect WebSocket and execute live trades
    "RUN_LIVE": True,

    # ── Pair selection ───────────────────────────────────────────────────────
    "PAIR_DISCOVERY_MODULE": "find_correlated_assets",
    "SIGNAL_ENGINE_MODULE": None,
    "ASSET_A": "GLD",   # e.g. GLD / IAU, SPY / IVV, XOM / CVX
    "ASSET_B": "IAU",

    # ── Strategy parameters ──────────────────────────────────────────────────
    "Z_ENTRY":     1.5,    # |Z| threshold to open a position
    "Z_EXIT":      0.0,    # |Z| threshold to close a position
    "Z_STOPLOSS":  3.5,    # emergency stop
    "LOOKBACK":    30,     # rolling window (bars) for OU calibration
    "MIN_HALF_LIFE": 2,    # minimum acceptable half-life (bars)
    "MAX_HALF_LIFE": 120,  # maximum acceptable half-life (bars)

    # ── Risk / capital ───────────────────────────────────────────────────────
    "CAPITAL":         1_000.0,   # USD
    "KELLY_FRACTION":  0.25,      # quarter-Kelly
    "MAX_POSITION_PCT": 0.40,     # hard cap: max 40 % of capital per leg

    # ── Backtest data ────────────────────────────────────────────────────────
    # Provide two DataFrames with DatetimeIndex and 'close' column
    # (populated in __main__ section with Alpaca historical bars)
    "BACKTEST_START": "2023-01-01",
    "BACKTEST_END":   "2024-01-01",
    "BAR_TIMEFRAME":  "1Hour",    # "1Day" or "1Hour"
}
# ─────────────────────────────────────────────────────────────────────────────


import asyncio
import contextlib
import logging
from logging.handlers import RotatingFileHandler
import math
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from statsmodels.tsa.stattools import adfuller, coint

# Alpaca SDK
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.enums import DataFeed
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.live import StockDataStream
from alpaca_data_utils import fetch_stock_bars_frame
from strategy_loader import build_signal_engine, discover_trade_pair

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
LOG_FILE = "trade_activity.log"

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))

file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=1_000_000,
    backupCount=3,
    encoding="utf-8",
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))

logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
log = logging.getLogger("PairsTrader")


# ─────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class OUParams:
    """Calibrated Ornstein-Uhlenbeck parameters."""
    mu: float = 0.0        # long-run mean of spread
    theta: float = 0.0     # speed of mean reversion
    sigma: float = 1.0     # diffusion coefficient
    half_life: float = 0.0 # ln(2)/theta in bars
    hedge_ratio: float = 1.0
    valid: bool = False


@dataclass
class Signal:
    direction: int = 0   # +1 long spread, -1 short spread, 0 flat
    z_score: float = 0.0
    spread: float = 0.0
    ou: OUParams = field(default_factory=OUParams)


@dataclass
class Position:
    active: bool = False
    direction: int = 0     # +1 long spread, -1 short spread
    qty_a: float = 0.0     # signed shares of Asset A
    qty_b: float = 0.0     # signed shares of Asset B
    entry_z: float = 0.0
    entry_price_a: float = 0.0
    entry_price_b: float = 0.0
    entry_spread: float = 0.0
    pnl: float = 0.0


@dataclass
class PendingSignal:
    signal: Signal
    queued_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    reason: str = ""


# ═════════════════════════════════════════════════════════════════════════════
# COMPONENT 2 — OUModelEngine
# ═════════════════════════════════════════════════════════════════════════════
class OUModelEngine:
    """
    Calibrates Ornstein-Uhlenbeck parameters from rolling price windows
    and generates trading signals in real-time or vectorised contexts.

    Spread definition:  X_t = price_A - hedge_ratio * price_B
    """

    def __init__(
        self,
        lookback: int = 30,
        z_entry: float = 1.5,
        z_exit: float = 0.0,
        z_stop: float = 3.5,
        min_half_life: float = 2,
        max_half_life: float = 120,
    ):
        self.lookback = lookback
        self.z_entry = z_entry
        self.z_exit = z_exit
        self.z_stop = z_stop
        self.min_half_life = min_half_life
        self.max_half_life = max_half_life

        self._prices_a: deque = deque(maxlen=lookback)
        self._prices_b: deque = deque(maxlen=lookback)
        self.last_ou: OUParams = OUParams()
        self.last_signal: Signal = Signal()

    # ── Calibration ──────────────────────────────────────────────────────────

    @staticmethod
    def _ols_hedge_ratio(a: np.ndarray, b: np.ndarray) -> float:
        """OLS regression: a = hedge * b + intercept."""
        b_with_const = np.column_stack([b, np.ones(len(b))])
        result = np.linalg.lstsq(b_with_const, a, rcond=None)
        hedge = float(result[0][0])
        return hedge

    @staticmethod
    def _fit_ou_mle(spread: np.ndarray) -> Tuple[float, float, float]:
        """
        MLE fit of OU parameters from a discrete time series.
        Discretised OU:  X_{t+1} - X_t = theta*(mu - X_t)*dt + sigma*eps
        with dt = 1 (bar unit).
        """
        n = len(spread) - 1
        x0 = spread[:-1]
        x1 = spread[1:]

        def neg_log_likelihood(params):
            theta, mu, sigma = params
            if theta <= 0 or sigma <= 0:
                return 1e12
            exp_decay = math.exp(-theta)
            ex = mu + (x0 - mu) * exp_decay
            var = sigma**2 * (1 - exp_decay**2) / (2 * theta)
            if var <= 0:
                return 1e12
            return 0.5 * n * math.log(2 * math.pi * var) + np.sum((x1 - ex)**2) / (2 * var)

        # initial guess via OLS on AR(1)
        try:
            ar_coef = np.polyfit(x0, x1, 1)
            theta0 = max(1e-4, -math.log(ar_coef[0]))
            mu0 = float(np.mean(spread))
            sigma0 = float(np.std(np.diff(spread)))
            res = minimize(
                neg_log_likelihood,
                x0=[theta0, mu0, sigma0],
                method="Nelder-Mead",
                options={"maxiter": 2000, "xatol": 1e-6, "fatol": 1e-6},
            )
            theta, mu, sigma = res.x
            return max(1e-6, float(theta)), float(mu), max(1e-8, float(sigma))
        except Exception:
            mu = float(np.mean(spread))
            sigma = float(np.std(spread))
            return 0.1, mu, sigma

    def calibrate(self, prices_a: np.ndarray, prices_b: np.ndarray) -> OUParams:
        """Full calibration: hedge ratio → spread → ADF → OU fit."""
        if len(prices_a) < self.lookback or len(prices_b) < self.lookback:
            return OUParams()

        try:
            hedge = self._ols_hedge_ratio(prices_a, prices_b)
            if hedge <= 0:
                return OUParams()

            spread = prices_a - hedge * prices_b

            # Stationarity check
            adf_stat, adf_pvalue, *_ = adfuller(spread, maxlag=1, autolag=None)
            if adf_pvalue > 0.10:   # relaxed for rolling windows
                return OUParams()

            theta, mu, sigma = self._fit_ou_mle(spread)
            half_life = math.log(2) / theta if theta > 0 else float("inf")

            valid = self.min_half_life <= half_life <= self.max_half_life

            return OUParams(
                mu=mu,
                theta=theta,
                sigma=sigma,
                half_life=half_life,
                hedge_ratio=hedge,
                valid=valid,
            )
        except Exception as exc:
            log.debug("Calibration error: %s", exc)
            return OUParams()

    # ── Z-score & Signal ─────────────────────────────────────────────────────

    def compute_zscore(self, spread: float, ou: OUParams) -> float:
        if not ou.valid or ou.sigma == 0:
            return 0.0
        return (spread - ou.mu) / ou.sigma

    def evaluate(
        self,
        price_a: float,
        price_b: float,
        current_position: Position,
    ) -> Signal:
        """
        Push new prices, recalibrate if window is full, return current signal.
        """
        self._prices_a.append(price_a)
        self._prices_b.append(price_b)

        if len(self._prices_a) < self.lookback:
            return Signal()

        arr_a = np.array(self._prices_a)
        arr_b = np.array(self._prices_b)

        ou = self.calibrate(arr_a, arr_b)
        self.last_ou = ou

        if not ou.valid:
            return Signal()

        spread = price_a - ou.hedge_ratio * price_b
        z = self.compute_zscore(spread, ou)

        direction = self._determine_direction(z, current_position)
        sig = Signal(direction=direction, z_score=z, spread=spread, ou=ou)
        self.last_signal = sig
        return sig

    def _determine_direction(self, z: float, pos: Position) -> int:
        """State machine for entry / exit / stop signals."""
        if pos.active:
            # Stop-loss overrides everything
            if abs(z) >= self.z_stop:
                return 0  # close
            # Normal mean-reversion exit
            if pos.direction == 1 and z >= self.z_exit:
                return 0
            if pos.direction == -1 and z <= self.z_exit:
                return 0
            return pos.direction  # hold

        # Entry conditions
        if z > self.z_entry:
            return -1   # short the spread
        if z < -self.z_entry:
            return 1    # long the spread
        return 0


# ═════════════════════════════════════════════════════════════════════════════
# COMPONENT 1 — PairsBacktester
# ═════════════════════════════════════════════════════════════════════════════
class PairsBacktester:
    """
    Vectorised backtest engine.

    Parameters
    ----------
    df_a, df_b : pd.DataFrame
        Must contain a 'close' column with a shared DatetimeIndex.
    config     : dict
        Subset of the top-level CONFIG dictionary.
    """

    def __init__(self, df_a: pd.DataFrame, df_b: pd.DataFrame, config: dict):
        self.cfg = config
        self.capital = config["CAPITAL"]
        self.lookback = config["LOOKBACK"]
        self.z_entry = config["Z_ENTRY"]
        self.z_exit = config["Z_EXIT"]
        self.z_stop = config["Z_STOPLOSS"]
        self.kelly_frac = config["KELLY_FRACTION"]
        self.max_pos_pct = config["MAX_POSITION_PCT"]

        # Align on common index
        merged = pd.concat(
            [df_a["close"].rename("A"), df_b["close"].rename("B")], axis=1
        ).dropna()
        self.data = merged
        self.engine = OUModelEngine(
            lookback=self.lookback,
            z_entry=self.z_entry,
            z_exit=self.z_exit,
            z_stop=self.z_stop,
            min_half_life=config["MIN_HALF_LIFE"],
            max_half_life=config["MAX_HALF_LIFE"],
        )

    # ── Rolling calibration ───────────────────────────────────────────────────

    def _rolling_params(self) -> pd.DataFrame:
        """Compute rolling hedge, spread, mu, sigma, z-score."""
        n = len(self.data)
        lb = self.lookback
        records = []

        for i in range(lb, n):
            window = self.data.iloc[i - lb: i]
            prices_a = window["A"].values
            prices_b = window["B"].values
            ou = self.engine.calibrate(prices_a, prices_b)

            current_a = self.data["A"].iloc[i]
            current_b = self.data["B"].iloc[i]
            if ou.valid:
                spread = current_a - ou.hedge_ratio * current_b
                z = (spread - ou.mu) / ou.sigma
            else:
                spread = z = np.nan

            records.append({
                "date": self.data.index[i],
                "price_a": current_a,
                "price_b": current_b,
                "hedge": ou.hedge_ratio if ou.valid else np.nan,
                "spread": spread,
                "mu": ou.mu if ou.valid else np.nan,
                "sigma": ou.sigma if ou.valid else np.nan,
                "half_life": ou.half_life if ou.valid else np.nan,
                "z": z,
                "valid": ou.valid,
            })

        return pd.DataFrame(records).set_index("date")

    # ── Signal generation ─────────────────────────────────────────────────────

    def _generate_signals(self, params: pd.DataFrame) -> pd.DataFrame:
        """State-machine signal generation matching OUModelEngine logic."""
        df = params.copy()
        df["signal"] = 0    # +1 long spread, -1 short spread
        df["position"] = 0

        active = False
        direction = 0
        for idx in df.index:
            row = df.loc[idx]
            if not row["valid"] or np.isnan(row["z"]):
                df.at[idx, "signal"] = 0
                df.at[idx, "position"] = 0
                active = False
                direction = 0
                continue

            z = row["z"]
            if active:
                if abs(z) >= self.z_stop:
                    df.at[idx, "signal"] = 0   # stop
                    active = False
                    direction = 0
                elif direction == 1 and z >= self.z_exit:
                    df.at[idx, "signal"] = 0   # exit long
                    active = False
                    direction = 0
                elif direction == -1 and z <= self.z_exit:
                    df.at[idx, "signal"] = 0   # exit short
                    active = False
                    direction = 0
                else:
                    df.at[idx, "signal"] = direction   # hold
            else:
                if z > self.z_entry:
                    df.at[idx, "signal"] = -1
                    active = True
                    direction = -1
                elif z < -self.z_entry:
                    df.at[idx, "signal"] = 1
                    active = True
                    direction = 1
                else:
                    df.at[idx, "signal"] = 0

            df.at[idx, "position"] = direction

        return df

    # ── P&L simulation ────────────────────────────────────────────────────────

    def _simulate_pnl(self, signals: pd.DataFrame) -> pd.DataFrame:
        """Compute bar-by-bar P&L using a fixed $capital per signal event."""
        df = signals.copy()
        df["trade_pnl"] = 0.0
        df["equity"] = self.capital

        equity = self.capital
        pos = Position()
        trade_returns: List[float] = []

        for i, idx in enumerate(df.index):
            row = df.loc[idx]
            new_sig = int(row["signal"])
            pa = row["price_a"]
            pb = row["price_b"]

            # Close existing position if direction changed or went flat
            if pos.active and (new_sig != pos.direction):
                pnl_a = pos.qty_a * (pa - pos.entry_price_a)
                pnl_b = pos.qty_b * (pb - pos.entry_price_b)
                pnl = pnl_a + pnl_b
                equity += pnl
                df.at[idx, "trade_pnl"] = pnl
                trade_returns.append(pnl / (self.capital * self.max_pos_pct))
                pos = Position()

            # Open new position
            if new_sig != 0 and not pos.active:
                alloc = equity * self.max_pos_pct
                hedge = row["hedge"] if not np.isnan(row["hedge"]) else 1.0
                # dollar-neutral sizing
                qty_a = alloc / pa
                qty_b = alloc / pb * hedge
                if new_sig == 1:    # long spread: long A, short B
                    pos = Position(
                        active=True, direction=1,
                        qty_a=qty_a, qty_b=-qty_b,
                        entry_price_a=pa, entry_price_b=pb,
                        entry_z=row["z"], entry_spread=row["spread"],
                    )
                else:               # short spread: short A, long B
                    pos = Position(
                        active=True, direction=-1,
                        qty_a=-qty_a, qty_b=qty_b,
                        entry_price_a=pa, entry_price_b=pb,
                        entry_z=row["z"], entry_spread=row["spread"],
                    )

            df.at[idx, "equity"] = equity

        # Close any residual position at last bar
        if pos.active:
            last = df.iloc[-1]
            pnl = pos.qty_a * (last["price_a"] - pos.entry_price_a) + \
                  pos.qty_b * (last["price_b"] - pos.entry_price_b)
            trade_returns.append(pnl / (self.capital * self.max_pos_pct))

        df["equity"] = df["equity"].ffill()
        return df, trade_returns

    # ── Performance metrics ───────────────────────────────────────────────────

    @staticmethod
    def _max_drawdown(equity: pd.Series) -> float:
        rolling_max = equity.cummax()
        drawdown = (equity - rolling_max) / rolling_max
        return float(drawdown.min())

    @staticmethod
    def _sharpe(returns: pd.Series, freq: int = 252) -> float:
        if returns.std() == 0:
            return 0.0
        return float(returns.mean() / returns.std() * math.sqrt(freq))

    @staticmethod
    def _sortino(returns: pd.Series, freq: int = 252) -> float:
        downside = returns[returns < 0].std()
        if downside == 0:
            return 0.0
        return float(returns.mean() / downside * math.sqrt(freq))

    @staticmethod
    def _kelly_params(trade_returns: List[float]) -> Tuple[float, float, float]:
        """
        Returns (win_rate p, profit_factor b, kelly_fraction f*).
        """
        if not trade_returns:
            return 0.0, 0.0, 0.0
        wins = [r for r in trade_returns if r > 0]
        losses = [r for r in trade_returns if r < 0]
        p = len(wins) / len(trade_returns)
        avg_win = np.mean(wins) if wins else 0.0
        avg_loss = abs(np.mean(losses)) if losses else 1e-9
        b = avg_win / avg_loss
        f_star = (p * (b + 1) - 1) / b if b > 0 else 0.0
        return p, b, f_star

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self) -> Dict:
        """
        Execute the full backtest.

        Returns
        -------
        dict with keys: metrics, signals_df, kelly_p, kelly_b, kelly_f
        """
        log.info("Backtester: computing rolling OU parameters …")
        params = self._rolling_params()

        log.info("Backtester: generating signals …")
        signals = self._generate_signals(params)

        log.info("Backtester: simulating P&L …")
        results, trade_returns = self._simulate_pnl(signals)

        equity = results["equity"]
        bar_returns = equity.pct_change().dropna()

        total_return = (equity.iloc[-1] - self.capital) / self.capital
        max_dd = self._max_drawdown(equity)
        sharpe = self._sharpe(bar_returns)
        sortino = self._sortino(bar_returns)
        n_trades = int((signals["signal"].diff().abs() > 0).sum() // 2)

        p, b, f_star = self._kelly_params(trade_returns)
        kelly_quarter = self.kelly_frac * f_star

        metrics = {
            "total_return_pct":  round(total_return * 100, 2),
            "max_drawdown_pct":  round(max_dd * 100, 2),
            "sharpe_ratio":      round(sharpe, 3),
            "sortino_ratio":     round(sortino, 3),
            "win_rate_p":        round(p, 4),
            "profit_factor_b":   round(b, 4),
            "kelly_f_star":      round(f_star, 4),
            "kelly_quarter":     round(kelly_quarter, 4),
            "n_trades":          n_trades,
            "final_equity":      round(float(equity.iloc[-1]), 2),
        }

        log.info("─" * 60)
        log.info("BACKTEST RESULTS")
        for k, v in metrics.items():
            log.info("  %-25s %s", k, v)
        log.info("─" * 60)

        return {
            "metrics": metrics,
            "signals_df": results,
            "kelly_p": p,
            "kelly_b": b,
            "kelly_f": kelly_quarter,
        }


# ═════════════════════════════════════════════════════════════════════════════
# COMPONENT 3 — AlpacaTradingBot (live async engine)
# ═════════════════════════════════════════════════════════════════════════════
class AlpacaTradingBot:
    """
    Asynchronous live trading bot using Alpaca WebSocket streams + REST API.

    Lifecycle:
      1. Fetch historical bars to warm up the OU model.
      2. Subscribe to live minute bars for both symbols.
      3. On every incoming bar, call OUModelEngine.evaluate().
      4. If signal direction differs from current position, execute both legs.
    """

    RECONNECT_DELAY = 5    # seconds before WebSocket reconnect attempt
    MAX_RECONNECTS  = 20

    def __init__(self, config: dict, kelly_p: float = 0.5, kelly_b: float = 1.0, engine=None):
        self.cfg = config
        self.asset_a = config["ASSET_A"]
        self.asset_b = config["ASSET_B"]
        self.capital = config["CAPITAL"]
        self.kelly_frac = config["KELLY_FRACTION"]
        self.max_pos_pct = config["MAX_POSITION_PCT"]

        # Position state
        self.position = Position()

        # OU engine
        self.engine = engine or OUModelEngine(
            lookback=config["LOOKBACK"],
            z_entry=config["Z_ENTRY"],
            z_exit=config["Z_EXIT"],
            z_stop=config["Z_STOPLOSS"],
            min_half_life=config["MIN_HALF_LIFE"],
            max_half_life=config["MAX_HALF_LIFE"],
        )

        # Kelly sizing — feed from backtest if available
        self.kelly_p = kelly_p
        self.kelly_b = kelly_b

        # Latest prices buffer for fractional share calculation
        self._latest_price: Dict[str, float] = {self.asset_a: 0.0, self.asset_b: 0.0}
        self._reconnect_count = 0
        self.pending_signal: Optional[PendingSignal] = None
        self._pending_signal_task: Optional[asyncio.Task] = None

        # Alpaca REST client
        self.trading_client = TradingClient(
            api_key=config["API_KEY"],
            secret_key=config["SECRET_KEY"],
            paper=(config["BASE_URL"] == "https://paper-api.alpaca.markets"),
        )

        # Alpaca Historical data client (for warm-up)
        self.hist_client = StockHistoricalDataClient(
            api_key=config["API_KEY"],
            secret_key=config["SECRET_KEY"],
        )

        # Optional runtime helpers injected by orchestrator
        self.results_store = config.get("RESULTS_STORE")
        self.risk_manager = config.get("RISK_MANAGER")
        self.strategy_name = config.get("STRATEGY_NAME", "pairs_trading")
        self._last_exposure = 0.0

        log.info("AlpacaTradingBot initialised for %s / %s", self.asset_a, self.asset_b)

    def _market_is_open(self) -> bool:
        try:
            return bool(self.trading_client.get_clock().is_open)
        except Exception as exc:
            log.warning("Market clock unavailable (%s). Falling back to ET session check.", exc)
            now_et = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
            if now_et.weekday() >= 5:
                return False
            return (now_et.hour, now_et.minute) >= (9, 30) and (now_et.hour, now_et.minute) < (16, 0)

    def _queue_signal(self, signal: Signal, reason: str):
        self.pending_signal = PendingSignal(signal=signal, reason=reason)
        log.info(
            "QUEUED SIGNAL | reason=%s direction=%+d z=%.3f valid=%s",
            reason,
            signal.direction,
            signal.z_score,
            signal.ou.valid,
        )

    async def _apply_signal(self, signal: Signal):
        needs_action = signal.direction != (self.position.direction if self.position.active else 0)
        if not needs_action:
            return

        if signal.direction == 0 and self.position.active:
            log.info("EXIT SIGNAL | z=%.3f", signal.z_score)
            await self._close_all()
            return

        if signal.direction != 0:
            log.info("ENTRY SIGNAL | direction=%+d z=%.3f", signal.direction, signal.z_score)
            if self.position.active:
                await self._close_all()
            await self._execute_orders(signal)

    async def _pending_signal_loop(self):
        while True:
            await asyncio.sleep(60)
            if self.pending_signal is None:
                continue
            if not self._market_is_open():
                continue
            queued = self.pending_signal
            self.pending_signal = None
            log.info(
                "MARKET OPEN | executing queued signal from %s (%s)",
                queued.queued_at.isoformat(),
                queued.reason,
            )
            await self._apply_signal(queued.signal)

    # ── Kelly-based position sizing ───────────────────────────────────────────

    def _kelly_allocation(self) -> float:
        """
        Quarter-Kelly fraction of capital allocated per leg.
        f* = (p*(b+1) - 1) / b
        """
        p, b = self.kelly_p, self.kelly_b
        if b <= 0:
            return 0.0
        f_star = (p * (b + 1) - 1) / b
        kelly_q = self.kelly_frac * max(0.0, f_star)
        # Apply hard cap
        allocation = min(kelly_q, self.max_pos_pct) * self.capital
        return allocation

    # ── Historical warm-up ────────────────────────────────────────────────────

    async def _warmup(self):
        """Fetch recent historical bars and seed the OU engine buffer."""
        log.info("Warming up OU model with historical bars …")
        timeframe_map = {
            "1Hour": TimeFrame(1, TimeFrameUnit.Hour),
            "1Day":  TimeFrame(1, TimeFrameUnit.Day),
        }
        tf = timeframe_map.get(self.cfg["BAR_TIMEFRAME"], TimeFrame(1, TimeFrameUnit.Hour))

        request = StockBarsRequest(
            symbol_or_symbols=[self.asset_a, self.asset_b],
            timeframe=tf,
            limit=self.cfg["LOOKBACK"] + 10,
        )

        try:
            bars = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.hist_client.get_stock_bars(request)
            )
        except Exception as exc:
            log.warning(
                "Warm-up data fetch failed (%s). Continuing with a cold start.",
                exc,
            )
            return False

        df_map: Dict[str, pd.DataFrame] = {}
        for sym in [self.asset_a, self.asset_b]:
            sym_bars = bars.data.get(sym, [])
            if not sym_bars:
                log.warning("No historical bars for %s", sym)
                continue
            prices = [b.close for b in sym_bars]
            df_map[sym] = prices

        if self.asset_a not in df_map or self.asset_b not in df_map:
            log.error("Warm-up failed — missing bars. Proceeding with cold start.")
            return False

        prices_a = df_map[self.asset_a]
        prices_b = df_map[self.asset_b]
        n = min(len(prices_a), len(prices_b))

        # Feed historical prices into the engine buffer (no signal generation)
        for i in range(n):
            self.engine._prices_a.append(prices_a[i])
            self.engine._prices_b.append(prices_b[i])
            self._latest_price[self.asset_a] = prices_a[i]
            self._latest_price[self.asset_b] = prices_b[i]

        log.info("Warm-up complete. Buffer: %d bars.", len(self.engine._prices_a))
        return True

    # ── Order execution ───────────────────────────────────────────────────────

    async def _execute_orders(self, signal: Signal):
        """
        Place both legs of a spread trade simultaneously via asyncio.gather.
        Handles fractional shares.
        """
        alloc = self._kelly_allocation()
        if alloc < 1.0:
            log.warning("Allocation too small ($%.2f). Skipping trade.", alloc)
            return

        # Risk check (per-strategy and cross-strategy)
        if self.risk_manager:
            try:
                allowed = self.risk_manager.can_execute_order(self.strategy_name, alloc)
            except Exception:
                allowed = True
            if not allowed:
                log.warning("RiskManager blocked order for %s (alloc=$%.2f)", self.strategy_name, alloc)
                return

        pa = self._latest_price[self.asset_a]
        pb = self._latest_price[self.asset_b]
        if pa == 0 or pb == 0:
            log.warning("Zero price — skipping order.")
            return

        hedge = signal.ou.hedge_ratio
        qty_a_notional = alloc
        qty_b_notional = alloc * hedge

        qty_a = round(qty_a_notional / pa, 6)
        qty_b = round(qty_b_notional / pb, 6)

        if signal.direction == 1:   # long spread: +A, -B
            side_a, side_b = OrderSide.BUY, OrderSide.SELL
        else:                        # short spread: -A, +B
            side_a, side_b = OrderSide.SELL, OrderSide.BUY

        async def place(symbol: str, side: OrderSide, qty: float):
            try:
                req = MarketOrderRequest(
                    symbol=symbol,
                    qty=qty,
                    side=side,
                    time_in_force=TimeInForce.DAY,
                )
                order = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self.trading_client.submit_order(req)
                )
                log.info(
                    "ORDER PLACED | %s %s %s @ market | id=%s",
                    side.value, qty, symbol, order.id,
                )
                return order
            except Exception as exc:
                log.error("Order failed for %s: %s", symbol, exc)
                return None

        results = await asyncio.gather(
            place(self.asset_a, side_a, qty_a),
            place(self.asset_b, side_b, qty_b),
        )

        if all(r is not None for r in results):
            if signal.direction != 0:
                self.position = Position(
                    active=True,
                    direction=signal.direction,
                    qty_a=qty_a * (1 if side_a == OrderSide.BUY else -1),
                    qty_b=qty_b * (1 if side_b == OrderSide.BUY else -1),
                    entry_price_a=pa,
                    entry_price_b=pb,
                    entry_z=signal.z_score,
                    entry_spread=signal.spread,
                )
            else:
                self.position = Position()
            log.info(
                "POSITION UPDATE | direction=%+d z=%.3f spread=%.4f",
                signal.direction, signal.z_score, signal.spread,
            )
            # record exposure and persistence
            exposure_notional = qty_a_notional + qty_b_notional
            self._last_exposure = exposure_notional
            if self.risk_manager:
                try:
                    self.risk_manager.record_exposure(self.strategy_name, exposure_notional)
                except Exception:
                    pass
            if self.results_store:
                try:
                    from datetime import datetime
                    self.results_store.save_trade_event({
                        "ts": datetime.now().isoformat(),
                        "strategy": self.strategy_name,
                        "symbol": f"{self.asset_a}/{self.asset_b}",
                        "side": "ENTRY",
                        "qty": exposure_notional,
                        "price": float(pa),
                        "note": f"entry_z={signal.z_score}",
                    })
                except Exception:
                    log.debug("Failed to persist trade event.")

    async def _close_all(self):
        """Flatten both legs at market."""
        if not self.position.active:
            return
        log.info("Closing all positions …")
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, self.trading_client.close_all_positions
            )
            # reduce exposure tracking
            if self.risk_manager and self._last_exposure:
                try:
                    self.risk_manager.record_exposure(self.strategy_name, -self._last_exposure)
                except Exception:
                    pass
                self._last_exposure = 0.0
            # persist close event
            if self.results_store:
                try:
                    from datetime import datetime
                    self.results_store.save_trade_event({
                        "ts": datetime.now().isoformat(),
                        "strategy": self.strategy_name,
                        "symbol": f"{self.asset_a}/{self.asset_b}",
                        "side": "EXIT",
                        "qty": 0.0,
                        "price": 0.0,
                        "note": "closed_all",
                    })
                except Exception:
                    log.debug("Failed to persist close event.")
            self.position = Position()
        except Exception as exc:
            log.error("Failed to close positions: %s", exc)

    # ── WebSocket handler ─────────────────────────────────────────────────────

    def _build_stream(self) -> StockDataStream:
        return StockDataStream(
            api_key=self.cfg["API_KEY"],
            secret_key=self.cfg["SECRET_KEY"],
            feed=DataFeed.IEX,   # use DataFeed.SIP for live account
        )

    async def _on_bar(self, bar):
        """Callback invoked for each incoming minute bar."""
        symbol = bar.symbol
        price = float(bar.close)
        self._latest_price[symbol] = price

        # Only evaluate when both prices are fresh
        if (self._latest_price[self.asset_a] == 0
                or self._latest_price[self.asset_b] == 0):
            return

        pa = self._latest_price[self.asset_a]
        pb = self._latest_price[self.asset_b]

        signal = self.engine.evaluate(pa, pb, self.position)

        # persist signal to results store if provided
        if self.results_store:
            try:
                self.results_store.save_signal(self.strategy_name, symbol, signal.direction, signal.z_score)
            except Exception:
                log.debug("Failed to persist signal.")

        log.info(
            "LIVE SIGNAL | %s z=%.3f dir=%+d valid=%s pos=%+d lookback=%d",
            symbol,
            signal.z_score,
            signal.direction,
            signal.ou.valid,
            self.position.direction if self.position.active else 0,
            self.engine.lookback,
        )

        log.debug(
            "BAR %s $%.4f | z=%.3f valid=%s dir=%+d",
            symbol, price, signal.z_score,
            signal.ou.valid, signal.direction,
        )

        needs_action = (
            signal.direction != (self.position.direction if self.position.active else 0)
        )
        if needs_action:
            if self._market_is_open():
                await self._apply_signal(signal)
            else:
                self._queue_signal(signal, reason="market closed")

    # ── Main run loop ─────────────────────────────────────────────────────────

    async def run(self):
        """Entry point: warm up → connect WebSocket → stream forever."""
        await self._warmup()
        self._pending_signal_task = asyncio.create_task(self._pending_signal_loop())

        try:
            while self._reconnect_count <= self.MAX_RECONNECTS:
                stream = self._build_stream()
                stream.subscribe_bars(self._on_bar, self.asset_a, self.asset_b)

                log.info(
                    "Connecting to Alpaca WebSocket … (attempt %d)",
                    self._reconnect_count + 1,
                )
                try:
                    await asyncio.to_thread(stream.run)
                except asyncio.CancelledError:
                    log.info("Stream cancelled by user — shutting down.")
                    await self._close_all()
                    break
                except Exception as exc:
                    self._reconnect_count += 1
                    if self._reconnect_count > self.MAX_RECONNECTS:
                        log.critical(
                            "Max reconnects (%d) exceeded. Exiting.", self.MAX_RECONNECTS
                        )
                        await self._close_all()
                        raise

                    wait = self.RECONNECT_DELAY * min(self._reconnect_count, 6)
                    log.warning(
                        "WebSocket disconnected (%s). Reconnecting in %ds …", exc, wait
                    )
                    await asyncio.sleep(wait)
                else:
                    # Clean exit from stream
                    self._reconnect_count = 0
        finally:
            if self._pending_signal_task is not None:
                self._pending_signal_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._pending_signal_task

        log.info("AlpacaTradingBot stopped.")


# ═════════════════════════════════════════════════════════════════════════════
# Helpers — Alpaca historical data fetch
# ═════════════════════════════════════════════════════════════════════════════

def fetch_historical_bars(
    cfg: dict,
    symbol: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Fetch daily or hourly OHLCV from Alpaca historical API.
    Returns DataFrame with DatetimeIndex and columns: open, high, low, close, volume.
    """
    client = StockHistoricalDataClient(
        api_key=cfg["API_KEY"],
        secret_key=cfg["SECRET_KEY"],
    )
    return fetch_stock_bars_frame(
        client,
        symbol,
        datetime.fromisoformat(start).replace(tzinfo=timezone.utc),
        datetime.fromisoformat(end).replace(tzinfo=timezone.utc),
        cfg["BAR_TIMEFRAME"],
    )


def select_pair_from_correlation_scan(cfg: dict) -> Optional[Tuple[str, str, Dict]]:
    """
    Discover correlated pairs using find_correlated_assets.py and select the
    top-ranked candidate (fastest mean reversion).

    Returns
    -------
    tuple(symbol_a, symbol_b) when found, else None.
    """
    try:
        return discover_trade_pair("find_correlated_assets", cfg)
    except Exception as exc:
        log.warning("Correlation scan failed (%s). Using configured pair.", exc)
        return None


# ═════════════════════════════════════════════════════════════════════════════
# Entry point
# ═════════════════════════════════════════════════════════════════════════════

def run_strategy(runtime_config: Optional[dict] = None):
    cfg = dict(CONFIG)
    if runtime_config:
        cfg.update(runtime_config)
    if "CAPITAL" in cfg:
        log.info(
            "Strategy capital set to $%.2f (portfolio weight=%s)",
            float(cfg["CAPITAL"]),
            cfg.get("PORTFOLIO_WEIGHT", "n/a"),
        )

    selected = discover_trade_pair(cfg["PAIR_DISCOVERY_MODULE"], cfg)
    if selected is None:
        selected = select_pair_from_correlation_scan(cfg)

    if selected is None:
        log.info(
            "Using configured pair from CONFIG: %s/%s",
            cfg["ASSET_A"],
            cfg["ASSET_B"],
        )
    else:
        sym_a, sym_b, metrics = selected
        cfg["ASSET_A"] = sym_a
        cfg["ASSET_B"] = sym_b
        log.info(
            "Selected pair from discovery module: %s/%s (corr=%.3f, half-life=%.1f bars, ADF p=%.4f)",
            sym_a,
            sym_b,
            metrics.get("correlation", float("nan")),
            metrics.get("half_life", float("nan")),
            metrics.get("adf_pvalue", float("nan")),
        )

    signal_engine = None
    if cfg.get("SIGNAL_ENGINE_MODULE"):
        signal_engine = build_signal_engine(
            cfg["SIGNAL_ENGINE_MODULE"],
            cfg,
            cfg["ASSET_A"],
            cfg["ASSET_B"],
            lambda: None,
        )

    if not cfg["RUN_LIVE"]:
        # ── BACKTEST MODE ────────────────────────────────────────────────────
        log.info("Mode: BACKTEST")
        log.info(
            "Fetching historical bars for %s and %s …",
            cfg["ASSET_A"], cfg["ASSET_B"],
        )
        df_a = fetch_historical_bars(
            cfg, cfg["ASSET_A"], cfg["BACKTEST_START"], cfg["BACKTEST_END"]
        )
        df_b = fetch_historical_bars(
            cfg, cfg["ASSET_B"], cfg["BACKTEST_START"], cfg["BACKTEST_END"]
        )
        log.info(
            "Bars fetched: %s=%d, %s=%d",
            cfg["ASSET_A"], len(df_a), cfg["ASSET_B"], len(df_b),
        )

        # Run initial cointegration test on full sample
        aligned = pd.concat(
            [df_a["close"].rename("A"), df_b["close"].rename("B")], axis=1
        ).dropna()
        _, pvalue, _ = coint(aligned["A"], aligned["B"])
        log.info("Full-sample cointegration p-value: %.4f", pvalue)
        if pvalue > 0.10:
            log.warning("Pair may not be cointegrated (p=%.4f). Proceed with caution.", pvalue)

        backtester = PairsBacktester(df_a, df_b, cfg)
        result = backtester.run()

        # Print Kelly sizing recommendation
        metrics = result["metrics"]
        kelly_q = result["kelly_f"]
        log.info(
            "Kelly sizing: allocate %.1f%% of capital ($%.2f) per leg.",
            kelly_q * 100,
            kelly_q * cfg["CAPITAL"],
        )
        return result

    else:
        # ── LIVE TRADING MODE ────────────────────────────────────────────────
        log.info("Mode: LIVE TRADING")
        log.warning(
            "LIVE mode active — real orders will be placed on %s!", cfg["BASE_URL"]
        )

        # Optionally run a quick backtest first to calibrate Kelly params
        kelly_p, kelly_b = 0.5, 1.0   # conservative defaults
        try:
            log.info("Running warm-up backtest to calibrate Kelly parameters …")
            df_a = fetch_historical_bars(
                cfg, cfg["ASSET_A"], cfg["BACKTEST_START"], cfg["BACKTEST_END"]
            )
            df_b = fetch_historical_bars(
                cfg, cfg["ASSET_B"], cfg["BACKTEST_START"], cfg["BACKTEST_END"]
            )
            bt = PairsBacktester(df_a, df_b, cfg)
            bt_result = bt.run()
            kelly_p = bt_result["kelly_p"]
            kelly_b = bt_result["kelly_b"]
            log.info("Kelly params from backtest: p=%.4f b=%.4f", kelly_p, kelly_b)
        except Exception as exc:
            log.warning("Pre-trade backtest failed (%s). Using defaults.", exc)

        bot = AlpacaTradingBot(cfg, kelly_p=kelly_p, kelly_b=kelly_b, engine=signal_engine)

        try:
            asyncio.run(bot.run())
        except KeyboardInterrupt:
            log.info("Keyboard interrupt received — shutting down gracefully.")


def main():
    return run_strategy()


if __name__ == "__main__":
    main()