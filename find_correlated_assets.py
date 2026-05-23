import logging
import math
from datetime import datetime, timedelta
from typing import List, Tuple, Dict

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from statsmodels.tsa.stattools import adfuller, coint

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca_data_utils import fetch_close_series

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

CONFIG = {
    # Alpaca credentials
    "API_KEY":    "PKVVYTWYE6225K3KL7PFZWH264",
    "SECRET_KEY": "CVia4fwtdRrdEjZbqvCniGwWTwhtaPqrEDAYgH7xWoaa",
    
    # Historical data parameters
    "LOOKBACK_DAYS": 252,      # ~1 year of daily data
    "LOOKBACK_BARS": 60,       # rolling window for OU calibration
    "BAR_TIMEFRAME": "1Day",   # daily bars for slower drift
    
    # Cointegration thresholds
    "MIN_CORRELATION": 0.60,   # minimum Pearson correlation
    "MAX_ADFSTAT_PVALUE": 0.05, # ADF p-value threshold (< 0.05 = stationary spread)
    "MIN_HALF_LIFE": 2,        # minimum acceptable mean-reversion period (bars)
    "MAX_HALF_LIFE": 252,      # maximum acceptable mean-reversion period
    
    # Asset universe to scan
    "ASSET_UNIVERSE": [
        # Gold ETFs
        "GLD", "IAU", "GLDM",
        # S&P 500 variants
        "SPY", "IVV", "VOO",
        # Energy
        "XLE", "OIL", "USO",
        # Tech
        "QQQ", "XLK",
        # Semiconductors
        "XSD", "SMH",
        # Financial
        "XLF", "IYF",
        # Healthcare
        "XLV", "VHT",
        # Consumer
        "XLY", "XLP",
    ],
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("AssetCorrelationFinder")


# ─────────────────────────────────────────────────────────────────────────────
# Correlation & Cointegration Engine
# ─────────────────────────────────────────────────────────────────────────────

class AssetCorrelationEngine:
    """Discovers correlated and cointegrated asset pairs."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.client = StockHistoricalDataClient(config["API_KEY"], config["SECRET_KEY"])
    
    def fetch_asset_prices(self, symbol: str, days_back: int) -> pd.Series:
        """Fetch OHLCV bars for a single asset."""
        try:
            return fetch_close_series(
                self.client,
                symbol,
                days_back,
                self.config.get("BAR_TIMEFRAME", "1Day"),
            )
        except Exception as e:
            log.error(f"Error fetching {symbol}: {e}")
            return pd.Series(dtype=float)
    
    def calculate_hedge_ratio(self, prices_a: np.ndarray, prices_b: np.ndarray) -> float:
        """Calculate optimal hedge ratio via linear regression."""
        hedge = np.polyfit(prices_b, prices_a, 1)[0]
        return float(hedge)
    
    @staticmethod
    def _fit_ou_mle(spread: np.ndarray) -> Tuple[float, float, float]:
        """Fit Ornstein-Uhlenbeck via MLE. Returns (theta, mu, sigma)."""
        n = len(spread) - 1
        x0 = spread[:-1]
        x1 = spread[1:]
        
        def neg_ll(params):
            theta, mu, sigma = params
            if sigma <= 0:
                return 1e10
            dt = 1.0
            exp_theta = np.exp(-theta * dt)
            predicted_mean = mu * (1 - exp_theta) + x0 * exp_theta
            residuals = x1 - predicted_mean
            ll = -0.5 * np.sum((residuals ** 2) / (sigma ** 2)) - n * np.log(sigma)
            return -ll
        
        try:
            mu0 = float(np.mean(spread))
            sigma0 = float(np.std(np.diff(spread)))
            x0_init = 0.05
            result = minimize(neg_ll, [x0_init, mu0, sigma0], method="Nelder-Mead")
            if result.success:
                theta, mu, sigma = result.x
                theta = max(theta, 1e-6)
                sigma = abs(sigma)
                return float(theta), float(mu), float(sigma)
        except Exception:
            pass
        
        mu = float(np.mean(spread))
        sigma = float(np.std(spread))
        return 0.05, mu, sigma
    
    def assess_cointegration(self, prices_a: np.ndarray, prices_b: np.ndarray) -> Dict:
        """Test if two assets form a cointegrated pair."""
        hedge = self.calculate_hedge_ratio(prices_a, prices_b)
        spread = prices_a - hedge * prices_b
        
        # ADF test for stationarity
        adf_stat, adf_pvalue, *_ = adfuller(spread, maxlag=1, autolag=None)
        
        # Johansen cointegration test (alternative metric)
        try:
            _, pvalue_coint, *_ = coint(prices_a, prices_b)
        except Exception:
            pvalue_coint = 1.0
        
        # OU calibration
        theta, mu, sigma = self._fit_ou_mle(spread)
        half_life = math.log(2) / theta if theta > 0 else float("inf")
        
        return {
            "hedge_ratio": hedge,
            "adf_stat": adf_stat,
            "adf_pvalue": adf_pvalue,
            "pvalue_coint": pvalue_coint,
            "theta": theta,
            "half_life": half_life,
            "mu": mu,
            "sigma": sigma,
            "spread_mean": float(np.mean(spread)),
            "spread_std": float(np.std(spread)),
        }
    
    def find_correlated_pairs(self) -> List[Tuple[str, str, Dict]]:
        """Scan all asset pairs, return ranked cointegrated candidates."""
        
        log.info(f"Fetching price data for {len(self.config['ASSET_UNIVERSE'])} assets...")
        price_data = {}
        
        for symbol in self.config["ASSET_UNIVERSE"]:
            prices = self.fetch_asset_prices(symbol, self.config["LOOKBACK_DAYS"])
            if len(prices) > self.config["LOOKBACK_BARS"]:
                price_data[symbol] = prices
            else:
                log.warning(f"Insufficient data for {symbol}")
        
        if len(price_data) < 2:
            log.error("Not enough assets with sufficient data")
            return []
        
        log.info(f"Testing {len(price_data)} assets for cointegration...")
        
        # Test all pairs
        candidate_pairs = []
        symbols = list(price_data.keys())
        
        for i, sym_a in enumerate(symbols):
            for sym_b in symbols[i + 1 :]:
                prices_a = price_data[sym_a].values
                prices_b = price_data[sym_b].values
                
                # Quick correlation filter
                corr = np.corrcoef(prices_a, prices_b)[0, 1]
                if abs(corr) < self.config["MIN_CORRELATION"]:
                    continue
                
                # Cointegration assessment
                metrics = self.assess_cointegration(prices_a, prices_b)
                
                # Filter by ADF p-value and half-life
                if metrics["adf_pvalue"] > self.config["MAX_ADFSTAT_PVALUE"]:
                    continue
                if (metrics["half_life"] < self.config["MIN_HALF_LIFE"] or 
                    metrics["half_life"] > self.config["MAX_HALF_LIFE"]):
                    continue
                
                metrics["correlation"] = corr
                candidate_pairs.append((sym_a, sym_b, metrics))
        
        # Sort by half-life (faster mean reversion = lower half-life = better)
        candidate_pairs.sort(key=lambda x: x[2]["half_life"])
        
        return candidate_pairs


# ─────────────────────────────────────────────────────────────────────────────
# Output & Reporting
# ─────────────────────────────────────────────────────────────────────────────

def print_results(pairs: List[Tuple[str, str, Dict]]):
    """Pretty-print results."""
    if not pairs:
        print("No cointegrated pairs found.")
        return
    
    print("\n" + "=" * 110)
    print("COINTEGRATED ASSET PAIRS (sorted by half-life)")
    print("=" * 110)
    print(f"{'Rank':<5} {'Pair':<12} {'Corr':<8} {'HL(bars)':<10} {'ADF p':<10} {'Hedge':<8} {'Coint p':<10}")
    print("-" * 110)
    
    for idx, (sym_a, sym_b, metrics) in enumerate(pairs[:20], 1):  # Top 20
        pair_name = f"{sym_a}/{sym_b}"
        half_life = metrics["half_life"]
        hedge = metrics["hedge_ratio"]
        
        print(
            f"{idx:<5} {pair_name:<12} {metrics['correlation']:>7.3f} "
            f"{half_life:>9.1f} {metrics['adf_pvalue']:>9.4f} "
            f"{hedge:>7.2f} {metrics['pvalue_coint']:>9.4f}"
        )
    
    print("=" * 110)
    print(f"\nTotal candidates: {len(pairs)}")
    print("\nTop 5 recommendations (fastest mean reversion):")
    for idx, (sym_a, sym_b, metrics) in enumerate(pairs[:5], 1):
        print(f"  {idx}. {sym_a}/{sym_b}: half-life={metrics['half_life']:.1f} bars, "
              f"correlation={metrics['correlation']:.3f}, ADF p={metrics['adf_pvalue']:.4f}")


def discover_trade_pair(config: Dict):
    """Standard discovery hook for use by main or a module loader."""
    engine = AssetCorrelationEngine(config)
    pairs = engine.find_correlated_pairs()
    if not pairs:
        return None
    return pairs[0]


def save_results_csv(pairs: List[Tuple[str, str, Dict]], filename: str = "correlated_pairs.csv"):
    """Save results to CSV."""
    rows = []
    for sym_a, sym_b, metrics in pairs:
        rows.append({
            "asset_a": sym_a,
            "asset_b": sym_b,
            "correlation": metrics["correlation"],
            "hedge_ratio": metrics["hedge_ratio"],
            "half_life_bars": metrics["half_life"],
            "adf_stat": metrics["adf_stat"],
            "adf_pvalue": metrics["adf_pvalue"],
            "coint_pvalue": metrics["pvalue_coint"],
            "spread_mean": metrics["spread_mean"],
            "spread_std": metrics["spread_std"],
        })
    
    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False)
    log.info(f"Results saved to {filename}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    engine = AssetCorrelationEngine(CONFIG)
    pairs = engine.find_correlated_pairs()
    
    print_results(pairs)
    if pairs:
        save_results_csv(pairs)
        print("\n✓ To use a discovered pair, update pairs_trading_system.py CONFIG:")
        sym_a, sym_b, metrics = pairs[0]
        print(f'    "ASSET_A": "{sym_a}",')
        print(f'    "ASSET_B": "{sym_b}",')
