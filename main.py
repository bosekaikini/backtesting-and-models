"""Orchestrator entrypoint for running one or more strategy modules."""

from __future__ import annotations

import asyncio
import importlib
import logging
import inspect
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from portfolio_allocator import allocate_capital
from results_store import ResultsStore
from risk_manager import RiskManager
import os
try:
    import yaml
except Exception:
    yaml = None


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("StrategyMain")


@dataclass
class StrategySpec:
    name: str
    module: str
    enabled: bool = True
    capital_weight: float = 1.0
    config: Dict[str, Any] = field(default_factory=dict)


DEFAULT_STRATEGIES: List[StrategySpec] = [
    StrategySpec(name="pairs_trading", module="pairs_trading_system", capital_weight=1.0),
]


PORTFOLIO_CAPITAL = 1000.0


def load_strategy_module(module_name: str):
    return importlib.import_module(module_name)


async def run_strategy_spec(spec: StrategySpec):
    if not spec.enabled:
        log.info("Skipping disabled strategy: %s", spec.name)
        return None

    module = load_strategy_module(spec.module)
    if hasattr(module, "run_strategy"):
        if inspect.iscoroutinefunction(module.run_strategy):
            return await module.run_strategy(spec.config)
        return await asyncio.to_thread(module.run_strategy, spec.config)

    if hasattr(module, "main"):
        if inspect.iscoroutinefunction(module.main):
            return await module.main()
        return await asyncio.to_thread(module.main)

    raise AttributeError(f"Strategy module '{spec.module}' must expose run_strategy() or main().")


async def run_all_strategies(strategies: Optional[List[StrategySpec]] = None):
    specs = strategies or DEFAULT_STRATEGIES
    # Load registry from config/strategies.yaml when available
    registry_path = os.path.join("config", "strategies.yaml")
    try:
        with open(registry_path, "r", encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
            specs = [
                StrategySpec(
                    name=s.get("name"),
                    module=s.get("module"),
                    enabled=s.get("enabled", True),
                    capital_weight=float(s.get("capital_weight", 1.0)),
                    config=s.get("config", {}),
                )
                for s in doc.get("strategies", [])
            ]
    except Exception:
        specs = strategies or DEFAULT_STRATEGIES
    active_specs = [spec for spec in specs if spec.enabled]
    allocations = allocate_capital(
        PORTFOLIO_CAPITAL,
        {spec.name: spec.capital_weight for spec in active_specs},
    )

    # Instantiate global helpers: results store and risk manager
    results = ResultsStore(path=os.path.join("results.db"))
    risk = RiskManager(total_capital=PORTFOLIO_CAPITAL)

    allocation_map = {allocation.strategy_name: allocation for allocation in allocations}
    enriched_specs: List[StrategySpec] = []
    for spec in active_specs:
        allocation = allocation_map[spec.name]
        merged_config = dict(spec.config)
        merged_config["CAPITAL"] = allocation.capital
        merged_config["PORTFOLIO_WEIGHT"] = allocation.weight
        # Inject runtime helpers
        merged_config["RESULTS_STORE"] = results
        merged_config["RISK_MANAGER"] = risk
        merged_config["STRATEGY_NAME"] = spec.name
        # Register strategy with risk manager
        try:
            risk.register_strategy(spec.name, allocation.capital)
        except Exception:
            pass
        enriched_specs.append(
            StrategySpec(
                name=spec.name,
                module=spec.module,
                enabled=spec.enabled,
                capital_weight=spec.capital_weight,
                config=merged_config,
            )
        )

    tasks = [asyncio.create_task(run_strategy_spec(spec)) for spec in enriched_specs]
    return await asyncio.gather(*tasks, return_exceptions=True)


def main():
    results = asyncio.run(run_all_strategies())
    for spec, result in zip(DEFAULT_STRATEGIES, results):
        if isinstance(result, Exception):
            log.error("Strategy %s failed: %s", spec.name, result)
        else:
            log.info("Strategy %s finished: %s", spec.name, result)


if __name__ == "__main__":
    main()