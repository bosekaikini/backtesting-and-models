"""Helpers for loading pluggable discovery and signal modules from main."""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any, Callable, Optional, Tuple


def load_module(module_name: Optional[str]) -> Optional[ModuleType]:
    if not module_name:
        return None
    return importlib.import_module(module_name)


def discover_trade_pair(module_name: Optional[str], cfg: dict) -> Optional[Tuple[str, str, dict]]:
    module = load_module(module_name)
    if module is None:
        return None

    if hasattr(module, "discover_trade_pair"):
        return module.discover_trade_pair(cfg)

    if hasattr(module, "AssetCorrelationEngine"):
        engine_config = dict(getattr(module, "CONFIG", cfg))
        engine_config["API_KEY"] = cfg["API_KEY"]
        engine_config["SECRET_KEY"] = cfg["SECRET_KEY"]
        engine = module.AssetCorrelationEngine(engine_config)
        pairs = engine.find_correlated_pairs()
        if not pairs:
            return None
        sym_a, sym_b, metrics = pairs[0]
        return sym_a, sym_b, metrics

    raise AttributeError(
        f"Strategy module '{module_name}' must define discover_trade_pair() or AssetCorrelationEngine."
    )


def build_signal_engine(
    module_name: Optional[str],
    cfg: dict,
    asset_a: str,
    asset_b: str,
    default_factory: Callable[[], Any],
) -> Any:
    module = load_module(module_name)
    if module is None:
        return default_factory()

    if hasattr(module, "create_signal_engine"):
        return module.create_signal_engine(cfg, asset_a, asset_b)

    return default_factory()