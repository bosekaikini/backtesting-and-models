"""Simple portfolio allocation helpers for multi-strategy orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence


@dataclass(frozen=True)
class Allocation:
    strategy_name: str
    weight: float
    capital: float


def normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    positive_weights = {name: max(0.0, weight) for name, weight in weights.items()}
    total = sum(positive_weights.values())
    if total <= 0:
        raise ValueError("At least one strategy weight must be greater than zero.")
    return {name: weight / total for name, weight in positive_weights.items()}


def allocate_capital(total_capital: float, weights: Dict[str, float]) -> List[Allocation]:
    normalized = normalize_weights(weights)
    return [
        Allocation(strategy_name=name, weight=weight, capital=total_capital * weight)
        for name, weight in normalized.items()
    ]
