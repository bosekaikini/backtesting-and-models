"""Example strategy module template.

Expose a `run_strategy(runtime_config: dict)` function that the orchestrator can call.
"""
from typing import Optional

def run_strategy(runtime_config: Optional[dict] = None):
    cfg = runtime_config or {}
    name = cfg.get("STRATEGY_NAME", "template_strategy")
    capital = cfg.get("CAPITAL", 0.0)
    print(f"Running {name} with capital ${capital}")
    # Implement strategy lifecycle here: warmup, subscribe, execute
    return {"status": "ok", "strategy": name}
