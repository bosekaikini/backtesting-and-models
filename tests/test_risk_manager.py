from risk_manager import RiskManager


def test_risk_manager_register_and_check():
    rm = RiskManager(total_capital=1000.0, max_exposure_pct=0.5)
    rm.register_strategy("s1", 500.0)
    assert rm.can_execute_order("s1", 100.0)
    # consuming exposure
    rm.record_exposure("s1", 400.0)
    # remaining available exposure = 500*0.5 - 400 = -150 -> should block
    assert not rm.can_execute_order("s1", 200.0)
