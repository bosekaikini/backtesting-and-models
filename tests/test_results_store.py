import os
from results_store import ResultsStore


def test_results_store_init(tmp_path):
    db_path = tmp_path / "test_results.db"
    store = ResultsStore(path=str(db_path))
    # should create file
    assert os.path.exists(str(db_path))
