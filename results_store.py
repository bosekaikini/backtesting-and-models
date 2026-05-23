import sqlite3
import threading
from typing import Dict, Optional


class ResultsStore:
    def __init__(self, path: str = "results.db"):
        self.path = path
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT,
                    strategy TEXT,
                    symbol TEXT,
                    side TEXT,
                    qty REAL,
                    price REAL,
                    note TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT,
                    strategy TEXT,
                    symbol TEXT,
                    direction INTEGER,
                    z_score REAL,
                    note TEXT
                )
                """
            )
            conn.commit()

    def _connect(self):
        return sqlite3.connect(self.path, check_same_thread=False)

    def save_trade_event(self, event: Dict):
        with self._lock, self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO trade_events (ts, strategy, symbol, side, qty, price, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    event.get("ts"),
                    event.get("strategy"),
                    event.get("symbol"),
                    event.get("side"),
                    event.get("qty"),
                    event.get("price"),
                    event.get("note"),
                ),
            )
            conn.commit()

    def save_signal(self, strategy: str, symbol: str, direction: int, z_score: float, note: Optional[str] = None):
        with self._lock, self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO signals (ts, strategy, symbol, direction, z_score, note) VALUES (datetime('now'), ?, ?, ?, ?, ?)",
                (strategy, symbol, direction, z_score, note),
            )
            conn.commit()
