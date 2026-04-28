import sqlite3
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS bonds (
    ticker          TEXT PRIMARY KEY,
    product_label   TEXT,
    maturity_date   TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS periods (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT NOT NULL,
    period_index        INTEGER NOT NULL,
    period_label        TEXT NOT NULL,
    period_start_date   TEXT,
    rate_pct            REAL,
    pdf_path            TEXT,
    parsed_at           TIMESTAMP,
    UNIQUE(ticker, period_index),
    FOREIGN KEY(ticker) REFERENCES bonds(ticker)
);

CREATE TABLE IF NOT EXISTS interest_entries (
    period_id   INTEGER NOT NULL,
    day_offset  INTEGER NOT NULL,
    interest    REAL NOT NULL,
    PRIMARY KEY(period_id, day_offset),
    FOREIGN KEY(period_id) REFERENCES periods(id)
);

CREATE INDEX IF NOT EXISTS idx_periods_ticker ON periods(ticker);
CREATE INDEX IF NOT EXISTS idx_entries_period ON interest_entries(period_id);
"""


class Database:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._init()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init(self):
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    # ── Bonds ────────────────────────────────────────────────────────────────

    def upsert_bond(self, ticker: str, product_label: str, maturity_date: Optional[str]):
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO bonds (ticker, product_label, maturity_date, updated_at)
                   VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(ticker) DO UPDATE SET
                       product_label = excluded.product_label,
                       maturity_date = excluded.maturity_date,
                       updated_at    = CURRENT_TIMESTAMP""",
                (ticker, product_label, maturity_date),
            )

    def get_bond(self, ticker: str) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM bonds WHERE ticker = ?", (ticker,)).fetchone()
        return dict(row) if row else None

    def list_bonds(self) -> List[dict]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM bonds ORDER BY ticker").fetchall()
        return [dict(r) for r in rows]

    # ── Periods ──────────────────────────────────────────────────────────────

    def get_existing_period_labels(self, ticker: str) -> List[str]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT period_label FROM periods WHERE ticker = ? AND pdf_path IS NOT NULL",
                (ticker,),
            ).fetchall()
        return [r["period_label"] for r in rows]

    def save_period_with_entries(
        self,
        ticker: str,
        period_index: int,
        period_label: str,
        period_start_date: Optional[str],
        rate_pct: Optional[float],
        pdf_path: str,
        entries: List[Tuple[int, float]],  # (day_offset, interest)
    ):
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO periods
                       (ticker, period_index, period_label, period_start_date, rate_pct, pdf_path, parsed_at)
                   VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(ticker, period_index) DO UPDATE SET
                       period_label      = excluded.period_label,
                       period_start_date = excluded.period_start_date,
                       rate_pct          = excluded.rate_pct,
                       pdf_path          = excluded.pdf_path,
                       parsed_at         = CURRENT_TIMESTAMP""",
                (ticker, period_index, period_label, period_start_date, rate_pct, pdf_path),
            )
            period_id = conn.execute(
                "SELECT id FROM periods WHERE ticker = ? AND period_index = ?",
                (ticker, period_index),
            ).fetchone()["id"]

            conn.execute("DELETE FROM interest_entries WHERE period_id = ?", (period_id,))
            conn.executemany(
                "INSERT INTO interest_entries (period_id, day_offset, interest) VALUES (?, ?, ?)",
                [(period_id, d, i) for d, i in entries],
            )

    def get_periods_with_entries(self, ticker: str) -> List[dict]:
        with self._conn() as conn:
            periods = conn.execute(
                """SELECT id, period_index, period_start_date, rate_pct, period_label
                   FROM periods WHERE ticker = ? AND pdf_path IS NOT NULL
                   ORDER BY period_index""",
                (ticker,),
            ).fetchall()

            result = []
            for p in periods:
                entries = conn.execute(
                    """SELECT day_offset, interest FROM interest_entries
                       WHERE period_id = ? ORDER BY day_offset""",
                    (p["id"],),
                ).fetchall()
                result.append(
                    {
                        "period_index": p["period_index"],
                        "period_label": p["period_label"],
                        "period_start_date": p["period_start_date"],
                        "rate_pct": p["rate_pct"],
                        "entries": [(e["day_offset"], e["interest"]) for e in entries],
                    }
                )
        return result

    def get_status(self) -> dict:
        with self._conn() as conn:
            bonds = conn.execute("SELECT COUNT(*) as n FROM bonds").fetchone()["n"]
            periods = conn.execute(
                "SELECT COUNT(*) as n FROM periods WHERE pdf_path IS NOT NULL"
            ).fetchone()["n"]
            entries = conn.execute("SELECT COUNT(*) as n FROM interest_entries").fetchone()["n"]
        return {"bonds": bonds, "periods": periods, "interest_entries": entries}
