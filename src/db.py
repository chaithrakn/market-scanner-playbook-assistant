# db.py
# Minimal DB helper for scanner project (sqlite3)
# Provides safe init + schema migration + simple CRUD for `inplay` table.

import sqlite3
from pathlib import Path
import json
import time
from typing import Optional, List, Dict, Set, Any

BASE = Path(__file__).parent
DB_PATH = BASE / "scanner.db"

DEFAULT_SCHEMA = {
    "ticker": "TEXT PRIMARY KEY",
    "raw_json": "TEXT",
    "changes_pct": "REAL",
    "price": "REAL",
    "ts": "INTEGER"
}


def _connect():
    """Return a sqlite3.Connection (caller should close or use context manager)."""
    return sqlite3.connect(DB_PATH)


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]  # second field is column name


def init_db():
    """
    Ensure the database and `inplay` table exist with the columns in DEFAULT_SCHEMA.
    If the table exists but is missing some columns, add them (non-destructive migration).
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        # create table if not exists with at least the primary key column
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS inplay (ticker TEXT PRIMARY KEY)"
        )
        conn.commit()

        existing_cols = _table_columns(conn, "inplay")
        # add missing columns based on DEFAULT_SCHEMA
        for col, ctype in DEFAULT_SCHEMA.items():
            if col not in existing_cols:
                # skip ticker since table created with ticker
                if col == "ticker":
                    continue
                sql = f"ALTER TABLE inplay ADD COLUMN {col} {ctype}"
                cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def upsert_inplay_raw(ticker: str, raw_obj: Dict[str, Any], changes_pct: Optional[float], price: Optional[float]):
    """
    Insert or replace an entry in inplay.
    raw_obj is stored as JSON string in raw_json.
    """
    init_db()  # ensure schema exists
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "REPLACE INTO inplay (ticker, raw_json, changes_pct, price, ts) VALUES (?, ?, ?, ?, ?)",
            (ticker, json.dumps(raw_obj), changes_pct, price, int(time.time())),
        )
        conn.commit()
    finally:
        conn.close()


def get_all_inplay() -> List[Dict[str, Any]]:
    """
    Return all rows as dicts. Parses raw_json back into object if present.
    """
    init_db()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT ticker, raw_json, changes_pct, price, ts FROM inplay")
        rows = cur.fetchall()
        out = []
        for r in rows:
            ticker, raw_json, changes_pct, price, ts = r
            try:
                raw = json.loads(raw_json) if raw_json else None
            except Exception:
                raw = None
            out.append({
                "ticker": ticker,
                "raw": raw,
                "changes_pct": changes_pct,
                "price": price,
                "ts": ts
            })
        return out
    finally:
        conn.close()


def get_inplay_sorted_by_changes(limit: Optional[int] = None, desc: bool = True) -> List[Dict[str, Any]]:
    """
    Return rows ordered by changes_pct (nulls treated as -inf if desc).
    """
    init_db()
    conn = _connect()
    try:
        cur = conn.cursor()
        order = "DESC" if desc else "ASC"
        # Use COALESCE to push NULLs to the end for DESC (small trick)
        sql = f"SELECT ticker, raw_json, changes_pct, price, ts FROM inplay ORDER BY COALESCE(changes_pct, -999999) {order}"
        if limit:
            sql += f" LIMIT {int(limit)}"
        cur.execute(sql)
        rows = cur.fetchall()
        out = []
        for r in rows:
            ticker, raw_json, changes_pct, price, ts = r
            try:
                raw = json.loads(raw_json) if raw_json else None
            except Exception:
                raw = None
            out.append({
                "ticker": ticker,
                "raw": raw,
                "changes_pct": changes_pct,
                "price": price,
                "ts": ts
            })
        return out
    finally:
        conn.close()


def delete_not_inplay(keep: Set[str]):
    """
    Delete rows whose ticker is not in the keep set.
    Useful for replacing the inplay table with current top-n set.
    """
    init_db()
    conn = _connect()
    try:
        cur = conn.cursor()
        if not keep:
            cur.execute("DELETE FROM inplay")
        else:
            placeholders = ",".join("?" for _ in keep)
            sql = f"DELETE FROM inplay WHERE ticker NOT IN ({placeholders})"
            cur.execute(sql, tuple(keep))
        conn.commit()
    finally:
        conn.close()


def clear_inplay():
    init_db()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM inplay")
        conn.commit()
    finally:
        conn.close()


# optional convenience: fetch one ticker
def get_inplay_ticker(ticker: str) -> Optional[Dict[str, Any]]:
    init_db()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT ticker, raw_json, changes_pct, price, ts FROM inplay WHERE ticker = ?", (ticker,))
        r = cur.fetchone()
        if not r:
            return None
        ticker, raw_json, changes_pct, price, ts = r
        try:
            raw = json.loads(raw_json) if raw_json else None
        except Exception:
            raw = None
        return {"ticker": ticker, "raw": raw, "changes_pct": changes_pct, "price": price, "ts": ts}
    finally:
        conn.close()


# small test when run directly
if __name__ == "__main__":
    print("Initializing DB and printing current inplay rows (if any)...")
    init_db()
    rows = get_all_inplay()
    print(rows)
