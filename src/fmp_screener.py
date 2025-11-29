# fmp_screener.py
# Minimal script: fetch FMP biggest-gainers, parse fields, upsert into scanner.db -> inplay table
# Usage: python fmp_screener.py
# Or import fetch_and_store_gainers and call from your app.

import requests
import sqlite3
import time
import json
import re
from pathlib import Path
from typing import Optional, List, Dict
import os
from dotenv import load_dotenv

# Load environment variables from .env file at startup
load_dotenv()

BASE = Path(__file__).parent
DB_PATH = BASE / "scanner.db"

# ---------------------------
# DB helpers
# ---------------------------
def init_inplay_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS inplay (
            ticker TEXT PRIMARY KEY,
            raw_json TEXT,
            changes_pct REAL,
            price REAL,
            ts INTEGER
        )
        """
    )
    conn.commit()
    conn.close()

def upsert_inplay_raw(ticker: str, raw_obj: dict, changes_pct: Optional[float], price: Optional[float]):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "REPLACE INTO inplay (ticker, raw_json, changes_pct, price, ts) VALUES (?, ?, ?, ?, ?)",
        (ticker, json.dumps(raw_obj), changes_pct, price, int(time.time())),
    )
    conn.commit()
    conn.close()

# ---------------------------
# Parsing helpers
# ---------------------------
_pct_re = re.compile(r"[-+]?[0-9]*\.?[0-9]+")

def parse_changes_pct(val) -> Optional[float]:
    """
    Parse fields like '12.34%', '(+12.34%)', '+12.34%', 12.34, None -> float or None
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val)
    m = _pct_re.search(s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None

def parse_price(val) -> Optional[float]:
    """
    Parse price if numeric or numeric-like string; else None
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None

# ---------------------------
# FMP call
# ---------------------------
FMP_BASE = "https://financialmodelingprep.com/stable"

def fetch_fmp_gainers(api_key: str, top_n: int = 10, timeout: int = 8) -> List[Dict]:
    """
    Call FMP biggest-gainers endpoint and return a list of raw item dicts (limited to top_n).
    """
    if not api_key:
        raise ValueError("FMP API key required")
    url = f"{FMP_BASE}/biggest-gainers"
    params = {"apikey": api_key}
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error {r.status_code}: {r.text}")
        print(f"URL: {url}")
        print(f"Possible causes:")
        print(f"  - API key invalid or expired: '{api_key[:10]}...'")
        print(f"  - API key lacks permissions for this endpoint")
        print(f"  - FMP account subscription level too low")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        raise
    data = r.json()
    # The endpoint returns a list; ensure we have a list
    if isinstance(data, dict):
        # defensive: try common keys
        for k in ("mostGainerLoser", "items", "gainers", "data"):
            if k in data and isinstance(data[k], list):
                data = data[k]
                break
        else:
            # if still a dict, wrap into list (best effort)
            data = [data]
    if not isinstance(data, list):
        return []
    return data[:top_n]

# ---------------------------
# Main: fetch, parse, upsert
# ---------------------------
def fetch_and_store_gainers(api_key: str, top_n: int = 10):
    init_inplay_table()
    items = fetch_fmp_gainers(api_key, top_n=top_n)
    if not items:
        print("No gainers returned by FMP.")
        return

    inserted = 0
    for it in items:
        # robustly find ticker symbol
        ticker = it.get("symbol") or it.get("ticker") or it.get("s") or it.get("code")
        if not ticker:
            # skip items without a symbol
            continue
        # parse changesPercentage (various field names possible)
        raw_change = it.get("changesPercentage") or it.get("changePercent") or it.get("changesPercentage")
        changes_pct = parse_changes_pct(raw_change)

        # try to find price in common fields
        raw_price = it.get("price") or it.get("currentPrice") or it.get("last") or it.get("price")
        price = parse_price(raw_price)

        # upsert raw object and numeric fields
        upsert_inplay_raw(ticker, it, changes_pct, price)
        inserted += 1

    print(f"Inserted/updated {inserted} tickers into inplay table.")

# ---------------------------
# CLI runner
# ---------------------------
if __name__ == "__main__":
    # API key is loaded from .env by load_dotenv() above
    FMP_KEY = os.getenv("FMP_API_KEY", "").strip()
    if not FMP_KEY:
        print("Error: FMP_API_KEY not found in environment or .env file.")
        print("Please add 'FMP_API_KEY=your_key_here' to .env and try again.")
        exit(1)
    fetch_and_store_gainers(FMP_KEY, top_n=10)
    print("Done.")
