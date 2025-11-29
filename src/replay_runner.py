"""
replay_runner.py (updated)

Usage:
    python replay_runner.py                  # uses previous trading day (original behavior)
    python replay_runner.py --date 2025-11-10  # replay for a specific date (YYYY-MM-DD)
    python replay_runner.py --date 2025-11-10 --speed 2.0  # slow replay (2x slower)
"""

import argparse
import yaml
import time
from pathlib import Path
import sqlite3
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.yaml"
DB_PATH = BASE_DIR / "scanner.db"


# ----------------------------
# DB helpers (compatible with app.py)
# ----------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS inplay (
            ticker TEXT PRIMARY KEY,
            reason TEXT,
            entry TEXT,
            last_price REAL,
            ts INTEGER
        )
        """
    )
    conn.commit()
    conn.close()


def upsert_inplay(ticker, reason, entry, last_price):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "REPLACE INTO inplay (ticker, reason, entry, last_price, ts) VALUES (?, ?, ?, ?, ?)",
        (ticker, reason, entry, last_price, int(time.time())),
    )
    conn.commit()
    conn.close()


# ----------------------------
# Signal engine (5-min)
# ----------------------------
def rolling_vwap(bars: pd.DataFrame):
    pv = (bars["Close"] * bars["Volume"]).cumsum()
    v = bars["Volume"].cumsum().replace(0, np.nan)
    return (pv / v).fillna(method="ffill")


def detect_long_wick(row, wick_ratio=0.6):
    body_top = max(row["Open"], row["Close"])
    upper_wick = row["High"] - body_top
    total_range = row["High"] - row["Low"] + 1e-9
    return (upper_wick / total_range) >= wick_ratio


def five_min_signals(bars: pd.DataFrame, premarket_high=None):
    bars = bars.copy().dropna(subset=["Close", "Volume"])
    bars = bars.sort_index()
    if bars.empty:
        return []

    vwap = rolling_vwap(bars)
    prev_close = bars["Close"].shift(1).fillna(bars["Open"])
    tr = np.maximum(bars["High"] - bars["Low"],
                    np.abs(bars["High"] - prev_close),
                    np.abs(bars["Low"] - prev_close))
    atr = tr.rolling(14, min_periods=1).mean().fillna(method="ffill")

    events = []
    entry_price_per_t = None
    for ts, row in bars.iterrows():
        price = float(row["Close"])
        cur_vwap = float(vwap.loc[ts])
        cur_atr = float(atr.loc[ts]) if ts in atr.index else float(atr.iloc[-1])

        # ENTRY: break above premarket high (if provided)
        if premarket_high is not None and price > premarket_high:
            events.append((ts, "ENTRY", "break_premarket", price))
            entry_price_per_t = price
            continue

        # ENTRY: reclaim VWAP
        if price > cur_vwap and row["Close"] > row["Open"]:
            events.append((ts, "ENTRY", "vwap_reclaim", price))
            entry_price_per_t = price
            continue

        # EXIT: price falls below VWAP
        if price < cur_vwap:
            events.append((ts, "EXIT", "vwap_loss", price))
            entry_price_per_t = None
            continue

        # EXIT: long upper wick
        if detect_long_wick(row):
            events.append((ts, "EXIT", "long_wick", price))
            entry_price_per_t = None
            continue

        # ATR stop if entry exists
        if entry_price_per_t is not None:
            stop = entry_price_per_t - 1.5 * cur_atr
            if price <= stop:
                events.append((ts, "EXIT", "atr_stop", price))
                entry_price_per_t = None
                continue

    return events


# ----------------------------
# Utilities: parse/normalize date & helper funcs
# ----------------------------
def parse_date_str(date_str):
    """Parse YYYY-MM-DD into a date object."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        raise ValueError("Date must be in YYYY-MM-DD format")


def filter_bars_for_date(df_5m: pd.DataFrame, target_date):
    """
    Return bars from df_5m that match `target_date` (a datetime.date).
    Works with tz-aware or naive DatetimeIndex by normalizing to date.
    """
    if df_5m.empty:
        return df_5m
    # Normalize index to date
    idx_dates = df_5m.index.normalize()
    target_ts = pd.to_datetime(target_date).date()
    return df_5m[idx_dates == pd.to_datetime(target_ts)]


def compute_premarket_high(df_5m: pd.DataFrame, market_open_hour=9, market_open_minute=30):
    """
    Compute the premarket high from df_5m using times strictly before market open time.
    Simple heuristic: uses local timestamps as-is (best-effort).
    """
    if df_5m.empty:
        return None
    pm_mask = []
    for ts in df_5m.index:
        # use hour/min of the timestamp
        if (ts.hour < market_open_hour) or (ts.hour == market_open_hour and ts.minute < market_open_minute):
            pm_mask.append(True)
        else:
            pm_mask.append(False)
    pm_df = df_5m[pd.Series(pm_mask, index=df_5m.index)]
    if pm_df.empty:
        return None
    return float(pm_df["High"].max())


# ----------------------------
# Main runner logic (date-aware)
# ----------------------------
def run_replay_for_date(target_date=None, speed=1.0):
    """
    If target_date is None, fall back to previous trading day logic.
    speed: replay speed multiplier (1.0 = default; >1.0 slows playback by that factor).
    """
    print("Replay runner starting...")
    init_db()

    # load config
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r") as f:
            cfg = yaml.safe_load(f) or {}
    else:
        cfg = {}

    tickers = cfg.get("tickers", ["AAPL", "TSLA", "AMD", "NVDA", "MSFT"])
    top_n = int(cfg.get("top_n_gappers", 10))
    gap_threshold = float(cfg.get("gap_threshold_percent", 0.1))

    # compute gap% per ticker using daily open vs prev close
    results = []
    for t in tickers:
        try:
            tk = yf.Ticker(t)
            d_hist = tk.history(period="10d", interval="1d", prepost=False)
            if d_hist.empty or "Open" not in d_hist.columns or "Close" not in d_hist.columns:
                print(f"no daily for {t}, skipping")
                continue

            if target_date is None:
                # previous trading day fallback: take last two rows
                if len(d_hist) < 2:
                    continue
                prev_close = float(d_hist["Close"].iloc[-2])
                open_today = float(d_hist["Open"].iloc[-1])
                gap_pct = (open_today - prev_close) / prev_close * 100.0
                results.append((t, gap_pct, prev_close, open_today))
            else:
                # if target_date provided, try to find that date in d_hist index
                # convert index to dates
                dates = d_hist.index.normalize().to_pydatetime()
                # find matching date row for target_date
                matched = None
                for idx in range(len(d_hist)):
                    row_date = d_hist.index[idx].date()
                    if row_date == target_date:
                        matched = idx
                        break
                if matched is None:
                    # skip ticker if we don't have daily data for target date
                    print(f"{t}: no daily row for {target_date}, skipping")
                    continue
                # need previous close (previous trading row)
                if matched == 0:
                    print(f"{t}: no previous day for {target_date}, skipping")
                    continue
                prev_close = float(d_hist["Close"].iloc[matched - 1])
                open_today = float(d_hist["Open"].iloc[matched])
                gap_pct = (open_today - prev_close) / prev_close * 100.0
                results.append((t, gap_pct, prev_close, open_today))
        except Exception as e:
            print(f"error computing gap for {t}: {e}")

    # sort and pick top N positive gappers above threshold
    results = sorted([r for r in results if r[1] >= gap_threshold], key=lambda x: x[1], reverse=True)
    top = results[:top_n]
    if not top:
        print("No gappers found by threshold. Exiting.")
        return

    print("Top gappers (open vs prev close):")
    for r in top:
        print(f"{r[0]}  gap%={r[1]:.2f}  prev_close={r[2]:.2f} open={r[3]:.2f}")

    # For each top ticker, fetch 5-minute bars for the request target date (or last trading day)
    for ticker, gap_pct, prev_close, open_price in top:
        print(f"\nReplaying {ticker} ...")
        tk = yf.Ticker(ticker)
        df_5m = tk.history(period="10d", interval="5m", prepost=True)
        if df_5m.empty:
            print(f"  no 5m bars for {ticker}, skipping")
            continue

        if target_date is None:
            # fallback to the most recent trading day if no explicit date provided
            last_day = df_5m.index.normalize().unique()[-1].date()
            day_bars = filter_bars_for_date(df_5m, last_day)
            used_date = last_day
        else:
            day_bars = filter_bars_for_date(df_5m, target_date)
            used_date = target_date

        if day_bars.empty:
            print(f"  no bars for {used_date} for {ticker}, skipping")
            continue

        pm_high = compute_premarket_high(df_5m)
        print(f"  replaying {len(day_bars)} bars for {ticker} (date={used_date}) premarket_high={pm_high}")

        events = five_min_signals(day_bars, premarket_high=pm_high)
        if not events:
            print(f"  no signals detected for {ticker}")
            continue

        # Upsert events into inplay table (front-end will pick these up)
        for ev in events:
            ts, kind, reason, price = ev
            reason_str = f"{kind}:{reason}"
            entry = f"{price:.2f}"
            print(f"  event {ts} {reason_str} price={price:.2f}")
            upsert_inplay(ticker, reason_str, entry, float(price))
            # sleep scaled by speed (higher speed -> slower replay)
            time.sleep(0.2 * float(speed))

    print("Replay complete. Open the UI to view in-play tickers.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay top-N gappers for a given date")
    parser.add_argument("--date", type=str, help="Replay date in YYYY-MM-DD (exchange local date)")
    parser.add_argument("--speed", type=float, default=1.0, help="Replay speed multiplier (>1.0 = slower)")
    args = parser.parse_args()
    target = None
    if args.date:
        try:
            target = parse_date_str(args.date)
        except ValueError as e:
            print(f"Invalid date: {e}")
            raise SystemExit(1)
    run_replay_for_date(target_date=target, speed=args.speed)
