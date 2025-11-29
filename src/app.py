import asyncio
import time
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import yaml
import yfinance as yf
import sqlite3

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "scanner.db"
CONFIG_PATH = BASE_DIR / "config.yaml"

app = FastAPI()
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


# --- DB helpers ---
def init_db():
    """Create the `inplay` table if it doesn't exist.

    Keeps DB usage minimal and local to sqlite.
    """
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


def get_all_inplay():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT ticker, reason, entry, last_price, ts FROM inplay")
    rows = c.fetchall()
    conn.close()
    return [dict(ticker=r[0], reason=r[1], entry=r[2], last_price=r[3], ts=r[4]) for r in rows]


# --- Scanner logic (very simple gap scan) ---
async def scanner_loop(config: dict):
    """Continuously scan tickers from config for gap moves.

    config keys:
      - scan_interval_seconds (int)
      - gap_threshold_percent (float)
      - tickers (list[str])
    """
    interval = config.get("scan_interval_seconds", 10)
    gap_threshold = config.get("gap_threshold_percent", 10.0)
    tickers = config.get("tickers", [])

    while True:
        for t in tickers:
            try:
                # use yfinance to get previous close and current price
                tk = yf.Ticker(t)
                hist = tk.history(period="2d", interval="1d")
                if hist.empty or len(hist["Close"]) < 2:
                    continue
                prev_close = hist["Close"].iloc[-2]

                # current price via fastest available (may be delayed); fallback to yesterday close
                latest = tk.history(period="1d", interval="1m")
                if not latest.empty:
                    last_price = float(latest["Close"].iloc[-1])
                else:
                    last_price = float(hist["Close"].iloc[-1])

                gap_pct = (last_price - prev_close) / prev_close * 100.0
                if gap_pct >= gap_threshold:
                    reason = f"gap {gap_pct:.2f}%"
                    entry = f"{prev_close:.2f} -> {last_price:.2f}"
                    upsert_inplay(t, reason, entry, last_price)
            except Exception as e:
                # keep scanning; in real project log exception
                print(f"scanner error for {t}: {e}")

        await asyncio.sleep(interval)


# --- Startup event to load config and start scanner ---
@app.on_event("startup")
async def startup_event():
    init_db()
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f)
    else:
        config = {"scan_interval_seconds": 10, "gap_threshold_percent": 5.0, "tickers": ["AAPL"]}

    # run scanner in background
    asyncio.create_task(scanner_loop(config))


# --- Endpoints ---
@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/inplay")
async def inplay():
    return JSONResponse(content={"inplay": get_all_inplay()})


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
