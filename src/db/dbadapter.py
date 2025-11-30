import asyncio
import aiosqlite
import json
import time
from typing import Callable, Awaitable, List, Optional

from core.models import InPlayRecord

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS inplay (
ticker TEXT PRIMARY KEY,
company TEXT,
price REAL,
change_pct REAL,
float_shares REAL,
market_cap REAL,
volume REAL,
news TEXT,
signal TEXT,
ts INTEGER
);
"""

class DBAdapter:

    def __init__(self, db_path:str="scanner.db") -> None:
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None
        self._subscribers: List[Callable[[InPlayRecord], Awaitable[None]]] = []
        self._sub_lock = asyncio.Lock()
        self._conn_lock = asyncio.Lock()

    async def init(self):
        """Initialize the database connection and ensure schema exists."""
        async with self._conn_lock:
            if self._conn:
                return
            self._conn = await aiosqlite.connect(self.db_path)
            await self._conn.execute(CREATE_TABLE_SQL)
            await self._conn.commit()

    async def close(self) -> None:
        """Close the DB connection."""
        async with self._conn_lock:
            if self._conn:
                await self._conn.close()
                self._conn = None
    

    async def upsert_inplay(self, record: InPlayRecord) -> None:
        """Idempotent upsert by `ticker` using `ts` to order updates.
            Behavior:
            - If no existing row -> insert
            - If existing row has ts <= record.ts -> update
            - If existing row has ts > record.ts -> skip (stale)
        """
        if not self._conn:
            raise RuntimeError("DBAdapter not initialized. Call .init() before use.")
        
        sql_select = "SELECT ts FROM inplay WHERE ticker = ?"
        async with self._conn.execute(sql_select, (record.ticker,)) as cursor:
            row = await cursor.fetchone()
        existing_ts = row[0] if row and row[0] is not None else None

        if existing_ts is not None and existing_ts > record.ts:
             return  # Stale update, skip
        
        params = (
            record.ticker,
            record.company,
            record.price,
            record.change_pct,
            record.float_shares,
            record.market_cap,
            record.volume,
            json.dumps(record.news) if record.news else None,
            record.signal,
            record.ts,
            )
        
        if existing_ts is None:
            sql = """
            INSERT INTO inplay (ticker, company, price, change_pct, float_shares, market_cap, volume, news, signal, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            await self._conn.execute(sql, params)
        else:
            sql = """
            UPDATE inplay
            SET company = ?, price = ?, change_pct = ?, float_shares = ?, market_cap = ?, volume = ?, news = ?, signal = ?, ts = ?
            WHERE ticker = ?
            """
            await self._conn.execute(sql, params[1:] + (record.ticker,))
        
        await self._conn.commit()

        # Notify subscribers but don't block DB writes on slow subscribers
        asyncio.create_task(self._notify_subscribers(record))

    async def _notify_subscribers(self, record: InPlayRecord) -> None:
        async with self._sub_lock:
        # copy to avoid mutation while iterating
            subs = list(self._subscribers)
        for cb in subs:
            try:
             # call and don't await each one sequentially; schedule them concurrently
                asyncio.create_task(cb(record))
            except Exception:
                # swallow subscriber exceptions to keep DB path safe
                pass

    async def get_inplay(self, ticker: str) -> Optional[InPlayRecord]:
        """Return the InPlayRecord for ticker or None."""
        if not self._conn:
            raise RuntimeError("DBAdapter not initialized. Call .init() before use.")

        sql = "SELECT ticker, company, price, change_pct, float_shares, market_cap, volume, news, signal, ts FROM inplay WHERE ticker = ?"
        async with self._conn.execute(sql, (ticker,)) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        return InPlayRecord(
            ticker=row[0],
            company=row[1],
            price=row[2],
            change_pct=row[3],
            float_shares=row[4],
            market_cap=row[5],
            volume=row[6],
            news=json.loads(row[7]) if row[7] else None,
            signal=row[8],
            ts=row[9],
        )

    async def list_inplay(self) -> List[InPlayRecord]:
        """Return all rows from the inplay table as InPlayRecord list."""
        if not self._conn:
            raise RuntimeError("DBAdapter not initialized. Call .init() before use.")

        sql = "SELECT ticker, company, price, change_pct, float_shares, market_cap, volume, news, signal, ts FROM inplay"
        records: List[InPlayRecord] = []
        async with self._conn.execute(sql) as cur:
            async for row in cur:
                records.append(
                    InPlayRecord(
                        ticker=row[0],
                        company=row[1],
                        price=row[2],
                        change_pct=row[3],
                        float_shares=row[4],
                        market_cap=row[5],
                        volume=row[6],
                        news=json.loads(row[7]) if row[7] else None,
                        signal=row[8],
                        ts=row[9],
                    )
                )
        return records
    
    async def subscribe_changes(self, callback: Callable[[InPlayRecord], Awaitable[None]]) -> None:
        """Register an async callback to be called for each upsert.

        The callback must be an `async def` function accepting one argument: InPlayRecord.
        Callers can use this to push updates to websockets or other listeners.
        """
        async with self._sub_lock:
            self._subscribers.append(callback)


    async def unsubscribe_changes(self, callback: Callable[[InPlayRecord], Awaitable[None]]) -> None:
        async with self._sub_lock:
            try:
                self._subscribers.remove(callback)
            except ValueError:
                pass

# helper to create and init DBAdapter instance (it's outside the class)
async def create_and_init(db_path: str = "scanner.db") -> DBAdapter:
    db = DBAdapter(db_path=db_path)
    await db.init()
    return db