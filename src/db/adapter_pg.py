# src/db/adapter_pg.py
from __future__ import annotations
import asyncio
import asyncpg
import json
import os
import time
from typing import List, Optional

try:
    from core.models import InPlayRecord
except Exception:
    from src.core.models import InPlayRecord

DATABASE_URL = os.getenv("DATABASE_URL")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS inplay (
    ticker TEXT PRIMARY KEY,
    company TEXT,
    price DOUBLE PRECISION,
    change_pct DOUBLE PRECISION,
    float_shares BIGINT,
    market_cap DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    news JSONB,
    signal TEXT,
    ts BIGINT
);
"""

class DBAdapter:
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or DATABASE_URL
        self._pool: Optional[asyncpg.pool.Pool] = None

    async def init(self) -> None:
        if not self.database_url:
            raise RuntimeError("DATABASE_URL not set")
        self._pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=10)
        # ensure table exists
        async with self._pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)

    async def aclose(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def upsert_inplay(self, record: InPlayRecord) -> None:
        """Upsert with ts-based ordering. Only update when EXCLUDED.ts >= inplay.ts."""
        if not self._pool:
            raise RuntimeError("DBAdapter not initialized")
        news_json = json.dumps(record.news) if record.news is not None else None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO inplay (ticker, company, price, change_pct, float_shares, market_cap, volume, news, signal, ts)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10)
                ON CONFLICT (ticker) DO UPDATE
                  SET company = EXCLUDED.company,
                      price = EXCLUDED.price,
                      change_pct = EXCLUDED.change_pct,
                      float_shares = EXCLUDED.float_shares,
                      market_cap = EXCLUDED.market_cap,
                      volume = EXCLUDED.volume,
                      news = EXCLUDED.news,
                      signal = EXCLUDED.signal,
                      ts = EXCLUDED.ts
                  WHERE EXCLUDED.ts >= inplay.ts;
                """,
                record.ticker,
                getattr(record, "company", None),
                record.price,
                record.change_pct,
                record.float_shares,
                record.market_cap,
                record.volume,
                news_json,
                record.signal,
                record.ts,
            )

    async def get_inplay(self, ticker: str) -> Optional[InPlayRecord]:
        if not self._pool:
            raise RuntimeError("DBAdapter not initialized")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT ticker, company, price, change_pct, float_shares, market_cap, volume,
                       news, signal, ts
                FROM inplay WHERE ticker = $1
                """,
                ticker,
            )
        if not row:
            return None
        news_val = row["news"]
        # news is JSONB already -> asyncpg maps to Python list/dict, keep as is
        return InPlayRecord(
            ticker=row["ticker"],
            price=row["price"],
            change_pct=row["change_pct"],
            float_shares=row["float_shares"],
            market_cap=row["market_cap"],
            volume=row["volume"],
            news=news_val,
            signal=row["signal"],
            ts=row["ts"],
        )

    async def list_inplay(self) -> List[InPlayRecord]:
        if not self._pool:
            raise RuntimeError("DBAdapter not initialized")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ticker, company, price, change_pct, float_shares, market_cap, volume,
                       news, signal, ts
                FROM inplay
                ORDER BY ts DESC NULLS LAST
                """
            )
        results = []
        for row in rows:
            results.append(
                InPlayRecord(
                    ticker=row["ticker"],
                    price=row["price"],
                    change_pct=row["change_pct"],
                    float_shares=row["float_shares"],
                    market_cap=row["market_cap"],
                    volume=row["volume"],
                    news=row["news"],
                    signal=row["signal"],
                    ts=row["ts"],
                )
            )
        return results
