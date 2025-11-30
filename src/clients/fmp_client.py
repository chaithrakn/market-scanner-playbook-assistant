import aiohttp
import asyncio
import time
import json
import pprint
import re
from typing import Optional, List, Dict
import os
import sys
from dotenv import load_dotenv
from pathlib import Path
from datetime import date, timedelta
from alpaca_trade_api import REST


# Add parent directory (src) to path so we can import core and db modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.models import InPlayRecord
from db.dbadapter import DBAdapter

# Load environment variables from .env (in project root)
load_dotenv(Path(__file__).parent.parent.parent / ".env")

FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_KEY = os.getenv('FMP_API_KEY')
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')

async def fetch_float(ticker: str, timeout: int = 8) -> Optional[float]:
    """
    Fetch float shares for a given ticker from FMP API.
    Returns float shares as float or None if not found/error.
    """
    if not FMP_KEY:
        return None
    
    url = f"{FMP_BASE}/shares-float"
    params = {"symbol": ticker, "apikey": FMP_KEY}

    async with aiohttp.ClientSession() as session:
        try:
            response = await session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            data = await response.json()
            
            # Handle both list and dict responses
            if isinstance(data, list):
                if len(data) > 0:
                    float_shares = data[0].get("floatShares") if isinstance(data[0], dict) else None
                else:
                    float_shares = None
            elif isinstance(data, dict):
                float_shares = data.get("floatShares")
            else:
                return None
            
            if float_shares:
                return round(float(float_shares), 2)
        except aiohttp.ClientError as e:
            print(f"Warning: FMP API request failed for {ticker}: {e}")
        
    return None

async def fetch_latest_news(ticker: str, timeout: int = 8) -> Optional[List[str]]:
    """
    Fetch up to 5 latest Alpaca news URLs for a ticker.
    Returns: List[str] or None
    """
    API_KEY = os.getenv('ALPACA_API_KEY')
    API_SECRET = os.getenv('ALPACA_SECRET_KEY')

    if not API_KEY or not API_SECRET:
        return None

    # Alpaca REST client (sync)
    rest_client = REST(API_KEY, API_SECRET)

    today = date.today()
    from_date = today - timedelta(days=3)

    try:
        # asyncio.to_thread() runs a blocking sync function in a background thread,
        # submits task to the default threadpool executor
        news_items = await asyncio.to_thread(
            rest_client.get_news, ticker, from_date, today
        )
    except Exception as e:
        print(f"News fetch error for {ticker}: {e}")
        return None

    print("Fetched news items for", ticker, news_items)
    if not news_items:
        return None

    # Extract URLs (latest first)
    urls = []
    for article in news_items:
        if getattr(article, "url", None):
            urls.append(article.url)

        if len(urls) == 5:   # keep max 5
            break

    print ("Fetched news URLs for", ticker, urls)

    return urls if urls else None

async def fetch_top_gainers(top_n: int = 10, timeout: int = 8) -> List[Dict]:
    """
    Call FMP biggest-gainers endpoint and return a list of raw item dicts (limited to top_n).
    Upsert each item into the DB as InPlayRecord.
    """
    print(f"Using API key: {FMP_KEY[:10]}...")  # Show first 10 chars only for security
    
    url = f"{FMP_BASE}/biggest-gainers"
    params = {"apikey": FMP_KEY}

    # Initialize DB adapter
    db = DBAdapter(db_path=str(Path(__file__).parent.parent.parent / "scanner.db"))
    await db.init()

    try:
        async with aiohttp.ClientSession() as session:
            try: 
                response = await session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
            except aiohttp.ClientError as e:
                raise RuntimeError(f"FMP API request failed: {e}") from e
        
            data = await response.json()
            #pprint.pprint(data)

            top_gainers = data if isinstance(data, list) else data.get("mostGainerStock", [])
            top_gainers = top_gainers[:top_n]
            
            # Process each item and upsert
            for item in top_gainers:
                # Extract fields from FMP response
                ticker = item.get("symbol")
                if not ticker:
                    continue
                
                company = item.get("name") 
                price = item.get("price")
                change_pct = item.get("changesPercentage")
                
                # Convert to float where applicable
                try:
                    price = float(price) if price else None
                except (ValueError, TypeError):
                    price = None
                
                try:
                    change_pct = round(float(change_pct), 2) if change_pct else None
                except (ValueError, TypeError):
                    change_pct = None

                # get float shares from another endpoint if needed
                float_shares = await fetch_float(ticker)
                news = await fetch_latest_news(ticker)

                print("float_shares", float_shares)
                print("news", news)
                
                # Construct InPlayRecord
                record = InPlayRecord(
                    ticker=ticker,
                    company=company,
                    price=price,
                    change_pct=change_pct,
                    float_shares=None,
                    market_cap=None,
                    volume=None,
                    news=news,
                    signal=None,
                    ts=int(time.time())
                )
                
                # Upsert to DB
                await db.upsert_inplay(record)
                print(f"Upserted: {ticker} ({company}) - {change_pct}% @ ${price}")
            
            print(f"Processed {len(top_gainers)} items.")
            upserted = await db.list_inplay()
            pprint.pprint(upserted)
            return top_gainers #this return is not needed
    finally:
        await db.close()


if __name__ == "__main__":
    async def main():
        gainers = await fetch_top_gainers(top_n=10)

    asyncio.run(main()) 