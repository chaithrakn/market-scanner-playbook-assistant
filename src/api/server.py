"""
FastAPI server providing read-only visibility and manual enrichment triggers.
This file wires:
- DBAdapter (for reading inplay table)
- optional publisher for enrichment jobs
- Web endpoints for control and inspection
"""


from fastapi import FastAPI, HTTPException
from typing import List


# Placeholder imports (will exist once you create the modules)
# from db.adapter import DBAdapter
# from enrich.tasks import celery_enrich
# from messaging.publisher import MessagePublisher
# from enrich.enricher import Enricher


app = FastAPI(title="Market Scanner Control API", version="0.1.0")


@app.get("/inplay", response_model=List[InPlayRecord])
async def list_inplay():
    """Return all tickers currently stored in the inplay table."""
    # records = await db.list_inplay()
    # return records
    raise NotImplementedError("DBAdapter.list_inplay() not wired yet.")


@app.get("/inplay/{ticker}", response_model=InPlayRecord)
async def get_ticker(ticker: str):
    """Return a single ticker's inplay row."""
    # record = await db.get_inplay(ticker)
    # if not record:
    #     raise HTTPException(status_code=404, detail="Ticker not found")
    # return record
    raise NotImplementedError("DBAdapter.get_inplay() not wired yet.")


@app.post("/inplay/{ticker}/re-enrich")
async def re_enrich(ticker: str):
    """Trigger re-enrichment of a ticker.
    If using RabbitMQ + Celery, publish a task.
    Otherwise call Enricher in-process (async background task).
    """
    # Example future logic:
    # if publisher:
    #     publisher.publish_enrich(ticker, base_ts=int(time.time()))
    # else:
    #     asyncio.create_task(enricher.enrich_and_upsert(ticker, base_ts=time.time()))
    return {"status": "queued", "ticker": ticker}


@app.post("/scanner/start")
async def start_scanner():
    """Start the FMP poller service via orchestrator."""
    # await orchestrator.start_all()
    return {"status": "scanner_started"}


@app.post("/scanner/stop")
async def stop_scanner():
    """Stop all running async services."""
    # await orchestrator.stop_all()
    return {"status": "scanner_stopped"}
