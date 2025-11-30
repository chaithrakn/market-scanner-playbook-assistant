"""
Data models for the scanner subsystem.
No implementation logic, only Pydantic models and typed structures.
"""

from pydantic import BaseModel, Field
from typing import Optional, List
import time


class InPlayRecord(BaseModel):
    """Canonical row stored in the inplay table.
    Only the DB adapter writes to this table.
    All subsystems construct this model and pass to DBAdapter.upsert_inplay().
    """
    ticker: str = Field(..., description="Ticker symbol")
    company: str = Field(..., description="Company name")
    price: Optional[float] = Field(None, description="Last price from provider")
    change_pct: Optional[float] = Field(None, description="Percent change")
    float_shares: Optional[int] = Field(None, description="Float size")
    market_cap: Optional[float] = Field(None, description="Market capitalization")
    volume: Optional[float] = Field(None, description="Volume")
    news: Optional[List[str]] = Field(None,description="List of up to 5 recent news URLs")
    signal: Optional[str] = Field(None, description="Computed signal")
    ts: int = Field(default_factory=lambda: int(time.time()), description="Update timestamp")