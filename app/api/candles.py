from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from datetime import datetime, timezone
import logging

from app.core.database import db
from app.services.candle_aggregator import candle_aggregator

router = APIRouter(tags=["Candles"])
logger = logging.getLogger(__name__)


def _to_utc_iso(value: datetime | None) -> str | None:
    """Serialize timestamps as valid UTC ISO strings."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@router.get("/candles")
async def get_candles(
    asset: str = Query(..., regex="^(bitcoin|gold|silver|stock_[a-z]+)$", description="Asset name (bitcoin, gold, silver, stock_aapl, etc.)"),
    interval: str = Query(..., regex="^(1s|5s|10s|15s|30s|1m|5m|15m)$", description="Candle interval"),
    limit: int = Query(300, ge=1, le=1000, description="Number of candles to return"),
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
):
    """
    Get candles -- serves from in-memory aggregator first, falls back to DB for
    historical data or filtered time ranges.

    Returns candles in ascending order (oldest first) for chart display.
    """
    try:
        # For filtered time ranges, always query DB (historical data)
        if start_time or end_time:
            return await _get_candles_from_db(asset, interval, limit, start_time, end_time)

        # Try in-memory candles first (most recent, always up to date)
        memory_candles = candle_aggregator.get_latest_candles(asset, interval, limit)

        if memory_candles:
            result = [
                {
                    "time": _to_utc_iso(c['time']),
                    "open": float(c['open']),
                    "high": float(c['high']),
                    "low": float(c['low']),
                    "close": float(c['close']),
                    "volume": float(c.get('volume', 0)),
                    "tick_count": int(c.get('tick_count', 0)),
                }
                for c in memory_candles
            ]

            # If we have fewer than requested, backfill from DB
            if len(result) < limit:
                earliest_memory = memory_candles[0]['time'] if memory_candles else None
                db_candles = await _get_candles_from_db(
                    asset, interval, limit - len(result),
                    end_time=earliest_memory.isoformat() if earliest_memory else None,
                )
                return db_candles + result

            return result

        # No in-memory candles (engine just started), fall back to DB
        return await _get_candles_from_db(asset, interval, limit, start_time, end_time)

    except Exception as e:
        logger.error(f"Error fetching candles: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch candles")


async def _get_candles_from_db(
    asset: str,
    interval: str,
    limit: int,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
) -> list:
    """Query candles from PostgreSQL/TimescaleDB."""
    query = """
        SELECT time, open, high, low, close, volume, tick_count
        FROM candles
        WHERE asset = $1 AND interval = $2
    """
    params = [asset, interval]
    param_idx = 3

    if start_time:
        try:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            query += f" AND time >= ${param_idx}"
            params.append(start_dt)
            param_idx += 1
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid start_time format")

    if end_time:
        try:
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            query += f" AND time < ${param_idx}"
            params.append(end_dt)
            param_idx += 1
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid end_time format")

    query += f" ORDER BY time DESC LIMIT ${param_idx}"
    params.append(limit)

    async with db.pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [
        {
            "time": _to_utc_iso(row['time']),
            "open": float(row['open']),
            "high": float(row['high']),
            "low": float(row['low']),
            "close": float(row['close']),
            "volume": float(row['volume']) if row['volume'] else 0.0,
            "tick_count": int(row['tick_count']) if row['tick_count'] else 0,
        }
        for row in reversed(rows)
    ]


@router.get("/candles/latest")
async def get_latest_candle(
    asset: str = Query(..., regex="^(bitcoin|gold|silver|stock_[a-z]+)$", description="Asset name (bitcoin, gold, silver, stock_aapl, etc.)"),
    interval: str = Query(..., regex="^(1s|5s|10s|15s|30s|1m|5m|15m)$"),
):
    """Get the most recent candle for an asset and interval."""
    # In-memory first
    memory_candles = candle_aggregator.get_latest_candles(asset, interval, 1)
    if memory_candles:
        c = memory_candles[-1]
        return {
            "time": _to_utc_iso(c['time']),
            "open": float(c['open']),
            "high": float(c['high']),
            "low": float(c['low']),
            "close": float(c['close']),
            "volume": float(c.get('volume', 0)),
            "tick_count": int(c.get('tick_count', 0)),
        }

    # Fallback to DB
    try:
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT time, open, high, low, close, volume, tick_count
                FROM candles WHERE asset = $1 AND interval = $2
                ORDER BY time DESC LIMIT 1
                """,
                asset, interval,
            )

        if not row:
            raise HTTPException(status_code=404, detail="No candles found")

        return {
            "time": _to_utc_iso(row['time']),
            "open": float(row['open']),
            "high": float(row['high']),
            "low": float(row['low']),
            "close": float(row['close']),
            "volume": float(row['volume']) if row['volume'] else 0.0,
            "tick_count": int(row['tick_count']) if row['tick_count'] else 0,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching latest candle: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch latest candle")


@router.get("/candles/stats")
async def get_candle_stats():
    """Get statistics about stored candles."""
    try:
        query = """
            SELECT 
                asset,
                interval,
                COUNT(*) as count,
                MIN(time) as earliest,
                MAX(time) as latest
            FROM candles
            GROUP BY asset, interval
            ORDER BY asset, interval
        """

        async with db.pool.acquire() as conn:
            rows = await conn.fetch(query)

        stats = [
            {
                "asset": row['asset'],
                "interval": row['interval'],
                "count": int(row['count']),
                "earliest": _to_utc_iso(row['earliest']),
                "latest": _to_utc_iso(row['latest']),
            }
            for row in rows
        ]

        return {"stats": stats}

    except Exception as e:
        logger.error(f"Error fetching candle stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch candle stats")
