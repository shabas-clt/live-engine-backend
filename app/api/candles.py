from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from datetime import datetime
import logging

from app.core.database import db

router = APIRouter(tags=["Candles"])
logger = logging.getLogger(__name__)


@router.get("/candles")
async def get_candles(
    asset: str = Query(..., regex="^(bitcoin|gold|silver)$", description="Asset name"),
    interval: str = Query(..., regex="^(1s|5s|10s|15s|30s|1m|5m|15m)$", description="Candle interval"),
    limit: int = Query(300, ge=1, le=1000, description="Number of candles to return"),
    start_time: Optional[str] = Query(None, description="Start time (ISO format)"),
    end_time: Optional[str] = Query(None, description="End time (ISO format)"),
):
    """
    Get historical candles from PostgreSQL/TimescaleDB
    
    Returns candles in ascending order (oldest first) for chart display.
    
    Example:
        GET /api/candles?asset=bitcoin&interval=1s&limit=300
    
    Response:
        [
            {
                "time": "2025-01-27T10:30:00Z",
                "open": 50000.0,
                "high": 50100.0,
                "low": 49900.0,
                "close": 50050.0,
                "volume": 1234.56,
                "tick_count": 42
            },
            ...
        ]
    """
    try:
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
                query += f" AND time <= ${param_idx}"
                params.append(end_dt)
                param_idx += 1
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid end_time format")
        
        query += f" ORDER BY time DESC LIMIT ${param_idx}"
        params.append(limit)
        
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        candles = [
            {
                "time": row['time'].isoformat() + 'Z',
                "open": float(row['open']),
                "high": float(row['high']),
                "low": float(row['low']),
                "close": float(row['close']),
                "volume": float(row['volume']) if row['volume'] else 0.0,
                "tick_count": int(row['tick_count']) if row['tick_count'] else 0,
            }
            for row in reversed(rows)
        ]
        
        return candles
        
    except Exception as e:
        logger.error(f"Error fetching candles: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch candles")


@router.get("/candles/latest")
async def get_latest_candle(
    asset: str = Query(..., regex="^(bitcoin|gold|silver)$"),
    interval: str = Query(..., regex="^(1s|5s|10s|15s|30s|1m|5m|15m)$"),
):
    """Get the most recent candle for an asset and interval"""
    try:
        query = """
            SELECT time, open, high, low, close, volume, tick_count
            FROM candles
            WHERE asset = $1 AND interval = $2
            ORDER BY time DESC
            LIMIT 1
        """
        
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow(query, asset, interval)
        
        if not row:
            raise HTTPException(status_code=404, detail="No candles found")
        
        return {
            "time": row['time'].isoformat() + 'Z',
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
    """Get statistics about stored candles"""
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
                "earliest": row['earliest'].isoformat() + 'Z' if row['earliest'] else None,
                "latest": row['latest'].isoformat() + 'Z' if row['latest'] else None,
            }
            for row in rows
        ]
        
        return {"stats": stats}
        
    except Exception as e:
        logger.error(f"Error fetching candle stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch candle stats")
