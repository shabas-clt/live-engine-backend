import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from collections import defaultdict

from app.core.config import settings
from app.core.database import db

logger = logging.getLogger(__name__)


class CandleAggregator:
    """Aggregates raw ticks into OHLCV candles for multiple timeframes"""

    INTERVALS = {
        '1s': 1,
        '5s': 5,
        '10s': 10,
        '15s': 15,
        '30s': 30,
        '1m': 60,
        '5m': 300,
        '15m': 900,
    }

    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_aggregation: Dict[str, datetime] = defaultdict(lambda: datetime.min)

    async def start(self):
        """Start candle aggregation background task"""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._aggregation_loop())
        logger.info("✅ Candle aggregator started")

    async def stop(self):
        """Stop candle aggregation"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("⏹️  Candle aggregator stopped")

    async def _aggregation_loop(self):
        """Main aggregation loop - runs every 1 second"""
        while self._running:
            try:
                await asyncio.sleep(1)
                
                if not self._running:
                    break

                assets = []
                if settings.COLLECT_BTC:
                    assets.append('bitcoin')
                if settings.COLLECT_GOLD:
                    assets.append('gold')
                if settings.COLLECT_SILVER:
                    assets.append('silver')

                for asset in assets:
                    for interval_name, interval_seconds in self.INTERVALS.items():
                        try:
                            await self._aggregate_interval(asset, interval_name, interval_seconds)
                        except Exception as e:
                            logger.error(f"Aggregation error for {asset} {interval_name}: {e}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Aggregation loop error: {e}")
                await asyncio.sleep(5)

    async def _aggregate_interval(self, asset: str, interval: str, seconds: int):
        """Aggregate ticks for a specific asset and interval"""
        now = datetime.utcnow()
        bucket_time = self._floor_time(now, seconds)
        
        last_agg_key = f"{asset}:{interval}"
        last_agg = self._last_aggregation[last_agg_key]
        
        if bucket_time <= last_agg:
            return
        
        start_time = bucket_time
        end_time = bucket_time + timedelta(seconds=seconds)
        
        async with db.pool.acquire() as conn:
            query = """
                SELECT 
                    price,
                    volume,
                    time
                FROM ticks
                WHERE asset = $1
                  AND time >= $2
                  AND time < $3
                ORDER BY time ASC
            """
            
            rows = await conn.fetch(query, asset, start_time, end_time)
            
            if not rows:
                return
            
            prices = [float(row['price']) for row in rows]
            volumes = [float(row['volume']) if row['volume'] else 0 for row in rows]
            
            candle = {
                'time': bucket_time,
                'asset': asset,
                'interval': interval,
                'open': prices[0],
                'high': max(prices),
                'low': min(prices),
                'close': prices[-1],
                'volume': sum(volumes),
                'tick_count': len(rows),
            }
            
            upsert_query = """
                INSERT INTO candles (time, asset, interval, open, high, low, close, volume, tick_count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (time, asset, interval)
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    tick_count = EXCLUDED.tick_count
            """
            
            await conn.execute(
                upsert_query,
                candle['time'],
                candle['asset'],
                candle['interval'],
                candle['open'],
                candle['high'],
                candle['low'],
                candle['close'],
                candle['volume'],
                candle['tick_count'],
            )
            
            self._last_aggregation[last_agg_key] = bucket_time
            logger.debug(f"Aggregated {asset} {interval}: {len(rows)} ticks -> OHLCV")

    @staticmethod
    def _floor_time(dt: datetime, seconds: int) -> datetime:
        """Floor datetime to interval boundary"""
        if seconds >= 60:
            minutes = seconds // 60
            floored = dt.replace(second=0, microsecond=0)
            minute_bucket = (floored.minute // minutes) * minutes
            return floored.replace(minute=minute_bucket)
        else:
            floored = dt.replace(microsecond=0)
            second_bucket = (floored.second // seconds) * seconds
            return floored.replace(second=second_bucket)


candle_aggregator = CandleAggregator()
