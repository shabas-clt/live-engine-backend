import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List

from app.core.config import settings
from app.core.database import db

logger = logging.getLogger(__name__)


class CandleAggregator:
    """Builds OHLCV candles from in-memory ticks and periodically persists them to DB.

    Previous approach queried ticks from PostgreSQL, but the tick buffer flushes
    every 5 seconds while the aggregator runs every 1 second -- causing missing data.
    Now ticks are fed directly via `ingest_tick()` and candles are built in memory.
    """

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

    MAX_CANDLES_IN_MEMORY = 600

    def __init__(self):
        self._running = False
        self._persist_task: Optional[asyncio.Task] = None
        # In-memory candle storage: (asset, interval) -> deque of candle dicts
        self._candles: Dict[tuple, deque] = defaultdict(
            lambda: deque(maxlen=self.MAX_CANDLES_IN_MEMORY)
        )
        self._lock = asyncio.Lock()
        # Track which candles have been modified since last DB persist
        self._dirty_keys: set = set()

    async def start(self):
        if self._running:
            return
        self._running = True
        self._persist_task = asyncio.create_task(self._persist_loop())
        logger.info("Candle aggregator started (in-memory mode)")

    async def stop(self):
        self._running = False
        if self._persist_task:
            self._persist_task.cancel()
            try:
                await self._persist_task
            except asyncio.CancelledError:
                pass
        # Final persist before shutdown
        await self._persist_dirty_candles()
        logger.info("Candle aggregator stopped")

    def ingest_tick(self, asset: str, price: float, volume: float, ts: datetime):
        """Feed a tick directly into the aggregator. Called from DataCollector."""
        for interval_name, interval_seconds in self.INTERVALS.items():
            bucket_time = self._floor_time(ts, interval_seconds)
            cache_key = (asset, interval_name)
            dq = self._candles[cache_key]

            if dq and dq[-1]['time'] == bucket_time:
                candle = dq[-1]
                candle['close'] = price
                candle['high'] = max(candle['high'], price)
                candle['low'] = min(candle['low'], price)
                candle['volume'] += volume
                candle['tick_count'] += 1
            else:
                dq.append({
                    'time': bucket_time,
                    'asset': asset,
                    'interval': interval_name,
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume,
                    'tick_count': 1,
                })

            self._dirty_keys.add(cache_key)

    def get_latest_candles(self, asset: str, interval: str, limit: int = 300) -> List[dict]:
        """Return recent candles from memory for the REST API."""
        cache_key = (asset, interval)
        dq = self._candles.get(cache_key)
        if not dq:
            return []
        return list(dq)[-limit:]

    async def _persist_loop(self):
        """Persist dirty candles to DB every 5 seconds."""
        while self._running:
            try:
                await asyncio.sleep(5)
                await self._persist_dirty_candles()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Candle persist loop error: {e}")
                await asyncio.sleep(5)

    async def _persist_dirty_candles(self):
        """Batch upsert all modified candles to PostgreSQL."""
        if not self._dirty_keys:
            return

        keys_to_persist = self._dirty_keys.copy()
        self._dirty_keys.clear()

        # Collect the most recent candles from each dirty key.
        # Only persist the last 2 candles per key (current + previous bucket)
        # to avoid huge batch sizes.
        records = []
        for cache_key in keys_to_persist:
            dq = self._candles.get(cache_key)
            if not dq:
                continue
            for candle in list(dq)[-2:]:
                records.append((
                    candle['time'],
                    candle['asset'],
                    candle['interval'],
                    candle['open'],
                    candle['high'],
                    candle['low'],
                    candle['close'],
                    candle['volume'],
                    candle['tick_count'],
                ))

        if not records:
            return

        try:
            async with db.pool.acquire() as conn:
                await conn.executemany(
                    """
                    INSERT INTO candles (time, asset, interval, open, high, low, close, volume, tick_count)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (time, asset, interval)
                    DO UPDATE SET
                        high = GREATEST(candles.high, EXCLUDED.high),
                        low = LEAST(candles.low, EXCLUDED.low),
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        tick_count = EXCLUDED.tick_count
                    """,
                    records,
                )
            logger.debug(f"Persisted {len(records)} candles to DB")
        except Exception as e:
            # Put keys back so they get retried
            self._dirty_keys.update(keys_to_persist)
            logger.error(f"Failed to persist candles: {e}")

    @staticmethod
    def _floor_time(dt: datetime, seconds: int) -> datetime:
        """Floor datetime to interval boundary."""
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
