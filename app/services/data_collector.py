import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Set, List
import websockets
import httpx

from app.core.config import settings
from app.core.database import db
from app.services.token_manager import token_manager

logger = logging.getLogger(__name__)


class DataCollector:
    """Collects real-time data from Tiingo using multiple tokens.

    Ticks are:
    1. Fed directly to CandleAggregator for in-memory candle building
    2. Buffered and batch-inserted to TimescaleDB for historical storage
    3. Broadcast to connected WebSocket subscribers
    """

    def __init__(self):
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._subscribers: Dict[str, Set] = {}
        self._lock = asyncio.Lock()

        self._tick_buffer: List[tuple] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None

    async def start(self):
        if self._running:
            return

        self._running = True
        logger.info("Starting data collector...")

        if settings.COLLECT_BTC:
            self._tasks.append(asyncio.create_task(self._collect_btc()))

        if settings.COLLECT_GOLD:
            self._tasks.append(asyncio.create_task(self._collect_gold()))

        if settings.COLLECT_SILVER:
            self._tasks.append(asyncio.create_task(self._collect_silver()))

        self._flush_task = asyncio.create_task(self._flush_ticks_loop())

        logger.info(f"Started {len(self._tasks)} collection tasks")

    async def stop(self):
        self._running = False

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        await self._flush_ticks()

        for task in [*self._tasks]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._tasks = []
        logger.info("Stopped data collector")

    async def subscribe(self, websocket, asset: str):
        async with self._lock:
            if asset not in self._subscribers:
                self._subscribers[asset] = set()
            self._subscribers[asset].add(websocket)
            logger.info(f"Client subscribed to {asset}")

    async def unsubscribe(self, websocket, asset: str):
        async with self._lock:
            if asset in self._subscribers:
                self._subscribers[asset].discard(websocket)
                if not self._subscribers[asset]:
                    del self._subscribers[asset]

    async def _broadcast(self, asset: str, data: dict):
        async with self._lock:
            subscribers = self._subscribers.get(asset, set()).copy()

        if not subscribers:
            return

        message = json.dumps(data)
        tasks = [self._send_safe(ws, message, asset) for ws in subscribers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_safe(self, ws, message: str, asset: str):
        try:
            await asyncio.wait_for(ws.send_text(message), timeout=0.1)
        except asyncio.TimeoutError:
            logger.warning(f"Client timeout on {asset}, removing")
            await self.unsubscribe(ws, asset)
        except Exception:
            await self.unsubscribe(ws, asset)

    def _process_tick(self, asset: str, price: float, volume: float, ts: datetime, token_id: str):
        """Common tick processing: feed to aggregator and buffer for DB storage."""
        # Lazy import to avoid circular dependency at module load time
        from app.services.candle_aggregator import candle_aggregator
        candle_aggregator.ingest_tick(asset, price, volume, ts)

    async def _collect_btc(self):
        asset = "bitcoin"
        ticker = "btcusd"
        backoff = 2.0

        while self._running:
            token_obj = await token_manager.get_next_token(preferred_asset=asset)
            if not token_obj:
                logger.error("No token available for BTC")
                await asyncio.sleep(10)
                continue

            try:
                ws_url = f"wss://api.tiingo.com/crypto?token={token_obj.token}"
                async with websockets.connect(ws_url, open_timeout=10) as ws:
                    await ws.send(
                        json.dumps({
                            "eventName": "subscribe",
                            "authorization": token_obj.token,
                            "eventData": {"tickers": [ticker]},
                        })
                    )
                    logger.info(f"Connected to Tiingo crypto stream (BTC) with {token_obj.name}")
                    backoff = 2.0

                    while self._running:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        payload = json.loads(raw)

                        if payload.get("messageType") != "A":
                            continue

                        data = payload.get("data", [])
                        if len(data) < 6:
                            continue

                        ts_raw = data[2]
                        price = float(data[5])
                        volume = float(data[4]) if len(data) > 4 else 0
                        ts = self._parse_timestamp(ts_raw)

                        self._process_tick(asset, price, volume, ts, token_obj.id)
                        await self._store_tick(asset, price, volume, ts, token_obj.id)
                        await self._broadcast(asset, {
                            "type": "tick",
                            "asset": asset,
                            "price": price,
                            "volume": volume,
                            "timestamp": ts.isoformat(),
                        })

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"BTC stream error: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _collect_gold(self):
        await self._collect_fx("gold", "xauusd")

    async def _collect_silver(self):
        await self._collect_fx("silver", "xagusd")

    async def _collect_fx(self, asset: str, ticker: str):
        backoff = 2.0

        while self._running:
            token_obj = await token_manager.get_next_token(preferred_asset=asset)
            if not token_obj:
                logger.error(f"No token available for {asset}")
                await asyncio.sleep(10)
                continue

            try:
                ws_url = f"wss://api.tiingo.com/fx?token={token_obj.token}"
                async with websockets.connect(ws_url, open_timeout=10) as ws:
                    await ws.send(
                        json.dumps({
                            "eventName": "subscribe",
                            "authorization": token_obj.token,
                            "eventData": {"tickers": [ticker]},
                        })
                    )
                    logger.info(f"Connected to Tiingo FX stream ({asset.upper()}) with {token_obj.name}")
                    backoff = 2.0

                    while self._running:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        payload = json.loads(raw)

                        if payload.get("messageType") != "A":
                            continue

                        data = payload.get("data", [])
                        if len(data) < 6:
                            continue

                        ts_raw = data[2]
                        # FX format: ["Q", ticker, ts, bidSize, bidPrice, midPrice, askSize, askPrice]
                        # Use midPrice if available, fallback to bidPrice, then askPrice
                        mid = data[5]
                        bid = data[4]
                        ask = data[7] if len(data) > 7 else None
                        price = float(mid) if mid is not None else float(bid) if bid is not None else float(ask)
                        ts = self._parse_timestamp(ts_raw)

                        self._process_tick(asset, price, 0, ts, token_obj.id)
                        await self._store_tick(asset, price, 0, ts, token_obj.id)
                        await self._broadcast(asset, {
                            "type": "tick",
                            "asset": asset,
                            "price": price,
                            "timestamp": ts.isoformat(),
                        })

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"{asset.upper()} stream error: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _flush_ticks_loop(self):
        while self._running:
            await asyncio.sleep(5)
            await self._flush_ticks()

    async def _flush_ticks(self):
        async with self._buffer_lock:
            if not self._tick_buffer:
                return
            ticks = self._tick_buffer.copy()
            self._tick_buffer.clear()

        try:
            async with db.pool.acquire() as conn:
                await conn.copy_records_to_table(
                    'ticks',
                    records=ticks,
                    columns=['time', 'asset', 'price', 'volume', 'source', 'token_id']
                )
            logger.debug(f"Flushed {len(ticks)} ticks to DB")
        except Exception as e:
            logger.error(f"Failed to flush ticks: {e}")

    async def _store_tick(self, asset: str, price: float, volume: float, ts: datetime, token_id: str):
        if not settings.STORE_RAW_TICKS:
            return

        tid = uuid.UUID(token_id) if token_id else None

        async with self._buffer_lock:
            self._tick_buffer.append((ts, asset, price, volume, "tiingo", tid))

            if len(self._tick_buffer) >= 1000:
                asyncio.create_task(self._flush_ticks())

    @staticmethod
    def _parse_timestamp(ts_raw) -> datetime:
        """Parse Tiingo timestamp to naive UTC datetime.

        TimescaleDB columns are TIMESTAMPTZ. We store naive datetimes but
        the server must run in UTC (enforced by PostgreSQL timezone setting).
        """
        raw = str(ts_raw).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(raw)
        # Convert to UTC and strip tzinfo so all stored values are naive-UTC
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed


data_collector = DataCollector()
