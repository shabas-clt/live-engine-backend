import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Set, List
import websockets

from app.core.config import settings
from app.core.database import db
from app.services.token_manager import token_manager

logger = logging.getLogger(__name__)


class StockCollector:
    """Collects real-time stock data from Tiingo IEX feed.

    Ticks are:
    1. Fed directly to CandleAggregator for in-memory candle building
    2. Buffered and batch-inserted to TimescaleDB for historical storage
    3. Broadcast to connected WebSocket subscribers
    """

    # US Stock tickers to collect
    STOCK_TICKERS = [
        "aapl",    # Apple Inc.
        "googl",   # Alphabet Inc. (Google)
        "msft",    # Microsoft Corp.
        "tsla",    # Tesla Inc.
        "amzn",    # Amazon.com Inc.
        "meta",    # Meta Platforms Inc. (Facebook)
        "nvda",    # NVIDIA Corp.
        "nflx",    # Netflix Inc.
        "amd",     # Advanced Micro Devices
        "intc",    # Intel Corp.
        "dis",     # Walt Disney Co.
        "ba",      # Boeing Co.
    ]

    # Stock metadata
    STOCK_METADATA = {
        "aapl": {"name": "Apple Inc.", "exchange": "NASDAQ"},
        "googl": {"name": "Alphabet Inc.", "exchange": "NASDAQ"},
        "msft": {"name": "Microsoft Corp.", "exchange": "NASDAQ"},
        "tsla": {"name": "Tesla Inc.", "exchange": "NASDAQ"},
        "amzn": {"name": "Amazon.com Inc.", "exchange": "NASDAQ"},
        "meta": {"name": "Meta Platforms Inc.", "exchange": "NASDAQ"},
        "nvda": {"name": "NVIDIA Corp.", "exchange": "NASDAQ"},
        "nflx": {"name": "Netflix Inc.", "exchange": "NASDAQ"},
        "amd": {"name": "Advanced Micro Devices", "exchange": "NASDAQ"},
        "intc": {"name": "Intel Corp.", "exchange": "NASDAQ"},
        "dis": {"name": "Walt Disney Co.", "exchange": "NYSE"},
        "ba": {"name": "Boeing Co.", "exchange": "NYSE"},
    }

    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._subscribers: Dict[str, Set] = {}
        self._lock = asyncio.Lock()

        self._tick_buffer: List[tuple] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None

        # Cache for last known prices (used outside market hours)
        self._last_prices: Dict[str, float] = {}
        self._price_lock = asyncio.Lock()

    async def start(self):
        if self._running:
            return

        self._running = True
        logger.info("Starting stock collector...")

        # Start collection task
        self._task = asyncio.create_task(self._collect_stocks())

        # Start flush task
        self._flush_task = asyncio.create_task(self._flush_ticks_loop())

        logger.info("Started stock collection task")

    async def stop(self):
        self._running = False

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        await self._flush_ticks()

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("Stopped stock collector")

    async def subscribe(self, websocket, stock_symbol: str):
        """Subscribe to stock updates"""
        async with self._lock:
            asset_key = f"stock_{stock_symbol.lower()}"
            if asset_key not in self._subscribers:
                self._subscribers[asset_key] = set()
            self._subscribers[asset_key].add(websocket)
            logger.info(f"Client subscribed to {stock_symbol}")

    async def unsubscribe(self, websocket, stock_symbol: str):
        """Unsubscribe from stock updates"""
        async with self._lock:
            asset_key = f"stock_{stock_symbol.lower()}"
            if asset_key in self._subscribers:
                self._subscribers[asset_key].discard(websocket)
                if not self._subscribers[asset_key]:
                    del self._subscribers[asset_key]

    async def get_last_price(self, stock_symbol: str) -> Optional[float]:
        """Get last known price for a stock"""
        async with self._price_lock:
            return self._last_prices.get(stock_symbol.lower())

    async def get_all_stocks(self) -> List[dict]:
        """Get all available stocks with metadata and last prices"""
        stocks = []
        async with self._price_lock:
            for ticker in self.STOCK_TICKERS:
                metadata = self.STOCK_METADATA.get(ticker, {})
                stocks.append({
                    "symbol": ticker.upper(),
                    "name": metadata.get("name", ticker.upper()),
                    "exchange": metadata.get("exchange", "NASDAQ"),
                    "currentPrice": self._last_prices.get(ticker),
                    "marketStatus": self._get_market_status(),
                })
        return stocks

    def _get_market_status(self) -> str:
        """Check if US market is currently open"""
        from pytz import timezone as pytz_timezone
        
        now_et = datetime.now(pytz_timezone('America/New_York'))
        
        # Check if weekend
        if now_et.weekday() >= 5:
            return "closed"
        
        # Check if within market hours (9:30 AM - 4:00 PM ET)
        market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
        
        if market_open <= now_et <= market_close:
            return "open"
        
        return "closed"

    async def _broadcast(self, stock_symbol: str, data: dict):
        """Broadcast tick to all subscribers of this stock"""
        asset_key = f"stock_{stock_symbol.lower()}"
        
        async with self._lock:
            subscribers = self._subscribers.get(asset_key, set()).copy()

        if not subscribers:
            return

        message = json.dumps(data)
        tasks = [self._send_safe(ws, message, stock_symbol) for ws in subscribers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_safe(self, ws, message: str, stock_symbol: str):
        """Safely send message to WebSocket client"""
        try:
            await asyncio.wait_for(ws.send_text(message), timeout=0.1)
        except asyncio.TimeoutError:
            logger.warning(f"Client timeout on {stock_symbol}, removing")
            await self.unsubscribe(ws, stock_symbol)
        except Exception:
            await self.unsubscribe(ws, stock_symbol)

    def _process_tick(self, stock_symbol: str, price: float, volume: float, ts: datetime, token_id: str):
        """Common tick processing: feed to aggregator and update cache"""
        # Lazy import to avoid circular dependency
        from app.services.candle_aggregator import candle_aggregator
        
        asset_key = f"stock_{stock_symbol.lower()}"
        candle_aggregator.ingest_tick(asset_key, price, volume, ts)

    async def _collect_stocks(self):
        """Collect stock data from Tiingo IEX feed"""
        backoff = 2.0

        while self._running:
            token_obj = await token_manager.get_next_token(preferred_asset="stocks")
            if not token_obj:
                logger.error("No token available for stocks")
                await asyncio.sleep(10)
                continue

            try:
                ws_url = f"wss://api.tiingo.com/iex?token={token_obj.token}"
                async with websockets.connect(ws_url, open_timeout=10) as ws:
                    # Subscribe to all stock tickers
                    await ws.send(
                        json.dumps({
                            "eventName": "subscribe",
                            "authorization": token_obj.token,
                            "eventData": {"tickers": self.STOCK_TICKERS},
                        })
                    )
                    logger.info(f"Connected to Tiingo IEX stream with {token_obj.name}")
                    backoff = 2.0

                    while self._running:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        
                        # Measure bandwidth usage
                        message_size_bytes = len(raw.encode('utf-8'))
                        message_size_kb = message_size_bytes / 1024.0
                        
                        payload = json.loads(raw)

                        # IEX message format: messageType "A" for trade data
                        if payload.get("messageType") != "A":
                            continue

                        data = payload.get("data", [])
                        if len(data) < 10:
                            continue

                        # IEX data format: [type, ticker, timestamp, ..., last_price, last_size, ...]
                        ticker = data[1].lower()
                        ts_raw = data[2]
                        last_price = float(data[9]) if len(data) > 9 else None
                        last_size = float(data[10]) if len(data) > 10 else 0
                        
                        if last_price is None:
                            continue

                        ts = self._parse_timestamp(ts_raw)

                        # Update last known price
                        async with self._price_lock:
                            self._last_prices[ticker] = last_price

                        # Process tick
                        self._process_tick(ticker, last_price, last_size, ts, token_obj.id)
                        await self._store_tick(ticker, last_price, last_size, ts, token_obj.id)
                        
                        # Record bandwidth usage
                        await token_manager._record_usage(token_obj, bandwidth_kb=message_size_kb, increment_requests=False)
                        
                        # Broadcast to subscribers
                        await self._broadcast(ticker, {
                            "type": "tick",
                            "asset": f"stock_{ticker}",
                            "symbol": ticker.upper(),
                            "price": last_price,
                            "volume": last_size,
                            "timestamp": ts.isoformat(),
                            "marketStatus": self._get_market_status(),
                        })

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Stock stream error: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _flush_ticks_loop(self):
        """Periodically flush buffered ticks to database"""
        while self._running:
            await asyncio.sleep(5)
            await self._flush_ticks()

    async def _flush_ticks(self):
        """Flush buffered ticks to TimescaleDB"""
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
            logger.debug(f"Flushed {len(ticks)} stock ticks to DB")
        except Exception as e:
            logger.error(f"Failed to flush stock ticks: {e}")

    async def _store_tick(self, ticker: str, price: float, volume: float, ts: datetime, token_id: str):
        """Buffer tick for database storage"""
        if not settings.STORE_RAW_TICKS:
            return

        tid = uuid.UUID(token_id) if token_id else None
        asset_key = f"stock_{ticker}"

        async with self._buffer_lock:
            self._tick_buffer.append((ts, asset_key, price, volume, "tiingo_iex", tid))

            if len(self._tick_buffer) >= 1000:
                asyncio.create_task(self._flush_ticks())

    @staticmethod
    def _parse_timestamp(ts_raw) -> datetime:
        """Parse Tiingo timestamp to naive UTC datetime"""
        raw = str(ts_raw).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(raw)
        # Convert to UTC and strip tzinfo
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed


# Global instance
stock_collector = StockCollector()
