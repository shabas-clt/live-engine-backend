import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Set, List
import yliveticker

from app.core.config import settings
from app.core.database import db

logger = logging.getLogger(__name__)


class StockCollector:
    """Collects real-time US stock data from Yahoo Finance WebSocket.

    Ticks are:
    1. Fed directly to CandleAggregator for in-memory candle building
    2. Buffered and batch-inserted to TimescaleDB for historical storage
    3. Broadcast to connected WebSocket subscribers
    """

    # US Stock tickers (Yahoo Finance format)
    STOCK_TICKERS = [
        "AAPL",    # Apple Inc.
        "GOOGL",   # Alphabet Inc. (Google)
        "MSFT",    # Microsoft Corp.
        "TSLA",    # Tesla Inc.
        "AMZN",    # Amazon.com Inc.
        "META",    # Meta Platforms Inc. (Facebook)
        "NVDA",    # NVIDIA Corp.
        "NFLX",    # Netflix Inc.
        "AMD",     # Advanced Micro Devices
        "INTC",    # Intel Corp.
        "DIS",     # Walt Disney Co.
        "BA",      # Boeing Co.
    ]

    # Stock metadata
    STOCK_METADATA = {
        "AAPL": {"name": "Apple Inc.", "exchange": "NASDAQ", "symbol": "AAPL"},
        "GOOGL": {"name": "Alphabet Inc.", "exchange": "NASDAQ", "symbol": "GOOGL"},
        "MSFT": {"name": "Microsoft Corp.", "exchange": "NASDAQ", "symbol": "MSFT"},
        "TSLA": {"name": "Tesla Inc.", "exchange": "NASDAQ", "symbol": "TSLA"},
        "AMZN": {"name": "Amazon.com Inc.", "exchange": "NASDAQ", "symbol": "AMZN"},
        "META": {"name": "Meta Platforms Inc.", "exchange": "NASDAQ", "symbol": "META"},
        "NVDA": {"name": "NVIDIA Corp.", "exchange": "NASDAQ", "symbol": "NVDA"},
        "NFLX": {"name": "Netflix Inc.", "exchange": "NASDAQ", "symbol": "NFLX"},
        "AMD": {"name": "Advanced Micro Devices", "exchange": "NASDAQ", "symbol": "AMD"},
        "INTC": {"name": "Intel Corp.", "exchange": "NASDAQ", "symbol": "INTC"},
        "DIS": {"name": "Walt Disney Co.", "exchange": "NYSE", "symbol": "DIS"},
        "BA": {"name": "Boeing Co.", "exchange": "NYSE", "symbol": "BA"},
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
        
        self._ws_ticker = None

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

    def _process_tick(self, stock_symbol: str, price: float, volume: float, ts: datetime):
        """Common tick processing: feed to aggregator and update cache"""
        from app.services.candle_aggregator import candle_aggregator
        
        asset_key = f"stock_{stock_symbol.lower()}"
        candle_aggregator.ingest_tick(asset_key, price, volume, ts)

    async def _collect_stocks(self):
        """Collect stock data from Yahoo Finance WebSocket"""
        backoff = 2.0

        while self._running:
            try:
                logger.info("Connecting to Yahoo Finance WebSocket for US stocks...")
                
                self._ws_ticker = yliveticker.YLiveTicker()
                
                for ticker in self.STOCK_TICKERS:
                    self._ws_ticker.subscribe(ticker)
                    logger.info(f"Subscribed to {ticker}")
                
                self._ws_ticker.on_ticker = self._on_ticker_sync
                
                await asyncio.get_event_loop().run_in_executor(None, self._ws_ticker.start)
                
                backoff = 2.0
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                market_status = self._get_market_status()
                error_msg = str(e) if str(e) else "Connection closed"
                if market_status == "closed":
                    logger.info(f"US stock market is closed ({error_msg}). Reconnecting in {backoff}s...")
                else:
                    logger.warning(f"US stock stream error: {error_msg}. Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _on_ticker_sync(self, msg: dict):
        """Synchronous callback for yliveticker - schedules async processing"""
        asyncio.create_task(self._process_ticker_message(msg))

    async def _process_ticker_message(self, msg: dict):
        """Process incoming ticker message from Yahoo Finance"""
        try:
            ticker = msg.get("id")
            if not ticker or ticker not in self.STOCK_TICKERS:
                return
            
            price = msg.get("price")
            if price is None:
                return
            
            price = float(price)
            volume = float(msg.get("dayVolume", 0))
            
            ts = datetime.utcnow()
            
            async with self._price_lock:
                self._last_prices[ticker] = price
            
            market_status = self._get_market_status()
            if market_status == "open":
                self._process_tick(ticker, price, volume, ts)
                await self._store_tick(ticker, price, volume, ts)
            
            metadata = self.STOCK_METADATA.get(ticker, {})
            symbol = metadata.get("symbol", ticker)
            await self._broadcast(ticker, {
                "type": "tick",
                "asset": f"stock_{symbol.lower()}",
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "timestamp": ts.isoformat(),
                "marketStatus": market_status,
            })
            
        except Exception as e:
            logger.error(f"Error processing ticker message: {e}")

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

    async def _store_tick(self, ticker: str, price: float, volume: float, ts: datetime):
        """Buffer tick for database storage"""
        if not settings.STORE_RAW_TICKS:
            return

        asset_key = f"stock_{ticker.lower()}"

        async with self._buffer_lock:
            self._tick_buffer.append((ts, asset_key, price, volume, "yahoo_finance", None))

            if len(self._tick_buffer) >= 1000:
                asyncio.create_task(self._flush_ticks())

    @staticmethod
    def _parse_timestamp(ts_raw) -> datetime:
        """Parse timestamp to naive UTC datetime"""
        if isinstance(ts_raw, datetime):
            return ts_raw.replace(tzinfo=None) if ts_raw.tzinfo else ts_raw
        raw = str(ts_raw).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed


# Global instance
stock_collector = StockCollector()
