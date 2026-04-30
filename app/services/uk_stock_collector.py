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


class UKStockCollector:
    """Collects real-time UK stock data from Yahoo Finance WebSocket.

    Ticks are:
    1. Fed directly to CandleAggregator for in-memory candle building
    2. Buffered and batch-inserted to TimescaleDB for historical storage
    3. Broadcast to connected WebSocket subscribers
    """

    # UK Stock tickers (LSE - London Stock Exchange)
    STOCK_TICKERS = [
        "HSBA.L",    # HSBC Holdings
        "BP.L",      # BP plc
        "SHEL.L",    # Shell plc
        "VOD.L",     # Vodafone Group
        "AZN.L",     # AstraZeneca
        "ULVR.L",    # Unilever
        "GSK.L",     # GSK plc
        "DGE.L",     # Diageo
        "BARC.L",    # Barclays
        "LLOY.L",    # Lloyds Banking Group
    ]

    # Stock metadata
    STOCK_METADATA = {
        "HSBA.L": {"name": "HSBC Holdings", "exchange": "LSE", "symbol": "HSBA"},
        "BP.L": {"name": "BP plc", "exchange": "LSE", "symbol": "BP"},
        "SHEL.L": {"name": "Shell plc", "exchange": "LSE", "symbol": "SHEL"},
        "VOD.L": {"name": "Vodafone Group", "exchange": "LSE", "symbol": "VOD"},
        "AZN.L": {"name": "AstraZeneca", "exchange": "LSE", "symbol": "AZN"},
        "ULVR.L": {"name": "Unilever", "exchange": "LSE", "symbol": "ULVR"},
        "GSK.L": {"name": "GSK plc", "exchange": "LSE", "symbol": "GSK"},
        "DGE.L": {"name": "Diageo", "exchange": "LSE", "symbol": "DGE"},
        "BARC.L": {"name": "Barclays", "exchange": "LSE", "symbol": "BARC"},
        "LLOY.L": {"name": "Lloyds Banking Group", "exchange": "LSE", "symbol": "LLOY"},
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
        logger.info("Starting UK stock collector...")

        # Start collection task
        self._task = asyncio.create_task(self._collect_stocks())

        # Start flush task
        self._flush_task = asyncio.create_task(self._flush_ticks_loop())

        logger.info("Started UK stock collection task")

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

        logger.info("Stopped UK stock collector")

    async def subscribe(self, websocket, stock_symbol: str):
        """Subscribe to stock updates"""
        async with self._lock:
            asset_key = f"uk_stock_{stock_symbol.lower()}"
            if asset_key not in self._subscribers:
                self._subscribers[asset_key] = set()
            self._subscribers[asset_key].add(websocket)
            logger.info(f"Client subscribed to {stock_symbol}")

    async def unsubscribe(self, websocket, stock_symbol: str):
        """Unsubscribe from stock updates"""
        async with self._lock:
            asset_key = f"uk_stock_{stock_symbol.lower()}"
            if asset_key in self._subscribers:
                self._subscribers[asset_key].discard(websocket)
                if not self._subscribers[asset_key]:
                    del self._subscribers[asset_key]

    async def get_last_price(self, stock_symbol: str) -> Optional[float]:
        """Get last known price for a stock"""
        async with self._price_lock:
            return self._last_prices.get(stock_symbol.upper())

    async def get_all_stocks(self) -> List[dict]:
        """Get all available stocks with metadata and last prices"""
        stocks = []
        async with self._price_lock:
            for ticker in self.STOCK_TICKERS:
                metadata = self.STOCK_METADATA.get(ticker, {})
                symbol = metadata.get("symbol", ticker.replace(".L", ""))
                stocks.append({
                    "symbol": symbol,
                    "name": metadata.get("name", symbol),
                    "exchange": metadata.get("exchange", "LSE"),
                    "currentPrice": self._last_prices.get(ticker),
                    "marketStatus": self._get_market_status(),
                })
        return stocks

    def _get_market_status(self) -> str:
        """Check if UK market is currently open"""
        from pytz import timezone as pytz_timezone
        
        now_gmt = datetime.now(pytz_timezone('Europe/London'))
        
        # Check if weekend
        if now_gmt.weekday() >= 5:
            return "closed"
        
        # Check if within market hours (8:00 AM - 4:30 PM GMT)
        market_open = now_gmt.replace(hour=8, minute=0, second=0, microsecond=0)
        market_close = now_gmt.replace(hour=16, minute=30, second=0, microsecond=0)
        
        if market_open <= now_gmt <= market_close:
            return "open"
        
        return "closed"

    async def _broadcast(self, stock_symbol: str, data: dict):
        """Broadcast tick to all subscribers of this stock"""
        metadata = self.STOCK_METADATA.get(stock_symbol, {})
        symbol = metadata.get("symbol", stock_symbol.replace(".L", ""))
        asset_key = f"uk_stock_{symbol.lower()}"
        
        async with self._lock:
            subscribers = self._subscribers.get(asset_key, set()).copy()

        if not subscribers:
            return

        message = json.dumps(data)
        tasks = [self._send_safe(ws, message, symbol) for ws in subscribers]
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
        # Lazy import to avoid circular dependency
        from app.services.candle_aggregator import candle_aggregator
        
        metadata = self.STOCK_METADATA.get(stock_symbol, {})
        symbol = metadata.get("symbol", stock_symbol.replace(".L", ""))
        asset_key = f"uk_stock_{symbol.lower()}"
        candle_aggregator.ingest_tick(asset_key, price, volume, ts)

    async def _collect_stocks(self):
        """Collect stock data from Yahoo Finance WebSocket"""
        import threading
        
        backoff = 2.0

        while self._running:
            try:
                logger.info(f"Connecting to Yahoo Finance WebSocket for UK stocks...")
                
                # Get event loop for scheduling coroutines from thread
                loop = asyncio.get_event_loop()
                
                # Flag to track if ticker is running
                ticker_running = threading.Event()
                ticker_error = None
                
                def on_ticker(ws, msg):
                    """Callback for ticker updates (runs in separate thread)"""
                    try:
                        # Schedule async processing in main event loop
                        future = asyncio.run_coroutine_threadsafe(
                            self._process_ticker_message(msg),
                            loop
                        )
                        # Wait for completion with timeout to avoid blocking
                        future.result(timeout=1.0)
                    except Exception as e:
                        logger.error(f"Error processing UK ticker: {e}")
                
                def run_ticker():
                    """Run YLiveTicker in separate thread"""
                    nonlocal ticker_error
                    try:
                        ticker_running.set()
                        yliveticker.YLiveTicker(
                            on_ticker=on_ticker,
                            ticker_names=self.STOCK_TICKERS
                        )
                    except Exception as e:
                        ticker_error = e
                        logger.error(f"YLiveTicker error: {e}")
                    finally:
                        ticker_running.clear()
                
                # Start YLiveTicker in daemon thread
                ticker_thread = threading.Thread(target=run_ticker, daemon=True)
                ticker_thread.start()
                
                # Wait for ticker to start
                ticker_running.wait(timeout=10)
                
                if not ticker_running.is_set():
                    raise Exception("Failed to start YLiveTicker")
                
                logger.info("✅ Connected to Yahoo Finance for UK stocks")
                backoff = 2.0
                
                # Keep running while ticker thread is alive
                while self._running and ticker_thread.is_alive():
                    await asyncio.sleep(1)
                
                # Check if thread died due to error
                if ticker_error:
                    raise ticker_error
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                market_status = self._get_market_status()
                error_msg = str(e) if str(e) else "Connection closed"
                if market_status == "closed":
                    logger.info(f"UK stock market is closed ({error_msg}). Reconnecting in {backoff}s...")
                else:
                    logger.warning(f"UK stock stream error: {error_msg}. Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _process_ticker_message(self, msg: dict):
        """Process incoming ticker message"""
        try:
            ticker = msg.get("id", "")
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
            symbol = metadata.get("symbol", ticker.replace(".L", ""))
            await self._broadcast(ticker, {
                "type": "tick",
                "asset": f"uk_stock_{symbol.lower()}",
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
            logger.debug(f"Flushed {len(ticks)} UK stock ticks to DB")
        except Exception as e:
            logger.error(f"Failed to flush UK stock ticks: {e}")

    async def _store_tick(self, ticker: str, price: float, volume: float, ts: datetime):
        """Buffer tick for database storage"""
        if not settings.STORE_RAW_TICKS:
            return

        metadata = self.STOCK_METADATA.get(ticker, {})
        symbol = metadata.get("symbol", ticker.replace(".L", ""))
        asset_key = f"uk_stock_{symbol.lower()}"

        async with self._buffer_lock:
            self._tick_buffer.append((ts, asset_key, price, volume, "yahoo_finance", None))

            if len(self._tick_buffer) >= 1000:
                asyncio.create_task(self._flush_ticks())


# Global instance
uk_stock_collector = UKStockCollector()
