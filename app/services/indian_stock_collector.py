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


class IndianStockCollector:
    """Collects real-time Indian stock data from Yahoo Finance WebSocket.

    Ticks are:
    1. Fed directly to CandleAggregator for in-memory candle building
    2. Buffered and batch-inserted to TimescaleDB for historical storage
    3. Broadcast to connected WebSocket subscribers
    """

    # Indian Stock tickers (NSE)
    STOCK_TICKERS = [
        "RELIANCE.NS",   # Reliance Industries
        "TCS.NS",        # Tata Consultancy Services
        "HDFCBANK.NS",   # HDFC Bank
        "INFY.NS",       # Infosys
        "ICICIBANK.NS",  # ICICI Bank
        "HINDUNILVR.NS", # Hindustan Unilever
        "BHARTIARTL.NS", # Bharti Airtel
        "ITC.NS",        # ITC Limited
        "SBIN.NS",       # State Bank of India
        "KOTAKBANK.NS",  # Kotak Mahindra Bank
    ]

    # Stock metadata
    STOCK_METADATA = {
        "RELIANCE.NS": {"name": "Reliance Industries", "exchange": "NSE", "symbol": "RELIANCE"},
        "TCS.NS": {"name": "Tata Consultancy Services", "exchange": "NSE", "symbol": "TCS"},
        "HDFCBANK.NS": {"name": "HDFC Bank", "exchange": "NSE", "symbol": "HDFCBANK"},
        "INFY.NS": {"name": "Infosys", "exchange": "NSE", "symbol": "INFY"},
        "ICICIBANK.NS": {"name": "ICICI Bank", "exchange": "NSE", "symbol": "ICICIBANK"},
        "HINDUNILVR.NS": {"name": "Hindustan Unilever", "exchange": "NSE", "symbol": "HINDUNILVR"},
        "BHARTIARTL.NS": {"name": "Bharti Airtel", "exchange": "NSE", "symbol": "BHARTIARTL"},
        "ITC.NS": {"name": "ITC Limited", "exchange": "NSE", "symbol": "ITC"},
        "SBIN.NS": {"name": "State Bank of India", "exchange": "NSE", "symbol": "SBIN"},
        "KOTAKBANK.NS": {"name": "Kotak Mahindra Bank", "exchange": "NSE", "symbol": "KOTAKBANK"},
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
        logger.info("Starting Indian stock collector...")

        # Start collection task
        self._task = asyncio.create_task(self._collect_stocks())

        # Start flush task
        self._flush_task = asyncio.create_task(self._flush_ticks_loop())

        logger.info("Started Indian stock collection task")

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

        logger.info("Stopped Indian stock collector")

    async def subscribe(self, websocket, stock_symbol: str):
        """Subscribe to stock updates"""
        async with self._lock:
            asset_key = f"indian_stock_{stock_symbol.lower()}"
            if asset_key not in self._subscribers:
                self._subscribers[asset_key] = set()
            self._subscribers[asset_key].add(websocket)
            logger.info(f"Client subscribed to {stock_symbol}")

    async def unsubscribe(self, websocket, stock_symbol: str):
        """Unsubscribe from stock updates"""
        async with self._lock:
            asset_key = f"indian_stock_{stock_symbol.lower()}"
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
                symbol = metadata.get("symbol", ticker.replace(".NS", ""))
                stocks.append({
                    "symbol": symbol,
                    "name": metadata.get("name", symbol),
                    "exchange": metadata.get("exchange", "NSE"),
                    "currentPrice": self._last_prices.get(ticker),
                    "marketStatus": self._get_market_status(),
                })
        return stocks

    def _get_market_status(self) -> str:
        """Check if Indian market is currently open"""
        from pytz import timezone as pytz_timezone
        
        now_ist = datetime.now(pytz_timezone('Asia/Kolkata'))
        
        # Check if weekend
        if now_ist.weekday() >= 5:
            return "closed"
        
        # Check if within market hours (9:15 AM - 3:30 PM IST)
        market_open = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
        
        if market_open <= now_ist <= market_close:
            return "open"
        
        return "closed"

    async def _broadcast(self, stock_symbol: str, data: dict):
        """Broadcast tick to all subscribers of this stock"""
        metadata = self.STOCK_METADATA.get(stock_symbol, {})
        symbol = metadata.get("symbol", stock_symbol.replace(".NS", ""))
        asset_key = f"indian_stock_{symbol.lower()}"
        
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
        symbol = metadata.get("symbol", stock_symbol.replace(".NS", ""))
        asset_key = f"indian_stock_{symbol.lower()}"
        candle_aggregator.ingest_tick(asset_key, price, volume, ts)

    async def _collect_stocks(self):
        """Collect stock data from Yahoo Finance WebSocket"""
        backoff = 2.0

        while self._running:
            try:
                logger.info(f"Connecting to Yahoo Finance WebSocket for Indian stocks...")
                
                # Create event loop for yliveticker
                loop = asyncio.get_event_loop()
                
                def on_ticker(ws, msg):
                    """Callback for ticker updates"""
                    try:
                        # Schedule async processing
                        asyncio.run_coroutine_threadsafe(
                            self._process_ticker_message(msg),
                            loop
                        )
                    except Exception as e:
                        logger.error(f"Error processing ticker: {e}")
                
                # Start Yahoo Finance WebSocket in thread
                await loop.run_in_executor(
                    None,
                    lambda: yliveticker.YLiveTicker(
                        on_ticker=on_ticker,
                        ticker_names=self.STOCK_TICKERS
                    )
                )
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                market_status = self._get_market_status()
                error_msg = str(e) if str(e) else "Connection closed"
                if market_status == "closed":
                    logger.info(f"Indian stock market is closed ({error_msg}). Reconnecting in {backoff}s...")
                else:
                    logger.warning(f"Indian stock stream error: {error_msg}. Reconnecting in {backoff}s...")
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
            
            # Use current time as timestamp
            ts = datetime.utcnow()
            
            # Update last known price
            async with self._price_lock:
                self._last_prices[ticker] = price
            
            # Process tick
            self._process_tick(ticker, price, volume, ts)
            await self._store_tick(ticker, price, volume, ts)
            
            # Broadcast to subscribers
            metadata = self.STOCK_METADATA.get(ticker, {})
            symbol = metadata.get("symbol", ticker.replace(".NS", ""))
            await self._broadcast(ticker, {
                "type": "tick",
                "asset": f"indian_stock_{symbol.lower()}",
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "timestamp": ts.isoformat(),
                "marketStatus": self._get_market_status(),
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
            logger.debug(f"Flushed {len(ticks)} Indian stock ticks to DB")
        except Exception as e:
            logger.error(f"Failed to flush Indian stock ticks: {e}")

    async def _store_tick(self, ticker: str, price: float, volume: float, ts: datetime):
        """Buffer tick for database storage"""
        if not settings.STORE_RAW_TICKS:
            return

        metadata = self.STOCK_METADATA.get(ticker, {})
        symbol = metadata.get("symbol", ticker.replace(".NS", ""))
        asset_key = f"indian_stock_{symbol.lower()}"

        async with self._buffer_lock:
            self._tick_buffer.append((ts, asset_key, price, volume, "yahoo_finance", None))

            if len(self._tick_buffer) >= 1000:
                asyncio.create_task(self._flush_ticks())


# Global instance
indian_stock_collector = IndianStockCollector()
