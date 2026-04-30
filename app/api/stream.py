from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Optional
import logging

from app.services.data_collector import data_collector
from app.services.stock_collector import stock_collector
from app.services.indian_stock_collector import indian_stock_collector
from app.services.uk_stock_collector import uk_stock_collector

router = APIRouter(tags=["WebSocket Stream"])
logger = logging.getLogger(__name__)


@router.websocket("/ws/stream")
async def websocket_stream(
    websocket: WebSocket,
    asset: Optional[str] = Query(None, description="Asset to subscribe (bitcoin, gold, silver, stock_aapl, indian_stock_reliance, uk_stock_hsba, etc.)"),
):
    """
    WebSocket endpoint for real-time data streaming
    
    Usage:
    - Crypto/Commodities: ws://localhost:8001/ws/stream?asset=bitcoin
    - US Stocks: ws://localhost:8001/ws/stream?asset=stock_aapl
    - Indian Stocks: ws://localhost:8001/ws/stream?asset=indian_stock_reliance
    - UK Stocks: ws://localhost:8001/ws/stream?asset=uk_stock_hsba
    
    Receives: {"type": "tick", "asset": "bitcoin", "price": 50000.0, "timestamp": "2024-01-01T00:00:00"}
    """
    await websocket.accept()
    
    if not asset:
        await websocket.send_json({
            "error": "Asset parameter required",
            "examples": ["bitcoin", "gold", "silver", "stock_aapl", "indian_stock_reliance", "uk_stock_hsba"]
        })
        await websocket.close()
        return
    
    asset = asset.lower()
    
    # Check if it's a UK stock asset
    if asset.startswith("uk_stock_"):
        stock_symbol = asset.replace("uk_stock_", "")
        
        # Map symbol to Yahoo Finance ticker
        ticker_map = {meta["symbol"].lower(): ticker for ticker, meta in uk_stock_collector.STOCK_METADATA.items()}
        
        if stock_symbol not in ticker_map:
            await websocket.send_json({
                "error": f"Invalid UK stock symbol: {stock_symbol}",
                "availableStocks": [f"uk_stock_{s}" for s in ticker_map.keys()]
            })
            await websocket.close()
            return
        
        # Subscribe to UK stock updates
        await uk_stock_collector.subscribe(websocket, stock_symbol)
        logger.info(f"Client connected to {asset} stream")
        
        try:
            # Keep connection alive
            while True:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
        
        except WebSocketDisconnect:
            logger.info(f"Client disconnected from {asset} stream")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            await uk_stock_collector.unsubscribe(websocket, stock_symbol)
    
    # Check if it's an Indian stock asset
    elif asset.startswith("indian_stock_"):
        stock_symbol = asset.replace("indian_stock_", "")
        
        # Map symbol to Yahoo Finance ticker
        ticker_map = {meta["symbol"].lower(): ticker for ticker, meta in indian_stock_collector.STOCK_METADATA.items()}
        
        if stock_symbol not in ticker_map:
            await websocket.send_json({
                "error": f"Invalid Indian stock symbol: {stock_symbol}",
                "availableStocks": [f"indian_stock_{s}" for s in ticker_map.keys()]
            })
            await websocket.close()
            return
        
        # Subscribe to Indian stock updates
        await indian_stock_collector.subscribe(websocket, stock_symbol)
        logger.info(f"Client connected to {asset} stream")
        
        try:
            # Keep connection alive
            while True:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
        
        except WebSocketDisconnect:
            logger.info(f"Client disconnected from {asset} stream")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            await indian_stock_collector.unsubscribe(websocket, stock_symbol)
    
    # Check if it's a US stock asset
    elif asset.startswith("stock_"):
        stock_symbol = asset.replace("stock_", "").upper()  # Convert to uppercase
        
        # Validate stock symbol
        if stock_symbol not in stock_collector.STOCK_TICKERS:
            await websocket.send_json({
                "error": f"Invalid US stock symbol: {stock_symbol}",
                "availableStocks": [f"stock_{s.lower()}" for s in stock_collector.STOCK_TICKERS]
            })
            await websocket.close()
            return
        
        # Subscribe to US stock updates
        await stock_collector.subscribe(websocket, stock_symbol)
        logger.info(f"Client connected to {asset} stream")
        
        try:
            # Keep connection alive
            while True:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
        
        except WebSocketDisconnect:
            logger.info(f"Client disconnected from {asset} stream")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            await stock_collector.unsubscribe(websocket, stock_symbol)
    
    else:
        # Handle crypto/commodity assets
        valid_assets = ["bitcoin", "gold", "silver"]
        
        if asset not in valid_assets:
            await websocket.send_json({
                "error": f"Invalid asset. Must be one of: {', '.join(valid_assets)}, stock_<symbol>, indian_stock_<symbol>, or uk_stock_<symbol>",
                "examples": ["bitcoin", "gold", "silver", "stock_aapl", "indian_stock_reliance", "uk_stock_hsba"]
            })
            await websocket.close()
            return
        
        # Subscribe to data updates
        await data_collector.subscribe(websocket, asset)
        logger.info(f"Client connected to {asset} stream")
        
        try:
            # Keep connection alive
            while True:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
        
        except WebSocketDisconnect:
            logger.info(f"Client disconnected from {asset} stream")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            await data_collector.unsubscribe(websocket, asset)
