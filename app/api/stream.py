from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Optional
import logging

from app.services.data_collector import data_collector
from app.services.stock_collector import stock_collector

router = APIRouter(tags=["WebSocket Stream"])
logger = logging.getLogger(__name__)


@router.websocket("/ws/stream")
async def websocket_stream(
    websocket: WebSocket,
    asset: Optional[str] = Query(None, description="Asset to subscribe (bitcoin, gold, silver, stock_aapl, stock_googl, etc.)"),
):
    """
    WebSocket endpoint for real-time data streaming
    
    Usage:
    - Crypto/Commodities: ws://localhost:8001/ws/stream?asset=bitcoin
    - Stocks: ws://localhost:8001/ws/stream?asset=stock_aapl
    
    Receives: {"type": "tick", "asset": "bitcoin", "price": 50000.0, "timestamp": "2024-01-01T00:00:00"}
    """
    await websocket.accept()
    
    if not asset:
        await websocket.send_json({
            "error": "Asset parameter required",
            "examples": ["bitcoin", "gold", "silver", "stock_aapl", "stock_googl", "stock_msft"]
        })
        await websocket.close()
        return
    
    asset = asset.lower()
    
    # Check if it's a stock asset
    if asset.startswith("stock_"):
        stock_symbol = asset.replace("stock_", "")
        
        # Validate stock symbol
        if stock_symbol not in stock_collector.STOCK_TICKERS:
            await websocket.send_json({
                "error": f"Invalid stock symbol: {stock_symbol}",
                "availableStocks": [f"stock_{s}" for s in stock_collector.STOCK_TICKERS]
            })
            await websocket.close()
            return
        
        # Subscribe to stock updates
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
                "error": f"Invalid asset. Must be one of: {', '.join(valid_assets)} or stock_<symbol>",
                "examples": ["bitcoin", "gold", "silver", "stock_aapl", "stock_googl"]
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
