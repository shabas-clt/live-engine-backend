from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Optional
import logging

from app.services.data_collector import data_collector

router = APIRouter(tags=["WebSocket Stream"])
logger = logging.getLogger(__name__)


@router.websocket("/ws/stream")
async def websocket_stream(
    websocket: WebSocket,
    asset: Optional[str] = Query(None, description="Asset to subscribe (bitcoin, gold, silver)"),
):
    """
    WebSocket endpoint for real-time data streaming
    
    Usage:
    - Connect to: ws://localhost:8001/ws/stream?asset=bitcoin
    - Receives: {"type": "tick", "asset": "bitcoin", "price": 50000.0, "timestamp": "2024-01-01T00:00:00"}
    """
    await websocket.accept()
    
    if not asset:
        await websocket.send_json({"error": "Asset parameter required (bitcoin, gold, silver)"})
        await websocket.close()
        return
    
    asset = asset.lower()
    valid_assets = ["bitcoin", "gold", "silver"]
    
    if asset not in valid_assets:
        await websocket.send_json({"error": f"Invalid asset. Must be one of: {', '.join(valid_assets)}"})
        await websocket.close()
        return
    
    # Subscribe to data updates
    await data_collector.subscribe(websocket, asset)
    logger.info(f"Client connected to {asset} stream")
    
    try:
        # Keep connection alive
        while True:
            # Wait for client messages (ping/pong)
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    
    except WebSocketDisconnect:
        logger.info(f"Client disconnected from {asset} stream")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await data_collector.unsubscribe(websocket, asset)
