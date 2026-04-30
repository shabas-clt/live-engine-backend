from fastapi import APIRouter
from typing import List, Optional
import logging

from app.services.stock_collector import stock_collector
from app.services.indian_stock_collector import indian_stock_collector

router = APIRouter(tags=["Stocks"])
logger = logging.getLogger(__name__)


@router.get("/stocks")
async def list_stocks():
    """
    Get list of all available stocks (US + Indian) with current prices
    
    Returns:
    - List of stocks with symbol, name, exchange, current price, and market status
    """
    us_stocks = await stock_collector.get_all_stocks()
    indian_stocks = await indian_stock_collector.get_all_stocks()
    
    all_stocks = us_stocks + indian_stocks
    
    # Determine overall market status
    us_status = us_stocks[0]["marketStatus"] if us_stocks else "unknown"
    indian_status = indian_stocks[0]["marketStatus"] if indian_stocks else "unknown"
    
    # If either market is open, show "open"
    overall_status = "open" if (us_status == "open" or indian_status == "open") else "closed"
    
    return {
        "stocks": all_stocks,
        "total": len(all_stocks),
        "marketStatus": overall_status,
        "markets": {
            "us": {"status": us_status, "count": len(us_stocks)},
            "india": {"status": indian_status, "count": len(indian_stocks)},
        }
    }


@router.get("/stocks/us")
async def list_us_stocks():
    """
    Get list of US stocks only
    
    Returns:
    - List of US stocks with symbol, name, exchange, current price, and market status
    """
    stocks = await stock_collector.get_all_stocks()
    
    return {
        "stocks": stocks,
        "total": len(stocks),
        "marketStatus": stocks[0]["marketStatus"] if stocks else "unknown",
        "market": "US"
    }


@router.get("/stocks/indian")
async def list_indian_stocks():
    """
    Get list of Indian stocks only
    
    Returns:
    - List of Indian stocks with symbol, name, exchange, current price, and market status
    """
    stocks = await indian_stock_collector.get_all_stocks()
    
    return {
        "stocks": stocks,
        "total": len(stocks),
        "marketStatus": stocks[0]["marketStatus"] if stocks else "unknown",
        "market": "India"
    }


@router.get("/stocks/{symbol}")
async def get_stock(symbol: str):
    """
    Get details for a specific stock
    
    Parameters:
    - symbol: Stock symbol (e.g., AAPL, GOOGL, MSFT)
    """
    symbol_lower = symbol.lower()
    
    # Check if stock exists
    if symbol_lower not in stock_collector.STOCK_TICKERS:
        return {
            "error": "Stock not found",
            "availableStocks": [s.upper() for s in stock_collector.STOCK_TICKERS]
        }
    
    # Get metadata
    metadata = stock_collector.STOCK_METADATA.get(symbol_lower, {})
    
    # Get last price
    last_price = await stock_collector.get_last_price(symbol_lower)
    
    return {
        "symbol": symbol.upper(),
        "name": metadata.get("name", symbol.upper()),
        "exchange": metadata.get("exchange", "NASDAQ"),
        "currentPrice": last_price,
        "marketStatus": stock_collector._get_market_status(),
    }


@router.get("/stocks/{symbol}/price")
async def get_stock_price(symbol: str):
    """
    Get current price for a specific stock
    
    Parameters:
    - symbol: Stock symbol (e.g., AAPL, GOOGL, MSFT)
    """
    symbol_lower = symbol.lower()
    
    # Check if stock exists
    if symbol_lower not in stock_collector.STOCK_TICKERS:
        return {
            "error": "Stock not found"
        }
    
    # Get last price
    last_price = await stock_collector.get_last_price(symbol_lower)
    
    if last_price is None:
        return {
            "symbol": symbol.upper(),
            "price": None,
            "message": "No price data available yet",
            "marketStatus": stock_collector._get_market_status(),
        }
    
    return {
        "symbol": symbol.upper(),
        "price": last_price,
        "marketStatus": stock_collector._get_market_status(),
    }
