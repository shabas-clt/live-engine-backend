from fastapi import APIRouter
from typing import List, Optional
import logging

from app.services.stock_collector import stock_collector
from app.services.indian_stock_collector import indian_stock_collector
from app.services.uk_stock_collector import uk_stock_collector

router = APIRouter(tags=["Stocks"])
logger = logging.getLogger(__name__)


@router.get("/stocks")
async def list_stocks():
    """
    Get list of all available stocks (US + Indian + UK) with current prices
    
    Returns:
    - List of stocks with symbol, name, exchange, current price, and market status
    """
    us_stocks = await stock_collector.get_all_stocks()
    indian_stocks = await indian_stock_collector.get_all_stocks()
    uk_stocks = await uk_stock_collector.get_all_stocks()
    
    all_stocks = us_stocks + indian_stocks + uk_stocks
    
    # Determine overall market status
    us_status = us_stocks[0]["marketStatus"] if us_stocks else "unknown"
    indian_status = indian_stocks[0]["marketStatus"] if indian_stocks else "unknown"
    uk_status = uk_stocks[0]["marketStatus"] if uk_stocks else "unknown"
    
    # If any market is open, show "open"
    overall_status = "open" if (us_status == "open" or indian_status == "open" or uk_status == "open") else "closed"
    
    return {
        "stocks": all_stocks,
        "total": len(all_stocks),
        "marketStatus": overall_status,
        "markets": {
            "us": {"status": us_status, "count": len(us_stocks)},
            "india": {"status": indian_status, "count": len(indian_stocks)},
            "uk": {"status": uk_status, "count": len(uk_stocks)},
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


@router.get("/stocks/uk")
async def list_uk_stocks():
    """
    Get list of UK stocks only
    
    Returns:
    - List of UK stocks with symbol, name, exchange, current price, and market status
    """
    stocks = await uk_stock_collector.get_all_stocks()
    
    return {
        "stocks": stocks,
        "total": len(stocks),
        "marketStatus": stocks[0]["marketStatus"] if stocks else "unknown",
        "market": "UK"
    }


@router.get("/stocks/{symbol}")
async def get_stock(symbol: str):
    """
    Get details for a specific stock (US, Indian, or UK)
    
    Parameters:
    - symbol: Stock symbol (e.g., AAPL, RELIANCE, HSBA)
    """
    symbol_upper = symbol.upper()
    symbol_lower = symbol.lower()
    
    # Check US stocks
    if symbol_upper in stock_collector.STOCK_TICKERS:
        metadata = stock_collector.STOCK_METADATA.get(symbol_upper, {})
        last_price = await stock_collector.get_last_price(symbol_upper)
        
        return {
            "symbol": symbol_upper,
            "name": metadata.get("name", symbol_upper),
            "exchange": metadata.get("exchange", "NASDAQ"),
            "currentPrice": last_price,
            "marketStatus": stock_collector._get_market_status(),
            "market": "US"
        }
    
    # Check Indian stocks
    indian_ticker_map = {meta["symbol"].lower(): ticker for ticker, meta in indian_stock_collector.STOCK_METADATA.items()}
    if symbol_lower in indian_ticker_map:
        ticker = indian_ticker_map[symbol_lower]
        metadata = indian_stock_collector.STOCK_METADATA.get(ticker, {})
        last_price = await indian_stock_collector.get_last_price(ticker)
        
        return {
            "symbol": symbol.upper(),
            "name": metadata.get("name", symbol.upper()),
            "exchange": metadata.get("exchange", "NSE"),
            "currentPrice": last_price,
            "marketStatus": indian_stock_collector._get_market_status(),
            "market": "India"
        }
    
    # Check UK stocks
    uk_ticker_map = {meta["symbol"].lower(): ticker for ticker, meta in uk_stock_collector.STOCK_METADATA.items()}
    if symbol_lower in uk_ticker_map:
        ticker = uk_ticker_map[symbol_lower]
        metadata = uk_stock_collector.STOCK_METADATA.get(ticker, {})
        last_price = await uk_stock_collector.get_last_price(ticker)
        
        return {
            "symbol": symbol.upper(),
            "name": metadata.get("name", symbol.upper()),
            "exchange": metadata.get("exchange", "LSE"),
            "currentPrice": last_price,
            "marketStatus": uk_stock_collector._get_market_status(),
            "market": "UK"
        }
    
    # Stock not found
    all_symbols = (
        [s.upper() for s in stock_collector.STOCK_TICKERS] +
        [s.upper() for s in indian_ticker_map.keys()] +
        [s.upper() for s in uk_ticker_map.keys()]
    )
    
    return {
        "error": "Stock not found",
        "availableStocks": all_symbols
    }


@router.get("/stocks/{symbol}/price")
async def get_stock_price(symbol: str):
    """
    Get current price for a specific stock (US, Indian, or UK)
    
    Parameters:
    - symbol: Stock symbol (e.g., AAPL, RELIANCE, HSBA)
    """
    symbol_upper = symbol.upper()
    symbol_lower = symbol.lower()
    
    # Check US stocks
    if symbol_upper in stock_collector.STOCK_TICKERS:
        last_price = await stock_collector.get_last_price(symbol_upper)
        
        if last_price is None:
            return {
                "symbol": symbol_upper,
                "price": None,
                "message": "No price data available yet",
                "marketStatus": stock_collector._get_market_status(),
                "market": "US"
            }
        
        return {
            "symbol": symbol_upper,
            "price": last_price,
            "marketStatus": stock_collector._get_market_status(),
            "market": "US"
        }
    
    # Check Indian stocks
    indian_ticker_map = {meta["symbol"].lower(): ticker for ticker, meta in indian_stock_collector.STOCK_METADATA.items()}
    if symbol_lower in indian_ticker_map:
        ticker = indian_ticker_map[symbol_lower]
        last_price = await indian_stock_collector.get_last_price(ticker)
        
        if last_price is None:
            return {
                "symbol": symbol.upper(),
                "price": None,
                "message": "No price data available yet",
                "marketStatus": indian_stock_collector._get_market_status(),
                "market": "India"
            }
        
        return {
            "symbol": symbol.upper(),
            "price": last_price,
            "marketStatus": indian_stock_collector._get_market_status(),
            "market": "India"
        }
    
    # Check UK stocks
    uk_ticker_map = {meta["symbol"].lower(): ticker for ticker, meta in uk_stock_collector.STOCK_METADATA.items()}
    if symbol_lower in uk_ticker_map:
        ticker = uk_ticker_map[symbol_lower]
        last_price = await uk_stock_collector.get_last_price(ticker)
        
        if last_price is None:
            return {
                "symbol": symbol.upper(),
                "price": None,
                "message": "No price data available yet",
                "marketStatus": uk_stock_collector._get_market_status(),
                "market": "UK"
            }
        
        return {
            "symbol": symbol.upper(),
            "price": last_price,
            "marketStatus": uk_stock_collector._get_market_status(),
            "market": "UK"
        }
    
    # Stock not found
    return {
        "error": "Stock not found"
    }
