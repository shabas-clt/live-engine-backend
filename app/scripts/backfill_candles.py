"""
Backfill historical candle data from Tiingo API to PostgreSQL

Usage:
    python -m app.scripts.backfill_candles --days 30
    python -m app.scripts.backfill_candles --start-date 2024-12-01 --end-date 2024-12-31
"""
import asyncio
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
import httpx
import asyncpg

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.core.config import settings
from app.core.database import db
from app.services.token_manager import token_manager


TIINGO_CRYPTO_ENDPOINT = "https://api.tiingo.com/tiingo/crypto/prices"
TIINGO_FX_ENDPOINT = "https://api.tiingo.com/tiingo/fx/prices"

ASSET_CONFIG = {
    'bitcoin': {
        'endpoint': TIINGO_CRYPTO_ENDPOINT,
        'ticker': 'btcusd',
    },
    'gold': {
        'endpoint': TIINGO_FX_ENDPOINT,
        'ticker': 'xauusd',
    },
    'silver': {
        'endpoint': TIINGO_FX_ENDPOINT,
        'ticker': 'xagusd',
    },
}

TIINGO_INTERVALS = {
    '1s': '1min',
    '5s': '1min',
    '10s': '1min',
    '15s': '1min',
    '30s': '1min',
    '1m': '1min',
    '5m': '5min',
    '15m': '15min',
}


async def fetch_tiingo_candles(
    asset: str,
    interval: str,
    start_date: datetime,
    end_date: datetime,
    token: str,
) -> list[dict]:
    """Fetch historical candles from Tiingo API"""
    config = ASSET_CONFIG[asset]
    tiingo_interval = TIINGO_INTERVALS[interval]
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                config['endpoint'],
                params={
                    'tickers': config['ticker'],
                    'startDate': start_date.date().isoformat(),
                    'endDate': end_date.date().isoformat(),
                    'resampleFreq': tiingo_interval,
                    'token': token,
                },
            )
            response.raise_for_status()
            data = response.json()
            
            if not data:
                return []
            
            candles = []
            for item in data:
                if 'priceData' not in item:
                    continue
                
                for price_data in item['priceData']:
                    try:
                        timestamp = datetime.fromisoformat(
                            price_data['date'].replace('Z', '+00:00')
                        ).replace(tzinfo=None)
                        
                        candle = {
                            'time': timestamp,
                            'asset': asset,
                            'interval': interval,
                            'open': float(price_data['open']),
                            'high': float(price_data['high']),
                            'low': float(price_data['low']),
                            'close': float(price_data['close']),
                            'volume': float(price_data.get('volume', 0)),
                            'tick_count': 0,
                        }
                        
                        if candle['low'] <= candle['open'] <= candle['high']:
                            candles.append(candle)
                    except (KeyError, ValueError, TypeError):
                        continue
            
            return candles
            
    except httpx.HTTPStatusError as e:
        print(f"    ❌ HTTP error {e.response.status_code}")
        return []
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return []


async def aggregate_1min_to_interval(candles_1min: list[dict], target_interval: str) -> list[dict]:
    """Aggregate 1-minute candles to smaller intervals (1s, 5s, 10s, 15s, 30s)"""
    if target_interval in ['1m', '5m', '15m']:
        return candles_1min
    
    interval_seconds = {
        '1s': 1,
        '5s': 5,
        '10s': 10,
        '15s': 15,
        '30s': 30,
    }
    
    seconds = interval_seconds.get(target_interval)
    if not seconds:
        return candles_1min
    
    aggregated = []
    for candle_1min in candles_1min:
        base_time = candle_1min['time']
        price = candle_1min['close']
        
        for offset in range(0, 60, seconds):
            bucket_time = base_time + timedelta(seconds=offset)
            aggregated.append({
                'time': bucket_time,
                'asset': candle_1min['asset'],
                'interval': target_interval,
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': candle_1min['volume'] / (60 / seconds),
                'tick_count': 0,
            })
    
    return aggregated


async def batch_insert_candles(candles: list[dict]):
    """Batch insert candles to PostgreSQL"""
    if not candles:
        return 0
    
    async with db.pool.acquire() as conn:
        query = """
            INSERT INTO candles (time, asset, interval, open, high, low, close, volume, tick_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (time, asset, interval) DO NOTHING
        """
        
        records = [
            (
                c['time'],
                c['asset'],
                c['interval'],
                c['open'],
                c['high'],
                c['low'],
                c['close'],
                c['volume'],
                c['tick_count'],
            )
            for c in candles
        ]
        
        await conn.executemany(query, records)
    
    return len(candles)


async def backfill_asset_interval(
    asset: str,
    interval: str,
    start_date: datetime,
    end_date: datetime,
    batch_days: int = 7,
):
    """Backfill one asset and interval"""
    total_stored = 0
    current_start = start_date
    
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=batch_days), end_date)
        
        token_obj = await token_manager.get_next_token()
        if not token_obj:
            print(f"    ⚠️  No tokens available, waiting...")
            await asyncio.sleep(60)
            continue
        
        print(f"  Fetching {current_start.date()} to {current_end.date()} with {token_obj.name}...")
        
        candles = await fetch_tiingo_candles(
            asset, interval, current_start, current_end, token_obj.token
        )
        
        if candles:
            if interval in ['1s', '5s', '10s', '15s', '30s']:
                candles = await aggregate_1min_to_interval(candles, interval)
            
            stored = await batch_insert_candles(candles)
            total_stored += stored
            print(f"    ✅ Stored {stored} candles")
        else:
            print(f"    ⚠️  No data returned")
        
        current_start = current_end
        await asyncio.sleep(0.5)
    
    return total_stored


async def backfill_candles(days: int = 30, start_date: datetime = None, end_date: datetime = None):
    """Main backfill function"""
    print("=" * 70)
    print("HISTORICAL CANDLE BACKFILL")
    print("=" * 70)
    
    if start_date and end_date:
        print(f"\nBackfilling from {start_date.date()} to {end_date.date()}...")
    else:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)
        print(f"\nBackfilling last {days} days...")
        print(f"From: {start_date.date()}")
        print(f"To:   {end_date.date()}")
    
    print()
    
    await db.connect()
    await token_manager.initialize()
    
    assets = []
    if settings.COLLECT_BTC:
        assets.append('bitcoin')
    if settings.COLLECT_GOLD:
        assets.append('gold')
    if settings.COLLECT_SILVER:
        assets.append('silver')
    
    intervals = ['1s', '5s', '10s', '15s', '30s', '1m', '5m', '15m']
    
    total_combinations = len(assets) * len(intervals)
    current = 0
    grand_total = 0
    
    for asset in assets:
        for interval in intervals:
            current += 1
            print(f"\n[{current}/{total_combinations}] Processing {asset} {interval}")
            
            try:
                stored = await backfill_asset_interval(asset, interval, start_date, end_date)
                grand_total += stored
                print(f"  ✅ Total stored for {asset} {interval}: {stored}")
            except Exception as e:
                print(f"  ❌ Failed: {e}")
                continue
    
    print("\n" + "=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)
    print(f"Total candles stored: {grand_total:,}")
    print()
    
    async with db.pool.acquire() as conn:
        count_query = "SELECT COUNT(*) FROM candles"
        total_count = await conn.fetchval(count_query)
        print(f"Total candles in database: {total_count:,}")
    
    print()
    await db.disconnect()


def main():
    parser = argparse.ArgumentParser(description="Backfill historical candle data")
    parser.add_argument('--days', type=int, default=30, help="Number of days to backfill")
    parser.add_argument('--start-date', type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument('--end-date', type=str, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()
    
    start_date = None
    end_date = None
    
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            print("Error: Invalid start-date format. Use YYYY-MM-DD")
            sys.exit(1)
    
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        except ValueError:
            print("Error: Invalid end-date format. Use YYYY-MM-DD")
            sys.exit(1)
    
    if (start_date and not end_date) or (end_date and not start_date):
        print("Error: Both --start-date and --end-date must be provided together")
        sys.exit(1)
    
    asyncio.run(backfill_candles(args.days, start_date, end_date))


if __name__ == "__main__":
    main()
