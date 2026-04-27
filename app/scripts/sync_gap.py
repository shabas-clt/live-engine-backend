"""
Sync gap between historical data and current time

This script fills the gap between the last candle in the database
and the current time, ensuring no data is missing before starting
live aggregation.

Usage:
    python -m app.scripts.sync_gap
"""
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.core.config import settings
from app.core.database import db
from app.services.token_manager import token_manager
from app.scripts.backfill_candles import fetch_tiingo_candles, batch_insert_candles, aggregate_1min_to_interval


async def get_latest_candle(asset: str, interval: str) -> dict | None:
    """Get the most recent candle for an asset and interval"""
    async with db.pool.acquire() as conn:
        query = """
            SELECT time, open, high, low, close, volume, tick_count
            FROM candles
            WHERE asset = $1 AND interval = $2
            ORDER BY time DESC
            LIMIT 1
        """
        row = await conn.fetchrow(query, asset, interval)
        
        if not row:
            return None
        
        return {
            'time': row['time'],
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': float(row['volume']) if row['volume'] else 0.0,
            'tick_count': int(row['tick_count']) if row['tick_count'] else 0,
        }


async def sync_gap():
    """Fill gap between last historical candle and current time"""
    print("=" * 70)
    print("GAP SYNC")
    print("=" * 70)
    print("\nChecking for gaps between historical data and current time...\n")
    
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
    
    current_time = datetime.utcnow()
    total_synced = 0
    
    for asset in assets:
        for interval in intervals:
            print(f"Checking {asset} {interval}...")
            
            try:
                last_candle = await get_latest_candle(asset, interval)
                
                if not last_candle:
                    print(f"  ⚠️  No historical data found")
                    continue
                
                last_time = last_candle['time']
                gap_hours = (current_time - last_time).total_seconds() / 3600
                
                if gap_hours < 0.1:
                    print(f"  ✅ No gap (last candle: {last_time})")
                    continue
                
                print(f"  🔄 Gap of {gap_hours:.1f} hours detected")
                print(f"     Last candle: {last_time}")
                print(f"     Current time: {current_time}")
                
                token_obj = await token_manager.get_next_token()
                if not token_obj:
                    print(f"  ⚠️  No tokens available")
                    continue
                
                print(f"  Fetching gap data with {token_obj.name}...")
                
                candles = await fetch_tiingo_candles(
                    asset,
                    interval,
                    last_time,
                    current_time,
                    token_obj.token,
                )
                
                if candles:
                    if interval in ['1s', '5s', '10s', '15s', '30s']:
                        candles = await aggregate_1min_to_interval(candles, interval)
                    
                    stored = await batch_insert_candles(candles)
                    total_synced += stored
                    print(f"  ✅ Synced {stored} candles")
                else:
                    print(f"  ⚠️  No data returned from Tiingo")
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                print(f"  ❌ Error: {e}")
                continue
    
    print("\n" + "=" * 70)
    print("GAP SYNC COMPLETE")
    print("=" * 70)
    print(f"Total candles synced: {total_synced:,}")
    print("\n✅ Ready to start live aggregation!\n")
    
    await db.disconnect()


def main():
    asyncio.run(sync_gap())


if __name__ == "__main__":
    main()
