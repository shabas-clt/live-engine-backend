import asyncpg
from typing import Optional
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)


class Database:
    """Database connection manager for PostgreSQL/TimescaleDB"""

    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Initialize database connection with optimized pool settings"""
        try:
            # PostgreSQL connection pool - optimized for high load
            self.pool = await asyncpg.create_pool(
                settings.timescaledb_url,
                min_size=10,      # Higher minimum for faster response
                max_size=50,      # Support 1000+ concurrent users
                command_timeout=30,  # Shorter timeout
                max_queries=50000,   # Queries per connection before recycling
                max_inactive_connection_lifetime=300,  # 5 min idle timeout
            )
            logger.info("✅ Connected to PostgreSQL/TimescaleDB (optimized pool: 10-50 connections)")

            # Create tables if not exist
            await self._init_schema()

        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    async def disconnect(self):
        """Close database connection"""
        if self.pool:
            await self.pool.close()
            logger.info("Closed PostgreSQL connection")

    async def _init_schema(self):
        """Initialize PostgreSQL schema with all tables"""
        async with self.pool.acquire() as conn:
            # Enable TimescaleDB extension
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")

            # Create admins table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    email VARCHAR(255) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(20) NOT NULL DEFAULT 'admin',
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    last_login TIMESTAMPTZ
                );
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_admins_email ON admins(email);
            """)

            # Create tokens table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    token VARCHAR(255) UNIQUE NOT NULL,
                    name VARCHAR(100) NOT NULL,
                    description TEXT,
                    status VARCHAR(20) DEFAULT 'active',
                    assigned_to VARCHAR(50),
                    hourly_requests INTEGER DEFAULT 0,
                    daily_requests INTEGER DEFAULT 0,
                    monthly_bandwidth_mb FLOAT DEFAULT 0.0,
                    hourly_limit INTEGER DEFAULT 50,
                    daily_limit INTEGER DEFAULT 1000,
                    monthly_bandwidth_limit_mb FLOAT DEFAULT 1024.0,
                    last_used TIMESTAMPTZ,
                    last_reset_hour TIMESTAMPTZ,
                    last_reset_day TIMESTAMPTZ,
                    last_reset_month TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tokens_status ON tokens(status);
                CREATE INDEX IF NOT EXISTS idx_tokens_assigned ON tokens(assigned_to);
            """)

            # Create ticks table (raw tick data)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ticks (
                    time TIMESTAMPTZ NOT NULL,
                    asset VARCHAR(20) NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    volume DOUBLE PRECISION,
                    source VARCHAR(50),
                    token_id UUID
                );
            """)

            # Convert to hypertable if not already
            try:
                await conn.execute("""
                    SELECT create_hypertable('ticks', 'time', 
                        if_not_exists => TRUE,
                        chunk_time_interval => INTERVAL '1 day'
                    );
                """)
            except Exception:
                pass  # Already a hypertable

            # Create index for faster queries
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ticks_asset_time 
                ON ticks (asset, time DESC);
            """)

            # Create candles table (aggregated OHLCV data)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                    time TIMESTAMPTZ NOT NULL,
                    asset VARCHAR(20) NOT NULL,
                    interval VARCHAR(10) NOT NULL,
                    open DOUBLE PRECISION NOT NULL,
                    high DOUBLE PRECISION NOT NULL,
                    low DOUBLE PRECISION NOT NULL,
                    close DOUBLE PRECISION NOT NULL,
                    volume DOUBLE PRECISION,
                    tick_count INTEGER,
                    UNIQUE(time, asset, interval)
                );
            """)

            # Convert to hypertable
            try:
                await conn.execute("""
                    SELECT create_hypertable('candles', 'time',
                        if_not_exists => TRUE,
                        chunk_time_interval => INTERVAL '7 days'
                    );
                """)
            except Exception:
                pass

            # Create index
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_candles_asset_interval_time
                ON candles (asset, interval, time DESC);
            """)

            # Enable compression for older data (saves 90%+ space)
            try:
                await conn.execute("""
                    ALTER TABLE ticks SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'asset'
                    );
                """)
                await conn.execute("""
                    SELECT add_compression_policy('ticks', INTERVAL '7 days');
                """)
            except Exception:
                pass  # Compression already enabled

            try:
                await conn.execute("""
                    ALTER TABLE candles SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'asset,interval'
                    );
                """)
                await conn.execute("""
                    SELECT add_compression_policy('candles', INTERVAL '30 days');
                """)
            except Exception:
                pass

            logger.info("✅ PostgreSQL schema initialized (admins, tokens, ticks, candles)")


# Global database instance
db = Database()
