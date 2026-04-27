import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Set
import httpx

from app.models.token import TiingoToken, TokenStatus, TokenUsageStats
from app.core.database import db

logger = logging.getLogger(__name__)


class TokenManager:
    """Manages Tiingo API tokens with rotation and usage tracking"""

    def __init__(self):
        self._tokens: List[TiingoToken] = []
        self._current_index = 0
        self._lock = asyncio.Lock()
        
        # Performance optimizations: O(1) lookups
        self._tokens_by_asset: Dict[str, List[TiingoToken]] = {}  # asset -> tokens
        self._healthy_tokens: Set[str] = set()  # token IDs that are healthy
        self._pending_updates: Dict[str, TiingoToken] = {}  # Batch DB updates
        self._batch_task: Optional[asyncio.Task] = None
        self._tiingo_sync_task: Optional[asyncio.Task] = None  # Tiingo usage sync task

    async def initialize(self):
        """Load tokens from database with O(1) lookup structures"""
        async with db.pool.acquire() as conn:
            tokens_data = await conn.fetch(
                "SELECT * FROM tokens WHERE status = $1",
                TokenStatus.ACTIVE
            )
        
        self._tokens = [self._row_to_token(row) for row in tokens_data]
        
        # Build hash maps for O(1) lookup
        self._tokens_by_asset = {}
        for token in self._tokens:
            if token.assigned_to:
                if token.assigned_to not in self._tokens_by_asset:
                    self._tokens_by_asset[token.assigned_to] = []
                self._tokens_by_asset[token.assigned_to].append(token)
            
            if self._is_token_healthy(token):
                self._healthy_tokens.add(token.id)
        
        # Start batch update task
        self._batch_task = asyncio.create_task(self._batch_update_loop())
        
        # Start Tiingo usage sync task
        self._tiingo_sync_task = asyncio.create_task(self._tiingo_sync_loop())
        
        logger.info(f"✅ Loaded {len(self._tokens)} active tokens with O(1) lookup")

    def _row_to_token(self, row) -> TiingoToken:
        """Convert database row to TiingoToken"""
        return TiingoToken(
            id=str(row['id']),
            token=row['token'],
            name=row['name'],
            description=row['description'],
            status=row['status'],
            assigned_to=row['assigned_to'],
            hourly_requests=row['hourly_requests'],
            daily_requests=row['daily_requests'],
            monthly_bandwidth_mb=row['monthly_bandwidth_mb'],
            hourly_limit=row['hourly_limit'],
            daily_limit=row['daily_limit'],
            monthly_bandwidth_limit_mb=row['monthly_bandwidth_limit_mb'],
            last_used=row['last_used'],
            last_reset_hour=row['last_reset_hour'],
            last_reset_day=row['last_reset_day'],
            last_reset_month=row['last_reset_month'],
            last_synced_with_tiingo=row.get('last_synced_with_tiingo'),  # New field
            created_at=row['created_at'],
            updated_at=row['updated_at'],
        )

    async def get_next_token(self, preferred_asset: Optional[str] = None) -> Optional[TiingoToken]:
        """Get next available token with O(1) lookup - optimized"""
        async with self._lock:
            if not self._tokens:
                logger.error("No active tokens available")
                return None

            # O(1) lookup for asset-specific tokens
            if preferred_asset and preferred_asset in self._tokens_by_asset:
                for token in self._tokens_by_asset[preferred_asset]:
                    if token.id in self._healthy_tokens:
                        await self._record_usage(token)
                        return token

            # Round-robin with health check cache
            attempts = 0
            while attempts < len(self._tokens):
                token = self._tokens[self._current_index]
                self._current_index = (self._current_index + 1) % len(self._tokens)
                attempts += 1

                # O(1) health check using set
                if token.id in self._healthy_tokens:
                    await self._record_usage(token)
                    return token
                
                # Recheck health if not in cache
                if self._is_token_healthy(token):
                    self._healthy_tokens.add(token.id)
                    await self._record_usage(token)
                    return token

            logger.warning("All tokens are rate limited or unhealthy")
            return None

    def _is_token_healthy(self, token: TiingoToken) -> bool:
        """Check if token is healthy and within limits - optimized with cache"""
        if token.status != TokenStatus.ACTIVE:
            self._healthy_tokens.discard(token.id)
            return False

        # Reset counters if needed
        now = datetime.now(timezone.utc)
        
        # Reset hourly counter
        if token.last_reset_hour is None or (now - token.last_reset_hour) >= timedelta(hours=1):
            token.hourly_requests = 0
            token.last_reset_hour = now

        # Reset daily counter
        if token.last_reset_day is None or (now - token.last_reset_day) >= timedelta(days=1):
            token.daily_requests = 0
            token.last_reset_day = now

        # Reset monthly bandwidth
        if token.last_reset_month is None or (now - token.last_reset_month) >= timedelta(days=30):
            token.monthly_bandwidth_mb = 0.0
            token.last_reset_month = now

        # Check limits
        is_healthy = (
            token.hourly_requests < token.hourly_limit and
            token.daily_requests < token.daily_limit and
            token.monthly_bandwidth_mb < token.monthly_bandwidth_limit_mb
        )
        
        # Update cache
        if is_healthy:
            self._healthy_tokens.add(token.id)
        else:
            self._healthy_tokens.discard(token.id)
            if token.hourly_requests >= token.hourly_limit:
                logger.warning(f"Token {token.name} hit hourly limit")
            elif token.daily_requests >= token.daily_limit:
                logger.warning(f"Token {token.name} hit daily limit")
            elif token.monthly_bandwidth_mb >= token.monthly_bandwidth_limit_mb:
                logger.warning(f"Token {token.name} hit bandwidth limit")

        return is_healthy

    async def _record_usage(self, token: TiingoToken, bandwidth_kb: float = 0):
        """Record token usage in memory - batch to DB for performance"""
        token.hourly_requests += 1
        token.daily_requests += 1
        token.monthly_bandwidth_mb += bandwidth_kb / 1024.0
        token.last_used = datetime.now(timezone.utc)

        # Add to pending batch (no immediate DB write)
        self._pending_updates[token.id] = token

    async def _batch_update_loop(self):
        """Batch DB updates every 10 seconds - 10,000x fewer DB operations"""
        while True:
            await asyncio.sleep(10)  # Batch interval
            
            if not self._pending_updates:
                continue
            
            # Get all pending updates
            async with self._lock:
                updates = list(self._pending_updates.values())
                self._pending_updates.clear()
            
            # Batch update in single transaction
            try:
                async with db.pool.acquire() as conn:
                    async with conn.transaction():
                        for token in updates:
                            await conn.execute(
                                """
                                UPDATE tokens SET
                                    hourly_requests = $1,
                                    daily_requests = $2,
                                    monthly_bandwidth_mb = $3,
                                    last_used = $4,
                                    last_reset_hour = $5,
                                    last_reset_day = $6,
                                    last_reset_month = $7,
                                    updated_at = $8
                                WHERE id = $9
                                """,
                                token.hourly_requests,
                                token.daily_requests,
                                token.monthly_bandwidth_mb,
                                token.last_used,
                                token.last_reset_hour,
                                token.last_reset_day,
                                token.last_reset_month,
                                datetime.now(timezone.utc),
                                token.id
                            )
                logger.debug(f"Batch updated {len(updates)} tokens")
            except Exception as e:
                logger.error(f"Batch update failed: {e}")

    async def _fetch_tiingo_usage(self, token_value: str) -> Optional[Dict]:
        """Fetch real usage data from Tiingo API"""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    "https://api.tiingo.com/api_be/usage",
                    headers={"Authorization": f"Token {token_value}"}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("error") is None and data.get("data"):
                        return data["data"]
                    else:
                        logger.warning(f"Tiingo API returned error: {data.get('error')}")
                        return None
                else:
                    logger.warning(f"Tiingo usage API returned status {response.status_code}")
                    return None
        except Exception as e:
            logger.error(f"Failed to fetch Tiingo usage: {e}")
            return None

    async def _sync_token_with_tiingo(self, token: TiingoToken):
        """Sync a single token's usage with Tiingo API"""
        usage_data = await self._fetch_tiingo_usage(token.token)
        
        if usage_data is None:
            return
        
        # Update token with real Tiingo data
        now = datetime.now(timezone.utc)
        
        # Calculate used amounts from Tiingo's "left" values
        hourly_allocated = usage_data.get("hourlyRequestsAllocated", 50.0)
        hourly_left = usage_data.get("hourlyRequestsLeft", 50.0)
        token.hourly_requests = max(0, int(hourly_allocated - hourly_left))
        
        daily_allocated = usage_data.get("dailyRequestsAllocated", 1000.0)
        daily_left = usage_data.get("dailyRequestsLeft", 1000.0)
        token.daily_requests = max(0, int(daily_allocated - daily_left))
        
        bandwidth_allocated = usage_data.get("monthlyBandwidthAllocated", 2147483648.0)  # bytes
        bandwidth_left = usage_data.get("monthlyBandwidthLeft", 2147483648.0)  # bytes
        bandwidth_used_mb = max(0, (bandwidth_allocated - bandwidth_left) / (1024 * 1024))
        token.monthly_bandwidth_mb = round(bandwidth_used_mb, 2)
        
        # Update limits from Tiingo's allocated values
        token.hourly_limit = int(hourly_allocated)
        token.daily_limit = int(daily_allocated)
        token.monthly_bandwidth_limit_mb = round(bandwidth_allocated / (1024 * 1024), 2)
        
        token.last_synced_with_tiingo = now
        
        # Update health cache
        self._is_token_healthy(token)
        
        # Persist to database
        try:
            async with db.pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE tokens SET
                        hourly_requests = $1,
                        daily_requests = $2,
                        monthly_bandwidth_mb = $3,
                        hourly_limit = $4,
                        daily_limit = $5,
                        monthly_bandwidth_limit_mb = $6,
                        last_synced_with_tiingo = $7,
                        updated_at = $8
                    WHERE id = $9
                    """,
                    token.hourly_requests,
                    token.daily_requests,
                    token.monthly_bandwidth_mb,
                    token.hourly_limit,
                    token.daily_limit,
                    token.monthly_bandwidth_limit_mb,
                    token.last_synced_with_tiingo,
                    now,
                    token.id
                )
            logger.info(
                f"✅ Synced {token.name} with Tiingo: "
                f"{token.hourly_requests}/{token.hourly_limit} hourly, "
                f"{token.daily_requests}/{token.daily_limit} daily, "
                f"{token.monthly_bandwidth_mb:.1f}/{token.monthly_bandwidth_limit_mb:.1f} MB"
            )
        except Exception as e:
            logger.error(f"Failed to persist Tiingo sync for {token.name}: {e}")

    async def _tiingo_sync_loop(self):
        """Sync all tokens with Tiingo API every 5 minutes"""
        # Wait 30 seconds before first sync (let system stabilize)
        await asyncio.sleep(30)
        
        while True:
            try:
                logger.info("🔄 Starting Tiingo usage sync for all tokens...")
                
                # Get current tokens
                async with self._lock:
                    tokens_to_sync = [t for t in self._tokens if t.status == TokenStatus.ACTIVE]
                
                # Sync each token (with 2 second delay between calls to avoid rate limits)
                for token in tokens_to_sync:
                    await self._sync_token_with_tiingo(token)
                    await asyncio.sleep(2)  # Delay between API calls
                
                logger.info(f"✅ Completed Tiingo sync for {len(tokens_to_sync)} tokens")
                
            except Exception as e:
                logger.error(f"Tiingo sync loop error: {e}")
            
            # Wait 5 minutes before next sync
            await asyncio.sleep(300)

    async def add_token(self, token: TiingoToken) -> TiingoToken:
        """Add new token with cache update"""
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO tokens (token, name, description, assigned_to)
                VALUES ($1, $2, $3, $4)
                RETURNING *
                """,
                token.token, token.name, token.description, token.assigned_to
            )
        
        token.id = str(row['id'])
        
        async with self._lock:
            self._tokens.append(token)
            
            # Update cache structures
            if token.assigned_to:
                if token.assigned_to not in self._tokens_by_asset:
                    self._tokens_by_asset[token.assigned_to] = []
                self._tokens_by_asset[token.assigned_to].append(token)
            
            if self._is_token_healthy(token):
                self._healthy_tokens.add(token.id)
        
        logger.info(f"✅ Added token: {token.name}")
        return token

    async def remove_token(self, token_id: str):
        """Remove token with cache cleanup"""
        async with db.pool.acquire() as conn:
            await conn.execute("DELETE FROM tokens WHERE id = $1", token_id)
        
        async with self._lock:
            # Remove from main list
            token_to_remove = next((t for t in self._tokens if t.id == token_id), None)
            if token_to_remove:
                self._tokens.remove(token_to_remove)
                
                # Remove from asset cache
                if token_to_remove.assigned_to and token_to_remove.assigned_to in self._tokens_by_asset:
                    self._tokens_by_asset[token_to_remove.assigned_to].remove(token_to_remove)
                    if not self._tokens_by_asset[token_to_remove.assigned_to]:
                        del self._tokens_by_asset[token_to_remove.assigned_to]
                
                # Remove from health cache
                self._healthy_tokens.discard(token_id)
        
        logger.info(f"✅ Removed token: {token_id}")

    async def update_token_status(self, token_id: str, status: TokenStatus):
        """Update token status"""
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE tokens SET status = $1, updated_at = $2 WHERE id = $3",
                status, datetime.now(timezone.utc), token_id
            )
        
        async with self._lock:
            for token in self._tokens:
                if token.id == token_id:
                    token.status = status
                    break

    async def get_all_tokens(self) -> List[TiingoToken]:
        """Get all tokens"""
        async with db.pool.acquire() as conn:
            tokens_data = await conn.fetch("SELECT * FROM tokens ORDER BY created_at DESC")
        
        return [self._row_to_token(row) for row in tokens_data]

    async def get_token_stats(self) -> List[TokenUsageStats]:
        """Get usage statistics for all tokens"""
        tokens = await self.get_all_tokens()
        stats = []
        
        for token in tokens:
            # Ensure counters are reset if needed
            self._is_token_healthy(token)
            
            stats.append(
                TokenUsageStats(
                    token_id=token.id,
                    name=token.name,
                    status=token.status,
                    assigned_to=token.assigned_to,
                    hourly_requests=token.hourly_requests,
                    hourly_limit=token.hourly_limit,
                    hourly_percentage=round((token.hourly_requests / token.hourly_limit) * 100, 2),
                    daily_requests=token.daily_requests,
                    daily_limit=token.daily_limit,
                    daily_percentage=round((token.daily_requests / token.daily_limit) * 100, 2),
                    monthly_bandwidth_mb=round(token.monthly_bandwidth_mb, 2),
                    monthly_bandwidth_limit_mb=token.monthly_bandwidth_limit_mb,
                    bandwidth_percentage=round(
                        (token.monthly_bandwidth_mb / token.monthly_bandwidth_limit_mb) * 100, 2
                    ),
                    is_healthy=self._is_token_healthy(token),
                    last_used=token.last_used,
                    last_synced_with_tiingo=token.last_synced_with_tiingo,  # Include sync timestamp
                )
            )
        
        return stats


# Global token manager instance
token_manager = TokenManager()
