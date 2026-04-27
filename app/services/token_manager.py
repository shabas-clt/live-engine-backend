import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Set

from app.models.token import TiingoToken, TokenStatus, TokenUsageStats
from app.core.database import db

logger = logging.getLogger(__name__)


class TokenManager:
    """Manages Tiingo API tokens with rotation and usage tracking"""

    def __init__(self):
        self._tokens: List[TiingoToken] = []
        self._current_index = 0
        self._lock = asyncio.Lock()
        self._tokens_by_asset: Dict[str, List[TiingoToken]] = {}
        self._healthy_tokens: Set[str] = set()
        self._pending_updates: Dict[str, TiingoToken] = {}
        self._batch_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Load tokens from database"""
        async with db.pool.acquire() as conn:
            tokens_data = await conn.fetch(
                "SELECT * FROM tokens WHERE status = $1",
                TokenStatus.ACTIVE
            )
        
        self._tokens = [self._row_to_token(row) for row in tokens_data]
        
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
        
        logger.info(f"✅ Loaded {len(self._tokens)} active tokens")

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
            created_at=row['created_at'],
            updated_at=row['updated_at'],
        )

    async def get_next_token(self, preferred_asset: Optional[str] = None) -> Optional[TiingoToken]:
        """Get next available token"""
        async with self._lock:
            if not self._tokens:
                logger.error("No active tokens available")
                return None

            if preferred_asset and preferred_asset in self._tokens_by_asset:
                for token in self._tokens_by_asset[preferred_asset]:
                    if token.id in self._healthy_tokens:
                        await self._record_usage(token)
                        return token

            attempts = 0
            while attempts < len(self._tokens):
                token = self._tokens[self._current_index]
                self._current_index = (self._current_index + 1) % len(self._tokens)
                attempts += 1

                if token.id in self._healthy_tokens:
                    await self._record_usage(token)
                    return token
                
                if self._is_token_healthy(token):
                    self._healthy_tokens.add(token.id)
                    await self._record_usage(token)
                    return token

            logger.warning("All tokens are rate limited or unhealthy")
            return None

    def _is_token_healthy(self, token: TiingoToken) -> bool:
        """Check if token is within limits"""
        if token.status != TokenStatus.ACTIVE:
            self._healthy_tokens.discard(token.id)
            return False

        now = datetime.now(timezone.utc)
        
        if token.last_reset_hour is None or (now - token.last_reset_hour) >= timedelta(hours=1):
            token.hourly_requests = 0
            token.last_reset_hour = now

        if token.last_reset_day is None or (now - token.last_reset_day) >= timedelta(days=1):
            token.daily_requests = 0
            token.last_reset_day = now

        if token.last_reset_month is None or (now - token.last_reset_month) >= timedelta(days=30):
            token.monthly_bandwidth_mb = 0.0
            token.last_reset_month = now

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
        """Record token usage"""
        token.hourly_requests += 1
        token.daily_requests += 1
        token.monthly_bandwidth_mb += bandwidth_kb / 1024.0
        token.last_used = datetime.now(timezone.utc)

        self._pending_updates[token.id] = token

    async def _batch_update_loop(self):
        """Batch DB updates every 10 seconds"""
        while True:
            await asyncio.sleep(10)
            
            if not self._pending_updates:
                continue
            
            async with self._lock:
                updates = list(self._pending_updates.values())
                self._pending_updates.clear()
            
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

    async def add_token(self, token: TiingoToken) -> TiingoToken:
        """Add new token"""
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
            
            if token.assigned_to:
                if token.assigned_to not in self._tokens_by_asset:
                    self._tokens_by_asset[token.assigned_to] = []
                self._tokens_by_asset[token.assigned_to].append(token)
            
            if self._is_token_healthy(token):
                self._healthy_tokens.add(token.id)
        
        logger.info(f"✅ Added token: {token.name}")
        return token

    async def remove_token(self, token_id: str):
        """Remove token"""
        async with db.pool.acquire() as conn:
            await conn.execute("DELETE FROM tokens WHERE id = $1", token_id)
        
        async with self._lock:
            token_to_remove = next((t for t in self._tokens if t.id == token_id), None)
            if token_to_remove:
                self._tokens.remove(token_to_remove)
                
                if token_to_remove.assigned_to and token_to_remove.assigned_to in self._tokens_by_asset:
                    self._tokens_by_asset[token_to_remove.assigned_to].remove(token_to_remove)
                    if not self._tokens_by_asset[token_to_remove.assigned_to]:
                        del self._tokens_by_asset[token_to_remove.assigned_to]
                
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
                )
            )
        
        return stats


# Global token manager instance
token_manager = TokenManager()
