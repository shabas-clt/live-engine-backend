import logging
from datetime import datetime

from app.core.config import settings
from app.core.database import db
from app.core.security import hash_password
from app.models.admin import Admin, AdminRole
from app.models.token import TiingoToken

logger = logging.getLogger(__name__)


async def seed_initial_admin():
    """Create initial super admin if not exists"""
    try:
        async with db.pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT id FROM admins WHERE email = $1",
                settings.DEFAULT_ADMIN_EMAIL
            )
        
        if existing:
            logger.info("✅ Super admin already exists")
            return
        
        async with db.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO admins (email, name, password_hash, role)
                VALUES ($1, $2, $3, $4)
                """,
                settings.DEFAULT_ADMIN_EMAIL,
                settings.DEFAULT_ADMIN_NAME,
                hash_password(settings.DEFAULT_ADMIN_PASSWORD),
                AdminRole.SUPER_ADMIN
            )
        
        logger.info(f"✅ Created super admin: {settings.DEFAULT_ADMIN_EMAIL}")
        
    except Exception as e:
        logger.error(f"❌ Failed to seed admin: {e}")


async def seed_initial_token():
    """Create initial Tiingo token if provided"""
    try:
        if not settings.INITIAL_TIINGO_TOKEN:
            logger.info("ℹ️  No initial token provided")
            return
        
        async with db.pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT id FROM tokens WHERE token = $1",
                settings.INITIAL_TIINGO_TOKEN
            )
        
        if existing:
            logger.info("✅ Initial token already exists")
            return
        
        async with db.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO tokens (token, name, description)
                VALUES ($1, $2, $3)
                """,
                settings.INITIAL_TIINGO_TOKEN,
                "Initial Token",
                "First token added during setup"
            )
        
        logger.info("✅ Created initial Tiingo token")
        
    except Exception as e:
        logger.error(f"❌ Failed to seed token: {e}")
