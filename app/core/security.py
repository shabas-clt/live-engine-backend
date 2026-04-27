from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
import bcrypt
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId

from app.core.config import settings
from app.models.admin import AdminRole

security = HTTPBearer()

# MongoDB client for admin authentication
_mongo_client: Optional[AsyncIOMotorClient] = None


def get_mongo_client() -> AsyncIOMotorClient:
    """Get MongoDB client instance"""
    global _mongo_client
    if _mongo_client is None:
        if not settings.MONGODB_URI:
            raise RuntimeError("MONGODB_URI not configured")
        _mongo_client = AsyncIOMotorClient(settings.MONGODB_URI)
    return _mongo_client


def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    password_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password_bytes, salt)
    return hashed.decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against a hash using bcrypt"""
    password_bytes = plain_password.encode('utf-8')
    hashed_bytes = hashed_password.encode('utf-8')
    return bcrypt.checkpw(password_bytes, hashed_bytes)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(hours=24)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.JWT_SECRET, algorithm="HS256")
    return encoded_jwt


def decode_token(token: str) -> dict:
    """Decode JWT token"""
    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=["HS256"])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )


async def get_current_admin(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """
    Get current authenticated admin from JWT token.
    Validates token and checks admin exists in MongoDB (shared with funfin-backend).
    """
    token = credentials.credentials
    payload = decode_token(token)
    
    # Check token type
    token_type = payload.get("tokenType")
    if token_type != "admin":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type for admin",
        )
    
    # Get admin ID from token
    admin_id = payload.get("userId") or payload.get("sub")
    if not admin_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )
    
    # Validate admin exists in MongoDB
    try:
        mongo_client = get_mongo_client()
        db = mongo_client.get_default_database()
        admin = await db.admins.find_one({"_id": ObjectId(admin_id)})
        
        if not admin:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Admin not found",
            )
        
        # Check if admin is active
        if not admin.get("isActive", True):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin account is inactive",
            )
        
        # Check token version (for token revocation)
        token_tv = payload.get("tv", 0)
        admin_tv = admin.get("tokenVersion", 0)
        if token_tv != admin_tv:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has been revoked",
            )
        
        return {
            "id": str(admin["_id"]),
            "email": admin.get("email"),
            "role": admin.get("role"),
            "fullName": admin.get("fullName"),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Authentication error: {str(e)}",
        )


async def require_super_admin(current_admin: dict = Depends(get_current_admin)) -> dict:
    """Require super admin role"""
    if current_admin.get("role") != "superadmin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Super admin access required",
        )
    return current_admin


def mask_token(token: str) -> str:
    """Mask token for display (show first 8 and last 4 characters)"""
    if len(token) <= 12:
        return "*" * len(token)
    return f"{token[:8]}...{token[-4:]}"
