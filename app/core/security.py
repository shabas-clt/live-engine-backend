from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.core.config import settings
from app.models.admin import AdminRole

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()


def hash_password(password: str) -> str:
    """Hash a password"""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against a hash"""
    return pwd_context.verify(plain_password, hashed_password)


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
    """Get current authenticated admin from token"""
    token = credentials.credentials
    payload = decode_token(token)
    
    admin_id = payload.get("sub")
    if not admin_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )
    
    return {
        "id": admin_id,
        "email": payload.get("email"),
        "role": payload.get("role"),
    }


async def require_super_admin(current_admin: dict = Depends(get_current_admin)) -> dict:
    """Require super admin role"""
    if current_admin.get("role") != AdminRole.SUPER_ADMIN:
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
