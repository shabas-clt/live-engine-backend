from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
from enum import Enum


class TokenStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"


class TiingoToken(BaseModel):
    id: Optional[str] = None
    token: str
    name: str  # e.g., "Token 1", "BTC Token"
    description: Optional[str] = None
    status: TokenStatus = TokenStatus.ACTIVE
    
    # Usage tracking
    hourly_requests: int = 0
    daily_requests: int = 0
    monthly_bandwidth_mb: float = 0.0
    
    # Limits (Tiingo free plan)
    hourly_limit: int = 50
    daily_limit: int = 1000
    monthly_bandwidth_limit_mb: float = 1024.0  # 1GB
    
    # Timestamps
    last_used: Optional[datetime] = None
    last_reset_hour: Optional[datetime] = None
    last_reset_day: Optional[datetime] = None
    last_reset_month: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Assignment (optional - which asset/task uses this token)
    assigned_to: Optional[str] = None  # e.g., "BTC", "GOLD", "SILVER"
    
    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class TokenCreate(BaseModel):
    token: str
    name: str
    description: Optional[str] = None


class TokenUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[TokenStatus] = None


class TokenUsageStats(BaseModel):
    token_id: str
    name: str
    status: TokenStatus
    assigned_to: Optional[str]
    
    # Current usage
    hourly_requests: int
    hourly_limit: int
    hourly_percentage: float
    
    daily_requests: int
    daily_limit: int
    daily_percentage: float
    
    monthly_bandwidth_mb: float
    monthly_bandwidth_limit_mb: float
    bandwidth_percentage: float
    
    # Health
    is_healthy: bool
    last_used: Optional[datetime]
    
    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class TokenResponse(BaseModel):
    id: str
    name: str
    token: str  # Masked in API responses
    description: Optional[str]
    status: TokenStatus
    assigned_to: Optional[str]
    hourly_requests: int
    daily_requests: int
    monthly_bandwidth_mb: float
    last_used: Optional[datetime]
    created_at: datetime
    
    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
