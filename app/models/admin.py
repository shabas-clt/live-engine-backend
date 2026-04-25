from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, Field
from enum import Enum
import uuid


class AdminRole(str, Enum):
    SUPER_ADMIN = "superadmin"
    ADMIN = "admin"


class Admin(BaseModel):
    id: Optional[str] = None
    email: EmailStr
    name: str
    password_hash: str
    role: AdminRole = AdminRole.ADMIN
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_login: Optional[datetime] = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


class AdminCreate(BaseModel):
    email: EmailStr
    name: str
    password: str
    role: AdminRole = AdminRole.ADMIN


class AdminLogin(BaseModel):
    email: EmailStr
    password: str


class AdminResponse(BaseModel):
    id: str
    email: EmailStr
    name: str
    role: AdminRole
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime] = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
