from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, status, Depends

from app.core.database import db
from app.core.security import hash_password, verify_password, create_access_token, get_current_admin
from app.models.admin import AdminLogin, AdminCreate, AdminResponse, Admin, AdminRole

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/login")
async def login(credentials: AdminLogin):
    """Admin login"""
    async with db.pool.acquire() as conn:
        admin_data = await conn.fetchrow(
            "SELECT * FROM admins WHERE email = $1",
            credentials.email
        )
    
    if not admin_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )
    
    if not admin_data['is_active']:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is inactive",
        )
    
    if not verify_password(credentials.password, admin_data['password_hash']):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )
    
    # Update last login
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE admins SET last_login = $1 WHERE id = $2",
            datetime.utcnow(), admin_data['id']
        )
    
    # Create access token
    access_token = create_access_token(
        data={
            "sub": str(admin_data['id']),
            "email": admin_data['email'],
            "role": admin_data['role'],
        }
    )
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "admin": AdminResponse(
            id=str(admin_data['id']),
            email=admin_data['email'],
            name=admin_data['name'],
            role=admin_data['role'],
            is_active=admin_data['is_active'],
            created_at=admin_data['created_at'],
            last_login=datetime.utcnow(),
        ),
    }


@router.get("/me", response_model=AdminResponse)
async def get_current_user(current_admin: dict = Depends(get_current_admin)):
    """Get current admin info"""
    async with db.pool.acquire() as conn:
        admin_data = await conn.fetchrow(
            "SELECT * FROM admins WHERE id = $1",
            current_admin["id"]
        )
    
    if not admin_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Admin not found",
        )
    
    return AdminResponse(
        id=str(admin_data['id']),
        email=admin_data['email'],
        name=admin_data['name'],
        role=admin_data['role'],
        is_active=admin_data['is_active'],
        created_at=admin_data['created_at'],
        last_login=admin_data['last_login'],
    )
