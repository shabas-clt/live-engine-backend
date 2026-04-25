from fastapi import APIRouter, HTTPException, status, Depends
from typing import List
from datetime import datetime

from app.core.database import db
from app.core.security import hash_password, get_current_admin, require_super_admin
from app.models.admin import AdminCreate, AdminResponse, Admin, AdminRole

router = APIRouter(prefix="/admins", tags=["Admin Management"])


@router.get("", response_model=List[AdminResponse])
async def get_all_admins(current_admin: dict = Depends(get_current_admin)):
    """Get all admins (requires authentication)"""
    async with db.pool.acquire() as conn:
        admins_data = await conn.fetch("SELECT * FROM admins ORDER BY created_at DESC")
    
    return [
        AdminResponse(
            id=str(admin["id"]),
            email=admin["email"],
            name=admin["name"],
            role=admin["role"],
            is_active=admin["is_active"],
            created_at=admin["created_at"],
            last_login=admin.get("last_login"),
        )
        for admin in admins_data
    ]


@router.post("", response_model=AdminResponse)
async def create_admin(
    admin_data: AdminCreate,
    current_admin: dict = Depends(require_super_admin),
):
    """Create new admin (super admin only)"""
    # Check if email already exists
    async with db.pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT id FROM admins WHERE email = $1",
            admin_data.email
        )
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered",
        )
    
    # Create admin
    async with db.pool.acquire() as conn:
        admin_row = await conn.fetchrow(
            """
            INSERT INTO admins (email, name, password_hash, role)
            VALUES ($1, $2, $3, $4)
            RETURNING *
            """,
            admin_data.email,
            admin_data.name,
            hash_password(admin_data.password),
            admin_data.role
        )
    
    return AdminResponse(
        id=str(admin_row["id"]),
        email=admin_row["email"],
        name=admin_row["name"],
        role=admin_row["role"],
        is_active=admin_row["is_active"],
        created_at=admin_row["created_at"],
        last_login=admin_row["last_login"],
    )


@router.delete("/{admin_id}")
async def delete_admin(
    admin_id: str,
    current_admin: dict = Depends(require_super_admin),
):
    """Delete admin (super admin only, cannot delete self)"""
    if admin_id == current_admin["id"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete yourself",
        )
    
    # Check if target is super admin
    async with db.pool.acquire() as conn:
        target_admin = await conn.fetchrow(
            "SELECT role FROM admins WHERE id = $1",
            admin_id
        )
    
    if not target_admin:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Admin not found",
        )
    
    # Count super admins
    async with db.pool.acquire() as conn:
        super_admin_count = await conn.fetchval(
            "SELECT COUNT(*) FROM admins WHERE role = $1",
            AdminRole.SUPER_ADMIN
        )
    
    # Prevent deleting last super admin
    if target_admin["role"] == AdminRole.SUPER_ADMIN and super_admin_count <= 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete the last super admin",
        )
    
    async with db.pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM admins WHERE id = $1",
            admin_id
        )
    
    if result == "DELETE 0":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Admin not found",
        )
    
    return {"message": "Admin deleted successfully"}


@router.patch("/{admin_id}/toggle-status")
async def toggle_admin_status(
    admin_id: str,
    current_admin: dict = Depends(require_super_admin),
):
    """Toggle admin active status (super admin only)"""
    if admin_id == current_admin["id"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot deactivate yourself",
        )
    
    async with db.pool.acquire() as conn:
        admin = await conn.fetchrow(
            "SELECT is_active FROM admins WHERE id = $1",
            admin_id
        )
    
    if not admin:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Admin not found",
        )
    
    new_status = not admin["is_active"]
    
    async with db.pool.acquire() as conn:
        await conn.execute(
            "UPDATE admins SET is_active = $1 WHERE id = $2",
            new_status, admin_id
        )
    
    return {"message": f"Admin {'activated' if new_status else 'deactivated'} successfully"}
