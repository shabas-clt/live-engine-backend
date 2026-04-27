from fastapi import APIRouter, HTTPException, status, Depends
from typing import List

from app.core.security import get_current_admin
from app.core.database import db
from app.models.token import TokenCreate, TokenUpdate, TokenResponse, TokenUsageStats, TiingoToken
from app.services.token_manager import token_manager

router = APIRouter(prefix="/tokens", tags=["Token Management"])


@router.get("", response_model=List[TokenResponse])
async def get_all_tokens(current_admin: dict = Depends(get_current_admin)):
    """Get all tokens with full values (requires admin authentication)"""
    tokens = await token_manager.get_all_tokens()
    return [
        TokenResponse(
            id=token.id,
            name=token.name,
            token=token.token,  # Return full token
            description=token.description,
            status=token.status,
            assigned_to=token.assigned_to,
            hourly_requests=token.hourly_requests,
            daily_requests=token.daily_requests,
            monthly_bandwidth_mb=round(token.monthly_bandwidth_mb, 2),
            last_used=token.last_used,
            created_at=token.created_at,
        )
        for token in tokens
    ]


@router.get("/stats", response_model=List[TokenUsageStats])
async def get_token_stats(current_admin: dict = Depends(get_current_admin)):
    """Get detailed usage statistics for all tokens (requires admin authentication)"""
    return await token_manager.get_token_stats()


@router.post("", response_model=TokenResponse)
async def create_token(
    token_data: TokenCreate,
    current_admin: dict = Depends(get_current_admin),
):
    """Add new Tiingo token (requires admin authentication)"""
    # Check if token already exists
    existing_tokens = await token_manager.get_all_tokens()
    if any(t.token == token_data.token for t in existing_tokens):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token already exists",
        )
    
    # Create token (auto-assign will be handled by token_manager)
    token = TiingoToken(
        token=token_data.token,
        name=token_data.name,
        description=token_data.description,
        assigned_to=None,  # Let system auto-assign
    )
    
    token = await token_manager.add_token(token)
    
    return TokenResponse(
        id=token.id,
        name=token.name,
        token=token.token,  # Return full token
        description=token.description,
        status=token.status,
        assigned_to=token.assigned_to,
        hourly_requests=token.hourly_requests,
        daily_requests=token.daily_requests,
        monthly_bandwidth_mb=token.monthly_bandwidth_mb,
        last_used=token.last_used,
        created_at=token.created_at,
    )


@router.patch("/{token_id}", response_model=TokenResponse)
async def update_token(
    token_id: str,
    token_data: TokenUpdate,
    current_admin: dict = Depends(get_current_admin),
):
    """Update token details (requires admin authentication)"""
    tokens = await token_manager.get_all_tokens()
    token = next((t for t in tokens if t.id == token_id), None)
    
    if not token:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Token not found",
        )
    
    # Update fields
    update_data = token_data.dict(exclude_unset=True)
    
    # Build SQL update query
    if update_data:
        set_clauses = []
        values = []
        param_num = 1
        
        for field, value in update_data.items():
            set_clauses.append(f"{field} = ${param_num}")
            values.append(value)
            param_num += 1
        
        values.append(token_id)
        
        async with db.pool.acquire() as conn:
            await conn.execute(
                f"UPDATE tokens SET {', '.join(set_clauses)} WHERE id = ${param_num}",
                *values
            )
    
    # Fetch updated token
    async with db.pool.acquire() as conn:
        updated_row = await conn.fetchrow("SELECT * FROM tokens WHERE id = $1", token_id)
    
    updated_token = token_manager._row_to_token(updated_row)
    
    return TokenResponse(
        id=updated_token.id,
        name=updated_token.name,
        token=updated_token.token,  # Return full token
        description=updated_token.description,
        status=updated_token.status,
        assigned_to=updated_token.assigned_to,
        hourly_requests=updated_token.hourly_requests,
        daily_requests=updated_token.daily_requests,
        monthly_bandwidth_mb=updated_token.monthly_bandwidth_mb,
        last_used=updated_token.last_used,
        created_at=updated_token.created_at,
    )


@router.delete("/{token_id}")
async def delete_token(
    token_id: str,
    current_admin: dict = Depends(get_current_admin),
):
    """Delete token (requires admin authentication)"""
    tokens = await token_manager.get_all_tokens()
    token = next((t for t in tokens if t.id == token_id), None)
    
    if not token:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Token not found",
        )
    
    await token_manager.remove_token(token_id)
    
    return {"message": "Token deleted successfully"}
