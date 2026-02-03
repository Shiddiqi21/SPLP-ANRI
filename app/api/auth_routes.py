"""
API Routes untuk Authentication
"""
from fastapi import APIRouter, HTTPException, Depends, Request, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr, Field
from typing import Optional

from app.services.auth_service import auth_service

router = APIRouter(prefix="/api/auth", tags=["Authentication"])
security = HTTPBearer(auto_error=False)


# === Schemas ===

class UserRegister(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=6)
    full_name: Optional[str] = None


class UserLogin(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user: dict


# === Dependency untuk protected routes ===

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency untuk mendapatkan current user dari token"""
    if not credentials:
        raise HTTPException(status_code=401, detail="Token tidak ditemukan")
    
    user = auth_service.get_current_user(credentials.credentials)
    if not user:
        raise HTTPException(status_code=401, detail="Token tidak valid atau expired")
    
    return user


async def get_optional_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency untuk mendapatkan user (optional, tidak error jika tidak ada)"""
    if not credentials:
        return None
    return auth_service.get_current_user(credentials.credentials)


# === Routes ===

@router.post("/register", summary="Register User Baru")
async def register(data: UserRegister):
    """
    Register user baru.
    
    - **username**: Username unik (min 3 karakter)
    - **email**: Email valid
    - **password**: Password (min 6 karakter)
    - **full_name**: Nama lengkap (opsional)
    """
    result = auth_service.register(
        username=data.username,
        email=data.email,
        password=data.password,
        full_name=data.full_name
    )
    
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    
    return result


@router.post("/login", summary="Login")
async def login(data: UserLogin):
    """
    Login dan dapatkan access token.
    
    - **username**: Username
    - **password**: Password
    
    Returns access token yang berlaku 24 jam.
    """
    try:
        result = auth_service.login(data.username, data.password)
        
        if result["status"] == "error":
            raise HTTPException(status_code=401, detail=result["message"])
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"[Login Error] {str(e)}")
        raise HTTPException(status_code=500, detail=f"Login error: {str(e)}")


@router.get("/me", summary="Get Current User")
async def get_me(current_user: dict = Depends(get_current_user)):
    """
    Get informasi user yang sedang login.
    
    Requires: Bearer token di header Authorization
    """
    return {"status": "success", "user": current_user}


@router.post("/logout", summary="Logout")
async def logout():
    """
    Logout user.
    
    Note: Karena menggunakan JWT stateless, logout dilakukan di client-side
    dengan menghapus token dari storage.
    """
    return {"status": "success", "message": "Logout berhasil. Hapus token dari client."}


@router.get("/verify", summary="Verify Token")
async def verify_token(current_user: dict = Depends(get_current_user)):
    """Verify apakah token masih valid"""
    return {"status": "valid", "user": current_user}
