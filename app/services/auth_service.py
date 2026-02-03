"""
Authentication Service - Login, Register, Token Management
"""
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from passlib.context import CryptContext
from jose import JWTError, jwt
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.sql import func

from app.models import Base
from app.database import get_db_context

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT settings
SECRET_KEY = "splp-anri-secret-key-2024-change-in-production"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours


class User(Base):
    """User model untuk authentication"""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), unique=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(100), nullable=True)
    is_active = Column(Boolean, default=True)
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())
    
    def to_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "email": self.email,
            "full_name": self.full_name,
            "is_active": self.is_active,
            "is_admin": self.is_admin,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }


class AuthService:
    """Service untuk authentication"""
    
    def create_tables(self):
        """Buat tabel users jika belum ada"""
        from app.database import engine
        Base.metadata.create_all(bind=engine)
    
    def hash_password(self, password: str) -> str:
        """Hash password dengan bcrypt"""
        return pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verifikasi password"""
        return pwd_context.verify(plain_password, hashed_password)
    
    def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None) -> str:
        """Buat JWT access token"""
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt
    
    def decode_token(self, token: str) -> Optional[dict]:
        """Decode JWT token"""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            return payload
        except JWTError:
            return None
    
    def register(self, username: str, email: str, password: str, full_name: str = None) -> Dict[str, Any]:
        """Register user baru"""
        with get_db_context() as db:
            # Check if username exists
            existing = db.query(User).filter(User.username == username).first()
            if existing:
                return {"status": "error", "message": "Username sudah digunakan"}
            
            # Check if email exists
            existing_email = db.query(User).filter(User.email == email).first()
            if existing_email:
                return {"status": "error", "message": "Email sudah digunakan"}
            
            try:
                user = User(
                    username=username,
                    email=email,
                    hashed_password=self.hash_password(password),
                    full_name=full_name,
                    is_active=True,
                    is_admin=False
                )
                db.add(user)
                db.commit()
                db.refresh(user)
                return {"status": "success", "user": user.to_dict()}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    def login(self, username: str, password: str) -> Dict[str, Any]:
        """Login user dan return token"""
        with get_db_context() as db:
            user = db.query(User).filter(User.username == username).first()
            
            if not user:
                return {"status": "error", "message": "Username tidak ditemukan"}
            
            if not user.is_active:
                return {"status": "error", "message": "Akun tidak aktif"}
            
            if not self.verify_password(password, user.hashed_password):
                return {"status": "error", "message": "Password salah"}
            
            # Create token
            access_token = self.create_access_token(
                data={"sub": user.username, "user_id": user.id, "is_admin": user.is_admin}
            )
            
            return {
                "status": "success",
                "access_token": access_token,
                "token_type": "bearer",
                "user": user.to_dict()
            }
    
    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Get user by username"""
        with get_db_context() as db:
            user = db.query(User).filter(User.username == username).first()
            if user:
                return user.to_dict()
            return None
    
    def get_current_user(self, token: str) -> Optional[Dict]:
        """Get current user from token"""
        payload = self.decode_token(token)
        if not payload:
            return None
        username = payload.get("sub")
        if not username:
            return None
        return self.get_user_by_username(username)
    
    def create_default_admin(self):
        """Buat admin default jika belum ada"""
        with get_db_context() as db:
            admin = db.query(User).filter(User.username == "admin").first()
            if not admin:
                admin = User(
                    username="admin",
                    email="admin@anri.go.id",
                    hashed_password=self.hash_password("admin123"),
                    full_name="Administrator",
                    is_active=True,
                    is_admin=True
                )
                db.add(admin)
                db.commit()
                print("[Auth] Default admin created: admin/admin123")
                return True
        return False


# Global instance
auth_service = AuthService()
