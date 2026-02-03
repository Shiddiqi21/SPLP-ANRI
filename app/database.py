"""
Database Connection Module
"""
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from contextlib import contextmanager
from typing import Generator

from app.config import get_settings

settings = get_settings()

# Base class for models
Base = declarative_base()

# Create engine with optimized pool settings
engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_recycle=1800,  # Recycle connections every 30 minutes
    pool_size=10,  # Number of connections to maintain
    max_overflow=20,  # Additional connections allowed
    pool_timeout=10,  # Wait max 10 seconds for connection
    echo=False  # Set to True for SQL debugging
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    """Dependency untuk mendapatkan database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_db_context():
    """Context manager untuk database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection() -> dict:
    """Test database connection"""
    try:
        with get_db_context() as db:
            result = db.execute(text("SELECT 1"))
            result.fetchone()
            return {"status": "connected", "database": settings.db_name}
    except Exception as e:
        return {"status": "error", "message": str(e)}
