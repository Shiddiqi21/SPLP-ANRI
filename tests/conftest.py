"""
Pytest Configuration and Fixtures
"""
import pytest
import asyncio
from typing import Generator, AsyncGenerator
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport

# Pytest asyncio configuration
pytest_plugins = ('pytest_asyncio',)


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_db_session():
    """Mock database session for unit tests"""
    session = MagicMock()
    session.query.return_value = session
    session.filter.return_value = session
    session.first.return_value = None
    session.all.return_value = []
    session.add = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    session.refresh = MagicMock()
    session.delete = MagicMock()
    return session


@pytest.fixture
def mock_db_context(mock_db_session):
    """Mock get_db_context context manager"""
    from contextlib import contextmanager
    
    @contextmanager
    def _mock_context():
        yield mock_db_session
    
    return _mock_context


@pytest.fixture
def test_client():
    """Create test client for API testing
    
    Note: This requires the database to be available.
    For pure unit tests, use mock_db_session instead.
    """
    # Import here to avoid circular imports
    from app.main import app
    from app.database import get_db_context
    
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


@pytest.fixture
async def async_client():
    """Create async test client for async API testing"""
    from app.main import app
    
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as client:
        yield client


@pytest.fixture
def sample_arsip_data():
    """Sample arsip data for testing"""
    return {
        "tanggal": "2026-01-27",
        "role_id": 1,
        "jenis_arsip": "Surat Masuk",
        "instansi_id": 1,
        "keterangan": "Test arsip data"
    }


@pytest.fixture
def sample_user_data():
    """Sample user data for testing"""
    return {
        "username": "testuser",
        "email": "test@example.com",
        "password": "testpass123",
        "full_name": "Test User"
    }


@pytest.fixture
def admin_token():
    """Generate admin token for testing protected routes"""
    from app.services.auth_service import auth_service
    
    token = auth_service.create_access_token(
        data={"sub": "admin", "user_id": 1, "is_admin": True}
    )
    return token


@pytest.fixture
def user_token():
    """Generate regular user token for testing"""
    from app.services.auth_service import auth_service
    
    token = auth_service.create_access_token(
        data={"sub": "testuser", "user_id": 2, "is_admin": False}
    )
    return token
