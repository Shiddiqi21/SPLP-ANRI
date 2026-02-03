"""
Test API Endpoints
"""
import pytest
from fastapi import status


class TestHealthEndpoint:
    """Test health check endpoint"""
    
    def test_health_check(self, test_client):
        """Test /api/health endpoint returns OK"""
        response = test_client.get("/api/health")
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "status" in data
    
    def test_api_info(self, test_client):
        """Test /api-info endpoint"""
        response = test_client.get("/api-info")
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["name"] == "SPLP Data Integrator"
        assert "version" in data


class TestAuthEndpoints:
    """Test authentication endpoints"""
    
    def test_login_page_accessible(self, test_client):
        """Test login page is accessible"""
        response = test_client.get("/login")
        
        assert response.status_code == status.HTTP_200_OK
        assert "SPLP Data Integrator" in response.text
    
    def test_login_endpoint_wrong_credentials(self, test_client):
        """Test login with wrong credentials"""
        response = test_client.post(
            "/api/auth/login",
            json={"username": "wronguser", "password": "wrongpass"}
        )
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_login_endpoint_missing_fields(self, test_client):
        """Test login with missing fields"""
        response = test_client.post(
            "/api/auth/login",
            json={"username": "admin"}  # Missing password
        )
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_register_endpoint_valid(self, test_client, sample_user_data):
        """Test register with valid data"""
        # Modify username to avoid conflicts
        sample_user_data["username"] = f"testuser_{pytest.importorskip('time').time()}"
        sample_user_data["email"] = f"test_{pytest.importorskip('time').time()}@example.com"
        
        response = test_client.post(
            "/api/auth/register",
            json=sample_user_data
        )
        
        # Either success or duplicate (if test runs multiple times)
        assert response.status_code in [status.HTTP_200_OK, status.HTTP_400_BAD_REQUEST]
    
    def test_register_endpoint_short_password(self, test_client):
        """Test register with too short password"""
        response = test_client.post(
            "/api/auth/register",
            json={
                "username": "newuser",
                "email": "new@example.com",
                "password": "123"  # Too short
            }
        )
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_verify_token_without_auth(self, test_client):
        """Test verify token without authorization header"""
        response = test_client.get("/api/auth/verify")
        
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    
    def test_verify_token_with_valid_token(self, test_client, admin_token):
        """Test verify token with valid admin token"""
        response = test_client.get(
            "/api/auth/verify",
            headers={"Authorization": f"Bearer {admin_token}"}
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "valid"


class TestArsipEndpoints:
    """Test arsip API endpoints"""
    
    def test_get_arsip_list(self, test_client):
        """Test GET /api/arsip returns list"""
        response = test_client.get("/api/arsip")
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "data" in data
        assert "total" in data
    
    def test_get_arsip_with_pagination(self, test_client):
        """Test GET /api/arsip with pagination params"""
        response = test_client.get("/api/arsip?limit=5&offset=0")
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "data" in data
    
    def test_get_arsip_with_filters(self, test_client):
        """Test GET /api/arsip with filter params"""
        response = test_client.get(
            "/api/arsip?role_id=1&instansi_id=1"
        )
        
        assert response.status_code == status.HTTP_200_OK
    
    def test_create_arsip(self, test_client, sample_arsip_data):
        """Test POST /api/arsip creates new record"""
        response = test_client.post(
            "/api/arsip",
            json=sample_arsip_data
        )
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "success"
    
    def test_create_arsip_invalid_data(self, test_client):
        """Test POST /api/arsip with invalid data"""
        response = test_client.post(
            "/api/arsip",
            json={"tanggal": "invalid-date"}
        )
        
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    
    def test_get_arsip_statistics(self, test_client):
        """Test GET /api/arsip/statistics"""
        response = test_client.get("/api/arsip/statistics")
        
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "total_records" in data


class TestSummaryEndpoints:
    """Test summary API endpoints"""
    
    def test_get_daily_summary(self, test_client):
        """Test GET /api/summary/daily"""
        response = test_client.get("/api/summary/daily")
        
        assert response.status_code == status.HTTP_200_OK
    
    def test_get_overview(self, test_client):
        """Test GET /api/summary/overview"""
        response = test_client.get("/api/summary/overview")
        
        assert response.status_code == status.HTTP_200_OK


class TestHomePage:
    """Test home page"""
    
    def test_home_page_accessible(self, test_client):
        """Test home page is accessible"""
        response = test_client.get("/")
        
        assert response.status_code == status.HTTP_200_OK
        assert "SPLP Data Integrator" in response.text
    
    def test_docs_page_accessible(self, test_client):
        """Test API docs page is accessible"""
        response = test_client.get("/docs")
        
        assert response.status_code == status.HTTP_200_OK
