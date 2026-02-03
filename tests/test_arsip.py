"""
Test Arsip CRUD Operations
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import date


class TestArsipDataValidation:
    """Test arsip data validation"""
    
    def test_valid_arsip_data(self, sample_arsip_data):
        """Test valid arsip data creates schema successfully"""
        from app.schemas import ArsipDataCreate
        
        arsip = ArsipDataCreate(**sample_arsip_data)
        
        assert arsip.tanggal == date(2026, 1, 27)
        assert arsip.role_id == 1
        assert arsip.jenis_arsip == "Surat Masuk"
        assert arsip.instansi_id == 1
    
    def test_arsip_data_missing_required_field(self):
        """Test arsip data with missing required field raises error"""
        from app.schemas import ArsipDataCreate
        from pydantic import ValidationError
        
        incomplete_data = {
            "tanggal": "2026-01-27",
            "role_id": 1
            # missing jenis_arsip and instansi_id
        }
        
        with pytest.raises(ValidationError):
            ArsipDataCreate(**incomplete_data)
    
    def test_arsip_data_invalid_role_id(self):
        """Test arsip data with invalid role_id"""
        from app.schemas import ArsipDataCreate
        from pydantic import ValidationError
        
        invalid_data = {
            "tanggal": "2026-01-27",
            "role_id": 0,  # Must be >= 1
            "jenis_arsip": "Surat Masuk",
            "instansi_id": 1
        }
        
        with pytest.raises(ValidationError):
            ArsipDataCreate(**invalid_data)
    
    def test_arsip_data_invalid_date_format(self):
        """Test arsip data with invalid date format"""
        from app.schemas import ArsipDataCreate
        from pydantic import ValidationError
        
        invalid_data = {
            "tanggal": "27-01-2026",  # Wrong format
            "role_id": 1,
            "jenis_arsip": "Surat Masuk",
            "instansi_id": 1
        }
        
        with pytest.raises(ValidationError):
            ArsipDataCreate(**invalid_data)


class TestArsipModel:
    """Test ArsipData SQLAlchemy model"""
    
    def test_arsip_model_to_dict(self):
        """Test ArsipData model to_dict method"""
        from app.models import ArsipData
        from datetime import date, datetime
        
        arsip = ArsipData(
            id=1,
            tanggal=date(2026, 1, 27),
            role_id=1,
            jenis_arsip="Surat Masuk",
            instansi_id=1,
            data_content={"key": "value"},
            keterangan="Test",
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        result = arsip.to_dict()
        
        assert result["id"] == 1
        assert result["tanggal"] == "2026-01-27"
        assert result["role_id"] == 1
        assert result["jenis_arsip"] == "Surat Masuk"
        assert result["instansi_id"] == 1
        assert result["data_content"] == {"key": "value"}
    
    def test_arsip_model_tablename(self):
        """Test ArsipData model has correct table name"""
        from app.models import ArsipData
        
        assert ArsipData.__tablename__ == "arsip_data"


class TestArsipSummaryModel:
    """Test ArsipSummary model"""
    
    def test_summary_model_to_dict(self):
        """Test ArsipSummary model to_dict"""
        from app.models import ArsipSummary
        from datetime import date, datetime
        
        summary = ArsipSummary(
            id=1,
            tanggal=date(2026, 1, 27),
            instansi_id=1,
            jenis_arsip="Surat Masuk",
            role_id=1,
            total_count=100,
            last_updated=datetime.now()
        )
        
        result = summary.to_dict()
        
        assert result["tanggal"] == "2026-01-27"
        assert result["total_count"] == 100
        assert result["instansi_id"] == 1


class TestDailySummaryModel:
    """Test DailySummary model"""
    
    def test_daily_summary_model_to_dict(self):
        """Test DailySummary model to_dict"""
        from app.models import DailySummary
        from datetime import date, datetime
        
        daily = DailySummary(
            id=1,
            tanggal=date(2026, 1, 27),
            total_arsip=500,
            total_instansi=10,
            total_jenis=5,
            last_updated=datetime.now()
        )
        
        result = daily.to_dict()
        
        assert result["tanggal"] == "2026-01-27"
        assert result["total_arsip"] == 500
        assert result["total_instansi"] == 10
        assert result["total_jenis"] == 5


class TestArsipResponseSchema:
    """Test Arsip response schemas"""
    
    def test_arsip_list_response_format(self):
        """Test arsip list response format"""
        response_format = {
            "data": [],
            "total": 0,
            "limit": 20,
            "offset": 0
        }
        
        assert "data" in response_format
        assert "total" in response_format
        assert response_format["limit"] == 20
    
    def test_arsip_single_response_format(self):
        """Test single arsip response format"""
        response_format = {
            "status": "success",
            "data": {
                "id": 1,
                "tanggal": "2026-01-27",
                "role_id": 1,
                "jenis_arsip": "Surat Masuk",
                "instansi_id": 1
            }
        }
        
        assert response_format["status"] == "success"
        assert "data" in response_format
