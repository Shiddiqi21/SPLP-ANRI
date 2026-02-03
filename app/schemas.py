"""
Pydantic Schemas untuk SPLP Data Integrator
"""
from pydantic import BaseModel, Field
from typing import Optional, Any, List
from datetime import date, datetime


# === ArsipData Schemas ===

class ArsipDataCreate(BaseModel):
    """Schema untuk membuat arsip data baru"""
    tanggal: date = Field(..., description="Tanggal arsip (YYYY-MM-DD)")
    role_id: int = Field(..., description="ID Role", ge=1)
    jenis_arsip: str = Field(..., description="Jenis/kategori arsip", min_length=1, max_length=255)
    instansi_id: int = Field(..., description="ID Instansi", ge=1)
    data_content: Optional[dict] = Field(None, description="Data tambahan dalam format JSON")
    keterangan: Optional[str] = Field(None, description="Catatan/deskripsi")
    
    class Config:
        json_schema_extra = {
            "example": {
                "tanggal": "2025-08-10",
                "role_id": 4,
                "jenis_arsip": "Naskah Keluar",
                "instansi_id": 2,
                "data_content": {"nomor_surat": "001/2025"},
                "keterangan": "Arsip surat keluar bulan Agustus"
            }
        }


class ArsipDataUpdate(BaseModel):
    """Schema untuk update arsip data"""
    tanggal: Optional[date] = None
    role_id: Optional[int] = Field(None, ge=1)
    jenis_arsip: Optional[str] = Field(None, min_length=1, max_length=255)
    instansi_id: Optional[int] = Field(None, ge=1)
    data_content: Optional[dict] = None
    keterangan: Optional[str] = None


class ArsipDataResponse(BaseModel):
    """Schema untuk response arsip data"""
    id: int
    tanggal: date
    role_id: int
    jenis_arsip: str
    instansi_id: int
    data_content: Optional[dict] = None
    keterangan: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class ArsipDataListResponse(BaseModel):
    """Schema untuk response list arsip data dengan pagination"""
    data: List[ArsipDataResponse]
    total: int
    limit: int
    offset: int
    filters_applied: dict


class ArsipDataFilter(BaseModel):
    """Schema untuk filter query"""
    tanggal_start: Optional[date] = Field(None, description="Tanggal mulai")
    tanggal_end: Optional[date] = Field(None, description="Tanggal akhir")
    role_id: Optional[int] = Field(None, description="Filter by role_id")
    jenis_arsip: Optional[str] = Field(None, description="Filter by jenis arsip (partial match)")
    instansi_id: Optional[int] = Field(None, description="Filter by instansi_id")


# === Upload Schemas ===

class UploadResponse(BaseModel):
    """Response untuk file upload"""
    status: str
    filename: str
    rows_processed: int
    rows_inserted: int
    rows_failed: int
    errors: Optional[List[str]] = None


class MessageResponse(BaseModel):
    """Generic message response"""
    status: str
    message: str
