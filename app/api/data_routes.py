"""
API Routes untuk Data Arsip (Instansi, Unit Kerja, Data Arsip)
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date

from app.services.data_service import data_service

router = APIRouter(prefix="/api", tags=["Data Arsip"])


# ==================== SCHEMAS ====================

class InstansiCreate(BaseModel):
    kode: str = Field(..., min_length=1, max_length=20)
    nama: str = Field(..., min_length=1, max_length=255)


class InstansiUpdate(BaseModel):
    kode: Optional[str] = None
    nama: Optional[str] = None


class UnitKerjaCreate(BaseModel):
    instansi_id: int
    kode: str = Field(..., min_length=1, max_length=50)
    nama: str = Field(..., min_length=1, max_length=255)


class DataArsipCreate(BaseModel):
    unit_kerja_id: int
    tanggal: date
    naskah_masuk: int = 0
    naskah_keluar: int = 0
    disposisi: int = 0
    berkas: int = 0
    retensi_permanen: int = 0
    retensi_musnah: int = 0
    naskah_ditindaklanjuti: int = 0


# ==================== INSTANSI ROUTES ====================

@router.get("/instansi", summary="Get All Instansi")
async def get_instansi(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """Get all instansi with pagination"""
    return data_service.get_all_instansi(limit=limit, offset=offset)


@router.get("/instansi/{instansi_id}", summary="Get Instansi by ID")
async def get_instansi_by_id(instansi_id: int):
    """Get instansi detail by ID"""
    result = data_service.get_instansi_by_id(instansi_id)
    if not result:
        raise HTTPException(status_code=404, detail="Instansi tidak ditemukan")
    return result


@router.post("/instansi", summary="Create Instansi")
async def create_instansi(data: InstansiCreate):
    """Create new instansi"""
    result = data_service.create_instansi(kode=data.kode, nama=data.nama)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.put("/instansi/{instansi_id}", summary="Update Instansi")
async def update_instansi(instansi_id: int, data: InstansiUpdate):
    """Update existing instansi"""
    result = data_service.update_instansi(
        instansi_id=instansi_id,
        kode=data.kode,
        nama=data.nama
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.delete("/instansi/{instansi_id}", summary="Delete Instansi")
async def delete_instansi(instansi_id: int):
    """Delete instansi (cascade deletes unit kerja and data)"""
    result = data_service.delete_instansi(instansi_id)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


# ==================== UNIT KERJA ROUTES ====================

@router.get("/unit-kerja", summary="Get All Unit Kerja")
async def get_all_unit_kerja(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """Get all unit kerja with instansi info"""
    return data_service.get_all_unit_kerja(limit=limit, offset=offset)


@router.get("/instansi/{instansi_id}/unit-kerja", summary="Get Unit Kerja by Instansi")
async def get_unit_kerja_by_instansi(
    instansi_id: int,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """Get all unit kerja for a specific instansi"""
    return data_service.get_unit_kerja_by_instansi(
        instansi_id=instansi_id,
        limit=limit,
        offset=offset
    )


@router.post("/unit-kerja", summary="Create Unit Kerja")
async def create_unit_kerja(data: UnitKerjaCreate):
    """Create new unit kerja under an instansi"""
    result = data_service.create_unit_kerja(
        instansi_id=data.instansi_id,
        kode=data.kode,
        nama=data.nama
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result

class UnitKerjaUpdate(BaseModel):
    kode: Optional[str] = None
    nama: Optional[str] = None

@router.put("/unit-kerja/{unit_id}", summary="Update Unit Kerja")
async def update_unit_kerja(unit_id: int, data: UnitKerjaUpdate):
    """Update existing unit kerja"""
    result = data_service.update_unit_kerja(
        unit_id=unit_id,
        kode=data.kode,
        nama=data.nama
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.delete("/unit-kerja/{unit_id}", summary="Delete Unit Kerja")
async def delete_unit_kerja(unit_id: int):
    """Delete unit kerja (cascade deletes data arsip)"""
    result = data_service.delete_unit_kerja(unit_id)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


# ==================== DATA ARSIP ROUTES ====================

@router.get("/data-arsip", summary="Get Data Arsip")
async def get_data_arsip(
    unit_kerja_id: Optional[int] = None,
    instansi_id: Optional[int] = None,
    tanggal_start: Optional[date] = None,
    tanggal_end: Optional[date] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """Get data arsip with filters"""
    return data_service.get_data_arsip(
        unit_kerja_id=unit_kerja_id,
        instansi_id=instansi_id,
        tanggal_start=tanggal_start,
        tanggal_end=tanggal_end,
        limit=limit,
        offset=offset
    )


@router.post("/data-arsip", summary="Create/Update Data Arsip")
async def create_data_arsip(data: DataArsipCreate):
    """Create or update data arsip for a unit kerja on a specific date"""
    result = data_service.create_or_update_data_arsip(
        unit_kerja_id=data.unit_kerja_id,
        tanggal=data.tanggal,
        naskah_masuk=data.naskah_masuk,
        naskah_keluar=data.naskah_keluar,
        disposisi=data.disposisi,
        berkas=data.berkas,
        retensi_permanen=data.retensi_permanen,
        retensi_musnah=data.retensi_musnah,
        naskah_ditindaklanjuti=data.naskah_ditindaklanjuti
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.delete("/data-arsip/{data_id}", summary="Delete Data Arsip")
async def delete_data_arsip(data_id: int):
    """Delete a specific data arsip entry"""
    result = data_service.delete_data_arsip(data_id)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


# ==================== STATISTICS ====================

@router.get("/statistics", summary="Get Dashboard Statistics")
async def get_statistics():
    """Get aggregated statistics for dashboard"""
    return data_service.get_statistics()


@router.get("/summary/by-instansi", summary="Get Summary by Instansi")
async def get_summary_by_instansi():
    """Get data summary grouped by instansi"""
    return data_service.get_summary_by_instansi()
