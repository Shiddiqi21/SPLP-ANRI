"""
API Routes untuk Arsip Data
"""
from fastapi import APIRouter, Query, HTTPException, UploadFile, File, Depends
from typing import Optional
from datetime import date

from app.schemas import (
    ArsipDataCreate, 
    ArsipDataUpdate, 
    ArsipDataResponse,
    ArsipDataListResponse,
    UploadResponse,
    MessageResponse
)
from app.services.arsip_service import arsip_service

router = APIRouter(prefix="/api/arsip", tags=["Arsip Data"])


@router.post("", response_model=dict, summary="Create Arsip Data")
async def create_arsip(data: ArsipDataCreate):
    """
    Tambah data arsip baru.
    
    - **tanggal**: Tanggal arsip (format: YYYY-MM-DD)
    - **role_id**: ID Role
    - **jenis_arsip**: Jenis/kategori arsip (bebas)
    - **instansi_id**: ID Instansi
    - **data_content**: Data tambahan (optional, JSON)
    - **keterangan**: Catatan (optional)
    """
    result = arsip_service.create(data)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.get("", response_model=ArsipDataListResponse, summary="Get Arsip Data with Filters")
async def get_arsip_list(
    tanggal_start: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    tanggal_end: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)"),
    role_id: Optional[int] = Query(None, description="Filter by Role ID"),
    jenis_arsip: Optional[str] = Query(None, description="Filter by jenis arsip (partial match)"),
    instansi_id: Optional[int] = Query(None, description="Filter by Instansi ID"),
    limit: int = Query(100, ge=1, le=1000, description="Limit hasil"),
    offset: int = Query(0, ge=0, description="Offset untuk pagination"),
    _t: Optional[str] = Query(None, description="Cache buster timestamp"),
    _nocache: Optional[bool] = Query(None, description="Bypass cache")
):
    """
    Ambil daftar data arsip dengan filter.
    
    Semua filter bersifat opsional dan bisa dikombinasikan.
    """
    # If cache buster is present, skip cache
    skip_cache = bool(_t) or bool(_nocache)
    
    return arsip_service.get_filtered(
        tanggal_start=tanggal_start,
        tanggal_end=tanggal_end,
        role_id=role_id,
        jenis_arsip=jenis_arsip,
        instansi_id=instansi_id,
        limit=limit,
        offset=offset,
        skip_cache=skip_cache
    )



@router.get("/statistics", summary="Get Statistics")
async def get_statistics():
    """Ambil statistik data arsip untuk dashboard"""
    return arsip_service.get_statistics()


@router.get("/{arsip_id}", response_model=dict, summary="Get Arsip by ID")
async def get_arsip_by_id(arsip_id: int):
    """Ambil detail data arsip berdasarkan ID"""
    result = arsip_service.get_by_id(arsip_id)
    if not result:
        raise HTTPException(status_code=404, detail="Data tidak ditemukan")
    return {"status": "success", "data": result}


@router.put("/{arsip_id}", response_model=dict, summary="Update Arsip Data")
async def update_arsip(arsip_id: int, data: ArsipDataUpdate):
    """Update data arsip berdasarkan ID"""
    result = arsip_service.update(arsip_id, data)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.delete("/{arsip_id}", response_model=MessageResponse, summary="Delete Arsip Data")
async def delete_arsip(arsip_id: int):
    """Hapus data arsip berdasarkan ID"""
    result = arsip_service.delete(arsip_id)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/upload", response_model=UploadResponse, summary="Upload CSV/Excel File")
async def upload_file(file: UploadFile = File(..., description="File CSV atau Excel (.xlsx)")):
    """
    Upload file CSV atau Excel untuk bulk insert data.
    
    **Format file harus memiliki kolom:**
    - tanggal (YYYY-MM-DD)
    - role_id (integer)
    - jenis_arsip (text)
    - instansi_id (integer)
    - keterangan (optional)
    
    Kolom tambahan akan disimpan di field `data_content` sebagai JSON.
    """
    # Validate file type
    if not file.filename:
        raise HTTPException(status_code=400, detail="Nama file tidak valid")
    
    allowed_extensions = ['.csv', '.xlsx', '.xls']
    if not any(file.filename.lower().endswith(ext) for ext in allowed_extensions):
        raise HTTPException(
            status_code=400, 
            detail="Format file tidak didukung. Gunakan CSV atau Excel (.xlsx, .xls)"
        )
    
    # Read file content
    content = await file.read()
    
    # Process upload
    result = arsip_service.upload_file(content, file.filename)
    
    if result["status"] == "error" and result["rows_inserted"] == 0:
        raise HTTPException(status_code=400, detail=result["errors"][0] if result["errors"] else "Upload gagal")
    
    return result


@router.get("/cache/stats", summary="Get Cache Statistics")
async def get_cache_stats():
    """Get cache statistics for performance monitoring"""
    return arsip_service.get_cache_stats()
