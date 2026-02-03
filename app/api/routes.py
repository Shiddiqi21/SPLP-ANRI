"""
API Routes
"""
from fastapi import APIRouter, Query, HTTPException
from typing import Optional

from app.services.integrator import integrator_service
from app.database import test_connection

router = APIRouter(prefix="/api", tags=["API"])


@router.get("/health")
async def health_check():
    """Check service health and database connection"""
    db_status = test_connection()
    return {
        "status": "ok",
        "service": "SPLP Data Integrator",
        "database": db_status
    }


@router.get("/data/summary")
async def get_summary():
    """Get data summary from all tables"""
    return integrator_service.get_summary()


@router.get("/data/tables/{table_name}")
async def get_table_data(
    table_name: str,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0)
):
    """Get data from specific table with pagination"""
    result = integrator_service.get_data(table_name, limit, offset)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.get("/data/kategori-instansi")
async def get_kategori_instansi():
    """Get kategori instansi data"""
    return {
        "data": integrator_service.get_kategori_instansi()
    }


@router.get("/data/transaksi-summary")
async def get_transaksi_summary():
    """Get transaksi summary data"""
    return {
        "data": integrator_service.get_transaksi_summary()
    }


@router.post("/sync")
async def sync_data():
    """Trigger manual data sync"""
    return integrator_service.sync_data()
