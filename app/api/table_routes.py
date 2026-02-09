"""
API Routes untuk Management Table Dinamis
"""
from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import date

from app.services.table_service import table_service

router = APIRouter(prefix="/api/tables", tags=["Dynamic Tables"])

# Schemas
class ColumnSchema(BaseModel):
    name: str
    display_name: str
    data_type: str = "integer"
    is_required: bool = False
    is_summable: bool = True

class TableCreate(BaseModel):
    name: str # internal name slug
    display_name: str
    description: Optional[str] = None
    columns: List[ColumnSchema]

class TableUpdate(BaseModel):
    display_name: Optional[str] = None
    description: Optional[str] = None
    is_default: Optional[bool] = None

from app.services.schema_inspector import schema_inspector

class TableRegister(BaseModel):
    name: str # Table name from DB
    display_name: str
    description: Optional[str] = None

# Routes
@router.get("/candidates", summary="Get Candidate Tables")
async def get_candidate_tables():
    """Get tables from DB that are not yet registered"""
    tables = schema_inspector.get_candidate_tables()
    return {"status": "success", "data": tables}

@router.post("/register", summary="Register Existing Table")
async def register_table(data: TableRegister):
    """Register an existing physical table"""
    # 1. Inspect columns
    columns = schema_inspector.get_table_columns(data.name)
    if not columns:
        raise HTTPException(status_code=400, detail=f"Tabel '{data.name}' tidak ditemukan atau tidak memiliki kolom.")
    
    # 2. Register metadata
    result = table_service.register_existing_table(
        name=data.name,
        display_name=data.display_name,
        description=data.description,
        columns=columns
    )
    
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result

@router.get("", summary="Get All Tables")
async def get_tables():
    """Get all defined tables"""
    return {"status": "success", "data": table_service.get_all_tables()}

@router.get("/default", summary="Get Default Table")
async def get_default_table():
    """Get default table definition"""
    table = table_service.get_default_table()
    if not table:
        raise HTTPException(status_code=404, detail="Default table not found")
    return {"status": "success", "data": table}

@router.get("/{table_id}", summary="Get Table Detail")
async def get_table(table_id: int):
    """Get table definition by ID"""
    table = table_service.get_table_by_id(table_id)
    if not table:
        raise HTTPException(status_code=404, detail="Table not found")
    return {"status": "success", "data": table}

@router.post("", summary="Create New Table")
async def create_table(data: TableCreate):
    """Create new custom table"""
    columns_dict = [c.dict() for c in data.columns]
    result = table_service.create_table(
        name=data.name,
        display_name=data.display_name,
        description=data.description,
        columns=columns_dict
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result

@router.put("/{table_id}", summary="Update Table")
async def update_table(table_id: int, data: TableUpdate):
    """Update table metadata"""
    result = table_service.update_table(
        table_id=table_id,
        display_name=data.display_name,
        description=data.description,
        is_default=data.is_default
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result

@router.delete("/{table_id}", summary="Delete Table")
async def delete_table(table_id: int):
    """Delete table and all its data"""
    result = table_service.delete_table(table_id)
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result

@router.get("/{table_id}/statistics", summary="Get Table Statistics")
async def get_table_statistics(table_id: int):
    """Get aggregated statistics for a table"""
    stats = table_service.get_statistics(table_id)
    if stats is None:
        raise HTTPException(status_code=404, detail="Table not found")
    return {"status": "success", "data": stats}


class DynamicDataCreate(BaseModel):
    unit_kerja_id: int
    tanggal: date
    data: Dict[str, Any]

@router.post("/{table_id}/data", summary="Create Table Data")
async def create_table_data(table_id: int, payload: DynamicDataCreate):
    """Create a new data record for a table"""
    result = table_service.create_dynamic_data(
        table_id=table_id,
        unit_kerja_id=payload.unit_kerja_id,
        tanggal=payload.tanggal,
        data=payload.data
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result

@router.get("/{table_id}/data", summary="Get Table Data")
async def get_table_data(
    table_id: int,
    instansi_id: Optional[int] = None,
    unit_kerja_id: Optional[int] = None,
    tanggal_start: Optional[date] = None,
    tanggal_end: Optional[date] = None,
    limit: int = 50,
    offset: int = 0
):
    """Get dynamic data for a table"""
    return table_service.get_dynamic_data(
        table_id=table_id,
        instansi_id=instansi_id,
        unit_kerja_id=unit_kerja_id,
        tanggal_start=tanggal_start,
        tanggal_end=tanggal_end,
        limit=limit,
        offset=offset
    )

@router.put("/{table_id}/data/{row_id}", summary="Update Table Data")
async def update_table_data(table_id: int, row_id: int, payload: Dict[str, Any]):
    """Update specific data row"""
    result = table_service.update_dynamic_data(
        table_id=table_id,
        row_id=row_id,
        data=payload
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result

@router.delete("/{table_id}/data/{row_id}", summary="Delete Table Data")
async def delete_table_data(table_id: int, row_id: int):
    """Delete specific data row"""
    result = table_service.delete_dynamic_data(
        table_id=table_id,
        row_id=row_id
    )
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    return result
