"""
API Routes untuk Upload File
"""
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, Form, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
import pandas as pd
import io

from app.database import get_db
from app.services.upload_service import UploadService

router = APIRouter(prefix="/api/upload", tags=["Upload"])


@router.post("/file")
async def upload_file(
    file: UploadFile = File(...),
    table_id: int = Form(None),
    db: Session = Depends(get_db)
):
    """
    Upload file Excel atau CSV untuk import data arsip.
    """
    # Validate file type
    allowed_extensions = ['.csv', '.xlsx', '.xls']
    file_extension = '.' + file.filename.split('.')[-1].lower()
    
    if file_extension not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Format file tidak didukung. Gunakan: {', '.join(allowed_extensions)}"
        )
    
    # Validate file size (max 10MB)
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(
            status_code=400,
            detail="Ukuran file terlalu besar. Maksimal 10MB."
        )
    
    # Process upload
    upload_service = UploadService(db)
    result = upload_service.process_upload(content, file.filename, table_id=table_id)
    
    if not result["success"] and result["stats"]["inserted"] == 0 and result["stats"]["updated"] == 0:
        raise HTTPException(status_code=400, detail=result)
    
    return result


@router.get("/template")
async def download_template(table_id: int = Query(None)):
    """
    Download template Excel untuk upload data.
    """
    from app.services.table_service import table_service
    
    if table_id:
        table = table_service.get_table_by_id(table_id)
    else:
        table = table_service.get_default_table()
        
    if not table:
         raise HTTPException(status_code=404, detail="Table definition not found")

    columns = table['columns']
    
    # Create sample data
    # Dynamic columns
    data = {
        'tanggal': ['2026-01-01', '2026-01-02', '2026-01-03'],
        'instansi': ['Arsip Nasional Republik Indonesia', 'Arsip Nasional Republik Indonesia', 'Arsip Nasional Republik Indonesia'],
        'unit_kerja': ['Biro Kepegawaian dan Umum', 'Direktorat Kearsipan Pusat', 'Inspektorat']
    }
    
    for col in columns:
        if col['data_type'] == 'integer':
            data[col['name']] = [0, 0, 0]
        else:
            data[col['name']] = ["Sample", "Sample", "Sample"]
    
    df = pd.DataFrame(data)
    
    # Create Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=table['display_name'][:30])
        
        # Add instructions sheet
        cols_info = ['tanggal', 'instansi', 'unit_kerja'] + [c['name'] for c in columns]
        desc_info = ['Tanggal data (YYYY-MM-DD) - WAJIB', 'Nama Instansi - WAJIB', 'Nama Unit Kerja - WAJIB'] + [c['display_name'] for c in columns]
        
        instructions = pd.DataFrame({
            'Kolom': cols_info,
            'Keterangan': desc_info
        })
        instructions.to_excel(writer, index=False, sheet_name='Petunjuk')
    
    output.seek(0)
    
    filename = f"template_{table['name']}.xlsx"
    
    return StreamingResponse(
        output,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={
            'Content-Disposition': f'attachment; filename={filename}'
        }
    )


@router.get("/template/csv")
async def download_template_csv(table_id: int = Query(None)):
    """
    Download template CSV untuk upload data.
    """
    from app.services.table_service import table_service
    
    if table_id:
        table = table_service.get_table_by_id(table_id)
    else:
        table = table_service.get_default_table()
        
    if not table:
         raise HTTPException(status_code=404, detail="Table definition not found")

    columns = table['columns']
    
    data = {
        'tanggal': ['2026-01-01', '2026-01-02', '2026-01-03'],
        'instansi': ['Arsip Nasional Republik Indonesia', 'Arsip Nasional Republik Indonesia', 'Arsip Nasional Republik Indonesia'],
        'unit_kerja': ['Biro Kepegawaian dan Umum', 'Direktorat Kearsipan Pusat', 'Inspektorat']
    }
    
    for col in columns:
        if col['data_type'] == 'integer':
            data[col['name']] = [0, 0, 0]
        else:
            data[col['name']] = ["Sample", "Sample", "Sample"]
    
    df = pd.DataFrame(data)
    
    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    
    filename = f"template_{table['name']}.csv"
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type='text/csv',
        headers={
            'Content-Disposition': f'attachment; filename={filename}'
        }
    )
