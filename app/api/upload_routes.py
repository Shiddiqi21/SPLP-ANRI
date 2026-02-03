"""
API Routes untuk Upload File
"""
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
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
    db: Session = Depends(get_db)
):
    """
    Upload file Excel atau CSV untuk import data arsip.
    
    Format kolom yang didukung:
    - tanggal: Tanggal data (wajib)
    - instansi: Nama instansi (wajib)
    - unit_kerja: Nama unit kerja (wajib)
    - naskah_masuk: Jumlah naskah masuk
    - naskah_keluar: Jumlah naskah keluar
    - disposisi: Jumlah disposisi
    - berkas: Jumlah berkas
    - retensi_permanen: Jumlah retensi permanen
    - retensi_musnah: Jumlah retensi musnah
    - naskah_ditindaklanjuti: Jumlah naskah ditindaklanjuti
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
    result = upload_service.process_upload(content, file.filename)
    
    if not result["success"] and result["stats"]["inserted"] == 0 and result["stats"]["updated"] == 0:
        raise HTTPException(status_code=400, detail=result)
    
    return result


@router.get("/template")
async def download_template():
    """
    Download template Excel untuk upload data.
    """
    # Create sample data
    sample_data = {
        'tanggal': ['2026-01-01', '2026-01-02', '2026-01-03'],
        'instansi': ['Arsip Nasional Republik Indonesia', 'Arsip Nasional Republik Indonesia', 'Arsip Nasional Republik Indonesia'],
        'unit_kerja': ['Biro Kepegawaian dan Umum', 'Direktorat Kearsipan Pusat', 'Inspektorat'],
        'naskah_masuk': [100, 50, 25],
        'naskah_keluar': [80, 40, 20],
        'disposisi': [60, 30, 15],
        'berkas': [40, 20, 10],
        'retensi_permanen': [20, 10, 5],
        'retensi_musnah': [10, 5, 2],
        'naskah_ditindaklanjuti': [70, 35, 18]
    }
    
    df = pd.DataFrame(sample_data)
    
    # Create Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data Arsip')
        
        # Add instructions sheet
        instructions = pd.DataFrame({
            'Kolom': [
                'tanggal', 'instansi', 'unit_kerja', 'naskah_masuk', 
                'naskah_keluar', 'disposisi', 'berkas', 'retensi_permanen',
                'retensi_musnah', 'naskah_ditindaklanjuti'
            ],
            'Keterangan': [
                'Tanggal data (format: YYYY-MM-DD atau DD/MM/YYYY) - WAJIB',
                'Nama instansi (opsional, default: ANRI)',
                'Nama unit kerja - WAJIB',
                'Jumlah naskah masuk',
                'Jumlah naskah keluar',
                'Jumlah disposisi',
                'Jumlah berkas',
                'Jumlah retensi permanen',
                'Jumlah retensi musnah',
                'Jumlah naskah ditindaklanjuti'
            ],
            'Contoh': [
                '2026-01-01', 'Arsip Nasional Republik Indonesia', 
                'Biro Kepegawaian dan Umum', '100', '80', '60', '40', '20', '10', '70'
            ]
        })
        instructions.to_excel(writer, index=False, sheet_name='Petunjuk')
    
    output.seek(0)
    
    return StreamingResponse(
        output,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={
            'Content-Disposition': 'attachment; filename=template_upload_arsip.xlsx'
        }
    )


@router.get("/template/csv")
async def download_template_csv():
    """
    Download template CSV untuk upload data.
    """
    sample_data = {
        'tanggal': ['2026-01-01', '2026-01-02', '2026-01-03'],
        'instansi': ['Arsip Nasional Republik Indonesia', 'Arsip Nasional Republik Indonesia', 'Arsip Nasional Republik Indonesia'],
        'unit_kerja': ['Biro Kepegawaian dan Umum', 'Direktorat Kearsipan Pusat', 'Inspektorat'],
        'naskah_masuk': [100, 50, 25],
        'naskah_keluar': [80, 40, 20],
        'disposisi': [60, 30, 15],
        'berkas': [40, 20, 10],
        'retensi_permanen': [20, 10, 5],
        'retensi_musnah': [10, 5, 2],
        'naskah_ditindaklanjuti': [70, 35, 18]
    }
    
    df = pd.DataFrame(sample_data)
    
    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type='text/csv',
        headers={
            'Content-Disposition': 'attachment; filename=template_upload_arsip.csv'
        }
    )
