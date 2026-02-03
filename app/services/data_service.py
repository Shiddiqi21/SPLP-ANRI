"""
Service untuk mengelola data Instansi, Unit Kerja, dan Data Arsip
"""
from typing import Dict, Any, List, Optional
from datetime import date
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from app.database import get_db_context
from app.models.arsip_models import Instansi, UnitKerja, DataArsip


class DataService:
    """Service untuk operasi CRUD pada data arsip"""
    
    # ==================== INSTANSI ====================
    
    def get_all_instansi(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """Get all instansi with pagination"""
        with get_db_context() as db:
            query = db.query(Instansi)
            total = query.count()
            data = query.order_by(Instansi.nama).offset(offset).limit(limit).all()
            return {
                "data": [i.to_dict() for i in data],
                "total": total,
                "limit": limit,
                "offset": offset
            }
    
    def get_instansi_by_id(self, instansi_id: int) -> Optional[Dict]:
        """Get instansi by ID"""
        with get_db_context() as db:
            instansi = db.query(Instansi).filter(Instansi.id == instansi_id).first()
            if instansi:
                return instansi.to_dict()
            return None
    
    def create_instansi(self, kode: str, nama: str) -> Dict[str, Any]:
        """Create new instansi"""
        with get_db_context() as db:
            try:
                existing = db.query(Instansi).filter(Instansi.kode == kode).first()
                if existing:
                    return {"status": "error", "message": f"Kode instansi '{kode}' sudah ada"}
                
                instansi = Instansi(kode=kode, nama=nama)
                db.add(instansi)
                db.commit()
                db.refresh(instansi)
                return {"status": "success", "data": instansi.to_dict()}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    def update_instansi(self, instansi_id: int, kode: str = None, nama: str = None) -> Dict[str, Any]:
        """Update instansi"""
        with get_db_context() as db:
            try:
                instansi = db.query(Instansi).filter(Instansi.id == instansi_id).first()
                if not instansi:
                    return {"status": "error", "message": "Instansi tidak ditemukan"}
                
                if kode:
                    instansi.kode = kode
                if nama:
                    instansi.nama = nama
                
                db.commit()
                db.refresh(instansi)
                return {"status": "success", "data": instansi.to_dict()}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    def delete_instansi(self, instansi_id: int) -> Dict[str, Any]:
        """Delete instansi (cascade deletes unit_kerja and data_arsip)"""
        with get_db_context() as db:
            try:
                instansi = db.query(Instansi).filter(Instansi.id == instansi_id).first()
                if not instansi:
                    return {"status": "error", "message": "Instansi tidak ditemukan"}
                
                db.delete(instansi)
                db.commit()
                return {"status": "success", "message": f"Instansi {instansi.nama} berhasil dihapus"}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    # ==================== UNIT KERJA ====================
    
    def get_unit_kerja_by_instansi(self, instansi_id: int, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """Get all unit kerja for an instansi"""
        with get_db_context() as db:
            query = db.query(UnitKerja).filter(UnitKerja.instansi_id == instansi_id)
            total = query.count()
            data = query.order_by(UnitKerja.nama).offset(offset).limit(limit).all()
            return {
                "data": [u.to_dict() for u in data],
                "total": total,
                "instansi_id": instansi_id,
                "limit": limit,
                "offset": offset
            }
    
    def get_all_unit_kerja(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """Get all unit kerja with instansi info"""
        with get_db_context() as db:
            query = db.query(UnitKerja).options(joinedload(UnitKerja.instansi))
            total = query.count()
            data = query.order_by(UnitKerja.nama).offset(offset).limit(limit).all()
            return {
                "data": [u.to_dict(include_instansi=True) for u in data],
                "total": total,
                "limit": limit,
                "offset": offset
            }
    
    def create_unit_kerja(self, instansi_id: int, kode: str, nama: str) -> Dict[str, Any]:
        """Create new unit kerja"""
        with get_db_context() as db:
            try:
                # Check instansi exists
                instansi = db.query(Instansi).filter(Instansi.id == instansi_id).first()
                if not instansi:
                    return {"status": "error", "message": "Instansi tidak ditemukan"}
                
                # Check duplicate
                existing = db.query(UnitKerja).filter(
                    UnitKerja.instansi_id == instansi_id,
                    UnitKerja.kode == kode
                ).first()
                if existing:
                    return {"status": "error", "message": f"Kode unit kerja '{kode}' sudah ada di instansi ini"}
                
                unit = UnitKerja(instansi_id=instansi_id, kode=kode, nama=nama)
                db.add(unit)
                db.commit()
                db.refresh(unit)
                return {"status": "success", "data": unit.to_dict()}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    def delete_unit_kerja(self, unit_id: int) -> Dict[str, Any]:
        """Delete unit kerja"""
        with get_db_context() as db:
            try:
                unit = db.query(UnitKerja).filter(UnitKerja.id == unit_id).first()
                if not unit:
                    return {"status": "error", "message": "Unit kerja tidak ditemukan"}
                
                db.delete(unit)
                db.commit()
                return {"status": "success", "message": f"Unit kerja {unit.nama} berhasil dihapus"}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    # ==================== DATA ARSIP ====================
    
    def get_data_arsip(
        self, 
        unit_kerja_id: int = None,
        instansi_id: int = None,
        tanggal_start: date = None,
        tanggal_end: date = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Get data arsip with filters"""
        with get_db_context() as db:
            query = db.query(DataArsip).options(
                joinedload(DataArsip.unit_kerja).joinedload(UnitKerja.instansi)
            )
            
            if unit_kerja_id:
                query = query.filter(DataArsip.unit_kerja_id == unit_kerja_id)
            
            if instansi_id:
                query = query.join(UnitKerja).filter(UnitKerja.instansi_id == instansi_id)
            
            if tanggal_start:
                query = query.filter(DataArsip.tanggal >= tanggal_start)
            
            if tanggal_end:
                query = query.filter(DataArsip.tanggal <= tanggal_end)
            
            total = query.count()
            data = query.order_by(DataArsip.tanggal.desc()).offset(offset).limit(limit).all()
            
            return {
                "data": [d.to_dict(include_unit_kerja=True) for d in data],
                "total": total,
                "limit": limit,
                "offset": offset
            }
    
    def create_or_update_data_arsip(
        self,
        unit_kerja_id: int,
        tanggal: date,
        naskah_masuk: int = 0,
        naskah_keluar: int = 0,
        disposisi: int = 0,
        berkas: int = 0,
        retensi_permanen: int = 0,
        retensi_musnah: int = 0,
        naskah_ditindaklanjuti: int = 0
    ) -> Dict[str, Any]:
        """Create or update (SUM) data arsip for a unit kerja on a specific date"""
        with get_db_context() as db:
            try:
                # Check unit kerja exists
                unit = db.query(UnitKerja).filter(UnitKerja.id == unit_kerja_id).first()
                if not unit:
                    return {"status": "error", "message": "Unit kerja tidak ditemukan"}
                
                # Check if data exists for this date
                data = db.query(DataArsip).filter(
                    DataArsip.unit_kerja_id == unit_kerja_id,
                    DataArsip.tanggal == tanggal
                ).first()
                
                if data:
                    # SUM with existing values (merge data)
                    data.naskah_masuk = (data.naskah_masuk or 0) + naskah_masuk
                    data.naskah_keluar = (data.naskah_keluar or 0) + naskah_keluar
                    data.disposisi = (data.disposisi or 0) + disposisi
                    data.berkas = (data.berkas or 0) + berkas
                    data.retensi_permanen = (data.retensi_permanen or 0) + retensi_permanen
                    data.retensi_musnah = (data.retensi_musnah or 0) + retensi_musnah
                    data.naskah_ditindaklanjuti = (data.naskah_ditindaklanjuti or 0) + naskah_ditindaklanjuti
                    data.calculate_total()
                else:
                    # Create new
                    data = DataArsip(
                        unit_kerja_id=unit_kerja_id,
                        tanggal=tanggal,
                        naskah_masuk=naskah_masuk,
                        naskah_keluar=naskah_keluar,
                        disposisi=disposisi,
                        berkas=berkas,
                        retensi_permanen=retensi_permanen,
                        retensi_musnah=retensi_musnah,
                        naskah_ditindaklanjuti=naskah_ditindaklanjuti
                    )
                    data.calculate_total()
                    db.add(data)
                
                db.commit()
                db.refresh(data)
                return {"status": "success", "data": data.to_dict()}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    def delete_data_arsip(self, data_id: int) -> Dict[str, Any]:
        """Delete data arsip"""
        with get_db_context() as db:
            try:
                data = db.query(DataArsip).filter(DataArsip.id == data_id).first()
                if not data:
                    return {"status": "error", "message": "Data tidak ditemukan"}
                
                db.delete(data)
                db.commit()
                return {"status": "success", "message": "Data berhasil dihapus"}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    # ==================== STATISTICS ====================
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get dashboard statistics"""
        with get_db_context() as db:
            total_instansi = db.query(func.count(Instansi.id)).scalar() or 0
            total_unit_kerja = db.query(func.count(UnitKerja.id)).scalar() or 0
            
            # Sum all data
            totals = db.query(
                func.sum(DataArsip.naskah_masuk).label('naskah_masuk'),
                func.sum(DataArsip.naskah_keluar).label('naskah_keluar'),
                func.sum(DataArsip.disposisi).label('disposisi'),
                func.sum(DataArsip.berkas).label('berkas'),
                func.sum(DataArsip.retensi_permanen).label('retensi_permanen'),
                func.sum(DataArsip.retensi_musnah).label('retensi_musnah'),
                func.sum(DataArsip.naskah_ditindaklanjuti).label('naskah_ditindaklanjuti'),
                func.sum(DataArsip.total).label('total')
            ).first()
            
            return {
                "total_instansi": total_instansi,
                "total_unit_kerja": total_unit_kerja,
                "total_naskah_masuk": totals.naskah_masuk or 0,
                "total_naskah_keluar": totals.naskah_keluar or 0,
                "total_disposisi": totals.disposisi or 0,
                "total_berkas": totals.berkas or 0,
                "total_retensi_permanen": totals.retensi_permanen or 0,
                "total_retensi_musnah": totals.retensi_musnah or 0,
                "total_naskah_ditindaklanjuti": totals.naskah_ditindaklanjuti or 0,
                "grand_total": totals.total or 0
            }
    
    def get_summary_by_instansi(self) -> List[Dict]:
        """Get summary grouped by instansi"""
        with get_db_context() as db:
            results = db.query(
                Instansi.id,
                Instansi.kode,
                Instansi.nama,
                func.count(UnitKerja.id.distinct()).label('unit_count'),
                func.sum(DataArsip.total).label('total_data')
            ).outerjoin(UnitKerja).outerjoin(DataArsip).group_by(
                Instansi.id, Instansi.kode, Instansi.nama
            ).all()
            
            return [
                {
                    "id": r.id,
                    "kode": r.kode,
                    "nama": r.nama,
                    "unit_count": r.unit_count or 0,
                    "total_data": r.total_data or 0
                }
                for r in results
            ]


# Singleton instance
data_service = DataService()
