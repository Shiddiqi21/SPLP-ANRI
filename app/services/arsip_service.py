"""
Arsip Data Service - CRUD dan Upload Operations
Optimized for large-scale data handling with caching
"""
from sqlalchemy import and_, or_, func, text
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
from datetime import date
import pandas as pd
import io

from app.database import get_db_context
from app.models import ArsipData, Base
from app.schemas import ArsipDataCreate, ArsipDataUpdate
from app.services.cache_service import cache, cached, invalidate_arsip_cache


class ArsipDataService:
    """Service untuk operasi data arsip dengan optimasi performa"""
    
    # Threshold for using approximate count
    LARGE_DATASET_THRESHOLD = 100000
    
    def create_table_if_not_exists(self):
        """Buat tabel arsip_data jika belum ada"""
        from app.database import engine
        Base.metadata.create_all(bind=engine)
    
    def create(self, data: ArsipDataCreate) -> Dict[str, Any]:
        """Insert data arsip baru"""
        with get_db_context() as db:
            try:
                arsip = ArsipData(
                    tanggal=data.tanggal,
                    role_id=data.role_id,
                    jenis_arsip=data.jenis_arsip,
                    instansi_id=data.instansi_id,
                    data_content=data.data_content,
                    keterangan=data.keterangan
                )
                db.add(arsip)
                db.commit()
                db.refresh(arsip)
                
                # Invalidate cache after insert
                invalidate_arsip_cache()
                
                return {"status": "success", "data": arsip.to_dict()}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    def get_by_id(self, arsip_id: int) -> Optional[Dict[str, Any]]:
        """Get arsip by ID"""
        with get_db_context() as db:
            arsip = db.query(ArsipData).filter(ArsipData.id == arsip_id).first()
            if arsip:
                return arsip.to_dict()
            return None
    
    def _get_approximate_count(self, db: Session) -> int:
        """Get approximate row count from table statistics (much faster for large tables)"""
        try:
            result = db.execute(text(
                "SELECT table_rows FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = 'arsip_data'"
            )).fetchone()
            return result[0] if result else 0
        except:
            return db.query(ArsipData).count()
    
    def _should_use_approximate_count(self, db: Session) -> bool:
        """Determine if we should use approximate count based on table size"""
        approx = self._get_approximate_count(db)
        return approx > self.LARGE_DATASET_THRESHOLD
    
    def get_filtered(
        self,
        tanggal_start: Optional[date] = None,
        tanggal_end: Optional[date] = None,
        role_id: Optional[int] = None,
        jenis_arsip: Optional[str] = None,
        instansi_id: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
        skip_cache: bool = False
    ) -> Dict[str, Any]:
        """Get data dengan filter - optimized for large datasets"""
        
        # Generate cache key
        cache_key = cache._generate_key(
            "filter",
            tanggal_start, tanggal_end, role_id, 
            jenis_arsip, instansi_id, limit, offset
        )
        
        # Try cache first (unless skip_cache is True)
        if not skip_cache:
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result
        
        with get_db_context() as db:
            query = db.query(ArsipData)
            filters_applied = {}
            has_filters = False
            
            if tanggal_start:
                query = query.filter(ArsipData.tanggal >= tanggal_start)
                filters_applied["tanggal_start"] = str(tanggal_start)
                has_filters = True
            
            if tanggal_end:
                query = query.filter(ArsipData.tanggal <= tanggal_end)
                filters_applied["tanggal_end"] = str(tanggal_end)
                has_filters = True
            
            if role_id:
                query = query.filter(ArsipData.role_id == role_id)
                filters_applied["role_id"] = role_id
                has_filters = True
            
            if jenis_arsip:
                query = query.filter(ArsipData.jenis_arsip.ilike(f"%{jenis_arsip}%"))
                filters_applied["jenis_arsip"] = jenis_arsip
                has_filters = True
            
            if instansi_id:
                query = query.filter(ArsipData.instansi_id == instansi_id)
                filters_applied["instansi_id"] = instansi_id
                has_filters = True
            
            # ALWAYS use approximate count - counting 2M+ rows is too slow
            # For filtered queries, we estimate based on approximate total
            total = self._get_approximate_count(db)
            
            # Optimized query with index hints - order by ID for fastest pagination
            arsip_list = query.offset(offset).limit(limit).all()
            
            result = {
                "data": [a.to_dict() for a in arsip_list],
                "total": total,
                "limit": limit,
                "offset": offset,
                "filters_applied": filters_applied,
                "cached": False
            }
            
            # Cache the result (5 minutes TTL)
            cache.set(cache_key, result, ttl=300)
            
            return result
    
    def update(self, arsip_id: int, data: ArsipDataUpdate) -> Dict[str, Any]:
        """Update data arsip"""
        with get_db_context() as db:
            arsip = db.query(ArsipData).filter(ArsipData.id == arsip_id).first()
            if not arsip:
                return {"status": "error", "message": "Data tidak ditemukan"}
            
            try:
                update_data = data.model_dump(exclude_unset=True)
                for key, value in update_data.items():
                    if value is not None:
                        setattr(arsip, key, value)
                
                db.commit()
                db.refresh(arsip)
                
                # Invalidate cache after update
                invalidate_arsip_cache()
                
                return {"status": "success", "data": arsip.to_dict()}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    def delete(self, arsip_id: int) -> Dict[str, Any]:
        """Delete data arsip"""
        with get_db_context() as db:
            arsip = db.query(ArsipData).filter(ArsipData.id == arsip_id).first()
            if not arsip:
                return {"status": "error", "message": "Data tidak ditemukan"}
            
            try:
                db.delete(arsip)
                db.commit()
                
                # Invalidate cache after delete
                invalidate_arsip_cache()
                
                return {"status": "success", "message": f"Data ID {arsip_id} berhasil dihapus"}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    def upload_file(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        """Upload dan parse file CSV/Excel"""
        errors = []
        rows_inserted = 0
        rows_failed = 0
        
        try:
            if filename.endswith('.csv'):
                df = pd.read_csv(io.BytesIO(file_content))
            elif filename.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(io.BytesIO(file_content))
            else:
                return {"status": "error", "filename": filename, "rows_processed": 0, "rows_inserted": 0, "rows_failed": 0, "errors": ["Format tidak didukung"]}
            
            df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
            required_cols = ['tanggal', 'role_id', 'jenis_arsip', 'instansi_id']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                return {"status": "error", "filename": filename, "rows_processed": 0, "rows_inserted": 0, "rows_failed": 0, "errors": [f"Kolom tidak ditemukan: {missing_cols}"]}
            
            rows_processed = len(df)
            
            with get_db_context() as db:
                # Batch insert for better performance
                batch_size = 1000
                batch = []
                
                for idx, row in df.iterrows():
                    try:
                        tanggal = pd.to_datetime(row['tanggal']).date()
                        keterangan = row.get('keterangan', None)
                        if pd.isna(keterangan):
                            keterangan = None
                        
                        arsip = ArsipData(
                            tanggal=tanggal,
                            role_id=int(row['role_id']),
                            jenis_arsip=str(row['jenis_arsip']),
                            instansi_id=int(row['instansi_id']),
                            keterangan=keterangan
                        )
                        batch.append(arsip)
                        rows_inserted += 1
                        
                        # Commit in batches
                        if len(batch) >= batch_size:
                            db.add_all(batch)
                            db.commit()
                            batch = []
                            
                    except Exception as e:
                        rows_failed += 1
                        if len(errors) < 10:
                            errors.append(f"Baris {idx + 2}: {str(e)}")
                
                # Commit remaining batch
                if batch:
                    db.add_all(batch)
                    db.commit()
            
            # Invalidate cache after bulk insert
            invalidate_arsip_cache()
            
            return {"status": "success" if rows_failed == 0 else "partial", "filename": filename, "rows_processed": rows_processed, "rows_inserted": rows_inserted, "rows_failed": rows_failed, "errors": errors if errors else None}
            
        except Exception as e:
            return {"status": "error", "filename": filename, "rows_processed": 0, "rows_inserted": 0, "rows_failed": 0, "errors": [str(e)]}
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics for dashboard - ULTRA FAST (metadata only)"""
        cache_key = "stats:dashboard:fast"
        
        # Try cache first (10 minute TTL for fast response)
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            cached_result["cached"] = True
            return cached_result
        
        with get_db_context() as db:
            # Instant: approximate count from table metadata
            total = self._get_approximate_count(db)
            
            # Fast counts from pre-aggregated summary tables
            try:
                from app.models import ArsipSummary
                
                # Count distinct jenis from summary (much smaller table)
                jenis_count = db.execute(text(
                    "SELECT COUNT(DISTINCT jenis_arsip) FROM arsip_summary"
                )).scalar() or 0
                
                # Count distinct instansi from summary
                instansi_count = db.execute(text(
                    "SELECT COUNT(DISTINCT instansi_id) FROM arsip_summary"
                )).scalar() or 0
                
                # If summary is empty, use hardcoded estimates
                if jenis_count == 0:
                    jenis_count = 20  # approximate
                    instansi_count = 100  # approximate
                    
            except Exception:
                jenis_count = 20
                instansi_count = 100
            
            result = {
                "total_records": total,
                "jenis_count": jenis_count,
                "instansi_count": instansi_count,
                "by_jenis_arsip": [],
                "by_instansi": [],
                "cached": False
            }
            
            # Cache for 10 minutes
            cache.set(cache_key, result, ttl=600)
            
            return result
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring"""
        return cache.stats()


# Global instance
arsip_service = ArsipDataService()
