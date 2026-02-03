"""
Data Integration Service
"""
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import Any, Dict, List
from datetime import datetime

from app.database import get_db_context


class IntegratorService:
    """Service untuk integrasi data dari database"""
    
    def __init__(self):
        self.last_sync: datetime = None
        self._cache: Dict[str, Any] = {}
    
    def get_summary(self) -> Dict[str, Any]:
        """Ambil ringkasan data"""
        with get_db_context() as db:
            summary = {
                "last_sync": self.last_sync.isoformat() if self.last_sync else None,
                "tables": self._get_table_stats(db),
                "total_records": 0
            }
            
            # Calculate total records
            for table in summary["tables"]:
                summary["total_records"] += table.get("count", 0)
            
            return summary
    
    def _get_table_stats(self, db: Session) -> List[Dict]:
        """Get statistics for each table"""
        tables = []
        
        # Get list of tables
        result = db.execute(text("SHOW TABLES"))
        table_names = [row[0] for row in result.fetchall()]
        
        for table_name in table_names:
            try:
                count_result = db.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                count = count_result.fetchone()[0]
                tables.append({
                    "name": table_name,
                    "count": count
                })
            except Exception as e:
                tables.append({
                    "name": table_name,
                    "count": 0,
                    "error": str(e)
                })
        
        return tables
    
    def get_data(self, table_name: str, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """Ambil data dari tabel tertentu"""
        with get_db_context() as db:
            try:
                # Get total count
                count_result = db.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                total = count_result.fetchone()[0]
                
                # Get data with pagination
                result = db.execute(
                    text(f"SELECT * FROM `{table_name}` LIMIT :limit OFFSET :offset"),
                    {"limit": limit, "offset": offset}
                )
                
                # Convert to list of dicts
                columns = result.keys()
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                
                return {
                    "table": table_name,
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "data": rows
                }
            except Exception as e:
                return {
                    "table": table_name,
                    "error": str(e)
                }
    
    def sync_data(self) -> Dict[str, Any]:
        """Sync/refresh data dari database"""
        self.last_sync = datetime.now()
        self._cache.clear()
        
        summary = self.get_summary()
        
        return {
            "status": "success",
            "synced_at": self.last_sync.isoformat(),
            "tables_synced": len(summary.get("tables", [])),
            "total_records": summary.get("total_records", 0)
        }
    
    def get_kategori_instansi(self) -> List[Dict]:
        """Ambil data kategori instansi"""
        with get_db_context() as db:
            try:
                result = db.execute(text("SELECT * FROM kategori_instansi"))
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result.fetchall()]
            except Exception as e:
                return [{"error": str(e)}]
    
    def get_transaksi_summary(self) -> List[Dict]:
        """Ambil data transaksi summary"""
        with get_db_context() as db:
            try:
                result = db.execute(text("SELECT * FROM transaksi_summary"))
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result.fetchall()]
            except Exception as e:
                return [{"error": str(e)}]


# Global instance
integrator_service = IntegratorService()
