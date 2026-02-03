"""
Aggregation Service - Pre-compute summary data untuk Grafana
"""
from sqlalchemy import func, text
from datetime import datetime, date
from typing import Dict, Any, List

from app.database import get_db_context
from app.models import ArsipData, ArsipSummary, DailySummary, Base


class AggregationService:
    """Service untuk aggregate data dan update summary tables"""
    
    def __init__(self):
        self.last_run: datetime = None
    
    def create_tables(self):
        """Buat tabel summary jika belum ada"""
        from app.database import engine
        Base.metadata.create_all(bind=engine)
    
    def aggregate_arsip_summary(self) -> Dict[str, Any]:
        """
        Aggregate data arsip berdasarkan tanggal, instansi, jenis, role
        dan simpan ke tabel arsip_summary
        """
        with get_db_context() as db:
            try:
                # Clear existing summary
                db.execute(text("DELETE FROM arsip_summary"))
                
                # Aggregate from arsip_data
                aggregated = db.query(
                    ArsipData.tanggal,
                    ArsipData.instansi_id,
                    ArsipData.jenis_arsip,
                    ArsipData.role_id,
                    func.count(ArsipData.id).label('total_count')
                ).group_by(
                    ArsipData.tanggal,
                    ArsipData.instansi_id,
                    ArsipData.jenis_arsip,
                    ArsipData.role_id
                ).all()
                
                # Insert new summary records
                inserted = 0
                for row in aggregated:
                    summary = ArsipSummary(
                        tanggal=row.tanggal,
                        instansi_id=row.instansi_id,
                        jenis_arsip=row.jenis_arsip,
                        role_id=row.role_id,
                        total_count=row.total_count
                    )
                    db.add(summary)
                    inserted += 1
                
                db.commit()
                self.last_run = datetime.now()
                
                return {
                    "status": "success",
                    "table": "arsip_summary",
                    "rows_inserted": inserted,
                    "timestamp": self.last_run.isoformat()
                }
                
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    def aggregate_daily_summary(self) -> Dict[str, Any]:
        """
        Aggregate summary harian untuk trend analysis
        """
        with get_db_context() as db:
            try:
                # Clear existing daily summary
                db.execute(text("DELETE FROM daily_summary"))
                
                # Get unique dates
                dates = db.query(ArsipData.tanggal).distinct().all()
                
                inserted = 0
                for (tanggal,) in dates:
                    # Count metrics for this date
                    total_arsip = db.query(func.count(ArsipData.id)).filter(
                        ArsipData.tanggal == tanggal
                    ).scalar() or 0
                    
                    total_instansi = db.query(func.count(func.distinct(ArsipData.instansi_id))).filter(
                        ArsipData.tanggal == tanggal
                    ).scalar() or 0
                    
                    total_jenis = db.query(func.count(func.distinct(ArsipData.jenis_arsip))).filter(
                        ArsipData.tanggal == tanggal
                    ).scalar() or 0
                    
                    daily = DailySummary(
                        tanggal=tanggal,
                        total_arsip=total_arsip,
                        total_instansi=total_instansi,
                        total_jenis=total_jenis
                    )
                    db.add(daily)
                    inserted += 1
                
                db.commit()
                
                return {
                    "status": "success",
                    "table": "daily_summary",
                    "rows_inserted": inserted,
                    "timestamp": datetime.now().isoformat()
                }
                
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    def run_all_aggregations(self) -> Dict[str, Any]:
        """Run all aggregation jobs"""
        results = {
            "arsip_summary": self.aggregate_arsip_summary(),
            "daily_summary": self.aggregate_daily_summary(),
            "run_at": datetime.now().isoformat()
        }
        return results
    
    def get_summary_for_grafana(self) -> List[Dict]:
        """Get pre-aggregated summary for Grafana"""
        with get_db_context() as db:
            summaries = db.query(ArsipSummary).order_by(
                ArsipSummary.tanggal.desc()
            ).limit(1000).all()
            return [s.to_dict() for s in summaries]
    
    def get_daily_trend(self, days: int = 30) -> List[Dict]:
        """Get daily trend for Grafana charts"""
        with get_db_context() as db:
            dailies = db.query(DailySummary).order_by(
                DailySummary.tanggal.desc()
            ).limit(days).all()
            return [d.to_dict() for d in dailies]


# Global instance
aggregation_service = AggregationService()
