from sqlalchemy import text, func
from datetime import date
from app.database import get_db_context
from app.models.arsip_models import DataArsip

class SummaryService:
    """Service to maintain data_arsip_monthly_summary in sync"""
    
    def update_summary(self, unit_kerja_id: int, year: int, month: int):
        """
        Recalculate summary for specific unit/year/month and upsert to summary table.
        This should be called AFTER modification of data_arsip.
        """
        table_name = "data_arsip_monthly_summary"
        
        with get_db_context() as db:
            try:
                # 1. Calculate totals from raw data
                totals = db.query(
                    func.sum(DataArsip.naskah_masuk).label('naskah_masuk'),
                    func.sum(DataArsip.naskah_keluar).label('naskah_keluar'),
                    func.sum(DataArsip.disposisi).label('disposisi'),
                    func.sum(DataArsip.berkas).label('berkas'),
                    func.sum(DataArsip.retensi_permanen).label('retensi_permanen'),
                    func.sum(DataArsip.retensi_musnah).label('retensi_musnah'),
                    func.sum(DataArsip.naskah_ditindaklanjuti).label('naskah_ditindaklanjuti'),
                    func.sum(DataArsip.total).label('total')
                ).filter(
                    DataArsip.unit_kerja_id == unit_kerja_id,
                    func.year(DataArsip.tanggal) == year,
                    func.month(DataArsip.tanggal) == month
                ).first()
                
                # If no data exists for this period/unit, we should delete the summary row (or set to 0)
                # Setting to 0 is safer for upsert logic, or delete if all 0?
                # Let's count if any rows exist
                count = db.query(DataArsip).filter(
                    DataArsip.unit_kerja_id == unit_kerja_id,
                    func.year(DataArsip.tanggal) == year,
                    func.month(DataArsip.tanggal) == month
                ).count()
                
                if count == 0:
                    # Delete summary row
                    sql_delete = f"""
                        DELETE FROM {table_name} 
                        WHERE unit_kerja_id = :uid AND year = :y AND month = :m
                    """
                    db.execute(text(sql_delete), {"uid": unit_kerja_id, "y": year, "m": month})
                else:
                    # Upsert
                    # Note: We need a UNIQUE constraint on (year, month, unit_kerja_id) for proper upsert
                    # My create_summary_table.py created indexes but didn't enforce UNIQUE constraint explicitly?
                    # But if we rely on DELETE + INSERT, it's safer without Unique Key expectation.
                    # Or check if row exists first.
                    
                    # Safer approach: Check existence
                    existing = db.execute(text(
                        f"SELECT id FROM {table_name} WHERE unit_kerja_id = :uid AND year = :y AND month = :m"
                    ), {"uid": unit_kerja_id, "y": year, "m": month}).fetchone()
                    
                    if existing:
                        sql_update = f"""
                            UPDATE {table_name} SET
                                naskah_masuk = :nm, naskah_keluar = :nk, disposisi = :d,
                                berkas = :b, retensi_permanen = :rp, retensi_musnah = :rm,
                                naskah_ditindaklanjuti = :nd, total = :t,
                                last_updated = NOW()
                            WHERE id = :id
                        """
                        params = {
                            "id": existing[0],
                            "nm": totals.naskah_masuk or 0, "nk": totals.naskah_keluar or 0, "d": totals.disposisi or 0,
                            "b": totals.berkas or 0, "rp": totals.retensi_permanen or 0, "rm": totals.retensi_musnah or 0,
                            "nd": totals.naskah_ditindaklanjuti or 0, "t": totals.total or 0
                        }
                        db.execute(text(sql_update), params)
                    else:
                        sql_insert = f"""
                            INSERT INTO {table_name}
                            (year, month, unit_kerja_id, naskah_masuk, naskah_keluar, disposisi, berkas, retensi_permanen, retensi_musnah, naskah_ditindaklanjuti, total)
                            VALUES (:y, :m, :uid, :nm, :nk, :d, :b, :rp, :rm, :nd, :t)
                        """
                        params = {
                            "y": year, "m": month, "uid": unit_kerja_id,
                            "nm": totals.naskah_masuk or 0, "nk": totals.naskah_keluar or 0, "d": totals.disposisi or 0,
                            "b": totals.berkas or 0, "rp": totals.retensi_permanen or 0, "rm": totals.retensi_musnah or 0,
                            "nd": totals.naskah_ditindaklanjuti or 0, "t": totals.total or 0
                        }
                        db.execute(text(sql_insert), params)
                
                db.commit()
                # print(f"[SummaryService] Synced {year}-{month} for Unit {unit_kerja_id}")
                
            except Exception as e:
                print(f"[SummaryService] Error syncing: {e}")
                db.rollback()

summary_service = SummaryService()
