"""
API Routes untuk Statistik - Grafana Integration
Endpoint khusus untuk integrasi dengan Grafana JSON Datasource
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional, List
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import joinedload

from app.database import get_db_context
from app.models.table_models import TableDefinition

router = APIRouter(prefix="/api/stats", tags=["Statistics - Grafana"])


# ============================================
# GRAFANA-OPTIMIZED ENDPOINTS (Flat Array Response)
# ============================================

@router.get("/grafana/monthly")
def get_grafana_monthly(
    table_id: int = Query(1, description="ID tabel"),
    year: int = Query(default=None, description="Tahun data"),
    columns: Optional[str] = Query(None, description="Kolom untuk diagregasi (pisah koma)"),
    months: Optional[str] = Query(None, description="Filter bulan (pisah koma, contoh: 1,2,3 untuk Jan-Mar)"),
    use_display_name: bool = Query(True, description="Gunakan nama tampilan (display name) dari sistem")
):
    """
    [GRAFANA OPTIMIZED] Statistik bulanan - Response langsung array tanpa wrapper.
    Lebih mudah diparsing oleh Grafana Infinity plugin.
    
    Parameter:
    - use_display_name=true akan menggunakan nama yang terlihat di sistem.
    - months=1,2,3 akan filter hanya bulan Januari, Februari, Maret
    """
    if year is None:
        year = datetime.now().year
    
    # Parse months filter
    month_filter = None
    if months:
        month_filter = [int(m.strip()) for m in months.split(',') if m.strip().isdigit()]
    
    with get_db_context() as db:
        table = db.query(TableDefinition).options(
            joinedload(TableDefinition.columns)
        ).filter(TableDefinition.id == table_id).first()
        
        if not table:
            return []
        
        safe_table_name = table.name.replace('-', '_').replace(' ', '_')
        
        # Build column mapping: name -> display_name
        col_mapping = {c.name: c.display_name for c in table.columns}
        available_cols = [c.name for c in table.columns if c.is_summable]
        
        if columns:
            selected_cols = [c.strip() for c in columns.split(',') if c.strip() in available_cols]
        else:
            selected_cols = available_cols
        
        if not selected_cols:
            selected_cols = available_cols
        
        sum_expressions = [f"COALESCE(SUM(t.{col}), 0) as `{col}`" for col in selected_cols]
        # Only add total if no specific columns selected (all columns mode)
        include_total = not columns  # columns is None means all columns
        if include_total:
            sum_expressions.append("COALESCE(SUM(t.total), 0) as total")
        
        sql = f"""
            SELECT 
                MONTH(t.tanggal) as bulan,
                MONTHNAME(t.tanggal) as nama_bulan,
                {', '.join(sum_expressions)}
            FROM {safe_table_name} t
            JOIN unit_kerja u ON t.unit_kerja_id = u.id
            WHERE YEAR(t.tanggal) = :year
            {'AND MONTH(t.tanggal) IN (' + ','.join(str(m) for m in month_filter) + ')' if month_filter else ''}
            GROUP BY MONTH(t.tanggal), MONTHNAME(t.tanggal)
            ORDER BY bulan
        """
        
        try:
            result = db.execute(text(sql), {"year": year}).mappings().all()
        except Exception as e:
            return []
        
        monthly_data = []
        for row in result:
            row_dict = dict(row)
            if use_display_name:
                # Convert column names to display names
                new_row = {
                    'Bulan': row_dict['bulan'],
                    'Nama Bulan': row_dict['nama_bulan']
                }
                if include_total:
                    new_row['Total'] = row_dict.get('total', 0)
                for col in selected_cols:
                    display = col_mapping.get(col, col)
                    new_row[display] = row_dict.get(col, 0)
                monthly_data.append(new_row)
            else:
                monthly_data.append(row_dict)
        
        # Fill missing months (only for filtered months or all if no filter)
        existing_months = {d.get('Bulan') or d.get('bulan') for d in monthly_data}
        month_names = ['', 'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
                       'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember']
        
        # Determine which months to fill (filtered or all)
        months_to_fill = month_filter if month_filter else list(range(1, 13))
        
        for m in months_to_fill:
            if m not in existing_months:
                if use_display_name:
                    zero_row = {'Bulan': m, 'Nama Bulan': month_names[m]}
                    if include_total:
                        zero_row['Total'] = 0
                    for col in selected_cols:
                        display = col_mapping.get(col, col)
                        zero_row[display] = 0
                else:
                    zero_row = {'bulan': m, 'nama_bulan': month_names[m]}
                    if include_total:
                        zero_row['total'] = 0
                    for col in selected_cols:
                        zero_row[col] = 0
                monthly_data.append(zero_row)
        
        monthly_data.sort(key=lambda x: x.get('Bulan') or x.get('bulan'))
        
        # Return FLAT ARRAY directly (no wrapper!)
        return monthly_data


@router.get("/grafana/combined")
def get_grafana_combined(
    table_ids: str = Query("1", description="ID tabel (pisah koma untuk multiple, contoh: 1,2,3)"),
    year: int = Query(default=None, description="Tahun data"),
    use_display_name: bool = Query(True, description="Gunakan nama tampilan dari sistem")
):
    """
    [GRAFANA OPTIMIZED] Gabungkan data dari beberapa tabel sekaligus.
    
    Contoh: /api/stats/grafana/combined?table_ids=1,2,3&year=2025
    
    Response berupa array dengan kolom dari semua tabel yang dipilih.
    """
    if year is None:
        year = datetime.now().year
    
    table_id_list = [int(t.strip()) for t in table_ids.split(',')]
    
    with get_db_context() as db:
        combined_data = {}  # bulan -> {data}
        all_columns = []
        
        for table_id in table_id_list:
            table = db.query(TableDefinition).options(
                joinedload(TableDefinition.columns)
            ).filter(TableDefinition.id == table_id).first()
            
            if not table:
                continue
            
            safe_table_name = table.name.replace('-', '_').replace(' ', '_')
            table_display = table.display_name if use_display_name else table.name
            
            # Build column mapping
            col_mapping = {c.name: c.display_name for c in table.columns}
            summable_cols = [c.name for c in table.columns if c.is_summable]
            
            sum_expressions = [f"COALESCE(SUM(t.{col}), 0) as `{col}`" for col in summable_cols]
            
            sql = f"""
                SELECT 
                    MONTH(t.tanggal) as bulan,
                    MONTHNAME(t.tanggal) as nama_bulan,
                    {', '.join(sum_expressions)}
                FROM {safe_table_name} t
                JOIN unit_kerja u ON t.unit_kerja_id = u.id
                WHERE YEAR(t.tanggal) = :year
                GROUP BY MONTH(t.tanggal), MONTHNAME(t.tanggal)
            """
            
            try:
                result = db.execute(text(sql), {"year": year}).mappings().all()
            except Exception:
                continue
            
            for row in result:
                row_dict = dict(row)
                bulan = row_dict['bulan']
                
                if bulan not in combined_data:
                    if use_display_name:
                        combined_data[bulan] = {'Bulan': bulan, 'Nama Bulan': row_dict['nama_bulan']}
                    else:
                        combined_data[bulan] = {'bulan': bulan, 'nama_bulan': row_dict['nama_bulan']}
                
                # Add columns with table prefix
                for col in summable_cols:
                    if use_display_name:
                        col_label = f"{table_display} - {col_mapping.get(col, col)}"
                    else:
                        col_label = f"{table.name}_{col}"
                    
                    combined_data[bulan][col_label] = row_dict.get(col, 0)
                    
                    if col_label not in all_columns:
                        all_columns.append(col_label)
        
        # Fill missing months
        month_names = ['', 'Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni',
                       'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember']
        
        for m in range(1, 13):
            if m not in combined_data:
                if use_display_name:
                    combined_data[m] = {'Bulan': m, 'Nama Bulan': month_names[m]}
                else:
                    combined_data[m] = {'bulan': m, 'nama_bulan': month_names[m]}
                
                for col in all_columns:
                    combined_data[m][col] = 0
            else:
                # Fill missing columns with 0
                for col in all_columns:
                    if col not in combined_data[m]:
                        combined_data[m][col] = 0
        
        # Convert to sorted list
        result_list = list(combined_data.values())
        result_list.sort(key=lambda x: x.get('Bulan') or x.get('bulan'))
        
        return result_list



@router.get("/grafana/yearly")
def get_grafana_yearly(
    table_id: int = Query(1, description="ID tabel"),
    years: str = Query("2024,2025", description="Tahun (pisah koma)")
):
    """
    [GRAFANA OPTIMIZED] Perbandingan tahunan - Response langsung array.
    """
    year_list = [int(y.strip()) for y in years.split(',')]
    
    with get_db_context() as db:
        table = db.query(TableDefinition).options(
            joinedload(TableDefinition.columns)
        ).filter(TableDefinition.id == table_id).first()
        
        if not table:
            return []
        
        safe_table_name = table.name.replace('-', '_').replace(' ', '_')
        available_cols = [c.name for c in table.columns if c.is_summable]
        
        sum_expressions = [f"COALESCE(SUM({col}), 0) as {col}" for col in available_cols]
        sum_expressions.append("COALESCE(SUM(total), 0) as total")
        
        sql = f"""
            SELECT 
                YEAR(tanggal) as tahun,
                {', '.join(sum_expressions)}
            FROM {safe_table_name}
            WHERE YEAR(tanggal) IN :years
            GROUP BY YEAR(tanggal)
            ORDER BY tahun
        """
        
        try:
            result = db.execute(text(sql), {"years": tuple(year_list)}).mappings().all()
            return [dict(row) for row in result]
        except Exception:
            return []


# ============================================
# ORIGINAL ENDPOINTS (with wrapper for compatibility)
# ============================================


@router.get("/monthly")
def get_monthly_stats(
    table_id: int = Query(1, description="ID tabel yang ingin diambil datanya"),
    year: int = Query(default=None, description="Tahun data (default: tahun ini)"),
    columns: Optional[str] = Query(None, description="Kolom yang ingin diagregasi, pisahkan dengan koma. Kosongkan untuk semua kolom."),
    instansi_id: Optional[int] = Query(None, description="Filter berdasarkan instansi ID")
):
    """
    Ambil statistik bulanan (12 bulan) untuk integrasi Grafana.
    
    Response format kompatibel dengan Grafana JSON Datasource plugin.
    
    Example:
    - /api/stats/monthly?table_id=1&year=2025
    - /api/stats/monthly?table_id=1&year=2025&columns=naskah_masuk,naskah_keluar
    - /api/stats/monthly?table_id=1&year=2025&instansi_id=1
    """
    if year is None:
        year = datetime.now().year
    
    with get_db_context() as db:
        # 1. Get table definition
        table = db.query(TableDefinition).options(
            joinedload(TableDefinition.columns)
        ).filter(TableDefinition.id == table_id).first()
        
        if not table:
            raise HTTPException(status_code=404, detail="Tabel tidak ditemukan")
        
        # Sanitize table name
        safe_table_name = table.name.replace('-', '_').replace(' ', '_')
        
        # 2. Determine columns to aggregate
        available_cols = [c.name for c in table.columns if c.is_summable]
        
        if columns:
            requested_cols = [c.strip() for c in columns.split(',')]
            # Validate requested columns exist
            selected_cols = [c for c in requested_cols if c in available_cols]
            if not selected_cols:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Kolom tidak valid. Kolom tersedia: {', '.join(available_cols)}"
                )
        else:
            selected_cols = available_cols
        
        # 3. Build SQL query
        sum_expressions = [f"COALESCE(SUM(t.{col}), 0) as {col}" for col in selected_cols]
        sum_expressions.append("COALESCE(SUM(t.total), 0) as total")
        
        sql = f"""
            SELECT 
                MONTH(t.tanggal) as bulan,
                MONTHNAME(t.tanggal) as nama_bulan,
                {', '.join(sum_expressions)}
            FROM {safe_table_name} t
            JOIN unit_kerja u ON t.unit_kerja_id = u.id
            WHERE YEAR(t.tanggal) = :year
        """
        
        params = {"year": year}
        
        if instansi_id:
            sql += " AND u.instansi_id = :instansi_id"
            params["instansi_id"] = instansi_id
        
        sql += """
            GROUP BY MONTH(t.tanggal), MONTHNAME(t.tanggal)
            ORDER BY bulan
        """
        
        try:
            result = db.execute(text(sql), params).mappings().all()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
        
        # 4. Format response for Grafana
        monthly_data = []
        for row in result:
            row_dict = dict(row)
            monthly_data.append(row_dict)
        
        # Fill missing months with zeros
        existing_months = {d['bulan'] for d in monthly_data}
        month_names = ['', 'January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December']
        
        for m in range(1, 13):
            if m not in existing_months:
                zero_row = {'bulan': m, 'nama_bulan': month_names[m], 'total': 0}
                for col in selected_cols:
                    zero_row[col] = 0
                monthly_data.append(zero_row)
        
        monthly_data.sort(key=lambda x: x['bulan'])
        
        return {
            "table_id": table_id,
            "table_name": table.display_name,
            "year": year,
            "columns": selected_cols,
            "data": monthly_data
        }


@router.get("/columns")
def get_available_columns(
    table_id: int = Query(1, description="ID tabel")
):
    """
    Ambil daftar kolom yang tersedia untuk tabel tertentu.
    Berguna untuk dropdown filter di Grafana.
    """
    with get_db_context() as db:
        table = db.query(TableDefinition).options(
            joinedload(TableDefinition.columns)
        ).filter(TableDefinition.id == table_id).first()
        
        if not table:
            raise HTTPException(status_code=404, detail="Tabel tidak ditemukan")
        
        columns = [
            {
                "name": c.name,
                "display_name": c.display_name,
                "data_type": c.data_type,
                "is_summable": c.is_summable
            }
            for c in table.columns
        ]
        
        return {
            "table_id": table_id,
            "table_name": table.display_name,
            "columns": columns
        }


@router.get("/tables")
def get_available_tables():
    """
    Ambil daftar tabel yang tersedia.
    Berguna untuk dropdown filter di Grafana.
    """
    with get_db_context() as db:
        tables = db.query(TableDefinition).all()
        
        return {
            "tables": [
                {
                    "id": t.id,
                    "name": t.name,
                    "display_name": t.display_name,
                    "is_default": t.is_default
                }
                for t in tables
            ]
        }


@router.get("/instansi")
def get_available_instansi():
    """
    Ambil daftar instansi yang tersedia.
    Berguna untuk dropdown filter di Grafana.
    """
    with get_db_context() as db:
        result = db.execute(text("SELECT id, kode, nama FROM instansi ORDER BY nama")).mappings().all()
        
        return {
            "instansi": [dict(row) for row in result]
        }


@router.get("/yearly")
def get_yearly_comparison(
    table_id: int = Query(1, description="ID tabel"),
    years: str = Query(..., description="Tahun yang ingin dibandingkan, pisahkan dengan koma (contoh: 2024,2025)"),
    columns: Optional[str] = Query(None, description="Kolom yang ingin diagregasi")
):
    """
    Ambil perbandingan statistik tahunan.
    Berguna untuk grafik perbandingan year-over-year di Grafana.
    """
    year_list = [int(y.strip()) for y in years.split(',')]
    
    with get_db_context() as db:
        table = db.query(TableDefinition).options(
            joinedload(TableDefinition.columns)
        ).filter(TableDefinition.id == table_id).first()
        
        if not table:
            raise HTTPException(status_code=404, detail="Tabel tidak ditemukan")
        
        safe_table_name = table.name.replace('-', '_').replace(' ', '_')
        available_cols = [c.name for c in table.columns if c.is_summable]
        
        if columns:
            selected_cols = [c.strip() for c in columns.split(',') if c.strip() in available_cols]
        else:
            selected_cols = available_cols
        
        sum_expressions = [f"COALESCE(SUM({col}), 0) as {col}" for col in selected_cols]
        sum_expressions.append("COALESCE(SUM(total), 0) as total")
        
        sql = f"""
            SELECT 
                YEAR(tanggal) as tahun,
                {', '.join(sum_expressions)}
            FROM {safe_table_name}
            WHERE YEAR(tanggal) IN :years
            GROUP BY YEAR(tanggal)
            ORDER BY tahun
        """
        
        result = db.execute(text(sql), {"years": tuple(year_list)}).mappings().all()
        
        return {
            "table_id": table_id,
            "table_name": table.display_name,
            "years": year_list,
            "columns": selected_cols,
            "data": [dict(row) for row in result]
        }
