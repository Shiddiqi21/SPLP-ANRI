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
from app.services.cache_service import cache

router = APIRouter(prefix="/api/stats", tags=["Statistics - Grafana"])


# ============================================
# GRAFANA-OPTIMIZED ENDPOINTS (Flat Array Response)
# ============================================

@router.get("/grafana/monthly")
def get_grafana_monthly(
    table_id: int = Query(1, description="ID tabel"),
    year: Optional[str] = Query(None, description="Tahun data (single atau multi, pisah koma)"),
    columns: Optional[str] = Query(None, description="Kolom untuk diagregasi (pisah koma)"),
    months: Optional[str] = Query(None, description="Filter bulan (pisah koma, contoh: 1,2,3 untuk Jan-Mar)"),
    instansi_id: Optional[str] = Query(None, description="Filter berdasarkan instansi ID"),
    unit_kerja_id: Optional[str] = Query(None, description="Filter berdasarkan unit kerja ID"),
    use_display_name: bool = Query(True, description="Gunakan nama tampilan (display name) dari sistem"),
    exclude_meta: bool = Query(False, description="Exclude Bulan/Nama Bulan dari response (untuk pie chart)")
):
    """
    [GRAFANA OPTIMIZED] Statistik bulanan - Response langsung array tanpa wrapper.
    Lebih mudah diparsing oleh Grafana Infinity plugin.
    
    Parameter:
    - use_display_name=true akan menggunakan nama yang terlihat di sistem.
    - months=1,2,3 akan filter hanya bulan Januari, Februari, Maret
    - instansi_id=1 akan filter berdasarkan instansi
    - unit_kerja_id=1 akan filter berdasarkan unit kerja
    - year=2024,2025 filter tahun (support multi)
    """
    # 1. Try Cache
    cache_key = f"grafana:monthly:{table_id}:{year}:{columns}:{months}:{instansi_id}:{unit_kerja_id}:{use_display_name}:{exclude_meta}"
    cached_data = cache.get(cache_key)
    if cached_data:
        return cached_data

    # Parse year (handle $__all and multi-value)
    year_list = []
    if year:
        clean_years = str(year).replace('{', '').replace('}', '')
        year_list = [int(y.strip()) for y in clean_years.split(',') if y.strip().isdigit()]
    
    if not year_list:
        year_list = [datetime.now().year]
    
    # Parse instansi_id (handle $__all and multi-value with Grafana glob {1,2})
    instansi_ids = []
    if instansi_id:
        # cleanup input: remove { } and whitespace
        clean_instansi = instansi_id.replace('{', '').replace('}', '')
        instansi_ids = [int(x.strip()) for x in clean_instansi.split(',') if x.strip().isdigit()]

    # Parse unit_kerja_id (handle $__all and multi-value with Grafana glob {1,2})
    unit_kerja_ids = []
    if unit_kerja_id:
        clean_uk = unit_kerja_id.replace('{', '').replace('}', '')
        unit_kerja_ids = [int(x.strip()) for x in clean_uk.split(',') if x.strip().isdigit()]
    
    # Parse months filter
    month_filter = None
    if months:
        clean_months = months.replace('{', '').replace('}', '')
        month_filter = [int(m.strip()) for m in clean_months.split(',') if m.strip().isdigit()]
    
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
        
        # Build WHERE conditions
        where_conditions = []
        params = {}
        
        # Year condition
        if len(year_list) == 1:
            where_conditions.append("YEAR(t.tanggal) = :year")
            params["year"] = year_list[0]
        else:
            where_conditions.append(f"YEAR(t.tanggal) IN ({','.join(str(y) for y in year_list)})")
        
        if month_filter:
            where_conditions.append(f"MONTH(t.tanggal) IN ({','.join(str(m) for m in month_filter)})")
        
        if instansi_ids:
            if len(instansi_ids) == 1:
                where_conditions.append("u.instansi_id = :instansi_id")
                params["instansi_id"] = instansi_ids[0]
            else:
                where_conditions.append(f"u.instansi_id IN ({','.join(str(i) for i in instansi_ids)})")
                
        if unit_kerja_ids:
            if len(unit_kerja_ids) == 1:
                where_conditions.append("t.unit_kerja_id = :unit_kerja_id")
                params["unit_kerja_id"] = unit_kerja_ids[0]
            else:
                where_conditions.append(f"t.unit_kerja_id IN ({','.join(str(u) for u in unit_kerja_ids)})")
        
        sql = f"""
            SELECT 
                MONTH(t.tanggal) as bulan,
                MONTHNAME(t.tanggal) as nama_bulan,
                {', '.join(sum_expressions)}
            FROM {safe_table_name} t
            LEFT JOIN unit_kerja u ON t.unit_kerja_id = u.id
            WHERE {' AND '.join(where_conditions)}
            GROUP BY MONTH(t.tanggal), MONTHNAME(t.tanggal)
            ORDER BY bulan
        """
        
        try:
            result = db.execute(text(sql), params).mappings().all()
        except Exception as e:
            return []
        
        monthly_data = []
        for row in result:
            row_dict = dict(row)
            if use_display_name:
                # Convert column names to display names
                new_row = {}
                if not exclude_meta:
                    new_row['Bulan'] = row_dict['bulan']
                    new_row['Nama Bulan'] = row_dict['nama_bulan']
                if include_total:
                    new_row['Total'] = row_dict.get('total', 0)
                for col in selected_cols:
                    display = col_mapping.get(col, col)
                    new_row[display] = row_dict.get(col, 0)
                monthly_data.append(new_row)
            else:
                if exclude_meta:
                    row_dict.pop('bulan', None)
                    row_dict.pop('nama_bulan', None)
                monthly_data.append(row_dict)
        
        if not exclude_meta:
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
        # Cache Result (5 minutes)
        cache.set(cache_key, monthly_data, ttl=300)
        return monthly_data


@router.get("/grafana/combined")
def get_grafana_combined(
    table_ids: str = Query("1", description="ID tabel (pisah koma untuk multiple, contoh: 1,2,3)"),
    year: Optional[str] = Query(None, description="Tahun data (single atau multi)"),
    instansi_id: Optional[str] = Query(None, description="Filter berdasarkan instansi ID"),
    unit_kerja_id: Optional[str] = Query(None, description="Filter berdasarkan unit kerja ID"),
    months: Optional[str] = Query(None, description="Filter bulan (pisah koma, contoh: 1,2,3)"),
    use_display_name: bool = Query(True, description="Gunakan nama tampilan dari sistem")
):
    """
    [GRAFANA OPTIMIZED] Gabungkan data dari beberapa tabel sekaligus.
    
    Contoh: /api/stats/grafana/combined?table_ids=1,2,3&year=2025
    
    Response berupa array dengan kolom dari semua tabel yang dipilih.
    Mendukung filter instansi_id, unit_kerja_id, dan months.
    """
    # 1. Try Cache
    cache_key = f"grafana:combined:{table_ids}:{year}:{instansi_id}:{unit_kerja_id}:{months}:{use_display_name}"
    cached_data = cache.get(cache_key)
    if cached_data:
        return cached_data

    # Parse year (handle $__all and multi-value)
    year_list = []
    if year:
        clean_years = str(year).replace('{', '').replace('}', '')
        year_list = [int(y.strip()) for y in clean_years.split(',') if y.strip().isdigit()]
    
    if not year_list:
        year_list = [datetime.now().year]
    
    # Parse instansi_id (handle $__all and multi-value with Grafana glob {1,2})
    instansi_ids = []
    if instansi_id:
        clean_instansi = instansi_id.replace('{', '').replace('}', '')
        instansi_ids = [int(x.strip()) for x in clean_instansi.split(',') if x.strip().isdigit()]

    # Parse unit_kerja_id (handle $__all and multi-value with Grafana glob {1,2})
    unit_kerja_ids = []
    if unit_kerja_id:
        clean_uk = unit_kerja_id.replace('{', '').replace('}', '')
        unit_kerja_ids = [int(x.strip()) for x in clean_uk.split(',') if x.strip().isdigit()]
    
    # Parse months filter
    month_filter = None
    if months:
        clean_months = months.replace('{', '').replace('}', '')
        month_filter = [int(m.strip()) for m in clean_months.split(',') if m.strip().isdigit()]
    
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
            
            # Build WHERE conditions
            where_conditions = []
            params = {}
            
            # Year condition
            if len(year_list) == 1:
                where_conditions.append("YEAR(t.tanggal) = :year")
                params["year"] = year_list[0]
            else:
                where_conditions.append(f"YEAR(t.tanggal) IN ({','.join(str(y) for y in year_list)})")
            
            if month_filter:
                where_conditions.append(f"MONTH(t.tanggal) IN ({','.join(str(m) for m in month_filter)})")
            
            if instansi_ids:
                if len(instansi_ids) == 1:
                    where_conditions.append("u.instansi_id = :instansi_id")
                    params["instansi_id"] = instansi_ids[0]
                else:
                    where_conditions.append(f"u.instansi_id IN ({','.join(str(i) for i in instansi_ids)})")
            
            if unit_kerja_ids:
                if len(unit_kerja_ids) == 1:
                    where_conditions.append("t.unit_kerja_id = :unit_kerja_id")
                    params["unit_kerja_id"] = unit_kerja_ids[0]
                else:
                    where_conditions.append(f"t.unit_kerja_id IN ({','.join(str(u) for u in unit_kerja_ids)})")
            
            sql = f"""
                SELECT 
                    MONTH(t.tanggal) as bulan,
                    MONTHNAME(t.tanggal) as nama_bulan,
                    {', '.join(sum_expressions)}
                FROM {safe_table_name} t
                LEFT JOIN unit_kerja u ON t.unit_kerja_id = u.id
                WHERE {' AND '.join(where_conditions)}
                GROUP BY MONTH(t.tanggal), MONTHNAME(t.tanggal)
            """
            
            try:
                result = db.execute(text(sql), params).mappings().all()
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
                # Ensure all columns exist for existing months
                for col in all_columns:
                    if col not in combined_data[m]:
                        combined_data[m][col] = 0
        
        combined_list = list(combined_data.values())
        combined_list.sort(key=lambda x: x.get('Bulan') or x.get('bulan'))
        
        # Cache Result (5 minutes)
        cache.set(cache_key, combined_list, ttl=300)
        return combined_list


@router.get("/grafana/var/tahun")
def get_grafana_var_tahun():
    """
    [GRAFANA VARIABLE] Daftar tahun untuk variable dropdown.
    Mengambil tahun yang tersedia dari database.
    """
    years = set()
    try:
        with get_db_context() as db:
            tables = db.query(TableDefinition).all()
            for t in tables:
                safe_name = t.name.replace("-", "_").replace(" ", "_")
                try:
                    # Get distinct years from each table
                    sql = f"SELECT DISTINCT YEAR(tanggal) FROM {safe_name} WHERE tanggal IS NOT NULL"
                    result = db.execute(text(sql)).fetchall()
                    for r in result:
                        if r[0]:
                            years.add(int(r[0]))
                except Exception:
                    continue
    except Exception as e:
        print(f"Error fetching years: {e}")
        # Fallback if DB query fails
        current_year = datetime.now().year
        years = set(range(current_year - 2, current_year + 2))
    
    sorted_years = sorted(list(years), reverse=True)
    return [{"text": str(y), "value": str(y), "__text": str(y), "__value": str(y)} for y in sorted_years]




@router.get("/grafana/yearly")
def get_grafana_yearly(
    table_id: int = Query(1, description="ID tabel"),
    years: str = Query("2024,2025", description="Tahun (pisah koma)")
):
    """
    [GRAFANA OPTIMIZED] Perbandingan tahunan - Response langsung array.
    """
    # 1. Try Cache
    cache_key = f"grafana:yearly:{table_id}:{years}"
    cached_data = cache.get(cache_key)
    if cached_data:
        return cached_data

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
            final_data = [dict(row) for row in result]
            cache.set(cache_key, final_data, ttl=300)
            return final_data
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


@router.get("/unit-kerja")
def get_available_unit_kerja(
    instansi_id: Optional[int] = Query(None, description="Filter berdasarkan instansi ID (untuk chained variable)")
):
    """
    Ambil daftar unit kerja yang tersedia.
    Berguna untuk dropdown variable di Grafana.
    Mendukung chained variable: pilih instansi -> unit kerja ikut berubah.
    """
    with get_db_context() as db:
        if instansi_id:
            result = db.execute(
                text("SELECT id, kode, nama, instansi_id FROM unit_kerja WHERE instansi_id = :instansi_id ORDER BY nama"),
                {"instansi_id": instansi_id}
            ).mappings().all()
        else:
            result = db.execute(
                text("SELECT u.id, u.kode, u.nama, u.instansi_id, i.nama as instansi_nama FROM unit_kerja u LEFT JOIN instansi i ON u.instansi_id = i.id ORDER BY i.nama, u.nama")
            ).mappings().all()
        
        return {
            "unit_kerja": [dict(row) for row in result]
        }


@router.get("/months")
def get_available_months():
    """
    Daftar 12 bulan untuk variable dropdown di Grafana.
    """
    return {
        "months": [
            {"id": 1, "name": "Januari"},
            {"id": 2, "name": "Februari"},
            {"id": 3, "name": "Maret"},
            {"id": 4, "name": "April"},
            {"id": 5, "name": "Mei"},
            {"id": 6, "name": "Juni"},
            {"id": 7, "name": "Juli"},
            {"id": 8, "name": "Agustus"},
            {"id": 9, "name": "September"},
            {"id": 10, "name": "Oktober"},
            {"id": 11, "name": "November"},
            {"id": 12, "name": "Desember"}
        ]
    }


# ============================================
# GRAFANA VARIABLE ENDPOINTS (flat __text/__value format)
# ============================================

@router.get("/grafana/var/instansi")
def get_grafana_var_instansi():
    """
    [GRAFANA VARIABLE] Universal Format.
    Returns keys for both standard (text/value) and legacy (__text/__value).
    """
    # DEBUG LOGGING
    try:
        with open("d:/splp-integrator/debug_vars.log", "a") as f:
            f.write(f"[{datetime.now()}] Instansi Request\n")
    except Exception as e:
        print(f"Log Error: {e}")

    with get_db_context() as db:
        result = db.execute(text("SELECT id, nama FROM instansi ORDER BY nama")).mappings().all()
        return [{
            "text": row["nama"], 
            "value": str(row["id"]),
            "__text": row["nama"],
            "__value": str(row["id"])
        } for row in result]


@router.get("/grafana/var/unit-kerja")
def get_grafana_var_unit_kerja(
    instansi_id: Optional[str] = Query(None, description="Filter instansi (support $__all)")
):
    """
    [GRAFANA VARIABLE] Universal Format.
    """
    # DEBUG LOGGING
    try:
        with open("d:/splp-integrator/debug_vars.log", "a") as f:
            f.write(f"[{datetime.now()}] UnitKerja Request - instansi_id: '{instansi_id}'\n")
    except Exception as e:
        print(f"Log Error: {e}")

    instansi_ids = []
    if instansi_id:
        clean_ids = instansi_id.replace('{', '').replace('}', '')
        if clean_ids.lower() != 'all' and '$__all' not in clean_ids:
             instansi_ids = [int(x.strip()) for x in clean_ids.split(',') if x.strip().isdigit()]
    
    with get_db_context() as db:
        if instansi_ids:
            if len(instansi_ids) == 1:
                result = db.execute(
                    text("SELECT id, nama FROM unit_kerja WHERE instansi_id = :iid ORDER BY nama"),
                    {"iid": instansi_ids[0]}
                ).mappings().all()
            else:
                result = db.execute(
                    text(f"SELECT id, nama FROM unit_kerja WHERE instansi_id IN ({','.join(str(i) for i in instansi_ids)}) ORDER BY nama")
                ).mappings().all()
        else:
            result = db.execute(
                text("SELECT u.id, u.nama, i.nama as instansi_nama FROM unit_kerja u LEFT JOIN instansi i ON u.instansi_id = i.id ORDER BY i.nama, u.nama")
            ).mappings().all()
        
        return [
            {
                "text": f"{row['nama']} ({row.get('instansi_nama', '')})" if row.get('instansi_nama') else row['nama'],
                "value": str(row["id"]),
                "__text": f"{row['nama']} ({row.get('instansi_nama', '')})" if row.get('instansi_nama') else row['nama'],
                "__value": str(row["id"])
            }
            for row in result
        ]


@router.get("/grafana/var/bulan")
def get_grafana_var_bulan():
    """
    [GRAFANA VARIABLE] Universal Format.
    """
    return [
        {"text": "Januari", "value": "1", "__text": "Januari", "__value": "1"},
        {"text": "Februari", "value": "2", "__text": "Februari", "__value": "2"},
        {"text": "Maret", "value": "3", "__text": "Maret", "__value": "3"},
        {"text": "April", "value": "4", "__text": "April", "__value": "4"},
        {"text": "Mei", "value": "5", "__text": "Mei", "__value": "5"},
        {"text": "Juni", "value": "6", "__text": "Juni", "__value": "6"},
        {"text": "Juli", "value": "7", "__text": "Juli", "__value": "7"},
        {"text": "Agustus", "value": "8", "__text": "Agustus", "__value": "8"},
        {"text": "September", "value": "9", "__text": "September", "__value": "9"},
        {"text": "Oktober", "value": "10", "__text": "Oktober", "__value": "10"},
        {"text": "November", "value": "11", "__text": "November", "__value": "11"},
        {"text": "Desember", "value": "12", "__text": "Desember", "__value": "12"}
    ]


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
    # 1. Try Cache
    cache_key = f"grafana:yearly:{table_id}:{years}"
    cached_data = cache.get(cache_key)
    if cached_data:
        return cached_data

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
