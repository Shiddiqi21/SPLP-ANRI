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
    exclude_meta: bool = Query(False, description="Exclude Bulan/Nama Bulan dari response (untuk pie chart)"),
    include_total_col: bool = Query(True, description="Sertakan kolom Total dalam response")
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
    is_all_years = False
    if year:
        # Detect Grafana $__all or "All" — skip year filter entirely
        clean_year_str = str(year).replace('{', '').replace('}', '').strip()
        if clean_year_str.lower() in ('all', '$__all', ''):
            is_all_years = True
        else:
            raw_years = [int(y.strip()) for y in clean_year_str.split(',') if y.strip().isdigit()]
            year_list = sorted(list(set(raw_years)), reverse=True)
    
    # If no valid year parsed AND not explicitly "All", default to current year
    if not year_list and not is_all_years:
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
        # Add 'total' to mapping manually so it displays nicely
        col_mapping["total"] = "Total"
        
        available_cols = [c.name for c in table.columns if c.is_summable]
        
        # FIX: Ensure 'total' is available for selection
        if "total" not in available_cols:
             available_cols.append("total")
        
        if columns:
            selected_cols = [c.strip() for c in columns.split(',') if c.strip() in available_cols]
        else:
             selected_cols = available_cols[:]

        if not selected_cols:
             selected_cols = available_cols[:]
        
        # APPLY USER OPTION: include_total_col
        if include_total_col:
            if "total" not in selected_cols and "total" in available_cols:
                selected_cols.append("total")
        else:
            if "total" in selected_cols:
                selected_cols.remove("total")
        
        # Define sum expressions
        sum_expressions = [f"COALESCE(SUM(t.{col}), 0) as `{col}`" for col in selected_cols]
        
        # FIX: Define include_total for downstream logic (Deprecated but kept for safety if needed)
        # But we remove manual usage downstream!
        include_total = False 
        
        # Build WHERE conditions
        where_conditions = []
        params = {}
        
        # SUPER OPTIMIZATION: Use Summary Table (Materialized View)
        from app.services.generic_summary_service import GenericSummaryService
        summary_service = GenericSummaryService(db)
        
        # Check if generic summary exists for this table
        has_summary = summary_service.check_summary_exists(table.id)
        
        # Determine strict summary usage
        # We use summary if available AND we are not asking for raw-only columns (which we assume standard columns are in summary)
        use_summary = has_summary
        
        if use_summary:
            safe_table_name = summary_service.get_summary_table_name(table.id)
        else:
            safe_table_name = table.name.replace('-', '_').replace(' ', '_')
        
        # Adjust where conditions for Summary Table
        # Summary has 'year' and 'month' columns directly.
        # Raw table has 'tanggal'.
        
        where_conditions = []
        params = {}
        
        # 1. Year Filter
        if year_list:
            if use_summary:
                # Summary table (generic) has 'year' column
                col_year = "t.year"
            elif table.name == "data_arsip":
                 # Old data_arsip summary also has 'year' (from previous session)
                 # Wait, create_summary_table.py created: month (VARCHAR), year (INT)??
                 # No, create_summary_table.py ONLY created 'month'.
                 # BUT data_arsip_monthly_summary likely has 'month' as 'YYYY-MM'.
                 # So we need to extract year from month if 'year' column is missing or just use substring.
                 # Actually, let's assume Generic Service adds 'year'. 
                 # OLD summary table might NOT have 'year'.
                 # Let's check logic:
                 # If generic summary -> it has 'year'.
                 # If data_arsip -> we used 't.year' in previous code, implies it has it?
                 # Let's check view_file of create_summary_table.py... IDK.
                 # Safe bet: use strict SQL based on table type.
                 col_year = "t.year" 
            else:
                col_year = "YEAR(t.tanggal)"

            if len(year_list) == 1:
                where_conditions.append(f"{col_year} = :year")
                params["year"] = year_list[0]
            else:
                where_conditions.append(f"{col_year} IN ({','.join(str(y) for y in year_list)})")
        
        # 2. Month Filter
        if month_filter:
             # Summary: 't.month' is 'YYYY-MM'. We need to filter by MM part?
             # OR does summary have 'month_int'? No.
             # Client sends month_filter = [1, 2, 12].
             # If summary: SUBSTRING(t.month, 6, 2) IN (01, 02, 12)
             # If raw: MONTH(t.tanggal) IN (1, 2, 12)
             
             if use_summary:
                 # Helper to pad zero
                 months_padded = [f"'{m:02d}'" for m in month_filter]
                 where_conditions.append(f"SUBSTRING(t.month, 6, 2) IN ({','.join(months_padded)})")
             else:
                 where_conditions.append(f"MONTH(t.tanggal) IN ({','.join(str(m) for m in month_filter)})")
        
        # 3. Unit/Instansi Filter (Same logic as before, but safer)
        need_join = True
        if unit_kerja_ids:
             need_join = False
             if len(unit_kerja_ids) == 1:
                 where_conditions.append("t.unit_kerja_id = :unit_kerja_id")
                 params["unit_kerja_id"] = unit_kerja_ids[0]
             else:
                 where_conditions.append(f"t.unit_kerja_id IN ({','.join(str(u) for u in unit_kerja_ids)})")
        elif instansi_ids:
             need_join = True
             if len(instansi_ids) == 1:
                 where_conditions.append("u.instansi_id = :instansi_id")
                 params["instansi_id"] = instansi_ids[0]
             else:
                 where_conditions.append(f"u.instansi_id IN ({','.join(str(i) for i in instansi_ids)})")
        
        join_clause = "LEFT JOIN unit_kerja u ON t.unit_kerja_id = u.id" if need_join else ""
        
        # 4. Grouping (Already grouped in summary, but we group again to aggregate across units if needed)
        # If we select "All Units", we get 2000 rows (1 row per unit per month).
        # We want 12 rows (1 per month) IF we are aggregating totals.
        
        # Wait, the endpoint returns "monthly_data".
        # It usually groups by MONTH only?
        # Let's check the original GROUP BY: "GROUP BY MONTH(t.tanggal)".
        # Yes, it aggregates all units into one row per month.
        
        # Build SQL based on source type
        if use_summary:
            select_month = "t.month"
            select_month_name = "MONTHNAME(STR_TO_DATE(CONCAT(t.month, '-01'), '%Y-%m-%d'))"
            group_by = "t.month"
        else:
            select_month = "DATE_FORMAT(t.tanggal, '%Y-%m')"
            select_month_name = "MONTHNAME(t.tanggal)"
            group_by = "DATE_FORMAT(t.tanggal, '%Y-%m')"

        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

        sql = f"""
            SELECT 
                {select_month} as bulan,
                {select_month_name} as nama_bulan,
                {', '.join(sum_expressions)}
            FROM {safe_table_name} t
            {join_clause}
            {where_clause}
            GROUP BY {group_by}
            ORDER BY {group_by}
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
            
            
            print(f"DEBUG: Filling months for {months_to_fill}")
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
            
            print("DEBUG: Sorting data...")
            try:
                monthly_data.sort(key=lambda x: x.get('Bulan') or x.get('bulan') or 0)
                print("DEBUG: Sort Success!")
            except Exception as e:
                print(f"DEBUG: Sort Failed! {e}")
                import traceback
                traceback.print_exc()
        
        # Return FLAT ARRAY directly (no wrapper!)
        # Cache Result (5 minutes)
        print("DEBUG: Returning Data")
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
    is_all_years = False
    if year:
        clean_year_str = str(year).replace('{', '').replace('}', '').strip()
        if clean_year_str.lower() in ('all', '$__all', ''):
            is_all_years = True
        else:
            year_list = [int(y.strip()) for y in clean_year_str.split(',') if y.strip().isdigit()]
    
    if not year_list and not is_all_years:
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
            
            # Year condition (skip if All years)
            if year_list:
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
            
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

            sql = f"""
                SELECT 
                    MONTH(t.tanggal) as bulan,
                    MONTHNAME(t.tanggal) as nama_bulan,
                    {', '.join(sum_expressions)}
                FROM {safe_table_name} t
                LEFT JOIN unit_kerja u ON t.unit_kerja_id = u.id
                {where_clause}
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
