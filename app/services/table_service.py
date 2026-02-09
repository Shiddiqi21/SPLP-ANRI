"""
Service untuk mengelola definisi tabel dinamis (Versi Fisik / Physical Table)
"""
from sqlalchemy import text
from typing import Dict, Any, List, Optional
from datetime import date, datetime
from app.database import get_db

from app.services.cache_service import cache, cached
from sqlalchemy.orm import joinedload
from app.database import get_db_context
from app.models.table_models import TableDefinition, ColumnDefinition
# DynamicData model is deprecated for storage in this physical mode

class TableService:
    """Service untuk operasi CRUD pada definisi tabel (Physical Table Mode)"""
    
    def _sanitize_name(self, name: str) -> str:
        """Sanitize table/column name to ensure safety (alphanumeric only)"""
        # Allow only lowercase letters, numbers, and underscores
        return "".join(c for c in name if c.isalnum() or c == '_').lower()

    def get_all_tables(self) -> List[Dict]:
        """Get all table definitions"""
        with get_db_context() as db:
            tables = db.query(TableDefinition).order_by(TableDefinition.id).all()
            return [t.to_dict(include_columns=True) for t in tables]
    
    def get_table_by_id(self, table_id: int) -> Optional[Dict]:
        """Get table definition by ID"""
        with get_db_context() as db:
            table = db.query(TableDefinition).options(
                joinedload(TableDefinition.columns)
            ).filter(TableDefinition.id == table_id).first()
            
            if table:
                return table.to_dict(include_columns=True)
            return None
            
    def get_default_table(self) -> Optional[Dict]:
        """Get the default table definition"""
        with get_db_context() as db:
            table = db.query(TableDefinition).options(
                joinedload(TableDefinition.columns)
            ).filter(TableDefinition.is_default == True).first()
            
            if table:
                return table.to_dict(include_columns=True)
            
            # If no default set, return the first one
            table = db.query(TableDefinition).options(
                joinedload(TableDefinition.columns)
            ).first()
            if table:
                return table.to_dict(include_columns=True)
            return None
    
    
    def register_existing_table(self, name: str, display_name: str, description: str = None, columns: List[Dict] = []) -> Dict[str, Any]:
        """Register EXISTING physical table (metadata only, NO DDL)"""
        # Name is already safe because it comes from SchemaInspector which reads actual DB tables
        
        with get_db_context() as db:
            try:
                # 1. Metadata: Check duplicate
                existing = db.query(TableDefinition).filter(TableDefinition.name == name).first()
                if existing:
                    return {"status": "error", "message": f"Tabel '{name}' sudah terdaftar"}
                
                # Metadata: Create record
                count = db.query(TableDefinition).count()
                is_default = (count == 0)
                
                table = TableDefinition(
                    name=name,
                    display_name=display_name,
                    description=description,
                    is_default=is_default
                )
                db.add(table)
                db.flush() 
                
                # Metadata: Add columns
                for i, col in enumerate(columns):
                    column = ColumnDefinition(
                        table_id=table.id,
                        name=col['name'],
                        display_name=col['display_name'],
                        data_type=col.get('data_type', 'integer'),
                        is_required=col.get('is_required', False),
                        is_summable=col.get('is_summable', True),
                        order=i
                    )
                    db.add(column)
                
                db.commit()
                db.refresh(table)
                return {"status": "success", "data": table.to_dict(include_columns=True)}
                
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": f"Gagal mendaftarkan tabel: {str(e)}"}

    def create_table(self, name: str, display_name: str, description: str = None, columns: List[Dict] = []) -> Dict[str, Any]:
        """Create new PHYSICAL table and metadata"""
        safe_name = self._sanitize_name(name)
        if not safe_name:
            return {"status": "error", "message": "Nama tabel tidak valid (hanya huruf dan angka)"}

        with get_db_context() as db:
            try:
                # 1. Metadata: Check duplicate
                existing = db.query(TableDefinition).filter(TableDefinition.name == safe_name).first()
                if existing:
                    return {"status": "error", "message": f"Tabel dengan nama internal '{safe_name}' sudah ada"}
                
                # Metadata: Create record
                count = db.query(TableDefinition).count()
                is_default = (count == 0)
                
                table = TableDefinition(
                    name=safe_name,
                    display_name=display_name,
                    description=description,
                    is_default=is_default
                )
                db.add(table)
                db.flush() 
                
                # Metadata: Add columns
                column_defs = []
                for i, col in enumerate(columns):
                    column = ColumnDefinition(
                        table_id=table.id,
                        name=col['name'],
                        display_name=col['display_name'],
                        data_type=col.get('data_type', 'integer'),
                        is_required=col.get('is_required', False),
                        is_summable=col.get('is_summable', True),
                        order=i
                    )
                    db.add(column)
                    column_defs.append(column)
                
                # 2. Physical: Create Table DDL
                # Core Columns: id, unit_kerja_id, tanggal, total, + custom columns
                ddl = f"""
                CREATE TABLE {safe_name} (
                    id SERIAL PRIMARY KEY,
                    unit_kerja_id INTEGER NOT NULL,
                    tanggal DATE NOT NULL,
                    total INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                """
                # Note: Foreign Key to unit_kerja added? Best to add it for integrity
                # But need to ensure unit_kerja table exists and casing matches. Assuming postgres/standard sql.
                
                for col in column_defs:
                    col_safe = self._sanitize_name(col.name)
                    # Simple type mapping
                    sql_type = "INTEGER DEFAULT 0" if col.data_type == "integer" else "TEXT"
                    ddl += f", {col_safe} {sql_type}"
                
                ddl += ");"
                
                # Add Foreign Key constraint separately or inline? Inline is cleaner but dialect specific.
                # Let's add simple FK if possible, but keep it robust.
                # db.execute(text(ddl)) -> will execute whatever string.
                
                db.execute(text(ddl))
                
                # Add indexes for performance
                try:
                    db.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{safe_name}_unit_kerja_id ON {safe_name} (unit_kerja_id)"))
                    db.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{safe_name}_tanggal ON {safe_name} (tanggal)"))
                    db.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{safe_name}_unit_tanggal ON {safe_name} (unit_kerja_id, tanggal)"))
                except Exception as e:
                    print(f"Warning: Failed to create indexes: {e}")

                # Add FK manually to be safe about table existence order or dialect quirks
                try:
                     db.execute(text(f"ALTER TABLE {safe_name} ADD CONSTRAINT fk_{safe_name}_unit FOREIGN KEY (unit_kerja_id) REFERENCES unit_kerja(id) ON DELETE CASCADE"))
                except:
                    pass # Ignore if fails (e.g. SQLite limitations)
                
                db.commit()
                
                # Fetch fresh object with columns for return
                try:
                    db.refresh(table)
                    # Force load columns while session is active
                    _ = table.columns
                    return_data = table.to_dict(include_columns=True)
                except Exception as e:
                    print(f"Warning: Failed to refresh table data after create: {e}")
                    return_data = {"id": table.id, "name": safe_name, "display_name": display_name}

                return {"status": "success", "data": return_data}
                
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": f"Terjadi kesalahan teknis: {str(e)}"}
    
    def update_table(self, table_id: int, display_name: str = None, description: str = None, is_default: bool = None) -> Dict[str, Any]:
        """Update table definition (Metadata only)"""
        with get_db_context() as db:
            try:
                table = db.query(TableDefinition).filter(TableDefinition.id == table_id).first()
                if not table:
                    return {"status": "error", "message": "Tabel tidak ditemukan"}
                
                if display_name:
                    table.display_name = display_name
                if description is not None:
                    table.description = description
                
                if is_default and not table.is_default:
                    # Unset other default
                    db.query(TableDefinition).update({TableDefinition.is_default: False})
                    table.is_default = True
                
                db.commit()
                db.refresh(table)
                return {"status": "success", "data": table.to_dict()}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}

    def delete_table(self, table_id: int) -> Dict[str, Any]:
        """Delete definition and PHYSICAL table"""
        with get_db_context() as db:
            try:
                table = db.query(TableDefinition).filter(TableDefinition.id == table_id).first()
                if not table:
                    return {"status": "error", "message": "Tabel tidak ditemukan"}
                
                # Drop Physical Table
                safe_name = self._sanitize_name(table.name)
                db.execute(text(f"DROP TABLE IF EXISTS {safe_name}"))
                
                # Delete Metadata
                db.delete(table)
                db.commit()
                return {"status": "success", "message": f"Tabel {table.display_name} berhasil dihapus permanen"}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
    def get_dynamic_data(self, table_id: int, instansi_id: int = None, unit_kerja_id: int = None, 
                        tanggal_start=None, tanggal_end=None, limit=50, offset=0) -> Dict[str, Any]:
        """Get data from PHYSICAL table"""
        with get_db_context() as db:
            table = db.query(TableDefinition).options(joinedload(TableDefinition.columns)).filter(TableDefinition.id == table_id).first()
            if not table:
                return {"data": [], "total": 0}
            
            safe_name = self._sanitize_name(table.name)
            
            # Check if table exists (to avoid error queries on old metadata that has no physical table)
            # For now assume it exists or catch error
            
            # Build Query
            query = f"""
                SELECT t.*, u.nama as unit_nama, i.nama as instansi_nama 
                FROM {safe_name} t
                JOIN unit_kerja u ON t.unit_kerja_id = u.id
                JOIN instansi i ON u.instansi_id = i.id
                WHERE 1=1
            """
            params = {}
            
            if unit_kerja_id:
                query += " AND t.unit_kerja_id = :unit_id"
                params['unit_id'] = unit_kerja_id
            
            if instansi_id:
                query += " AND u.instansi_id = :instansi_id"
                params['instansi_id'] = instansi_id
                
            if tanggal_start:
                query += " AND t.tanggal >= :t_start"
                params['t_start'] = tanggal_start
            
            if tanggal_end:
                query += " AND t.tanggal <= :t_end"
                params['t_end'] = tanggal_end
                
            # Count total (Optimized)
            try:
                # Use cache if no filters are active
                cache_key = f"total_count_{table_id}"
                total = None
                has_filters = any([instansi_id, unit_kerja_id, tanggal_start, tanggal_end])
                
                if not has_filters:
                    total = cache.get(cache_key)
                
                if total is None:
                    # Build count query separately to avoid unnecessary joins
                    count_query = f"SELECT COUNT(*) FROM {safe_name} t"
                    count_params = {}
                    count_where = ["1=1"]
                    
                    # Only join if filtering by instansi (need u.instansi_id)
                    if instansi_id:
                         count_query += " JOIN unit_kerja u ON t.unit_kerja_id = u.id"
                         count_where.append("u.instansi_id = :instansi_id")
                         count_params['instansi_id'] = instansi_id
                    
                    if unit_kerja_id:
                        count_where.append("t.unit_kerja_id = :unit_id")
                        count_params['unit_id'] = unit_kerja_id

                    if tanggal_start:
                        count_where.append("t.tanggal >= :t_start")
                        count_params['t_start'] = tanggal_start
                    
                    if tanggal_end:
                        count_where.append("t.tanggal <= :t_end")
                        count_params['t_end'] = tanggal_end
                    
                    count_query += " WHERE " + " AND ".join(count_where)
                    
                    total = db.execute(text(count_query), count_params).scalar()
                    
                    # Cache the total if no filters (TTL 5 mins)
                    if not has_filters:
                        cache.set(cache_key, total, ttl=300)
                        
            except Exception:
                # Likely table lookup failed
                return {"data": [], "total": 0}
            
            # Fetch data using cursor mappings
            query += " ORDER BY t.tanggal DESC LIMIT :limit OFFSET :offset"
            params['limit'] = limit
            params['offset'] = offset
            
            rows = db.execute(text(query), params).mappings().all()
            
            formatted_data = []
            for row in rows:
                row_dict = dict(row)
                
                # Dynamic columns data
                custom_data = {}
                for col in table.columns:
                    col_safe = self._sanitize_name(col.name)
                    # Handle case where column might not exist in physical table yet (migration gap)
                    custom_data[col.name] = row_dict.get(col_safe)
                
                formatted_data.append({
                    "id": row_dict['id'],
                    "unit_kerja_id": row_dict['unit_kerja_id'],
                    "tanggal": row_dict['tanggal'].isoformat() if row_dict.get('tanggal') else None,
                    "total": row_dict.get('total', 0),
                    "unit_kerja": {
                        "id": row_dict['unit_kerja_id'],
                        "nama": row_dict.get('unit_nama'),
                        "instansi": {"nama": row_dict.get('instansi_nama')}
                    },
                    "data": custom_data
                })
            
            return {
                "data": formatted_data,
                "total": total,
                "limit": limit,
                "offset": offset
            }

    @cached(prefix="stats_table", ttl=300)
    def get_statistics(self, table_id: int) -> Dict[str, Any]:
        """Get statistics from PHYSICAL table"""
        with get_db_context() as db:
            table = db.query(TableDefinition).options(joinedload(TableDefinition.columns)).filter(TableDefinition.id == table_id).first()
            if not table:
                return None
            
            safe_name = self._sanitize_name(table.name)
            
            try:
                # 1. Total Instansi & Unit (Query Master Data directly for speed)
                # Previously queried transaction table with DISTINCT which is slow on 2M+ rows.
                # Since the dashboard cards link to the full list of Master Data, these counts should match the Master Data count.
                
                count_unit_sql = "SELECT COUNT(*) FROM unit_kerja"
                total_unit = db.execute(text(count_unit_sql)).scalar()
                
                count_inst_sql = "SELECT COUNT(*) FROM instansi"
                total_instansi = db.execute(text(count_inst_sql)).scalar()
                
                total_instansi = db.execute(text(count_inst_sql)).scalar()
                
            except Exception as e:
                import traceback
                print(f"Error getting stats: {e}")
                traceback.print_exc()
                return {
                    "total_instansi": 0, "total_unit_kerja": 0, "grand_total": 0, "column_stats": {}
                }
            
            # 2. Column Sums
            summable_cols = [c for c in table.columns if c.is_summable]
            
            col_stats = {}
            grand_total = 0
            
            if summable_cols:
                sums = [f"SUM({self._sanitize_name(c.name)}) as {self._sanitize_name(c.name)}" for c in summable_cols]
                # Also sum total column
                sums.append("SUM(total) as grand_total")
                
                agg_sql = f"SELECT {', '.join(sums)} FROM {safe_name}"
                try:
                    res = db.execute(text(agg_sql)).mappings().first()
                    if res:
                        grand_total = res.get('grand_total') or 0
                        for c in summable_cols:
                            safe_c_name = self._sanitize_name(c.name)
                            col_stats[c.name] = res.get(safe_c_name) or 0
                except Exception:
                    pass
            
            return {
                "total_instansi": total_instansi,
                "total_unit_kerja": total_unit,
                "grand_total": grand_total,
                "column_stats": col_stats
            }

    def create_dynamic_data(self, table_id: int, unit_kerja_id: int, tanggal: date, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert data into PHYSICAL table"""
        with get_db_context() as db:
            try:
                table = db.query(TableDefinition).options(joinedload(TableDefinition.columns)).filter(TableDefinition.id == table_id).first()
                if not table:
                    return {"status": "error", "message": "Tabel tidak ditemukan"}
                
                safe_name = self._sanitize_name(table.name)
                
                # Prepare Insert
                cols = ["unit_kerja_id", "tanggal"]
                vals = [":unit_kerja_id", ":tanggal"]
                params = {
                    "unit_kerja_id": unit_kerja_id,
                    "tanggal": tanggal
                }
                
                total = 0
                
                for col in table.columns:
                    safe_col_name = self._sanitize_name(col.name)
                    if col.name in data:
                        cols.append(safe_col_name)
                        vals.append(f":{safe_col_name}")
                        params[safe_col_name] = data[col.name]
                        
                        if col.is_summable:
                            try: 
                                total += int(data[col.name])
                            except: 
                                pass
                
                # Add total column
                cols.append("total")
                vals.append(":total")
                params["total"] = total
                
                # SQL syntax: INSERT ... VALUES ... RETURNING id
                # For compatibility with some DBs that don't support RETURNING, we might need separate SELECT, 
                # but FastAPI usually runs on Postgres/SQLAlchemy capable DBs. Assuming Postgres usage or standard SQL.
                
                sql = f"INSERT INTO {safe_name} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
                
                # Check dialect if possible, but execute returns cursor
                # For PostgreSQL: RETURNING id
                # For SQLite: cursor.lastrowid (handled by driver usually)
                
                # Let's try generic approach: Execute then commit. 
                # If we need ID, we might need dialect specific returning.
                # Assuming Postgres from context? Or SQLite dev?
                # User is on Windows, might be SQLite or Postgres in Docker.
                # Safer to add RETURNING id only if we suspect Postgres, or try-catch.
                # Let's assume standard SQL insert for now.
                
                db.execute(text(sql), params)
                db.commit()

                # Invalidate cache
                cache.invalidate_prefix(f"stats_table")
                cache.delete(f"total_count_{table_id}")
                cache.delete("dashboard_stats")
                
                return {"status": "success", "data": {"total": total}}
                
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}

    def upsert_data(self, table_id: int, unit_kerja_id: int, tanggal: date, data: Dict[str, Any], db=None, table_obj=None) -> Dict[str, Any]:
        """
        Upsert (Insert or Merge/Sum) data into PHYSICAL table
        :param db: Optional SQLAlchemy Session. If provided, function will NOT commit.
        :param table_obj: Optional TableDefinition object. If provided, skips looking it up.
        """
        # Helper to run logic inside a session
        def _process(session, tbl):
            try:
                if not tbl:
                    tbl = session.query(TableDefinition).options(joinedload(TableDefinition.columns)).filter(TableDefinition.id == table_id).first()
                
                if not tbl:
                    return {"status": "error", "message": "Tabel tidak ditemukan"}
                
                safe_name = self._sanitize_name(tbl.name)
                
                # Check existing record
                sel_cols = ["id", "total"] + [self._sanitize_name(c.name) for c in tbl.columns]
                sel_sql = f"SELECT {', '.join(sel_cols)} FROM {safe_name} WHERE unit_kerja_id = :uid AND tanggal = :tgl"
                
                existing = session.execute(text(sel_sql), {"uid": unit_kerja_id, "tgl": tanggal}).mappings().first()
                
                if existing:
                    # UPDATE (Merge Sums)
                    update_sets = []
                    params = {"id": existing['id'], "uid": unit_kerja_id, "tgl": tanggal} 
                    
                    new_total = 0
                    
                    for col in tbl.columns:
                        safe_col = self._sanitize_name(col.name)
                        new_val = data.get(col.name, 0)
                        
                        if col.is_summable and col.data_type == 'integer':
                            old_val = existing.get(safe_col) or 0
                            try:
                                final_val = int(old_val) + int(new_val)
                            except:
                                final_val = int(old_val)
                            
                            update_sets.append(f"{safe_col} = :{safe_col}")
                            params[safe_col] = final_val
                            new_total += final_val
                        else:
                            val = data.get(col.name)
                            update_sets.append(f"{safe_col} = :{safe_col}")
                            params[safe_col] = val
                            
                    update_sets.append("total = :total")
                    params["total"] = new_total
                    update_sets.append("updated_at = :now")
                    params["now"] = datetime.utcnow()
                    
                    sql = f"UPDATE {safe_name} SET {', '.join(update_sets)} WHERE id = :id"
                    session.execute(text(sql), params)
                    if not db: session.commit() # Only commit if we own the session
                    # Invalidate cache
                    cache.invalidate_prefix(f"stats_table")
                    cache.delete(f"total_count_{table_id}")
                    cache.delete("dashboard_stats")
                    return {"status": "success", "action": "updated"}
                    
                else:
                    # INSERT
                    cols = ["unit_kerja_id", "tanggal"]
                    vals = [":uid", ":tgl"]
                    params = {"uid": unit_kerja_id, "tgl": tanggal}
                    
                    total = 0
                    for col in tbl.columns:
                        safe_col = self._sanitize_name(col.name)
                        if col.name in data:
                            cols.append(safe_col)
                            vals.append(f":{safe_col}")
                            params[safe_col] = data[col.name]
                            
                            if col.is_summable:
                                try: total += int(data[col.name])
                                except: pass
                    
                    cols.append("total")
                    vals.append(":total")
                    params["total"] = total
                    
                    sql = f"INSERT INTO {safe_name} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
                    session.execute(text(sql), params)
                    if not db: session.commit()
                    
                    # Invalidate cache
                    cache.invalidate_prefix(f"stats_table")
                    cache.delete(f"total_count_{table_id}")
                    cache.delete("dashboard_stats")
                    
                    return {"status": "success", "action": "inserted"}

            except Exception as e:
                # Only rollback if we own the transaction, otherwise let caller handle
                if not db: session.rollback()
                # But if we don't rollback here, the error bubbles up? 
                # Retain original behavior: return error dict
                return {"status": "error", "message": str(e)}

        # Main logic
        if db:
            return _process(db, table_obj)
        else:
            with get_db_context() as session:
                return _process(session, table_obj)


    def update_dynamic_data(self, table_id: int, row_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update specific row in PHYSICAL table"""
        with get_db_context() as db:
            try:
                table = db.query(TableDefinition).options(joinedload(TableDefinition.columns)).filter(TableDefinition.id == table_id).first()
                if not table:
                    return {"status": "error", "message": "Tabel tidak ditemukan"}
                
                safe_name = self._sanitize_name(table.name)
                
                # Check existing record
                check_sql = f"SELECT id FROM {safe_name} WHERE id = :id"
                existing = db.execute(text(check_sql), {"id": row_id}).first()
                if not existing:
                    return {"status": "error", "message": "Data tidak ditemukan"}
                
                # Prepare Update
                update_sets = []
                params = {"id": row_id}
                new_total = 0
                
                # Fetch current data for summable calc
                sel_cols = [self._sanitize_name(c.name) for c in table.columns]
                # Handle empty columns case
                if not sel_cols:
                    pass 
                else: 
                     sel_sql = f"SELECT {', '.join(sel_cols)} FROM {safe_name} WHERE id = :id"
                     current_data = dict(db.execute(text(sel_sql), {"id": row_id}).mappings().first())
                
                for col in table.columns:
                    safe_col = self._sanitize_name(col.name)
                    # Use new value if provided, else old value
                    val = data.get(col.name)
                    
                    if val is not None: # Only update if provided
                        params[safe_col] = val
                        update_sets.append(f"{safe_col} = :{safe_col}")
                        
                        if col.is_summable and col.data_type == 'integer':
                            try: new_total += int(val)
                            except: pass
                    else:
                        # Keep old value for total calc
                        if col.is_summable and col.data_type == 'integer':
                            try: new_total += int(current_data.get(safe_col) or 0)
                            except: pass

                # Always update updated_at if possible
                update_sets.append("updated_at = :now")
                params["now"] = datetime.utcnow()
                
                if update_sets:
                    # Update total if we have summable columns
                    # Wait, if we use partially new total, we need to be careful.
                    # Simpler approach: Recalculate total from ALL columns (old properties + new properties)
                    
                    final_total = 0
                    for col in table.columns:
                        safe_col = self._sanitize_name(col.name)
                        val = data.get(col.name, current_data.get(safe_col))
                        if col.is_summable and col.data_type == 'integer':
                            try: final_total += int(val or 0)
                            except: pass
                    
                    update_sets.append("total = :total")
                    params["total"] = final_total
                    
                    sql = f"UPDATE {safe_name} SET {', '.join(update_sets)} WHERE id = :id"
                    db.execute(text(sql), params)
                    db.commit()
                
                # Invalidate cache
                cache.invalidate_prefix(f"stats_table")
                cache.delete(f"total_count_{table_id}")
                cache.delete("dashboard_stats")
                
                return {"status": "success", "message": "Data berhasil diupdate"}
                
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}

    def delete_dynamic_data(self, table_id: int, row_id: int) -> Dict[str, Any]:
        """Delete specific row from PHYSICAL table"""
        with get_db_context() as db:
            try:
                table = db.query(TableDefinition).filter(TableDefinition.id == table_id).first()
                if not table:
                    return {"status": "error", "message": "Tabel tidak ditemukan"}
                
                safe_name = self._sanitize_name(table.name)
                
                sql = f"DELETE FROM {safe_name} WHERE id = :id"
                result = db.execute(text(sql), {"id": row_id})
                db.commit()
                
                # Invalidate cache
                cache.invalidate_prefix(f"stats_table")
                cache.delete(f"total_count_{table_id}")
                cache.delete("dashboard_stats")
                
                return {"status": "success", "message": "Data berhasil dihapus"}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}

table_service = TableService()
