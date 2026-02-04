"""
Service untuk mengelola definisi tabel dinamis (Versi Fisik / Physical Table)
"""
from typing import Dict, Any, List, Optional
from datetime import date
from sqlalchemy import text
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
                
                # Add FK manually to be safe about table existence order or dialect quirks
                try:
                     db.execute(text(f"ALTER TABLE {safe_name} ADD CONSTRAINT fk_{safe_name}_unit FOREIGN KEY (unit_kerja_id) REFERENCES unit_kerja(id) ON DELETE CASCADE"))
                except:
                    pass # Ignore if fails (e.g. SQLite limitations)
                
                db.commit()
                db.refresh(table)
                
                return {"status": "success", "data": table.to_dict(include_columns=True)}
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}
    
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
                
            # Count total
            try:
                count_query = f"SELECT COUNT(*) FROM ({query}) as sub"
                total = db.execute(text(count_query), params).scalar()
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

    def get_statistics(self, table_id: int) -> Dict[str, Any]:
        """Get statistics from PHYSICAL table"""
        with get_db_context() as db:
            table = db.query(TableDefinition).options(joinedload(TableDefinition.columns)).filter(TableDefinition.id == table_id).first()
            if not table:
                return None
            
            safe_name = self._sanitize_name(table.name)
            
            try:
                # 1. Total Instansi & Unit
                count_unit_sql = f"SELECT COUNT(DISTINCT unit_kerja_id) FROM {safe_name}"
                total_unit = db.execute(text(count_unit_sql)).scalar()
                
                count_inst_sql = f"""
                    SELECT COUNT(DISTINCT u.instansi_id) 
                    FROM {safe_name} t
                    JOIN unit_kerja u ON t.unit_kerja_id = u.id
                """
                total_instansi = db.execute(text(count_inst_sql)).scalar()
            except Exception:
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
                
                return {"status": "success", "data": {"total": total}}
                
            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}

    def upsert_data(self, table_id: int, unit_kerja_id: int, tanggal: date, data: Dict[str, Any]) -> Dict[str, Any]:
        """Upsert (Insert or Merge/Sum) data into PHYSICAL table"""
        with get_db_context() as db:
            try:
                table = db.query(TableDefinition).options(joinedload(TableDefinition.columns)).filter(TableDefinition.id == table_id).first()
                if not table:
                    return {"status": "error", "message": "Tabel tidak ditemukan"}
                
                safe_name = self._sanitize_name(table.name)
                
                # Check existing record
                # Query: SELECT id, [cols] FROM table WHERE unit_kerja_id = :uid AND tanggal = :tgl
                sel_cols = ["id", "total"] + [self._sanitize_name(c.name) for c in table.columns]
                sel_sql = f"SELECT {', '.join(sel_cols)} FROM {safe_name} WHERE unit_kerja_id = :uid AND tanggal = :tgl"
                
                existing = db.execute(text(sel_sql), {"uid": unit_kerja_id, "tgl": tanggal}).mappings().first()
                
                if existing:
                    # UPDATE (Merge Sums)
                    update_sets = []
                    params = {"id": existing['id'], "uid": unit_kerja_id, "tgl": tanggal} # uid/tgl not used in set but for context
                    
                    new_total = 0
                    
                    for col in table.columns:
                        safe_col = self._sanitize_name(col.name)
                        new_val = data.get(col.name, 0)
                        
                        if col.is_summable and col.data_type == 'integer':
                            # Sum with existing
                            old_val = existing.get(safe_col) or 0
                            try:
                                final_val = int(old_val) + int(new_val)
                            except:
                                final_val = int(old_val)
                            
                            update_sets.append(f"{safe_col} = :{safe_col}")
                            params[safe_col] = final_val
                            new_total += final_val
                        else:
                            # Overwrite non-summable (or keep? Upload usually overwrites)
                            val = data.get(col.name)
                            # Handle text/etc
                            update_sets.append(f"{safe_col} = :{safe_col}")
                            params[safe_col] = val
                            
                            # Note: Non-summable ints don't add to total? 
                            # Logic in existing code: total = sum of summable columns.
                            
                    update_sets.append("total = :total")
                    params["total"] = new_total
                    update_sets.append("updated_at = :now")
                    params["now"] = datetime.utcnow()
                    
                    sql = f"UPDATE {safe_name} SET {', '.join(update_sets)} WHERE id = :id"
                    db.execute(text(sql), params)
                    db.commit()
                    return {"status": "success", "action": "updated"}
                    
                else:
                    # INSERT (Reuse create logic but we are inside context)
                    # We can call create_dynamic_data but we are already in transaction? 
                    # create_dynamic_data creates its own session context.
                    # Copy logic here to be safe and efficient.
                    
                    cols = ["unit_kerja_id", "tanggal"]
                    vals = [":uid", ":tgl"]
                    params = {"uid": unit_kerja_id, "tgl": tanggal}
                    
                    total = 0
                    for col in table.columns:
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
                    db.execute(text(sql), params)
                    db.commit()
                    return {"status": "success", "action": "inserted"}

            except Exception as e:
                db.rollback()
                return {"status": "error", "message": str(e)}

table_service = TableService()
