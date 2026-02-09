
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from app.database import get_db_context, engine
from app.models.table_models import TableDefinition

class SchemaInspector:
    """Service to inspect database schema and list candidate tables"""
    
    # System tables to exclude from candidates
    SYSTEM_TABLES = {
        'alembic_version',
        'users',
        'instansi', 
        'unit_kerja',
        # 'data_arsip', # data_arsip IS a candidate if they want to manage it dynamically, but usually it's core.
        # Let's exclude core models to be safe, or allow them if user wants "Read Only" view in dashboard?
        # User said "mksd saya itu tabel dynamic nya itu di hapus sajaaa soalnya kan disitem saa punya fitur utk buat tabel"
        # Implies they want to see THEIR custom tables.
        # Let's exclude internal metadata tables.
        'table_definitions',
        'column_definitions',
        'dynamic_data' # Just in case
    }

    def get_candidate_tables(self) -> List[str]:
        """List tables that are NOT system tables and NOT yet registered"""
        inspector = inspect(engine)
        all_tables = set(inspector.get_table_names())
        
        # Get already registered tables
        with get_db_context() as db:
            registered_tables = {t.name for t in db.query(TableDefinition).all()}
            
        # Filter
        candidates = []
        for table in all_tables:
            if table in self.SYSTEM_TABLES:
                continue
            if table in registered_tables:
                continue
            candidates.append(table)
            
        return sorted(list(candidates))

    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column details for a table"""
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        
        # Normalize types for our simple system
        # We generally support: integer, text, date
        # Map SQLAlchemy types to our string types
        
        results = []
        for col in columns:
            col_name = col['name']
            
            # Skip internal columns if we want to enforce structure?
            # Or just import everything.
            # Best to import everything as is.
            
            # Simple type mapping
            type_str = str(col['type']).lower()
            data_type = 'text'
            if 'int' in type_str:
                data_type = 'integer'
            elif 'date' in type_str or 'time' in type_str:
                data_type = 'date'
            
            # Determine logic
            is_summable = (data_type == 'integer' and col_name not in ['id', 'unit_kerja_id'])
            
            results.append({
                "name": col_name,
                "display_name": col_name.replace('_', ' ').title(),
                "data_type": data_type,
                "is_required": not col['nullable'],
                "is_summable": is_summable
            })
            
        return results

schema_inspector = SchemaInspector()
