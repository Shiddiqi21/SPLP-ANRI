
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect, MetaData, Table, Column, Integer, String, Date, Float
from app.models.table_models import TableDefinition
from app.services.schema_inspector import SchemaInspector
import logging

logger = logging.getLogger(__name__)

class GenericSummaryService:
    def __init__(self, db: Session):
        self.db = db
        self.inspector = SchemaInspector()

    def get_summary_table_name(self, table_id: int) -> str:
        # Check if this is the core data_arsip table (usually ID 1, but better check name)
        # Since we don't have name here, we might need to fetch it.
        # OPTIMIZATION: create_summary_table fetches definition anyway.
        # But check_summary_exists only calls this.
        # Let's fetch the name if possible, or just check standard IDs.
        # BETTER: For Generic Service, we should probably stick to table_{id} UNLESS it's data_arsip.
        # To avoid DB hit in get_summary_table_name, we can't check name easily.
        # But check_summary_exists is efficient.
        
        # Let's query DB for name. It's safer.
        table_def = self.db.query(TableDefinition).filter(TableDefinition.id == table_id).first()
        if table_def and table_def.name == 'data_arsip':
            return "data_arsip_monthly_summary"
            
        return f"table_{table_id}_monthly_summary"

    def create_summary_table(self, table_id: int) -> dict:
        """
        Creates a summary table for the given table_id.
        Aggregates by Year, Month, Unit Kerja.
        """
        # 1. Get Table Definition
        table_def = self.db.query(TableDefinition).filter(TableDefinition.id == table_id).first()
        if not table_def:
            return {"success": False, "message": "Table definition not found"}

        source_table_name = table_def.name
        summary_table_name = self.get_summary_table_name(table_id)

        # 2. Check source columns
        columns = self.inspector.get_table_columns(source_table_name)
        col_names = [c['name'] for c in columns]
        
        # Validation: Must have 'tanggal' and 'unit_kerja_id'
        if 'tanggal' not in col_names:
             return {"success": False, "message": "Table must have 'tanggal' column"}
        
        # Check if unit_kerja_id exists OR unit_kerja exists
        # Ideally we want unit_kerja_id for joining. 
        # But uploaded tables might not have FK? 
        # Wait, UploadService converts them to ID and stores in physical table.
        # Let's check SchemaInspector output for a known uploaded table?
        # Assuming standard schema from UploadService: unit_kerja_id IS present.
        if 'unit_kerja_id' not in col_names:
             return {"success": False, "message": "Table must have 'unit_kerja_id' column"}

        # 3. Identify Metrics (Integer/Float columns)
        # SchemaInspector normalizes types to 'integer', 'text', 'date'
        metric_cols = [c['name'] for c in columns if c['data_type'] == 'integer' and c['name'] not in ['id', 'unit_kerja_id', 'instansi_id']]
        
        if not metric_cols:
            return {"success": False, "message": "No metric columns (numbers) found to summarize"}

        # 4. Drop existing summary table
        try:
            self.db.execute(text(f"DROP TABLE IF EXISTS {summary_table_name}"))
        except Exception as e:
            logger.error(f"Error dropping summary table: {e}")

        # 5. Create Summary Table
        # We use CREATE TABLE AS SELECT ... WITH NO DATA (Limit 0) or just defining it.
        # Better: Execute CREATE TABLE directly.
        
        cols_sql = []
        sum_sql = []
        
        for col in metric_cols:
            cols_sql.append(f"`{col}` BIGINT DEFAULT 0") # Use BIGINT for sums
            sum_sql.append(f"SUM(t.`{col}`) as `{col}`")

        create_sql = f"""
        CREATE TABLE {summary_table_name} (
            `month` VARCHAR(7) NOT NULL, -- YYYY-MM
            `year` INT NOT NULL,         -- YYYY (For easier filtering)
            `unit_kerja_id` INT NOT NULL,
            {', '.join(cols_sql)},
            PRIMARY KEY (`month`, `unit_kerja_id`),
            INDEX `idx_month` (`month`),
            INDEX `idx_year` (`year`),
            INDEX `idx_unit` (`unit_kerja_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        
        try:
            self.db.execute(text(create_sql))
            logger.info(f"Created summary table {summary_table_name}")
        except Exception as e:
            return {"success": False, "message": f"Failed to create table: {e}"}

        # 6. Populate Data
        # Group by DATE_FORMAT(tanggal, '%Y-%m') and unit_kerja_id
        
        insert_sql = f"""
        INSERT INTO {summary_table_name} (`month`, `year`, `unit_kerja_id`, {', '.join(metric_cols)})
        SELECT 
            DATE_FORMAT(t.tanggal, '%Y-%m') as month,
            YEAR(t.tanggal) as year,
            t.unit_kerja_id,
            {', '.join(sum_sql)}
        FROM {source_table_name} t
        WHERE t.tanggal IS NOT NULL
        GROUP BY month, year, t.unit_kerja_id
        """
        
        try:
            self.db.execute(text(insert_sql))
            row_count = self.db.execute(text(f"SELECT COUNT(*) FROM {summary_table_name}")).scalar()
            self.db.commit()
            
            # Update TableDefinition? Add 'has_summary' flag?
            # We don't have that column yet. User can just check if table exists?
            # Or we can store it in 'description' or a new column.
            # For now, we just rely on table existence check in stats_routes.
            
            return {
                "success": True, 
                "message": f"Summary created successfully with {row_count} rows (Original: Aggregated by Month)",
                "rows": row_count
            }
        except Exception as e:
            self.db.rollback()
            return {"success": False, "message": f"Failed to populate data: {e}"}

    def check_summary_exists(self, table_id: int) -> bool:
        """Check if summary table exists"""
        table_name = self.get_summary_table_name(table_id)
        path = self.inspector.get_table_columns(table_name)
        return len(path) > 0

