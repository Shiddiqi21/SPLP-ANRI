"""
Service untuk upload dan proses file Excel/CSV dengan dukungan Dynamic Table
"""
import io
from datetime import datetime, date
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
from sqlalchemy.orm import Session, joinedload
from app.models.arsip_models import Instansi, UnitKerja 
from app.models.table_models import TableDefinition, ColumnDefinition, DynamicData
from app.services.table_service import table_service

class UploadService:
    """Service untuk handle upload file Excel/CSV"""
    
    COLUMN_ALIASES = {
        'tanggal': ['tanggal', 'date', 'tgl'],
        'unit_kerja': ['unit_kerja', 'unit kerja', 'unit', 'nama_unit', 'nama unit'],
        'instansi': ['instansi', 'nama_instansi', 'nama instansi'],
    }
    
    def __init__(self, db: Session):
        self.db = db
    
    def normalize_column_name(self, col: str) -> str:
        """Normalize column name to standard format"""
        col_lower = col.lower().strip()
        for standard, aliases in self.COLUMN_ALIASES.items():
            if col_lower in [a.lower() for a in aliases]:
                return standard
        return col_lower
    
    def parse_file(self, file_content: bytes, filename: str) -> Tuple[pd.DataFrame, str]:
        """Parse Excel or CSV file"""
        try:
            if filename.endswith('.csv'):
                # Try different encodings
                for encoding in ['utf-8', 'latin-1', 'cp1252']:
                    try:
                        df = pd.read_csv(io.BytesIO(file_content), encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    return None, "Tidak dapat membaca file CSV. Format encoding tidak didukung."
            elif filename.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(io.BytesIO(file_content))
            else:
                return None, "Format file tidak didukung. Gunakan .csv, .xlsx, atau .xls"
            
            # Normalize standard columns
            df.columns = [self.normalize_column_name(col) for col in df.columns]
            
            return df, None
        except Exception as e:
            return None, f"Error membaca file: {str(e)}"
    
    def validate_data(self, df: pd.DataFrame, table_def: Dict) -> Tuple[bool, List[str]]:
        """Validate dataframe columns and data against table definition"""
        errors = []
        columns = table_def['columns']
        
        # Check required system columns
        missing_cols = []
        for col in ['tanggal', 'instansi', 'unit_kerja']:
            if col not in df.columns:
                missing_cols.append(col)
        
        if missing_cols:
            errors.append(f"Kolom wajib tidak ditemukan: {', '.join(missing_cols)}")
        
        # Check table specific columns
        # Map display names or aliases to internal names?
        # For now assume user provides headers matching internal names OR display names
        
        # Create mapping of possible headers -> internal_name
        col_mapping = {}
        for col in columns:
            col_mapping[col['name'].lower()] = col['name']
            col_mapping[col['display_name'].lower()] = col['name']
            # Add underscore version of display name
            col_mapping[col['display_name'].lower().replace(' ', '_')] = col['name']
        
        # Check if at least one data column exists in the file
        found_data_cols = 0
        for file_col in df.columns:
            if file_col.lower() in col_mapping:
                # Rename column in DF to internal name
                df.rename(columns={file_col: col_mapping[file_col.lower()]}, inplace=True)
                found_data_cols += 1
        
        if found_data_cols == 0:
             errors.append("File tidak memiliki kolom data yang sesuai dengan template tabel ini")

        # Check for empty dataframe
        if len(df) == 0:
            errors.append("File tidak memiliki data")
        
        return len(errors) == 0, errors
    
    def get_or_create_instansi(self, nama: str) -> Instansi:
        """Get existing or create new instansi"""
        # Default to ANRI if not specified
        if not nama or pd.isna(nama):
            nama = "Arsip Nasional Republik Indonesia"
        
        instansi = self.db.query(Instansi).filter(Instansi.nama == nama).first()
        if not instansi:
            # Generate kode from nama
            kode = ''.join([word[0].upper() for word in nama.split()[:3]])
            instansi = Instansi(kode=kode, nama=nama)
            self.db.add(instansi)
            self.db.flush()
        return instansi
    
    def get_or_create_unit_kerja(self, nama: str, instansi_id: int) -> UnitKerja:
        """Get existing or create new unit kerja"""
        unit = self.db.query(UnitKerja).filter(
            UnitKerja.nama == nama,
            UnitKerja.instansi_id == instansi_id
        ).first()
        
        if not unit:
            # Generate kode
            existing_count = self.db.query(UnitKerja).filter(
                UnitKerja.instansi_id == instansi_id
            ).count()
            kode = f"UK-{existing_count + 1:03d}"
            
            unit = UnitKerja(
                instansi_id=instansi_id,
                kode=kode,
                nama=nama
            )
            self.db.add(unit)
            self.db.flush()
        return unit
    
    def parse_date(self, date_val) -> date:
        """Parse date from various formats"""
        if pd.isna(date_val):
            return None
        
        if isinstance(date_val, (datetime, date)):
             if isinstance(date_val, datetime):
                 return date_val.date()
             return date_val
        
        if isinstance(date_val, str):
            # Try various date formats
            formats = ['%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d', '%d %B %Y', '%d-%b-%Y']
            for fmt in formats:
                try:
                    return datetime.strptime(date_val, fmt).date()
                except ValueError:
                    continue
        
        # Try pandas to_datetime
        try:
            val = pd.to_datetime(date_val)
            if pd.isna(val): return None
            return val.date()
        except:
            return None
    
    def safe_value(self, val, data_type='integer'):
        """Safely convert value based on type"""
        if pd.isna(val) or val == '':
            return 0 if data_type == 'integer' else ''
        try:
            if data_type == 'integer':
                return int(float(val))
            return str(val)
        except (ValueError, TypeError):
            return 0 if data_type == 'integer' else str(val)
    
    def process_upload(self, file_content: bytes, filename: str, table_id: Optional[int] = None) -> Dict[str, Any]:
        """Process uploaded file and insert to DynamicData"""
        result = {
            "success": False,
            "message": "",
            "stats": {
                "total_rows": 0,
                "inserted": 0,
                "updated": 0,
                "skipped": 0,
                "errors": []
            }
        }
        
        # Get table definition
        if table_id:
            table_def = table_service.get_table_by_id(table_id)
        else:
            table_def = table_service.get_default_table()
            
        if not table_def:
            result["message"] = "Definisi tabel tidak ditemukan"
            return result
            
        # Get table info (Pre-fetch for cache)
        table_def_obj = self.db.query(TableDefinition).options(joinedload(TableDefinition.columns)).filter(TableDefinition.id == table_id).first()
        if not table_def_obj:
            result["message"] = "Tabel tidak ditemukan"
            return result
            
        columns = [col.to_dict() for col in table_def_obj.columns] # Use loaded columns
        
        # Parse file
        df, error = self.parse_file(file_content, filename)
        if error:
            result["message"] = error
            return result
        
        # Validate
        valid, errors = self.validate_data(df, table_def)
        if not valid:
            result["message"] = "Validasi gagal"
            result["stats"]["errors"] = errors
            return result
        
        result["stats"]["total_rows"] = len(df)
        
        # Process each row
        print(f"[Upload] Starting bulk process for {len(df)} rows...")
        for idx, row in df.iterrows():
            try:
                # Get unit kerja
                unit_nama = str(row.get('unit_kerja', '')).strip()
                if not unit_nama:
                    result["stats"]["skipped"] += 1
                    result["stats"]["errors"].append(f"Baris {idx+2}: unit_kerja kosong")
                    continue
                
                # Get instansi (required)
                instansi_nama = row.get('instansi', None)
                if not instansi_nama or pd.isna(instansi_nama) or str(instansi_nama).strip() == '':
                    result["stats"]["skipped"] += 1
                    result["stats"]["errors"].append(f"Baris {idx+2}: instansi kosong")
                    continue
                instansi = self.get_or_create_instansi(str(instansi_nama).strip())
                
                unit = self.get_or_create_unit_kerja(unit_nama, instansi.id)
                
                # Parse date
                tanggal = self.parse_date(row.get('tanggal'))
                if not tanggal:
                    result["stats"]["skipped"] += 1
                    result["stats"]["errors"].append(f"Baris {idx+2}: tanggal tidak valid")
                    continue
                
                # Build JSON data
                json_data = {}
                for col in columns:
                    name = col['name']
                    # Look for column in row (already normalized in validate)
                    if name in df.columns:
                        json_data[name] = self.safe_value(row.get(name), col['data_type'])
                    else:
                         json_data[name] = 0 if col['data_type'] == 'integer' else ''
                
                # Call TableService to Upsert (Insert/Sum) into Physical Table
                # PASS SESSION AND TABLE OBJECT FOR OPTIMIZATION
                upsert_res = table_service.upsert_data(
                    table_id=table_id,
                    unit_kerja_id=unit.id,
                    tanggal=tanggal,
                    data=json_data,
                    db=self.db,           # reuse session
                    table_obj=table_def_obj # reuse definition
                )
                
                if upsert_res["status"] == "success":
                    if upsert_res["action"] == "inserted":
                        result["stats"]["inserted"] += 1
                    else:
                        result["stats"]["updated"] += 1
                else:
                    result["stats"]["skipped"] += 1
                    result["stats"]["errors"].append(f"Baris {idx+2}: {upsert_res['message']}")
                    
            except Exception as e:
                result["stats"]["skipped"] += 1
                result["stats"]["errors"].append(f"Baris {idx+2}: {str(e)}")
        
        # Commit
        try:
            self.db.commit()
            result["success"] = True
            result["message"] = f"Upload berhasil! {result['stats']['inserted']} data baru, {result['stats']['updated']} data diupdate."
        except Exception as e:
            self.db.rollback()
            result["message"] = f"Error saat menyimpan: {str(e)}"
        
        return result
