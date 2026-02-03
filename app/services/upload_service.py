"""
Service untuk upload dan proses file Excel/CSV
"""
import io
from datetime import datetime
from typing import List, Dict, Any, Tuple
import pandas as pd
from sqlalchemy.orm import Session
from app.models.arsip_models import Instansi, UnitKerja, DataArsip


class UploadService:
    """Service untuk handle upload file Excel/CSV"""
    
    REQUIRED_COLUMNS = [
        'tanggal', 'unit_kerja', 'naskah_masuk', 'naskah_keluar', 
        'disposisi', 'berkas', 'retensi_permanen', 'retensi_musnah', 
        'naskah_ditindaklanjuti'
    ]
    
    COLUMN_ALIASES = {
        'tanggal': ['tanggal', 'date', 'tgl'],
        'unit_kerja': ['unit_kerja', 'unit kerja', 'unit', 'nama_unit', 'nama unit'],
        'instansi': ['instansi', 'nama_instansi', 'nama instansi'],
        'naskah_masuk': ['naskah_masuk', 'naskah masuk', 'masuk', 'incoming'],
        'naskah_keluar': ['naskah_keluar', 'naskah keluar', 'keluar', 'outgoing'],
        'disposisi': ['disposisi', 'disposition'],
        'berkas': ['berkas', 'files', 'file'],
        'retensi_permanen': ['retensi_permanen', 'retensi permanen', 'permanen', 'permanent'],
        'retensi_musnah': ['retensi_musnah', 'retensi musnah', 'musnah', 'destroy'],
        'naskah_ditindaklanjuti': ['naskah_ditindaklanjuti', 'naskah ditindaklanjuti', 'ditindaklanjuti', 'followed_up'],
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
            
            # Normalize column names
            df.columns = [self.normalize_column_name(col) for col in df.columns]
            
            return df, None
        except Exception as e:
            return None, f"Error membaca file: {str(e)}"
    
    def validate_data(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate dataframe columns and data"""
        errors = []
        
        # Check required columns
        missing_cols = []
        for col in ['tanggal', 'unit_kerja']:
            if col not in df.columns:
                missing_cols.append(col)
        
        if missing_cols:
            errors.append(f"Kolom wajib tidak ditemukan: {', '.join(missing_cols)}")
        
        # Check if at least one data column exists
        data_cols = ['naskah_masuk', 'naskah_keluar', 'disposisi', 'berkas', 
                     'retensi_permanen', 'retensi_musnah', 'naskah_ditindaklanjuti']
        has_data_col = any(col in df.columns for col in data_cols)
        if not has_data_col:
            errors.append("Minimal satu kolom data harus ada (naskah_masuk, naskah_keluar, dll)")
        
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
    
    def parse_date(self, date_val) -> datetime.date:
        """Parse date from various formats"""
        if pd.isna(date_val):
            return None
        
        if isinstance(date_val, datetime):
            return date_val.date()
        
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
            return pd.to_datetime(date_val).date()
        except:
            return None
    
    def safe_int(self, val) -> int:
        """Safely convert value to int"""
        if pd.isna(val):
            return 0
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return 0
    
    def process_upload(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        """Process uploaded file and insert data"""
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
        
        # Parse file
        df, error = self.parse_file(file_content, filename)
        if error:
            result["message"] = error
            return result
        
        # Validate
        valid, errors = self.validate_data(df)
        if not valid:
            result["message"] = "Validasi gagal"
            result["stats"]["errors"] = errors
            return result
        
        result["stats"]["total_rows"] = len(df)
        
        # Get default instansi (ANRI)
        default_instansi = self.get_or_create_instansi("Arsip Nasional Republik Indonesia")
        
        # Process each row
        for idx, row in df.iterrows():
            try:
                # Get unit kerja
                unit_nama = str(row.get('unit_kerja', '')).strip()
                if not unit_nama:
                    result["stats"]["skipped"] += 1
                    result["stats"]["errors"].append(f"Baris {idx+2}: unit_kerja kosong")
                    continue
                
                # Get instansi (use default if not specified)
                instansi_nama = row.get('instansi', None)
                if instansi_nama and not pd.isna(instansi_nama):
                    instansi = self.get_or_create_instansi(str(instansi_nama).strip())
                else:
                    instansi = default_instansi
                
                unit = self.get_or_create_unit_kerja(unit_nama, instansi.id)
                
                # Parse date
                tanggal = self.parse_date(row.get('tanggal'))
                if not tanggal:
                    result["stats"]["skipped"] += 1
                    result["stats"]["errors"].append(f"Baris {idx+2}: tanggal tidak valid")
                    continue
                
                # Check existing data
                existing = self.db.query(DataArsip).filter(
                    DataArsip.unit_kerja_id == unit.id,
                    DataArsip.tanggal == tanggal
                ).first()
                
                data_values = {
                    "naskah_masuk": self.safe_int(row.get('naskah_masuk', 0)),
                    "naskah_keluar": self.safe_int(row.get('naskah_keluar', 0)),
                    "disposisi": self.safe_int(row.get('disposisi', 0)),
                    "berkas": self.safe_int(row.get('berkas', 0)),
                    "retensi_permanen": self.safe_int(row.get('retensi_permanen', 0)),
                    "retensi_musnah": self.safe_int(row.get('retensi_musnah', 0)),
                    "naskah_ditindaklanjuti": self.safe_int(row.get('naskah_ditindaklanjuti', 0)),
                }
                
                if existing:
                    # Update existing
                    for key, val in data_values.items():
                        setattr(existing, key, val)
                    existing.calculate_total()
                    result["stats"]["updated"] += 1
                else:
                    # Insert new
                    data = DataArsip(
                        unit_kerja_id=unit.id,
                        tanggal=tanggal,
                        **data_values
                    )
                    data.calculate_total()
                    self.db.add(data)
                    result["stats"]["inserted"] += 1
                    
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
    
    def get_template_columns(self) -> List[str]:
        """Get template column names"""
        return [
            'tanggal', 'instansi', 'unit_kerja', 'naskah_masuk', 'naskah_keluar',
            'disposisi', 'berkas', 'retensi_permanen', 'retensi_musnah', 'naskah_ditindaklanjuti'
        ]
