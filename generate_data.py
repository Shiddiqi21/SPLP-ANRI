"""
Script untuk generate data arsip dalam jumlah besar
Usage: python generate_data.py [jumlah]
Contoh: python generate_data.py 10000
"""
import sys
import random
from datetime import date, timedelta

# Add project to path
sys.path.insert(0, '.')

from app.database import get_db_context
from app.models import ArsipData

# Sample data lists
JENIS_ARSIP = [
    'Naskah Keluar', 'Naskah Masuk', 'Berkas Arsip', 'Nota Dinas',
    'Surat Keputusan', 'Laporan Bulanan', 'Dokumen Kepegawaian',
    'Arsip Keuangan', 'Berita Acara', 'Surat Tugas', 'Memo Internal',
    'Proposal Kegiatan', 'Kontrak Kerja', 'MoU', 'Surat Perjanjian',
    'Laporan Tahunan', 'SK Mutasi', 'Surat Edaran', 'Instruksi Kerja',
    'SOP', 'Pedoman', 'Peraturan', 'Keputusan', 'Pengumuman'
]

KETERANGAN = [
    'Arsip tahun berjalan', 'Dokumen penting', 'Perlu tindak lanjut',
    'Sudah diproses', 'Menunggu verifikasi', 'Arsip aktif',
    'Dokumen rahasia', 'Urgent', 'Prioritas tinggi', 'Review diperlukan',
    None, None, None  # Some null values
]


def generate_data(count: int = 10000):
    """Generate random arsip data"""
    print(f"\n{'='*50}")
    print(f"SPLP Data Generator")
    print(f"{'='*50}")
    print(f"Generating {count:,} records...")
    
    # Date range: last 2 years
    end_date = date.today()
    start_date = end_date - timedelta(days=730)  # 2 years back
    date_range = (end_date - start_date).days
    
    batch_size = 5000  # Insert in batches
    total_inserted = 0
    
    with get_db_context() as db:
        batch = []
        
        for i in range(count):
            # Generate random data
            random_date = start_date + timedelta(days=random.randint(0, date_range))
            
            arsip = ArsipData(
                tanggal=random_date,
                role_id=random.randint(1, 10),
                jenis_arsip=random.choice(JENIS_ARSIP),
                instansi_id=random.randint(1, 50),
                keterangan=random.choice(KETERANGAN)
            )
            batch.append(arsip)
            
            # Commit in batches
            if len(batch) >= batch_size:
                db.add_all(batch)
                db.commit()
                total_inserted += len(batch)
                progress = (total_inserted / count) * 100
                print(f"  Progress: {total_inserted:,}/{count:,} ({progress:.1f}%)")
                batch = []
        
        # Insert remaining
        if batch:
            db.add_all(batch)
            db.commit()
            total_inserted += len(batch)
    
    print(f"\n✅ Successfully inserted {total_inserted:,} records!")
    print(f"{'='*50}\n")


def get_current_count():
    """Get current record count"""
    with get_db_context() as db:
        count = db.query(ArsipData).count()
        return count


if __name__ == "__main__":
    # Get count from argument or use default
    if len(sys.argv) > 1:
        try:
            count = int(sys.argv[1])
        except ValueError:
            print("Usage: python generate_data.py [jumlah]")
            print("Contoh: python generate_data.py 10000")
            sys.exit(1)
    else:
        count = 10000  # Default
    
    # Show current count
    current = get_current_count()
    print(f"\nCurrent records in database: {current:,}")
    
    # Confirm for large inserts
    if count >= 100000:
        confirm = input(f"Generate {count:,} records? This may take a while. (y/n): ")
        if confirm.lower() != 'y':
            print("Cancelled.")
            sys.exit(0)
    
    # Generate
    generate_data(count)
    
    # Show final count
    final = get_current_count()
    print(f"Total records now: {final:,}")
