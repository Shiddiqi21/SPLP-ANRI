"""
Script untuk memasukkan data ANRI ke database
- Instansi: ANRI
- Unit Kerja: Semua unit dari gambar + Sekretariat (Lama) + Unit Kearsipan
- Data Arsip: Random data untuk setiap unit
"""
import random
from datetime import date, timedelta
from app.database import SessionLocal
from app.models.arsip_models import Instansi, UnitKerja, DataArsip

# Daftar semua Unit Kerja ANRI
UNIT_KERJA_LIST = [
    # Dari gambar 1
    (1, "Eksternal SRIKANDI"),
    (45, "Biro Kepegawaian dan Umum"),
    (44, "Biro Hukum, Kerja Sama, dan Hubungan Masyarakat"),
    (43, "Biro Manajemen Kinerja, Keuangan, dan Organisasi"),
    (73, "Direktorat Sumber Daya Manusia Kearsipan dan Sertifikasi"),
    (54, "Direktorat Penyelamatan Arsip"),
    (57, "Direktorat Layanan dan Pemanfaatan Arsip"),
    (70, "Direktorat Kearsipan Pusat"),
    (64, "Pusat Data, Informasi, dan Jasa Teknis Kearsipan"),
    (56, "Direktorat Pelestarian dan Pelindungan Arsip"),
    (71, "Direktorat Kearsipan Daerah I"),
    (74, "Inspektorat"),
    (4, "Kepala ANRI"),
    (62, "Pusat Pelatihan Sumber Daya Manusia"),
    (66, "Pusat Pengawasan dan Akreditasi Kearsipan"),
    
    # Dari gambar 2
    (53, "Deputi Bidang Penyelamatan, Pelestarian, dan Pelindungan Arsip"),
    (75, "Sekretaris Utama"),
    (72, "Direktorat Kearsipan Daerah II"),
    (60, "Direktorat Teknologi Informasi Kearsipan"),
    (55, "Direktorat Pengolahan Arsip"),
    (39, "Balai Arsip Statis dan Tsunami"),
    (59, "Direktorat Sistem Kearsipan"),
    (52, "Deputi Bidang Tata Kelola Kearsipan Nasional"),
    (68, "Pusat Studi Arsip Statis Kepresidenan"),
    (49, "Subbagian Tata Usaha Deputi Bidang Tata Kelola Kearsipan Nasional"),
    (47, "Subbagian Rumah Tangga dan Pengamanan, Biro Kepegawaian dan Umum"),
    (61, "Direktorat Informasi Kearsipan"),
    (63, "Subbagian Umum Pusat Pelatihan Sumber Daya Manusia"),
    (58, "Deputi Bidang Sistem dan Informasi Kearsipan Nasional"),
    (51, "Subbagian Tata Usaha Deputi Bidang Sistem dan Informasi Kearsipan Nasional"),
    
    # Dari gambar 3
    (16, "Direktorat Kearsipan Daerah II (Lama)"),
    (46, "Bagian Perlengkapan, Kearsipan, Tata Usaha, dan Layanan Pengadaan"),
    (23, "Subbagian Tata Usaha Balai Arsip Statis Dan Tsunami"),
    (65, "Subbagian Umum Pusat Data, Informasi dan Jasa Teknis Kearsipan"),
    (50, "Subbagian Tata Usaha Deputi Bidang Penyelamatan, Pelestarian, dan Pelindungan Arsip"),
    (67, "Subbagian Umum Pusat Pengawasan dan Akreditasi Kearsipan"),
    (76, "Subbagian Tata Usaha Inspektorat"),
    (14, "Direktorat Kearsipan Pusat (LAMA)"),
    (69, "Subbagian Umum Pusat Studi Arsip Statis Kepresidenan"),
    (48, "Subbagian Tata Usaha Kepala dan Sekretariat Utama"),
    (21, "Direktorat Preservasi (Lama)"),
    (22, "Direktorat Layanan dan Pemanfaatan (Lama)"),
    (19, "Direktorat Akuisisi (Lama)"),
    (2, "Arsip Nasional Republik Indonesia"),
    (15, "Direktorat Kearsipan Daerah I (Lama)"),
    
    # Dari gambar 4
    (34, "Inspektorat (Lama)"),
    (8, "Biro Umum (Lama)"),
    (28, "Pusat Pendidikan dan Pelatihan Kearsipan (Lama)"),
    (27, "Pusat Pengkajian dan Pengembangan Sistem Kearsipan (Lama)"),
    (10, "Subbagian Tata Usaha Deputi Bidang Pembinaan Kearsipan (Lama)"),
    (77, "Subdirektorat"),
    (42, "Subbagian Umum Pusat Studi Arsip Statis Kepresidenan (Lama)"),
    (41, "Pusat Studi Arsip Statis Kepresidenan (Lama)"),
    (40, "Subbagian Protokol Dan Pengamanan"),
    (38, "Bagian Perlengkapan, Tata Usaha, Kearsipan, Dan Protokol (Lama)"),
    (37, "BIRO PERENCANAAN DAN HUBUNGAN MASYARAKAT (SALAH)"),
    (36, "PUSAT JASA KEARSIPAN (SALAH)"),
    (35, "Subbagian Tata Usaha Inspektorat (Lama)"),
    (33, "Subbagian Tata Usaha Pusat Akreditasi Kearsipan (Lama)"),
    (32, "Pusat Akreditasi Kearsipan (Lama)"),
    
    # Dari gambar 5
    (31, "Subbagian Tata Usaha Pusat Jasa Kearsipan (Lama)"),
    (30, "Pusat Jasa Kearsipan (Lama)"),
    (29, "Subbagian Tata Usaha Pusat Pendidikan Dan Pelatihan Kearsipan (Lama)"),
    (26, "Pusat Data dan Informasi (Lama)"),
    (25, "Pusat Sistem dan Jaringan Informasi Kearsipan Nasional (Lama)"),
    (24, "Deputi Bidang Informasi dan Pengembangan Sistem Kearsipan (Lama)"),
    (20, "Direktorat Pengolahan (Lama)"),
    (18, "Deputi Bidang Konservasi Arsip (Lama)"),
    (17, "Direktorat SDM Kearsipan dan Sertifikasi (Lama)"),
    (13, "Deputi Bidang Pembinaan Kearsipan (Lama)"),
    (12, "Subbagian Tata Usaha Deputi Bidang Informasi Dan Pengembangan Sistem Kearsipan (Lama)"),
    (11, "Subbagian Tata Usaha Deputi Bidang Konservasi Arsip (Lama)"),
    (9, "Subbagian Perlengkapan Dan Rumah Tangga (Lama)"),
    (7, "Biro Organisasi, Kepegawaian, Dan Hukum (Lama)"),
    (6, "Biro Perencanaan dan Hubungan Masyarakat (Lama)"),
    
    # Tambahan dari user
    (78, "Sekretariat (Lama)"),
    (79, "Unit Kearsipan"),
]

def generate_random_data():
    """Generate random integer data"""
    return {
        "naskah_masuk": random.randint(0, 500),
        "naskah_keluar": random.randint(0, 300),
        "disposisi": random.randint(0, 200),
        "berkas": random.randint(0, 100),
        "retensi_permanen": random.randint(0, 50),
        "retensi_musnah": random.randint(0, 30),
        "naskah_ditindaklanjuti": random.randint(0, 150),
    }

def main():
    db = SessionLocal()
    
    try:
        # 1. Cek/buat instansi ANRI
        instansi = db.query(Instansi).filter(Instansi.kode == "ANRI").first()
        if not instansi:
            instansi = Instansi(
                kode="ANRI",
                nama="Arsip Nasional Republik Indonesia"
            )
            db.add(instansi)
            db.commit()
            db.refresh(instansi)
            print(f"[+] Instansi ANRI dibuat dengan ID: {instansi.id}")
        else:
            print(f"[*] Instansi ANRI sudah ada dengan ID: {instansi.id}")
        
        # 2. Masukkan semua unit kerja
        unit_count = 0
        for kode_num, nama in UNIT_KERJA_LIST:
            kode = f"UK-{kode_num:03d}"
            
            # Cek apakah sudah ada
            existing = db.query(UnitKerja).filter(
                UnitKerja.instansi_id == instansi.id,
                UnitKerja.kode == kode
            ).first()
            
            if not existing:
                unit = UnitKerja(
                    instansi_id=instansi.id,
                    kode=kode,
                    nama=nama
                )
                db.add(unit)
                unit_count += 1
        
        db.commit()
        print(f"[+] {unit_count} unit kerja baru ditambahkan")
        
        # 3. Generate random data arsip untuk setiap unit
        units = db.query(UnitKerja).filter(UnitKerja.instansi_id == instansi.id).all()
        data_count = 0
        
        # Generate data untuk 30 hari terakhir
        today = date.today()
        for unit in units:
            for days_ago in range(30):
                tanggal = today - timedelta(days=days_ago)
                
                # Cek apakah data sudah ada
                existing_data = db.query(DataArsip).filter(
                    DataArsip.unit_kerja_id == unit.id,
                    DataArsip.tanggal == tanggal
                ).first()
                
                if not existing_data:
                    random_data = generate_random_data()
                    data = DataArsip(
                        unit_kerja_id=unit.id,
                        tanggal=tanggal,
                        **random_data
                    )
                    data.calculate_total()
                    db.add(data)
                    data_count += 1
        
        db.commit()
        print(f"[+] {data_count} data arsip baru ditambahkan")
        
        # 4. Summary
        total_units = db.query(UnitKerja).filter(UnitKerja.instansi_id == instansi.id).count()
        total_data = db.query(DataArsip).count()
        print("\n" + "=" * 50)
        print("  SUMMARY")
        print("=" * 50)
        print(f"  Instansi: ANRI")
        print(f"  Total Unit Kerja: {total_units}")
        print(f"  Total Data Arsip: {total_data}")
        print("=" * 50)
        
    except Exception as e:
        db.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
