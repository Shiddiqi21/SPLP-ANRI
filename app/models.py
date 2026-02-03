"""
SQLAlchemy Models untuk SPLP Data Integrator
"""
from sqlalchemy import Column, Integer, String, Date, DateTime, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class ArsipData(Base):
    """Model untuk tabel arsip_data"""
    __tablename__ = "arsip_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    tanggal = Column(Date, nullable=False, index=True)
    role_id = Column(Integer, nullable=False, index=True)
    jenis_arsip = Column(String(255), nullable=False)
    instansi_id = Column(Integer, nullable=False, index=True)
    data_content = Column(JSON, nullable=True)
    keterangan = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    def to_dict(self):
        """Convert model to dictionary"""
        return {
            "id": self.id,
            "tanggal": str(self.tanggal) if self.tanggal else None,
            "role_id": self.role_id,
            "jenis_arsip": self.jenis_arsip,
            "instansi_id": self.instansi_id,
            "data_content": self.data_content,
            "keterangan": self.keterangan,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }


class ArsipSummary(Base):
    """
    Model untuk tabel arsip_summary (Pre-Aggregated Data)
    Tabel ini di-update secara berkala untuk mengurangi beban query Grafana
    """
    __tablename__ = "arsip_summary"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    tanggal = Column(Date, nullable=False, index=True)
    instansi_id = Column(Integer, nullable=False, index=True)
    jenis_arsip = Column(String(255), nullable=False, index=True)
    role_id = Column(Integer, nullable=False, index=True)
    
    # Aggregated metrics
    total_count = Column(Integer, default=0)  # Jumlah arsip
    
    # Metadata
    last_updated = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    def to_dict(self):
        return {
            "id": self.id,
            "tanggal": str(self.tanggal) if self.tanggal else None,
            "instansi_id": self.instansi_id,
            "jenis_arsip": self.jenis_arsip,
            "role_id": self.role_id,
            "total_count": self.total_count,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None
        }


class DailySummary(Base):
    """
    Summary harian untuk trend analysis
    """
    __tablename__ = "daily_summary"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    tanggal = Column(Date, nullable=False, unique=True, index=True)
    total_arsip = Column(Integer, default=0)
    total_instansi = Column(Integer, default=0)
    total_jenis = Column(Integer, default=0)
    last_updated = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    def to_dict(self):
        return {
            "tanggal": str(self.tanggal) if self.tanggal else None,
            "total_arsip": self.total_arsip,
            "total_instansi": self.total_instansi,
            "total_jenis": self.total_jenis,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None
        }

