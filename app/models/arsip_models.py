"""
SQLAlchemy Models untuk SPLP Data Integrator
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Date, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from app.database import Base


class Instansi(Base):
    """Model untuk tabel instansi (institusi)"""
    __tablename__ = "instansi"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    kode = Column(String(20), unique=True, nullable=False)
    nama = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    unit_kerja = relationship("UnitKerja", back_populates="instansi", cascade="all, delete-orphan")
    
    def to_dict(self):
        return {
            "id": self.id,
            "kode": self.kode,
            "nama": self.nama,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }


class UnitKerja(Base):
    """Model untuk tabel unit_kerja"""
    __tablename__ = "unit_kerja"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    instansi_id = Column(Integer, ForeignKey("instansi.id", ondelete="CASCADE"), nullable=False)
    kode = Column(String(50), nullable=False)
    nama = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    instansi = relationship("Instansi", back_populates="unit_kerja")
    data_arsip = relationship("DataArsip", back_populates="unit_kerja", cascade="all, delete-orphan")
    
    def to_dict(self, include_instansi=False):
        result = {
            "id": self.id,
            "instansi_id": self.instansi_id,
            "kode": self.kode,
            "nama": self.nama,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
        if include_instansi and self.instansi:
            result["instansi"] = self.instansi.to_dict()
        return result


class DataArsip(Base):
    """Model untuk tabel data_arsip"""
    __tablename__ = "data_arsip"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    unit_kerja_id = Column(Integer, ForeignKey("unit_kerja.id", ondelete="CASCADE"), nullable=False)
    tanggal = Column(Date, nullable=False)
    naskah_masuk = Column(Integer, default=0)
    naskah_keluar = Column(Integer, default=0)
    disposisi = Column(Integer, default=0)
    berkas = Column(Integer, default=0)
    retensi_permanen = Column(Integer, default=0)
    retensi_musnah = Column(Integer, default=0)
    naskah_ditindaklanjuti = Column(Integer, default=0)
    total = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    unit_kerja = relationship("UnitKerja", back_populates="data_arsip")
    
    def calculate_total(self):
        """Calculate total from all data columns"""
        self.total = (
            (self.naskah_masuk or 0) +
            (self.naskah_keluar or 0) +
            (self.disposisi or 0) +
            (self.berkas or 0) +
            (self.retensi_permanen or 0) +
            (self.retensi_musnah or 0) +
            (self.naskah_ditindaklanjuti or 0)
        )
        return self.total
    
    def to_dict(self, include_unit_kerja=False):
        result = {
            "id": self.id,
            "unit_kerja_id": self.unit_kerja_id,
            "tanggal": self.tanggal.isoformat() if self.tanggal else None,
            "naskah_masuk": self.naskah_masuk or 0,
            "naskah_keluar": self.naskah_keluar or 0,
            "disposisi": self.disposisi or 0,
            "berkas": self.berkas or 0,
            "retensi_permanen": self.retensi_permanen or 0,
            "retensi_musnah": self.retensi_musnah or 0,
            "naskah_ditindaklanjuti": self.naskah_ditindaklanjuti or 0,
            "total": self.total or 0,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
        if include_unit_kerja and self.unit_kerja:
            result["unit_kerja"] = self.unit_kerja.to_dict(include_instansi=True)
        return result
