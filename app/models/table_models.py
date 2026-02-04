"""
SQLAlchemy Models untuk Dynamic Table System
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Date, DateTime, ForeignKey, Text, Boolean, JSON
from sqlalchemy.orm import relationship
from app.database import Base


class TableDefinition(Base):
    """Model untuk menyimpan definisi tabel custom"""
    __tablename__ = "table_definitions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)  # Nama internal (slug)
    display_name = Column(String(255), nullable=False)  # Nama tampilan
    description = Column(Text, nullable=True)
    is_default = Column(Boolean, default=False)  # Tandai tabel default
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    columns = relationship("ColumnDefinition", back_populates="table", cascade="all, delete-orphan", order_by="ColumnDefinition.order")
    data = relationship("DynamicData", back_populates="table", cascade="all, delete-orphan")
    
    def to_dict(self, include_columns=False):
        result = {
            "id": self.id,
            "name": self.name,
            "display_name": self.display_name,
            "description": self.description,
            "is_default": self.is_default,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
        if include_columns:
            result["columns"] = [c.to_dict() for c in self.columns]
        return result


class ColumnDefinition(Base):
    """Model untuk menyimpan definisi kolom dalam tabel"""
    __tablename__ = "column_definitions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_id = Column(Integer, ForeignKey("table_definitions.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(100), nullable=False)  # Nama internal kolom
    display_name = Column(String(255), nullable=False)  # Nama tampilan
    data_type = Column(String(50), default="integer")  # integer, text, date, decimal
    is_required = Column(Boolean, default=False)
    is_summable = Column(Boolean, default=True)  # Apakah ikut dihitung di total
    order = Column(Integer, default=0)  # Urutan tampilan
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    table = relationship("TableDefinition", back_populates="columns")
    
    def to_dict(self):
        return {
            "id": self.id,
            "table_id": self.table_id,
            "name": self.name,
            "display_name": self.display_name,
            "data_type": self.data_type,
            "is_required": self.is_required,
            "is_summable": self.is_summable,
            "order": self.order
        }


class DynamicData(Base):
    """
    [DEPRECATED] Model untuk menyimpan data dinamis dengan struktur JSON.
    Sekarang sistem menggunakan Physical Table Mode (Tabel fisik asli) sehingga model ini tidak lagi digunakan untuk penyimpanan tabel baru.
    Dibiarkan ada untuk referensi data lama sebelum migrasi.
    """
    __tablename__ = "dynamic_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_id = Column(Integer, ForeignKey("table_definitions.id", ondelete="CASCADE"), nullable=False)
    unit_kerja_id = Column(Integer, ForeignKey("unit_kerja.id", ondelete="CASCADE"), nullable=False)
    tanggal = Column(Date, nullable=False)
    data = Column(JSON, nullable=False, default={})  # Menyimpan data kolom dalam format JSON
    total = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    table = relationship("TableDefinition", back_populates="data")
    unit_kerja = relationship("UnitKerja")
    
    def calculate_total(self, columns):
        """Calculate total from summable columns"""
        total = 0
        for col in columns:
            if col.is_summable and col.name in self.data:
                try:
                    total += int(self.data.get(col.name, 0) or 0)
                except (ValueError, TypeError):
                    pass
        self.total = total
        return self.total
    
    def to_dict(self, include_unit_kerja=False):
        result = {
            "id": self.id,
            "table_id": self.table_id,
            "unit_kerja_id": self.unit_kerja_id,
            "tanggal": self.tanggal.isoformat() if self.tanggal else None,
            "data": self.data or {},
            "total": self.total or 0,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
        if include_unit_kerja and self.unit_kerja:
            result["unit_kerja"] = self.unit_kerja.to_dict(include_instansi=True)
        return result
