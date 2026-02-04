"""
Models package for SPLP Data Integrator
"""
from app.database import Base
from app.models.arsip_models import Instansi, UnitKerja, DataArsip

from app.models.table_models import TableDefinition, ColumnDefinition, DynamicData

# Backward compatibility aliases
# The old ArsipData is replaced by the new DataArsip structure
ArsipData = DataArsip
ArsipSummary = None  # Deprecated - use aggregation queries instead
DailySummary = None  # Deprecated - use aggregation queries instead

__all__ = [
    "Base", 
    "Instansi", 
    "UnitKerja", 
    "DataArsip", 
    "ArsipData",
    "TableDefinition",
    "ColumnDefinition",
    "DynamicData"
]
