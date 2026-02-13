
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.generic_summary_service import GenericSummaryService
import logging

router = APIRouter(prefix="/api/summary", tags=["Summary"])
logger = logging.getLogger(__name__)

@router.post("/generate/{table_id}")
async def generate_summary(table_id: int, db: Session = Depends(get_db)):
    """
    Trigger manual generation of summary table for a given table_id.
    """
    try:
        service = GenericSummaryService(db)
        result = service.create_summary_table(table_id)
        
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["message"])
            
        return result
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/{table_id}")
async def check_summary_status(table_id: int, db: Session = Depends(get_db)):
    """
    Check if a summary table exists for the given table_id.
    """
    service = GenericSummaryService(db)
    exists = service.check_summary_exists(table_id)
    return {"exists": exists}
