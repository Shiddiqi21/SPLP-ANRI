"""
API Routes untuk Summary/Aggregated Data (untuk Grafana)
"""
from fastapi import APIRouter, Query
from typing import Optional

from app.services.aggregation_service import aggregation_service

router = APIRouter(prefix="/api/summary", tags=["Summary Data (Grafana)"])


@router.get("", summary="Get Pre-Aggregated Summary")
async def get_summary():
    """
    Get pre-aggregated summary data untuk Grafana.
    Data sudah di-aggregate sehingga query lebih ringan.
    """
    return {
        "data": aggregation_service.get_summary_for_grafana(),
        "source": "arsip_summary",
        "note": "Pre-aggregated data - updated periodically"
    }


@router.get("/daily", summary="Get Daily Trend")
async def get_daily_trend(days: int = Query(30, ge=1, le=365)):
    """
    Get daily trend data untuk time-series chart di Grafana.
    """
    return {
        "data": aggregation_service.get_daily_trend(days),
        "source": "daily_summary",
        "days_requested": days
    }


@router.post("/refresh", summary="Manually Refresh Aggregations")
async def refresh_aggregations():
    """
    Trigger manual refresh untuk semua aggregation.
    Biasanya dilakukan otomatis oleh scheduler.
    """
    result = aggregation_service.run_all_aggregations()
    return result


@router.get("/status", summary="Get Aggregation Status")
async def get_aggregation_status():
    """Get status of last aggregation run"""
    return {
        "last_run": aggregation_service.last_run.isoformat() if aggregation_service.last_run else None,
        "status": "ready" if aggregation_service.last_run else "never_run"
    }
