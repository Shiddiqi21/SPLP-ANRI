"""
SPLP Data Integrator - Main Application
Arsip Nasional Republik Indonesia
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
import os

from app.config import get_settings
from app.api.routes import router
from app.api.arsip_routes import router as arsip_router
from app.api.summary_routes import router as summary_router
from app.api.auth_routes import router as auth_router
from app.api.data_routes import router as data_router
from app.api.summary_routes import router as summary_router
from app.api.upload_routes import router as upload_router
from app.api.table_routes import router as table_router
from app.api.stats_routes import router as stats_router
from app.services.integrator import integrator_service
from app.services.arsip_service import arsip_service
from app.services.aggregation_service import aggregation_service
from app.services.auth_service import auth_service

settings = get_settings()

# Scheduler for background jobs
scheduler = BackgroundScheduler()

# Base directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def scheduled_sync():
    """Background job untuk sync data"""
    print(f"[Scheduler] Running data sync...")
    result = integrator_service.sync_data()
    print(f"[Scheduler] Sync complete: {result}")


def scheduled_aggregation():
    """Background job untuk update aggregated data"""
    print(f"[Scheduler] Running data aggregation...")
    result = aggregation_service.run_all_aggregations()
    print(f"[Scheduler] Aggregation complete")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    # Startup
    print("=" * 50)
    print("  SPLP Data Integrator v2.2 Starting...")
    print("=" * 50)
    print("[Database] Skipping table checks (already initialized)")
    print("[Scheduler] DISABLED - Manual sync only (use /api/integrator/sync)")
    print("[Server] Ready to accept connections!")

    
    yield
    
    # Shutdown
    try:
        if scheduler.running:
            scheduler.shutdown()
    except Exception:
        pass  # Ignore scheduler shutdown errors
    print("SPLP Data Integrator Stopped")


# Create FastAPI app
app = FastAPI(
    title="SPLP Data Integrator",
    description="Data Integrator untuk Sistem Pengelolaan Layanan Publik - ANRI",
    version="2.2.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*", "http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:8000", "http://11.1.239.6:3000", "http://11.1.239.6:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Middleware to prevent caching of HTML pages
@app.middleware("http")
async def add_no_cache_headers(request: Request, call_next):
    response = await call_next(request)
    # Add no-cache headers for HTML pages
    if request.url.path in ["/", "/login"] or request.url.path.endswith(".html"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


# Static files & Templates
static_path = os.path.join(BASE_DIR, "static")
templates_path = os.path.join(BASE_DIR, "templates")
os.makedirs(static_path, exist_ok=True)
os.makedirs(templates_path, exist_ok=True)

app.mount("/static", StaticFiles(directory=static_path), name="static")
templates = Jinja2Templates(directory=templates_path)

# Include API routes
app.include_router(router)
app.include_router(arsip_router)
app.include_router(summary_router)
app.include_router(auth_router)
app.include_router(data_router)
app.include_router(upload_router)
app.include_router(table_router)
app.include_router(stats_router)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Homepage - Web Data Management"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Login Page"""
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    """Register Page"""
    return templates.TemplateResponse("register.html", {"request": request})


@app.get("/upload", response_class=HTMLResponse)
async def upload_page(request: Request):
    """Upload Page"""
    return templates.TemplateResponse("upload.html", {"request": request})


@app.get("/grafana-builder", response_class=HTMLResponse)
async def grafana_builder_page(request: Request):
    """Grafana URL Builder Page"""
    return templates.TemplateResponse("grafana_builder.html", {"request": request})



@app.get("/api-info")
async def api_info():
    """API Info endpoint"""
    return {
        "name": "SPLP Data Integrator",
        "organization": "Arsip Nasional Republik Indonesia",
        "version": "2.2.0",
        "features": ["Authentication", "Data Input", "Pre-Aggregation", "Grafana Ready"],
        "docs": "/docs"
    }

