# SPLP Data Integrator - ANRI

Data Integrator untuk Sistem Pengelolaan Layanan Publik (SPLP) Arsip Nasional RI.

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Database
Copy `.env.example` ke `.env` dan edit:
```bash
cp .env.example .env
```

### 3. Run Server
Cara Termudah (Windows):
Double click file `run.bat`

Atau via Terminal:
```bash
python run.py
```

Cara Manual (Debug):
```bash
python -m uvicorn app.main:app --reload --port 8000
```

### 4. Access
- API Docs: http://localhost:8000/docs
- Health: http://localhost:8000/api/health

## 📚 API Endpoints

| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| `GET` | `/api/health` | Status service |
| `GET` | `/api/data/summary` | Ringkasan data |
| `GET` | `/api/data/arsip` | Data arsip |
| `POST` | `/api/sync` | Trigger sync |
