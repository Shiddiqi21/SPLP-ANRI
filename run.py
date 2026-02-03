"""
Run script untuk SPLP Data Integrator
Usage: python run.py

Features:
- Auto-create .env from .env.example
- Auto-create database if not exists
- Auto-run migrations on startup
- Auto-create default admin user
"""
import uvicorn
import subprocess
import sys
import shutil
from pathlib import Path


def setup_env_file():
    """Copy .env.example to .env if .env doesn't exist"""
    env_file = Path(".env")
    env_example = Path(".env.example")
    
    if not env_file.exists():
        if env_example.exists():
            shutil.copy(env_example, env_file)
            print("[Setup] Created .env from .env.example")
            print("[Setup] Edit .env if your MySQL config is different from default")
        else:
            # Create default .env
            default_env = """# Database Configuration (MySQL)
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=
DB_NAME=anri

# App Configuration
APP_ENV=development
SYNC_INTERVAL_SECONDS=300
"""
            env_file.write_text(default_env)
            print("[Setup] Created default .env file")
    return True

def create_database_if_not_exists():
    """Create database if it doesn't exist"""
    try:
        import pymysql
        from app.config import get_settings
        
        settings = get_settings()
        
        # Connect without database to create it
        conn = pymysql.connect(
            host=settings.db_host,
            port=settings.db_port,
            user=settings.db_user,
            password=settings.db_password or ''
        )
        cursor = conn.cursor()
        
        # Create database if not exists
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS {settings.db_name} "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        conn.commit()
        print(f"[Database] Database '{settings.db_name}' ready")
        
        conn.close()
        return True
    except Exception as e:
        print(f"[Database] Warning: Could not create database - {e}")
        return False


def run_migrations():
    """Run alembic migrations"""
    try:
        print("[Migration] Checking for pending migrations...")
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            cwd="."
        )
        
        if result.returncode == 0:
            # Check if any migrations were run
            if "Running upgrade" in result.stderr:
                print("[Migration] Migrations applied successfully!")
            else:
                print("[Migration] Database is up to date")
            return True
        else:
            print(f"[Migration] Warning: {result.stderr}")
            return False
    except Exception as e:
        print(f"[Migration] Warning: Could not run migrations - {e}")
        return False


def create_default_admin():
    """Create default admin user if not exists"""
    try:
        from app.database import SessionLocal
        from app.services.auth_service import User, AuthService
        
        db = SessionLocal()
        auth_service = AuthService()
        
        # Check if admin exists
        admin = db.query(User).filter(User.username == "admin").first()
        if not admin:
            admin = User(
                username="admin",
                email="admin@anri.go.id",
                hashed_password=auth_service.hash_password("admin123"),
                full_name="Administrator",
                is_active=True,
                is_admin=True
            )
            db.add(admin)
            db.commit()
            print("[Auth] Default admin user created (admin/admin123)")
        
        db.close()
        return True
    except Exception as e:
        print(f"[Auth] Warning: Could not create default admin - {e}")
        return False


if __name__ == "__main__":
    print("=" * 50)
    print("  SPLP Data Integrator - Starting...")
    print("=" * 50)
    
    # Step 0: Setup .env file if not exists
    setup_env_file()
    
    # Step 1: Create database if not exists
    create_database_if_not_exists()
    
    # Step 2: Run migrations
    run_migrations()
    
    # Step 3: Create default admin
    create_default_admin()
    
    print("=" * 50)
    print("  Server starting on http://127.0.0.1:8000")
    print("=" * 50)
    
    # Start server
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
