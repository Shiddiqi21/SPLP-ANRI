"""
SPLP Data Integrator Configuration
"""
import os
from functools import lru_cache
from dotenv import load_dotenv

# Load .env file
load_dotenv()


class Settings:
    """Application settings loaded from environment variables"""
    
    def __init__(self):
        # Database
        self.db_host: str = os.getenv("DB_HOST", "localhost")
        self.db_port: int = int(os.getenv("DB_PORT", "3306"))
        self.db_user: str = os.getenv("DB_USER", "root")
        self.db_password: str = os.getenv("DB_PASSWORD", "")
        self.db_name: str = os.getenv("DB_NAME", "datatest")
        
        # App
        self.app_env: str = os.getenv("APP_ENV", "development")
        self.sync_interval_seconds: int = int(os.getenv("SYNC_INTERVAL_SECONDS", "300"))
    
    @property
    def database_url(self) -> str:
        """Generate database connection URL"""
        return f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()
