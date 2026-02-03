"""
Caching Service with Redis + In-Memory Fallback
Supports Redis for production, falls back to in-memory if Redis unavailable
"""
from typing import Any, Optional, Dict
from datetime import datetime, timedelta
from functools import wraps
import threading
import hashlib
import json
import os

# Try to import Redis
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None


class InMemoryCacheBackend:
    """In-memory cache backend (fallback when Redis unavailable)"""
    
    def __init__(self, max_size: int = 5000):
        self._cache: Dict[str, tuple] = {}  # key: (value, expires_at)
        self._lock = threading.RLock()
        self._max_size = max_size
    
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            value, expires_at = entry
            if datetime.now() > expires_at:
                del self._cache[key]
                return None
            return value
    
    def set(self, key: str, value: Any, ttl: int = 300) -> None:
        with self._lock:
            if len(self._cache) >= self._max_size:
                # Evict expired entries
                now = datetime.now()
                expired = [k for k, (v, exp) in self._cache.items() if now > exp]
                for k in expired:
                    del self._cache[k]
                # If still full, remove oldest 10%
                if len(self._cache) >= self._max_size:
                    to_remove = list(self._cache.keys())[:self._max_size // 10]
                    for k in to_remove:
                        del self._cache[k]
            
            expires_at = datetime.now() + timedelta(seconds=ttl)
            self._cache[key] = (value, expires_at)
    
    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
    
    def keys(self, pattern: str = "*") -> list:
        with self._lock:
            if pattern == "*":
                return list(self._cache.keys())
            # Simple pattern matching for prefix*
            prefix = pattern.rstrip("*")
            return [k for k in self._cache.keys() if k.startswith(prefix)]
    
    def size(self) -> int:
        return len(self._cache)


class RedisCacheBackend:
    """Redis cache backend for production"""
    
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0, password: str = None):
        self._client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
            socket_connect_timeout=5
        )
        self._prefix = "splp:"  # Namespace prefix
    
    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"
    
    def ping(self) -> bool:
        """Test Redis connection"""
        try:
            return self._client.ping()
        except:
            return False
    
    def get(self, key: str) -> Optional[Any]:
        try:
            data = self._client.get(self._key(key))
            if data:
                return json.loads(data)
            return None
        except:
            return None
    
    def set(self, key: str, value: Any, ttl: int = 300) -> None:
        try:
            self._client.setex(self._key(key), ttl, json.dumps(value, default=str))
        except:
            pass
    
    def delete(self, key: str) -> bool:
        try:
            return self._client.delete(self._key(key)) > 0
        except:
            return False
    
    def clear(self) -> None:
        try:
            keys = self._client.keys(f"{self._prefix}*")
            if keys:
                self._client.delete(*keys)
        except:
            pass
    
    def keys(self, pattern: str = "*") -> list:
        try:
            full_pattern = f"{self._prefix}{pattern}"
            keys = self._client.keys(full_pattern)
            return [k.replace(self._prefix, "") for k in keys]
        except:
            return []
    
    def size(self) -> int:
        try:
            return len(self._client.keys(f"{self._prefix}*"))
        except:
            return 0


class CacheService:
    """Unified cache service with automatic backend selection"""
    
    def __init__(self):
        self._backend = None
        self._backend_type = "none"
        self._hits = 0
        self._misses = 0
        self._initialize_backend()
    
    def _initialize_backend(self):
        """Initialize cache backend (Redis preferred, fallback to in-memory)"""
        # Try Redis first
        if REDIS_AVAILABLE:
            redis_host = os.getenv("REDIS_HOST", "localhost")
            redis_port = int(os.getenv("REDIS_PORT", "6379"))
            redis_password = os.getenv("REDIS_PASSWORD", None)
            
            try:
                redis_backend = RedisCacheBackend(
                    host=redis_host,
                    port=redis_port,
                    password=redis_password
                )
                if redis_backend.ping():
                    self._backend = redis_backend
                    self._backend_type = "redis"
                    print(f"[Cache] Connected to Redis at {redis_host}:{redis_port}")
                    return
            except Exception as e:
                print(f"[Cache] Redis connection failed: {e}")
        
        # Fallback to in-memory
        self._backend = InMemoryCacheBackend(max_size=5000)
        self._backend_type = "in-memory"
        print("[Cache] Using in-memory cache (Redis not available)")
    
    def _generate_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate unique cache key from function arguments"""
        key_data = f"{prefix}:{json.dumps(args, default=str)}:{json.dumps(kwargs, sort_keys=True, default=str)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        result = self._backend.get(key)
        if result is not None:
            self._hits += 1
        else:
            self._misses += 1
        return result
    
    def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """Set value in cache"""
        self._backend.set(key, value, ttl)
    
    def delete(self, key: str) -> bool:
        """Delete specific key"""
        return self._backend.delete(key)
    
    def invalidate_prefix(self, prefix: str) -> int:
        """Invalidate all keys matching prefix"""
        keys = self._backend.keys(f"{prefix}*")
        count = 0
        for key in keys:
            if self._backend.delete(key):
                count += 1
        return count
    
    def clear(self) -> None:
        """Clear entire cache"""
        self._backend.clear()
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total = self._hits + self._misses
        hit_rate = (self._hits / total * 100) if total > 0 else 0
        return {
            "backend": self._backend_type,
            "size": self._backend.size(),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{hit_rate:.1f}%",
            "redis_available": REDIS_AVAILABLE
        }


# Global cache instance
cache = CacheService()


def cached(prefix: str, ttl: int = 300):
    """Decorator for caching function results"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Skip 'self' argument for methods
            cache_args = args[1:] if args and hasattr(args[0], '__class__') else args
            key = cache._generate_key(prefix, *cache_args, **kwargs)
            
            # Try to get from cache
            result = cache.get(key)
            if result is not None:
                return result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache.set(key, result, ttl)
            return result
        
        wrapper.cache_invalidate = lambda: cache.invalidate_prefix(prefix)
        return wrapper
    return decorator


def invalidate_arsip_cache():
    """Invalidate all arsip-related caches"""
    cache.invalidate_prefix("arsip")
    cache.invalidate_prefix("stats")
    cache.invalidate_prefix("filter")
