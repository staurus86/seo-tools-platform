"""
Progress tracking for long-running tasks
"""
import json
import redis
import logging
from typing import Optional, Dict, Any
from app.config import settings

logger = logging.getLogger(__name__)


class ProgressTracker:
    """Отслеживание прогресса задач через Redis (with fallback to memory)"""
    
    def __init__(self):
        self._redis_client = None
        self._redis_available = True
        self._memory_store = {}  # Fallback storage
        self.ttl = 3600 * 2  # 2 hours
    
    @property
    def redis_client(self):
        """Lazy initialization of Redis client"""
        if self._redis_client is None and self._redis_available:
            try:
                self._redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
                self._redis_client.ping()
                logger.info("Progress tracker: Redis connection established")
            except Exception as e:
                logger.warning(f"Progress tracker: Redis unavailable, using memory fallback: {e}")
                self._redis_available = False
                self._redis_client = None
        return self._redis_client
    
    def _get_key(self, task_id: str) -> str:
        return f"task_progress:{task_id}"
    
    def update_progress(
        self,
        task_id: str,
        current: int,
        total: int,
        message: str = "",
        extra: Optional[Dict[str, Any]] = None
    ):
        """Обновляет прогресс задачи"""
        data = {
            "current": current,
            "total": total,
            "percentage": round((current / total * 100), 2) if total > 0 else 0,
            "message": message,
            "extra": extra or {}
        }
        
        if self.redis_client:
            try:
                key = self._get_key(task_id)
                self.redis_client.setex(key, self.ttl, json.dumps(data))
                return
            except Exception as e:
                logger.error(f"Error updating progress in Redis: {e}")
        
        # Fallback to memory
        self._memory_store[task_id] = data
    
    def get_progress(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Получает текущий прогресс задачи"""
        if self.redis_client:
            try:
                key = self._get_key(task_id)
                data = self.redis_client.get(key)
                if data:
                    return json.loads(data)
            except Exception as e:
                logger.error(f"Error getting progress from Redis: {e}")
        
        # Fallback to memory
        return self._memory_store.get(task_id)
    
    def clear_progress(self, task_id: str):
        """Очищает прогресс задачи"""
        if self.redis_client:
            try:
                key = self._get_key(task_id)
                self.redis_client.delete(key)
            except Exception as e:
                logger.error(f"Error clearing progress from Redis: {e}")
        
        # Clear from memory fallback
        if task_id in self._memory_store:
            del self._memory_store[task_id]


# Singleton
progress_tracker = ProgressTracker()
