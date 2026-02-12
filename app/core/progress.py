"""
Progress tracking for long-running tasks
"""
import json
import redis
from typing import Optional, Dict, Any
from app.config import settings


class ProgressTracker:
    """Отслеживание прогресса задач через Redis"""
    
    def __init__(self):
        self.redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
        self.ttl = 3600 * 2  # 2 hours
    
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
        key = self._get_key(task_id)
        
        data = {
            "current": current,
            "total": total,
            "percentage": round((current / total * 100), 2) if total > 0 else 0,
            "message": message,
            "extra": extra or {}
        }
        
        self.redis_client.setex(key, self.ttl, json.dumps(data))
    
    def get_progress(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Получает текущий прогресс задачи"""
        key = self._get_key(task_id)
        data = self.redis_client.get(key)
        
        if data:
            return json.loads(data)
        return None
    
    def clear_progress(self, task_id: str):
        """Очищает прогресс задачи"""
        key = self._get_key(task_id)
        self.redis_client.delete(key)


# Singleton
progress_tracker = ProgressTracker()
