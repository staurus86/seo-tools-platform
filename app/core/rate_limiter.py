"""
Rate Limiting с использованием Redis
"""
import redis
from fastapi import HTTPException, Request
from app.config import settings


class RateLimiter:
    """Rate limiter на основе Redis"""
    
    def __init__(self):
        self.redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
        self.limit = settings.RATE_LIMIT_PER_HOUR
        self.window = settings.RATE_LIMIT_WINDOW
    
    def _get_key(self, ip: str) -> str:
        """Генерирует ключ для Redis"""
        return f"rate_limit:{ip}"
    
    def check_rate_limit(self, ip: str) -> dict:
        """
        Проверяет rate limit для IP
        
        Returns:
            dict: {
                'allowed': bool,
                'remaining': int,
                'reset_in': int  # seconds
            }
        """
        key = self._get_key(ip)
        
        # Получаем текущее количество запросов
        current = self.redis_client.get(key)
        
        if current is None:
            # Первый запрос - устанавливаем счетчик
            pipe = self.redis_client.pipeline()
            pipe.setex(key, self.window, 1)
            pipe.execute()
            return {
                'allowed': True,
                'remaining': self.limit - 1,
                'reset_in': self.window
            }
        
        current = int(current)
        
        if current >= self.limit:
            # Лимит исчерпан
            ttl = self.redis_client.ttl(key)
            return {
                'allowed': False,
                'remaining': 0,
                'reset_in': max(0, ttl)
            }
        
        # Увеличиваем счетчик
        self.redis_client.incr(key)
        ttl = self.redis_client.ttl(key)
        
        return {
            'allowed': True,
            'remaining': self.limit - current - 1,
            'reset_in': max(0, ttl)
        }
    
    def get_remaining(self, ip: str) -> int:
        """Возвращает оставшееся количество запросов"""
        key = self._get_key(ip)
        current = self.redis_client.get(key)
        if current is None:
            return self.limit
        return max(0, self.limit - int(current))


# Singleton instance
rate_limiter = RateLimiter()


def check_rate_limit_http(request: Request):
    """
    Dependency для FastAPI - проверяет rate limit
    
    Raises:
        HTTPException: 429 Too Many Requests если лимит исчерпан
    """
    # Получаем IP клиента
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        ip = forwarded.split(",")[0].strip()
    else:
        ip = request.client.host
    
    result = rate_limiter.check_rate_limit(ip)
    
    if not result['allowed']:
        minutes = result['reset_in'] // 60
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Rate limit exceeded. Try again in {minutes} minutes.",
                "reset_in": result['reset_in'],
                "limit": rate_limiter.limit
            }
        )
    
    return result
