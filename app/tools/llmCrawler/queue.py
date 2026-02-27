"""Redis queue and job storage for LLM Crawler."""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover - environment dependent
    redis = None

from app.config import settings


_redis_client: Optional[Any] = None
_redis_retry_after_ts: float = 0.0
_mem_jobs: Dict[str, Dict[str, Any]] = {}
_mem_queue: list[Dict[str, Any]] = []


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_redis_client() -> Optional[Any]:
    global _redis_client, _redis_retry_after_ts
    if redis is None:
        return None
    now_ts = time.time()
    if _redis_client is not None:
        try:
            _redis_client.ping()
            return _redis_client
        except Exception:
            _redis_client = None
            _redis_retry_after_ts = now_ts + 5.0
            return None
    if now_ts < _redis_retry_after_ts:
        return None
    try:
        _redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
        _redis_client.ping()
        _redis_retry_after_ts = 0.0
    except Exception:
        _redis_client = None
        _redis_retry_after_ts = now_ts + 5.0
    return _redis_client


def queue_key() -> str:
    return str(getattr(settings, "LLM_CRAWLER_QUEUE_KEY", "llmCrawler:queue") or "llmCrawler:queue")


def job_key(job_id: str) -> str:
    return f"llmCrawler:job:{job_id}"


def _job_ttl() -> int:
    return max(3600, int(getattr(settings, "LLM_CRAWLER_JOB_TTL_SEC", 72 * 3600) or (72 * 3600)))


def new_job_id() -> str:
    return f"llmcrawler-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"


def create_job_record(
    *,
    job_id: str,
    request_id: str,
    requested_url: str,
    options: Dict[str, Any],
    status_message: str = "Queued",
) -> Dict[str, Any]:
    return {
        "jobId": job_id,
        "requestId": request_id,
        "status": "queued",
        "progress": 0,
        "status_message": status_message,
        "requested_url": requested_url,
        "options": options,
        "result": None,
        "error": None,
        "createdAt": _utc_now(),
        "updatedAt": _utc_now(),
    }


def save_job_record(job: Dict[str, Any]) -> None:
    client = get_redis_client()
    payload = dict(job)
    payload["updatedAt"] = _utc_now()
    if not client:
        _mem_jobs[str(job.get("jobId") or "")] = payload
        return
    client.setex(job_key(str(job.get("jobId") or "")), _job_ttl(), json.dumps(payload))


def get_job_record(job_id: str) -> Optional[Dict[str, Any]]:
    client = get_redis_client()
    if not client:
        return _mem_jobs.get(job_id)
    try:
        raw = client.get(job_key(job_id))
        if not raw:
            return _mem_jobs.get(job_id)
        return json.loads(raw)
    except Exception:
        return _mem_jobs.get(job_id)


def update_job_record(job_id: str, **fields: Any) -> Dict[str, Any]:
    current = get_job_record(job_id) or {
        "jobId": job_id,
        "status": "queued",
        "progress": 0,
        "createdAt": _utc_now(),
        "updatedAt": _utc_now(),
    }
    current.update(fields)
    if "progress" in current:
        try:
            current["progress"] = max(0, min(100, int(current["progress"])))
        except Exception:
            current["progress"] = 0
    save_job_record(current)
    return current


def enqueue_job(job: Dict[str, Any]) -> str:
    client = get_redis_client()
    save_job_record(job)
    if not client:
        _mem_queue.append({"jobId": job["jobId"], "requestId": job.get("requestId", "")})
        return str(job["jobId"])
    client.lpush(queue_key(), json.dumps({"jobId": job["jobId"], "requestId": job.get("requestId", "")}))
    return str(job["jobId"])


def pop_job(timeout_sec: int = 5) -> Optional[Dict[str, Any]]:
    client = get_redis_client()
    if not client:
        return _mem_queue.pop(0) if _mem_queue else None
    try:
        item = client.brpop(queue_key(), timeout=max(1, int(timeout_sec)))
        if not item:
            return None
        return json.loads(item[1])
    except Exception:
        return None


def queue_depth() -> int:
    client = get_redis_client()
    if not client:
        return len(_mem_queue)
    try:
        return int(client.llen(queue_key()) or 0)
    except Exception:
        return len(_mem_queue)


def check_rate_limit(subject: str, bucket: str, limit: int, window_sec: int) -> Dict[str, Any]:
    client = get_redis_client()
    if not client:
        return {"allowed": True, "remaining": limit, "reset_in": window_sec}
    safe_limit = max(1, int(limit))
    safe_window = max(1, int(window_sec))
    key = f"llmCrawler:rate:{bucket}:{subject}"
    try:
        current = int(client.incr(key))
        if current == 1:
            client.expire(key, safe_window)
        ttl = int(client.ttl(key) or safe_window)
        return {
            "allowed": current <= safe_limit,
            "remaining": max(0, safe_limit - current),
            "reset_in": max(0, ttl),
        }
    except Exception:
        return {"allowed": True, "remaining": safe_limit, "reset_in": safe_window}


def set_worker_heartbeat(extra: Optional[Dict[str, Any]] = None) -> None:
    client = get_redis_client()
    if not client:
        return
    payload: Dict[str, Any] = {"updatedAt": _utc_now()}
    if extra:
        payload.update(extra)
    ttl = max(30, int(getattr(settings, "LLM_CRAWLER_WORKER_HEARTBEAT_TTL_SEC", 120) or 120))
    try:
        key = str(
            getattr(
                settings,
                "LLM_CRAWLER_WORKER_HEARTBEAT_KEY",
                "llmCrawler:worker:heartbeat",
            )
            or "llmCrawler:worker:heartbeat"
        )
        client.setex(key, ttl, json.dumps(payload))
    except Exception:
        return


def get_worker_heartbeat() -> Optional[Dict[str, Any]]:
    client = get_redis_client()
    if not client:
        return None
    key = str(
        getattr(
            settings,
            "LLM_CRAWLER_WORKER_HEARTBEAT_KEY",
            "llmCrawler:worker:heartbeat",
        )
        or "llmCrawler:worker:heartbeat"
    )
    try:
        raw = client.get(key)
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None
