"""Standalone Redis worker for LLM crawler jobs."""
from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict

from app.config import settings

from .queue import (
    get_job_record,
    pop_job,
    queue_depth,
    set_worker_heartbeat,
    update_job_record,
)
from .service import run_llm_crawler_simulation


def _log(payload: Dict[str, Any]) -> None:
    print("[LLM_WORKER] " + json.dumps(payload, ensure_ascii=False))


def _process_job(job_id: str) -> None:
    record = get_job_record(job_id)
    if not record:
        _log({"event": "job_missing", "jobId": job_id})
        return

    request_id = str(record.get("requestId") or "")
    requested_url = str(record.get("requested_url") or "")
    options = dict(record.get("options") or {})
    started = time.perf_counter()
    _log({"event": "job_started", "jobId": job_id, "requestId": request_id, "url": requested_url})
    try:
        update_job_record(job_id, status="running", progress=5, error=None)

        def _progress(progress: int, message: str) -> None:
            update_job_record(job_id, status="running", progress=progress, status_message=message)

        result = run_llm_crawler_simulation(
            requested_url=requested_url,
            options=options,
            request_id=request_id,
            progress_callback=_progress,
        )
        duration = int((time.perf_counter() - started) * 1000)
        update_job_record(job_id, status="done", progress=100, result=result, error=None, duration_ms=duration)
        _log(
            {
                "event": "job_done",
                "jobId": job_id,
                "requestId": request_id,
                "duration_ms": duration,
                "score": (((result or {}).get("score") or {}).get("total")),
            }
        )
    except Exception as exc:
        duration = int((time.perf_counter() - started) * 1000)
        update_job_record(
            job_id,
            status="error",
            progress=100,
            error=str(exc),
            duration_ms=duration,
        )
        _log(
            {
                "event": "job_error",
                "jobId": job_id,
                "requestId": request_id,
                "duration_ms": duration,
                "error": str(exc),
            }
        )


def _worker_loop(worker_id: int) -> None:
    _log({"event": "worker_thread_started", "worker": worker_id})
    while True:
        message = pop_job(timeout_sec=5)
        set_worker_heartbeat({"queue_depth": queue_depth(), "worker": worker_id})
        if not message:
            continue
        job_id = str(message.get("jobId") or "").strip()
        if not job_id:
            continue
        _process_job(job_id)


def run_worker() -> None:
    concurrency = max(1, min(8, int(getattr(settings, "JOB_CONCURRENCY", 2) or 2)))
    _log({"event": "worker_boot", "concurrency": concurrency, "queue_depth": queue_depth()})
    threads = []
    for idx in range(concurrency):
        thread = threading.Thread(target=_worker_loop, args=(idx + 1,), daemon=True)
        thread.start()
        threads.append(thread)
    try:
        while True:
            set_worker_heartbeat({"queue_depth": queue_depth(), "concurrency": concurrency})
            time.sleep(10)
    except KeyboardInterrupt:
        _log({"event": "worker_shutdown"})


if __name__ == "__main__":
    run_worker()

