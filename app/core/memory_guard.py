"""
Lightweight memory guard:
- tracks app activity;
- runs registered cleanup callbacks in background;
- triggers GC when app is idle.
"""
from __future__ import annotations

import gc
import logging
import os
import threading
import time
from typing import Callable, Dict, Any

from app.config import settings

logger = logging.getLogger(__name__)

CleanupCallback = Callable[[float, bool], Dict[str, Any]]


class MemoryGuard:
    def __init__(self) -> None:
        self._callbacks: Dict[str, CleanupCallback] = {}
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_activity_ts = time.time()
        self._last_gc_ts = 0.0

    @property
    def sweep_interval_sec(self) -> int:
        return max(5, int(getattr(settings, "MEMORY_SWEEP_INTERVAL_SEC", 60) or 60))

    @property
    def idle_cleanup_sec(self) -> int:
        return max(self.sweep_interval_sec, int(getattr(settings, "MEMORY_IDLE_CLEANUP_SEC", 300) or 300))

    @property
    def gc_cooldown_sec(self) -> int:
        return max(30, int(getattr(settings, "MEMORY_GC_COOLDOWN_SEC", 120) or 120))

    def mark_activity(self, source: str = "unknown") -> None:
        self._last_activity_ts = time.time()
        logger.debug("MemoryGuard activity: %s", source)

    def register_cleanup(self, name: str, callback: CleanupCallback) -> None:
        with self._lock:
            self._callbacks[name] = callback

    def unregister_cleanup(self, name: str) -> None:
        with self._lock:
            self._callbacks.pop(name, None)

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._loop, name="memory-guard", daemon=True)
            self._thread.start()
            logger.info(
                "MemoryGuard started (interval=%ss, idle_cleanup=%ss, gc_cooldown=%ss)",
                self.sweep_interval_sec,
                self.idle_cleanup_sec,
                self.gc_cooldown_sec,
            )

    def stop(self) -> None:
        with self._lock:
            if not self._thread:
                return
            self._stop_event.set()
            self._thread.join(timeout=2.0)
            self._thread = None
            logger.info("MemoryGuard stopped")

    def _loop(self) -> None:
        while not self._stop_event.wait(self.sweep_interval_sec):
            now = time.time()
            idle_seconds = max(0.0, now - self._last_activity_ts)
            aggressive = idle_seconds >= self.idle_cleanup_sec
            self.run_cleanup(idle_seconds=idle_seconds, aggressive=aggressive)

    def run_cleanup(self, idle_seconds: float, aggressive: bool, force_gc: bool = False) -> Dict[str, Any]:
        summaries: Dict[str, Any] = {}
        with self._lock:
            callbacks = list(self._callbacks.items())

        for name, callback in callbacks:
            try:
                summaries[name] = callback(idle_seconds, aggressive)
            except Exception as exc:
                logger.warning("MemoryGuard callback failed (%s): %s", name, exc)
                summaries[name] = {"error": str(exc)}

        should_gc = force_gc or aggressive
        now = time.time()
        if should_gc and (now - self._last_gc_ts >= self.gc_cooldown_sec):
            reclaimed = gc.collect()
            self._last_gc_ts = now
            summaries["gc"] = {"collected_objects": reclaimed}

        return summaries

    def get_status(self) -> Dict[str, Any]:
        now = time.time()
        return {
            "sweep_interval_sec": self.sweep_interval_sec,
            "idle_cleanup_sec": self.idle_cleanup_sec,
            "gc_cooldown_sec": self.gc_cooldown_sec,
            "idle_seconds": round(max(0.0, now - self._last_activity_ts), 2),
            "last_gc_seconds_ago": round(max(0.0, now - self._last_gc_ts), 2) if self._last_gc_ts else None,
            "callbacks": list(self._callbacks.keys()),
        }


def _read_linux_rss_bytes() -> int | None:
    statm = "/proc/self/statm"
    if not os.path.exists(statm):
        return None
    try:
        with open(statm, "r", encoding="utf-8") as f:
            parts = f.read().strip().split()
        if len(parts) < 2:
            return None
        rss_pages = int(parts[1])
        return rss_pages * os.sysconf("SC_PAGE_SIZE")
    except Exception:
        return None


_guard = MemoryGuard()


def register_cleanup_callback(name: str, callback: CleanupCallback) -> None:
    _guard.register_cleanup(name, callback)


def unregister_cleanup_callback(name: str) -> None:
    _guard.unregister_cleanup(name)


def mark_activity(source: str = "unknown") -> None:
    _guard.mark_activity(source)


def start_memory_guard() -> None:
    _guard.start()


def stop_memory_guard() -> None:
    _guard.stop()


def run_memory_cleanup_now(force_gc: bool = False) -> Dict[str, Any]:
    status = _guard.get_status()
    return _guard.run_cleanup(
        idle_seconds=float(status.get("idle_seconds") or 0.0),
        aggressive=bool((status.get("idle_seconds") or 0) >= _guard.idle_cleanup_sec),
        force_gc=force_gc,
    )


def get_process_memory_snapshot() -> Dict[str, Any]:
    rss_bytes = _read_linux_rss_bytes()
    return {
        "rss_bytes": rss_bytes,
        "rss_mb": round(rss_bytes / (1024 * 1024), 2) if rss_bytes is not None else None,
    }


def get_memory_guard_status() -> Dict[str, Any]:
    return _guard.get_status()
