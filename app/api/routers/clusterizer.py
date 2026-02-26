"""
Keyword Clusterizer router.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, field_validator

from app.api.routers._task_store import create_task_pending, update_task_state

router = APIRouter(tags=["SEO Tools"])


def _parse_clusterizer_frequency(value: Any) -> Optional[float]:
    from app.tools.clusterizer.input_parser import parse_clusterizer_frequency

    return parse_clusterizer_frequency(value)


def _split_clusterizer_text_rows(raw_text: str) -> List[str]:
    from app.tools.clusterizer.input_parser import split_clusterizer_text_rows

    return split_clusterizer_text_rows(raw_text)


def _parse_clusterizer_keyword_line(raw_line: str) -> List[Dict[str, Any]]:
    from app.tools.clusterizer.input_parser import parse_clusterizer_keyword_line

    return parse_clusterizer_keyword_line(raw_line)


def _collect_clusterizer_keyword_rows(
    keywords: Optional[List[str]], keywords_text: Optional[str]
) -> List[Dict[str, Any]]:
    from app.tools.clusterizer.input_parser import collect_clusterizer_keyword_rows

    return collect_clusterizer_keyword_rows(keywords, keywords_text)


def check_keywords_clusterizer(
    *,
    keywords: Optional[List[str]] = None,
    keyword_rows: Optional[List[Dict[str, Any]]] = None,
    method: str = "jaccard",
    similarity_threshold: float = 0.35,
    min_cluster_size: int = 2,
    clustering_mode: str = "balanced",
    progress_callback=None,
) -> Dict[str, Any]:
    """Basic keyword clusterizer without SERP data."""
    from app.tools.clusterizer import run_keyword_clusterizer

    return run_keyword_clusterizer(
        keywords=keywords or [],
        keyword_rows=keyword_rows or [],
        method=method,
        similarity_threshold=similarity_threshold,
        min_cluster_size=min_cluster_size,
        clustering_mode=clustering_mode,
        progress_callback=progress_callback,
    )


class ClusterizerRequest(BaseModel):
    keywords: Optional[List[str]] = None
    keywords_text: Optional[str] = None
    method: Optional[str] = "jaccard"
    clustering_mode: Optional[str] = "balanced"
    similarity_threshold_pct: int = 35
    min_cluster_size: int = 2

    @field_validator("keywords", mode="before")
    @classmethod
    def _normalize_keywords(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            return [x.strip() for x in str(value).replace("\r", "\n").split("\n") if x.strip()]
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return []

    @field_validator("keywords_text", mode="before")
    @classmethod
    def _normalize_keywords_text(cls, value):
        if value is None:
            return ""
        return str(value).strip()

    @field_validator("clustering_mode", mode="before")
    @classmethod
    def _normalize_clustering_mode(cls, value):
        if value is None:
            return "balanced"
        return str(value).strip().lower()


@router.post("/tasks/clusterizer")
async def create_clusterizer_task(data: ClusterizerRequest, background_tasks: BackgroundTasks):
    """Keyword clustering by textual similarity without search-engine fetching."""
    from app.config import settings

    keyword_rows = _collect_clusterizer_keyword_rows(data.keywords, data.keywords_text)
    keywords = [str(item.get("keyword") or "").strip() for item in keyword_rows if str(item.get("keyword") or "").strip()]
    input_demand_total = sum(float(item.get("frequency") or 0.0) for item in keyword_rows)

    max_keywords = max(1, int(getattr(settings, "CLUSTERIZER_MAX_KEYWORDS", 2000) or 2000))
    if not keywords:
        raise HTTPException(status_code=422, detail="Добавьте хотя бы один ключ для кластеризации")
    if len(keyword_rows) > max_keywords:
        raise HTTPException(status_code=422, detail=f"Слишком много ключей: максимум {max_keywords}")

    method = str(data.method or "jaccard").strip().lower()
    if method not in {"jaccard", "overlap", "dice"}:
        method = "jaccard"
    clustering_mode = str(data.clustering_mode or "balanced").strip().lower()
    if clustering_mode not in {"strict", "balanced", "broad"}:
        clustering_mode = "balanced"
    similarity_threshold_pct = max(1, min(100, int(data.similarity_threshold_pct or 35)))
    min_cluster_size = max(1, min(50, int(data.min_cluster_size or 2)))
    similarity_threshold = similarity_threshold_pct / 100.0

    task_id = f"clusterizer-{datetime.now().timestamp()}"
    create_task_pending(
        task_id,
        "clusterizer",
        f"keywords:{len(keyword_rows)}",
        status_message="Задача поставлена в очередь",
    )

    def _run_clusterizer_task() -> None:
        try:
            update_task_state(
                task_id,
                status="RUNNING",
                progress=5,
                status_message="Подготовка ключей",
            )

            def _progress(progress: int, message: str) -> None:
                update_task_state(
                    task_id,
                    status="RUNNING",
                    progress=progress,
                    status_message=message,
                )

            result = check_keywords_clusterizer(
                keywords=[],
                keyword_rows=keyword_rows,
                method=method,
                similarity_threshold=similarity_threshold,
                min_cluster_size=min_cluster_size,
                clustering_mode=clustering_mode,
                progress_callback=_progress,
            )

            result_payload = result.get("results", {}) if isinstance(result, dict) else {}
            summary_payload = result_payload.get("summary", {}) if isinstance(result_payload, dict) else {}
            summary_payload["input_demand_total"] = round(
                float(summary_payload.get("input_demand_total") or input_demand_total), 4
            )
            if isinstance(result_payload, dict):
                result_payload["summary"] = summary_payload
            if isinstance(result, dict):
                result["results"] = result_payload

            update_task_state(
                task_id,
                status="SUCCESS",
                progress=100,
                status_message="Кластеризация завершена",
                result=result,
                error=None,
            )
        except Exception as exc:
            update_task_state(
                task_id,
                status="FAILURE",
                progress=100,
                status_message="Кластеризация завершилась с ошибкой",
                error=str(exc),
            )

    background_tasks.add_task(_run_clusterizer_task)
    return {"task_id": task_id, "status": "PENDING", "message": "Кластеризация ключей запущена"}


@router.post("/tasks/clusterizer/upload")
async def create_clusterizer_task_upload(
    background_tasks: BackgroundTasks,
    keywords_file: UploadFile = File(...),
    method: str = Form("jaccard"),
    clustering_mode: str = Form("balanced"),
    similarity_threshold_pct: int = Form(35),
    min_cluster_size: int = Form(2),
):
    """Keyword clustering from uploaded CSV or XLSX file (columns: keyword, frequency)."""
    from app.config import settings
    from app.tools.clusterizer.input_parser import parse_clusterizer_file

    filename = str(keywords_file.filename or "")
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in {"csv", "xlsx"}:
        raise HTTPException(status_code=422, detail="Поддерживаются только файлы .csv и .xlsx")

    file_bytes = await keywords_file.read()
    if not file_bytes:
        raise HTTPException(status_code=422, detail="Файл пустой")

    max_file_size = max(1024, int(getattr(settings, "CLUSTERIZER_MAX_FILE_SIZE_BYTES", 50 * 1024 * 1024) or (50 * 1024 * 1024)))
    if len(file_bytes) > max_file_size:
        raise HTTPException(status_code=422, detail=f"Файл превышает лимит {max_file_size} байт")

    try:
        keyword_rows = parse_clusterizer_file(file_bytes, filename)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Ошибка чтения файла: {exc}") from exc

    keywords = [str(item.get("keyword") or "").strip() for item in keyword_rows if str(item.get("keyword") or "").strip()]
    if not keywords:
        raise HTTPException(status_code=422, detail="Файл не содержит ключевых слов")

    max_keywords = max(1, int(getattr(settings, "CLUSTERIZER_MAX_KEYWORDS", 25000) or 25000))
    if len(keyword_rows) > max_keywords:
        raise HTTPException(status_code=422, detail=f"Слишком много ключей: максимум {max_keywords}")

    _method = str(method or "jaccard").strip().lower()
    if _method not in {"jaccard", "overlap", "dice"}:
        _method = "jaccard"
    _clustering_mode = str(clustering_mode or "balanced").strip().lower()
    if _clustering_mode not in {"strict", "balanced", "broad"}:
        _clustering_mode = "balanced"
    _threshold_pct = max(1, min(100, int(similarity_threshold_pct or 35)))
    _min_cluster_size = max(1, min(50, int(min_cluster_size or 2)))
    _similarity_threshold = _threshold_pct / 100.0
    input_demand_total = sum(float(item.get("frequency") or 0.0) for item in keyword_rows)

    task_id = f"clusterizer-{datetime.now().timestamp()}"
    create_task_pending(
        task_id,
        "clusterizer",
        f"file:{filename}:{len(keyword_rows)}kw",
        status_message="Задача поставлена в очередь",
    )

    def _run_upload_task() -> None:
        try:
            update_task_state(task_id, status="RUNNING", progress=5, status_message="Подготовка ключей из файла")

            def _progress(progress: int, message: str) -> None:
                update_task_state(task_id, status="RUNNING", progress=progress, status_message=message)

            result = check_keywords_clusterizer(
                keywords=[],
                keyword_rows=keyword_rows,
                method=_method,
                similarity_threshold=_similarity_threshold,
                min_cluster_size=_min_cluster_size,
                clustering_mode=_clustering_mode,
                progress_callback=_progress,
            )

            result_payload = result.get("results", {}) if isinstance(result, dict) else {}
            summary_payload = result_payload.get("summary", {}) if isinstance(result_payload, dict) else {}
            summary_payload["input_demand_total"] = round(
                float(summary_payload.get("input_demand_total") or input_demand_total), 4
            )
            if isinstance(result_payload, dict):
                result_payload["summary"] = summary_payload
            if isinstance(result, dict):
                result["results"] = result_payload

            update_task_state(
                task_id,
                status="SUCCESS",
                progress=100,
                status_message="Кластеризация завершена",
                result=result,
                error=None,
            )
        except Exception as exc:
            update_task_state(
                task_id,
                status="FAILURE",
                progress=100,
                status_message="Кластеризация завершилась с ошибкой",
                error=str(exc),
            )

    background_tasks.add_task(_run_upload_task)
    return {"task_id": task_id, "status": "PENDING", "message": f"Кластеризация {len(keyword_rows)} ключей из файла запущена"}
