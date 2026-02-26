"""
Link Profile Audit router.
"""
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, BackgroundTasks, HTTPException, UploadFile, File, Form

from app.validators import normalize_http_input as _normalize_http_input
from app.api.routers._task_store import create_task_pending, update_task_state

router = APIRouter(tags=["SEO Tools"])


@router.post("/tasks/link-profile-audit")
async def create_link_profile_audit(
    background_tasks: BackgroundTasks,
    our_domain: str = Form(...),
    backlink_files: List[UploadFile] = File(...),
    batch_file: Optional[UploadFile] = File(None),
    commercial_keywords: str = Form(""),
    informational_keywords: str = Form(""),
    spam_keywords: str = Form(""),
    brand_keywords: str = Form(""),
):
    """Backlink profile audit from uploaded backlink exports and one batch file."""
    from app.config import settings
    from app.tools.link_profile import run_link_profile_audit

    domain = _normalize_http_input(our_domain) if "://" in str(our_domain or "") else str(our_domain or "").strip()
    if not str(domain).strip():
        raise HTTPException(status_code=422, detail="Укажите домен проекта")
    if not backlink_files:
        raise HTTPException(status_code=422, detail="Добавьте хотя бы один файл бэклинков")
    # batch_file is optional for all-in-one XLSX packs (e.g. test links.xlsx).

    max_backlink_files = max(1, min(200, int(getattr(settings, "LINK_PROFILE_MAX_BACKLINK_FILES", 20) or 20)))
    max_file_size_bytes = max(1024, int(getattr(settings, "LINK_PROFILE_MAX_FILE_SIZE_BYTES", 25 * 1024 * 1024) or (25 * 1024 * 1024)))
    max_batch_file_size_bytes = max(
        1024,
        int(getattr(settings, "LINK_PROFILE_MAX_BATCH_FILE_SIZE_BYTES", 50 * 1024 * 1024) or (50 * 1024 * 1024)),
    )
    max_total_upload_bytes = max(
        max_file_size_bytes,
        int(getattr(settings, "LINK_PROFILE_MAX_TOTAL_UPLOAD_BYTES", 150 * 1024 * 1024) or (150 * 1024 * 1024)),
    )
    if len(backlink_files) > max_backlink_files:
        raise HTTPException(status_code=422, detail=f"Слишком много файлов бэклинков: максимум {max_backlink_files}")

    allowed_ext = (".csv", ".xlsx")
    backlog_payloads: List[tuple[str, bytes]] = []
    total_upload_bytes = 0
    for up in backlink_files:
        name = str(up.filename or "")
        if not name.lower().endswith(allowed_ext):
            raise HTTPException(status_code=422, detail=f"Неподдерживаемый формат файла: {name}")
        blob = await up.read()
        if not blob:
            raise HTTPException(status_code=422, detail=f"Пустой файл: {name}")
        if len(blob) > max_file_size_bytes:
            raise HTTPException(
                status_code=422,
                detail=f"Файл {name} превышает лимит {max_file_size_bytes} байт",
            )
        total_upload_bytes += len(blob)
        if total_upload_bytes > max_total_upload_bytes:
            raise HTTPException(
                status_code=422,
                detail=f"Суммарный размер файлов превышает лимит {max_total_upload_bytes} байт",
            )
        backlog_payloads.append((name, blob))

    batch_name = ""
    batch_payload = b""
    if batch_file and str(batch_file.filename or "").strip():
        batch_name = str(batch_file.filename or "")
        if not batch_name.lower().endswith(allowed_ext):
            raise HTTPException(status_code=422, detail="Batch файл должен быть .csv или .xlsx")
        batch_payload = await batch_file.read()
        if not batch_payload:
            raise HTTPException(status_code=422, detail="Batch файл пустой")
        if len(batch_payload) > max_batch_file_size_bytes:
            raise HTTPException(
                status_code=422,
                detail=f"Batch файл превышает лимит {max_batch_file_size_bytes} байт",
            )
        total_upload_bytes += len(batch_payload)
        if total_upload_bytes > max_total_upload_bytes:
            raise HTTPException(
                status_code=422,
                detail=f"Суммарный размер файлов превышает лимит {max_total_upload_bytes} байт",
            )

    task_id = f"link-profile-{datetime.now().timestamp()}"
    create_task_pending(task_id, "link_profile_audit", str(our_domain or "").strip(), status_message="Задача поставлена в очередь")

    def _run_link_profile_task() -> None:
        try:
            update_task_state(task_id, status="RUNNING", progress=10, status_message="Подготовка данных для анализа")

            def _progress(progress: int, message: str) -> None:
                update_task_state(
                    task_id,
                    status="RUNNING",
                    progress=progress,
                    status_message=message,
                )

            result = run_link_profile_audit(
                our_domain=str(our_domain or "").strip(),
                backlink_files=backlog_payloads,
                batch_file=(batch_name, batch_payload) if batch_name else None,
                commercial_keywords=commercial_keywords,
                informational_keywords=informational_keywords,
                spam_keywords=spam_keywords,
                brand_keywords=brand_keywords,
                progress_callback=_progress,
            )
            update_task_state(
                task_id,
                status="SUCCESS",
                progress=100,
                status_message="Аудит ссылочного профиля завершен",
                result=result,
                error=None,
            )
        except Exception as exc:
            update_task_state(
                task_id,
                status="FAILURE",
                progress=100,
                status_message="Аудит ссылочного профиля завершился с ошибкой",
                error=str(exc),
            )

    background_tasks.add_task(_run_link_profile_task)
    return {"task_id": task_id, "status": "PENDING", "message": "Аудит ссылочного профиля запущен"}
