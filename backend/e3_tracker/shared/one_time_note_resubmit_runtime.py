import io
import json
import mimetypes
import os
import secrets
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

from e3_tracker.shared.config import load_env_defaults


RUN_ID = "20260826-data-structures-resubmit-v1"
STATUS_ROUTE = "/healthz/note-resubmit-20260826-ds-v1"
SUBJECT = "資料結構"
TERMINAL_STATES = {"complete", "failed"}


def _write_status(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)


def _read_status(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {"run_id": RUN_ID, "status": "pending"}
    return payload if isinstance(payload, dict) else {"run_id": RUN_ID, "status": "pending"}


def _stage_images(client: Any, image_paths: List[Path], base_url: str) -> str:
    upload_id = ""
    for image_index, image_path in enumerate(image_paths, start=1):
        response = client.post(
            "/admin/study-recall/upload-staging",
            data={
                "upload_id": upload_id,
                "image_index": str(image_index),
                "total_images": str(len(image_paths)),
                "note_image": (
                    io.BytesIO(image_path.read_bytes()),
                    image_path.name,
                    mimetypes.guess_type(image_path.name)[0] or "image/jpeg",
                ),
            },
            headers={"X-E3-Study-Upload": "1", "X-E3-Study-Reprocess": "1"},
            content_type="multipart/form-data",
            base_url=base_url,
        )
        payload = response.get_json(silent=True) or {}
        if response.status_code == 409:
            raise RuntimeError("ACTIVE_UPLOAD")
        if response.status_code != 200 or not payload.get("ok"):
            raise RuntimeError(str(payload.get("error") or "source image staging failed"))
        upload_id = str(payload.get("upload_id") or "")
    if not upload_id:
        raise RuntimeError("source image staging returned no upload id")
    return upload_id


def _validate_result(storage: Any, session_id: int, image_count: int) -> Dict[str, Any]:
    note = storage.get_study_recall_session(session_id)
    if not note:
        raise RuntimeError("new note was not saved")
    images = [str(name) for name in note.get("image_filenames") or []]
    cards = [card for card in note.get("key_concepts") or [] if isinstance(card, dict)]
    pages = [page for page in note.get("source_transcription") or [] if isinstance(page, dict)]
    if len(images) != image_count:
        raise RuntimeError("new note image count does not match the source")
    if len(pages) != image_count:
        raise RuntimeError("new note page transcription is incomplete")
    if not cards:
        raise RuntimeError("new note contains no study cards")
    return {
        "new_session_id": session_id,
        "image_count": len(images),
        "card_count": len(cards),
        "page_count": len(pages),
    }


def _run_once(app: Any, status_path: Path, lock_path: Path, upload_root: Path) -> None:
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(lock_fd)
    except FileExistsError:
        return

    storage = app.extensions.get("e3_storage")
    try:
        _write_status(status_path, {"run_id": RUN_ID, "status": "selecting_source"})
        rows = storage.list_study_recall_sessions(limit=None)
        source_row = next((row for row in rows if str(row.get("subject") or "") == SUBJECT), None)
        if not source_row:
            raise RuntimeError("no existing Data Structures note was found")
        source_id = int(source_row["id"])
        source = storage.get_study_recall_session(source_id)
        if not source:
            raise RuntimeError("the selected source note disappeared")
        filenames = [str(name) for name in source.get("image_filenames") or []]
        image_paths = [upload_root / str(source_id) / filename for filename in filenames]
        missing = [path.name for path in image_paths if not path.is_file()]
        if not image_paths or missing:
            raise RuntimeError(f"source images are missing: {', '.join(missing) or 'all'}")

        config = load_env_defaults()
        username = str(config.get("admin_user_id") or "112550103")
        canonical_host = str(config.get("canonical_host") or "www.e3hwtool.space").strip()
        base_url = f"https://{canonical_host}"
        token = f"one-time-resubmit-{secrets.token_urlsafe(24)}"
        storage.save_web_session(token, username)
        client = app.test_client()
        with client.session_transaction(base_url=base_url) as flask_session:
            flask_session.update(
                username=username,
                session_token=token,
                is_admin=True,
                is_guest=False,
            )

        _write_status(
            status_path,
            {
                "run_id": RUN_ID,
                "status": "waiting_for_upload_slot",
                "source_session_id": source_id,
                "image_count": len(image_paths),
            },
        )
        deadline = time.monotonic() + 7200
        while True:
            try:
                upload_id = _stage_images(client, image_paths, base_url)
                break
            except RuntimeError as exc:
                if str(exc) != "ACTIVE_UPLOAD" or time.monotonic() >= deadline:
                    raise
                time.sleep(10)

        response = client.post(
            "/admin/study-recall/upload",
            data={
                "upload_id": upload_id,
                "study_date": source.get("study_date") or "",
                "subject": SUBJECT,
                "title": source.get("title") or "",
                "allow_corrections": "1",
            },
            headers={"X-E3-Study-Upload": "1", "X-E3-Study-Reprocess": "1"},
            base_url=base_url,
        )
        payload = response.get_json(silent=True) or {}
        if response.status_code != 202 or not payload.get("job_id"):
            raise RuntimeError(str(payload.get("error") or "failed to start note processing"))
        if "已有一份" in str(payload.get("message") or ""):
            raise RuntimeError("another upload started before the resubmission")
        job_id = str(payload["job_id"])
        last_progress = -1
        while time.monotonic() < deadline:
            job_response = client.get(
                f"/admin/study-recall/upload-jobs/{job_id}",
                base_url=base_url,
            )
            job = job_response.get_json(silent=True) or {}
            if job_response.status_code != 200 or not job.get("ok"):
                raise RuntimeError(str(job.get("error") or "note processing job disappeared"))
            progress = int(job.get("progress") or 0)
            if progress != last_progress:
                _write_status(
                    status_path,
                    {
                        "run_id": RUN_ID,
                        "status": "processing",
                        "source_session_id": source_id,
                        "image_count": len(image_paths),
                        "progress": progress,
                    },
                )
                last_progress = progress
            if str(job.get("status") or "running") != "running":
                if job.get("status") != "success" or not job.get("session_id"):
                    raise RuntimeError(str(job.get("message") or "note processing failed"))
                validation = _validate_result(storage, int(job["session_id"]), len(image_paths))
                _write_status(
                    status_path,
                    {
                        "run_id": RUN_ID,
                        "status": "complete",
                        "source_session_id": source_id,
                        **validation,
                    },
                )
                return
            time.sleep(5)
        raise RuntimeError("note processing exceeded two hours")
    except Exception as exc:
        _write_status(
            status_path,
            {
                "run_id": RUN_ID,
                "status": "failed",
                "error": str(exc)[:240],
            },
        )


def install_one_time_note_resubmit_runtime(web_module: Any) -> None:
    if getattr(web_module, "__e3_one_time_note_resubmit_installed", False):
        return
    web_module.__e3_one_time_note_resubmit_installed = True
    original_create_app = web_module.create_app

    def create_app_with_resubmit(*args: Any, **kwargs: Any):
        app = original_create_app(*args, **kwargs)
        config = load_env_defaults()
        upload_root = Path(str(config.get("study_upload_dir") or "")).expanduser()
        state_root = upload_root / "_one_time_jobs"
        status_path = state_root / f"{RUN_ID}.json"
        lock_path = state_root / f"{RUN_ID}.lock"

        @app.get(STATUS_ROUTE)
        def one_time_note_resubmit_status():
            payload = _read_status(status_path)
            public_payload = {
                key: value
                for key, value in payload.items()
                if key
                in {
                    "run_id",
                    "status",
                    "source_session_id",
                    "new_session_id",
                    "image_count",
                    "card_count",
                    "page_count",
                    "progress",
                    "error",
                }
            }
            return public_payload, 200, {"Cache-Control": "no-store, max-age=0"}

        if os.getenv("RAILWAY_ENVIRONMENT") and _read_status(status_path).get("status") not in TERMINAL_STATES:
            threading.Thread(
                target=_run_once,
                args=(app, status_path, lock_path, upload_root),
                daemon=True,
            ).start()
        return app

    web_module.create_app = create_app_with_resubmit
