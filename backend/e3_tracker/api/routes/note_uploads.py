"""Note uploads routes and their feature helpers."""

from ...services.study_upload_batches import STUDY_UPLOAD_ANALYSIS_START_PROGRESS
import secrets
import shutil
import threading
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from flask import flash, redirect, request, url_for
from werkzeug.utils import secure_filename


def register_note_uploads_routes(*,
    STUDY_NOTE_MAX_IMAGE_BYTES,
    STUDY_PLAN_SUBJECTS,
    _NOTE_IMAGE_MIME_TYPES,
    _StudyUploadCancelled,
    _active_study_upload_job,
    _analyze_study_note_images,
    _build_recall_widget_context,
    _cleanup_expired_study_upload_staging,
    _ensure_private_dir,
    _find_study_upload_staging_for_job,
    _is_study_upload_request,
    _raise_if_study_upload_cancelled,
    _read_study_upload_manifest,
    _rebuild_all_study_recall_relations,
    _remove_study_upload_staging,
    _set_study_upload_job,
    _study_plan_business_date,
    _study_upload_error,
    _study_upload_job_can_resume,
    _study_upload_staging_directory,
    _write_study_upload_manifest,
    admin_required,
    app,
    current_user,
    record_ui_event,
    storage,
    study_relation_rebuild_lock,
    study_upload_context,
    study_upload_jobs,
    study_upload_jobs_lock,
    study_upload_root,
    study_upload_staging_lock,
):
    @app.post("/admin/study-recall/<int:session_id>/delete")
    @admin_required
    def admin_study_recall_delete(session_id: int):
        recall_session = storage.get_study_recall_session(session_id)
        if not recall_session:
            flash("找不到這份筆記紀錄。", "error")
            return redirect(url_for("admin_study_recall"))
        if not storage.delete_study_recall_session(session_id):
            flash("筆記刪除失敗，請再試一次。", "error")
            return redirect(url_for("admin_study_recall", session_id=session_id))
        upload_root = study_upload_root.resolve()
        image_directory = (upload_root / str(session_id)).resolve()
        if image_directory.parent == upload_root and image_directory.is_dir():
            try:
                shutil.rmtree(image_directory)
            except OSError:
                pass
        record_ui_event("study_recall_note_deleted", meta={"session_id": session_id, "subject": recall_session.get("subject")})
        flash("已刪除筆記、所屬重點卡、複習紀錄與原始圖片。", "success")
        return redirect(url_for("admin_study_recall"))

    @app.post("/admin/study-recall/<int:session_id>/rename")
    @admin_required
    def admin_study_recall_rename(session_id: int):
        recall_session = storage.get_study_recall_session(session_id)
        if not recall_session:
            flash("找不到這份筆記紀錄。", "error")
            return redirect(url_for("admin_study_recall"))
        title = " ".join(str(request.form.get("title") or "").split()).strip()
        if not title:
            flash("筆記名稱不能留空。", "error")
            return redirect(url_for("admin_study_recall", session_id=session_id))
        if len(title) > 120:
            flash("筆記名稱最多 120 個字。", "error")
            return redirect(url_for("admin_study_recall", session_id=session_id))
        if not storage.rename_study_recall_session(session_id, title):
            flash("筆記名稱修改失敗，請再試一次。", "error")
            return redirect(url_for("admin_study_recall", session_id=session_id))
        record_ui_event(
            "study_recall_note_renamed",
            meta={"session_id": session_id, "subject": recall_session.get("subject")},
        )
        flash("筆記名稱已更新。", "success")
        return redirect(url_for("admin_study_recall", session_id=session_id))

    @app.post("/admin/study-recall/upload-staging")
    @admin_required
    def admin_study_recall_upload_staging():
        user = current_user() or {}
        username = str(user.get("username") or "")
        if _active_study_upload_job(username):
            return _study_upload_error("已有一份筆記正在背景整理，請完成或取消後再上傳下一份。", 409)
        with study_upload_staging_lock:
            _cleanup_expired_study_upload_staging()
        try:
            image_index = int(request.form.get("image_index") or 0)
            total_images = int(request.form.get("total_images") or 0)
        except (TypeError, ValueError):
            return _study_upload_error("圖片上傳順序資料不正確。")
        if total_images < 1 or image_index < 1 or image_index > total_images:
            return _study_upload_error("圖片上傳順序資料不正確。")
        item = request.files.get("note_image")
        if not item or not item.filename:
            return _study_upload_error("找不到要上傳的筆記照片。")
        filename = secure_filename(item.filename) or "note-image"
        extension = Path(filename).suffix.lower()
        mime_type = _NOTE_IMAGE_MIME_TYPES.get(extension)
        if not mime_type:
            return _study_upload_error("筆記僅支援 JPG、PNG、WEBP 或 GIF 圖片。")
        image_bytes = item.stream.read(STUDY_NOTE_MAX_IMAGE_BYTES + 1)
        if not image_bytes or len(image_bytes) > STUDY_NOTE_MAX_IMAGE_BYTES:
            return _study_upload_error("每張筆記照片壓縮後必須小於 2MB。")

        upload_id = str(request.form.get("upload_id") or "").strip()
        with study_upload_staging_lock:
            manifest: Optional[Dict[str, Any]] = None
            directory: Optional[Path] = None
            if upload_id:
                manifest, directory = _read_study_upload_manifest(upload_id, username)
                if manifest is None or directory is None:
                    return _study_upload_error("這次暫存上傳已失效，請重新選擇照片。", 404)
                if int(manifest.get("expected_count") or 0) != total_images:
                    return _study_upload_error("圖片總數與這次暫存上傳不一致。")
            else:
                upload_id = secrets.token_urlsafe(24)
                directory = _study_upload_staging_directory(upload_id)
                if directory is None:
                    return _study_upload_error("無法建立圖片暫存空間。", 500)
                _ensure_private_dir(directory)
                manifest = {
                    "username": username,
                    "expected_count": total_images,
                    "files": {},
                    "created_at": time.time(),
                }
            assert manifest is not None and directory is not None
            stored_name = f"{image_index:06d}{extension}"
            (directory / stored_name).write_bytes(image_bytes)
            files = manifest.get("files") if isinstance(manifest.get("files"), dict) else {}
            files[str(image_index)] = {
                "stored_name": stored_name,
                "original_name": filename,
                "mime_type": mime_type,
                "size": len(image_bytes),
            }
            manifest["files"] = files
            manifest["updated_at"] = time.time()
            _write_study_upload_manifest(directory, manifest)
        return {
            "ok": True,
            "upload_id": upload_id,
            "uploaded_count": len(files),
            "total_images": total_images,
        }

    @app.post("/admin/study-recall/upload-staging/<upload_id>/cancel")
    @admin_required
    def admin_study_recall_cancel_upload_staging(upload_id: str):
        user = current_user() or {}
        username = str(user.get("username") or "")
        with study_upload_staging_lock:
            removed = _remove_study_upload_staging(upload_id, username)
        return {"ok": True, "removed": removed}

    def _launch_study_note_upload_job(
        *,
        username: str,
        study_date: str,
        subject: str,
        requested_title: str,
        allow_corrections: bool,
        skip_relation_rebuild: bool,
        images: List[Tuple[str, Any, str]],
        staging_directory: Optional[Path],
        staging_upload_id: str,
        existing_job_id: Optional[str] = None,
    ) -> str:
        job_id = existing_job_id or secrets.token_urlsafe(18)
        now = time.time()
        cancel_event = threading.Event()
        with study_upload_jobs_lock:
            previous = study_upload_jobs.get(job_id) or {}
            study_upload_jobs[job_id] = {
                "username": username,
                "status": "running",
                "progress": STUDY_UPLOAD_ANALYSIS_START_PROGRESS,
                "message": "照片已接收，正在從最近的完成點開始整理。",
                "cancel_event": cancel_event,
                "created_at": float(previous.get("created_at") or now),
                "updated_at": now,
            }
        _set_study_upload_job(job_id)

        if staging_directory is not None and staging_directory.is_dir():
            with study_upload_staging_lock:
                manifest, _directory = _read_study_upload_manifest(staging_upload_id, username)
                if manifest is not None:
                    manifest.update(
                        {
                            "job_id": job_id,
                            "study_date": study_date,
                            "subject": subject,
                            "requested_title": requested_title,
                            "allow_corrections": bool(allow_corrections),
                            "skip_relation_rebuild": bool(skip_relation_rebuild),
                            "updated_at": now,
                        }
                    )
                    _write_study_upload_manifest(staging_directory, manifest)

        def _run_study_upload() -> None:
            recall_id: Optional[int] = None
            destination: Optional[Path] = None
            remove_staging_after_run = False
            study_upload_context.cancel_event = cancel_event
            study_upload_context.job_id = job_id

            def report_progress(progress: int, message: str) -> None:
                _raise_if_study_upload_cancelled()
                _set_study_upload_job(job_id, progress=progress, message=message)

            def cleanup_partial_upload() -> None:
                if recall_id is not None:
                    storage.delete_study_recall_session(recall_id)
                if destination is not None and destination.is_dir():
                    try:
                        shutil.rmtree(destination)
                    except OSError:
                        pass

            try:
                _raise_if_study_upload_cancelled()
                analysis, error = _analyze_study_note_images(
                    images,
                    subject=subject,
                    allow_corrections=allow_corrections,
                    progress_callback=report_progress,
                    checkpoint_directory=staging_directory,
                )
                if error or not analysis:
                    raise RuntimeError(error or "筆記分析失敗。")
                report_progress(98, "來源驗證與原圖定位完成，正在儲存原始圖片與卡片。")
                stored_names = [
                    f"{index + 1:02d}-{secrets.token_hex(5)}{Path(name).suffix.lower()}"
                    for index, (name, _bytes, _mime) in enumerate(images)
                ]
                title = requested_title or str(analysis.get("detected_topic") or "").strip()[:120] or f"{subject}筆記"
                _raise_if_study_upload_cancelled()
                recall_id = storage.create_study_recall_session(
                    study_date=study_date,
                    subject=subject,
                    title=title,
                    image_filenames=stored_names,
                    summary=analysis["summary"],
                    key_concepts=analysis["key_concepts"],
                    source_transcription=analysis.get("source_transcription") or [],
                    uncertain_fragments=analysis.get("uncertain_fragments") or [],
                    correction_records=analysis.get("correction_records") or [],
                    organization_mode=str(analysis.get("organization_mode") or "faithful"),
                )
                _raise_if_study_upload_cancelled()
                destination = _ensure_private_dir(study_upload_root / str(recall_id))
                for stored_name, (_original_name, image_source, _mime_type) in zip(stored_names, images):
                    _raise_if_study_upload_cancelled()
                    target = destination / stored_name
                    if isinstance(image_source, Path):
                        shutil.copyfile(image_source, target)
                    else:
                        target.write_bytes(image_source)
                if skip_relation_rebuild:
                    relation_error = None
                    final_message = "重點卡已完成；批次結束時會統一更新關聯與聯想。"
                else:
                    report_progress(99, "正在重新分析所有新舊重點卡的關聯與聯想。")
                    _raise_if_study_upload_cancelled()
                    with study_relation_rebuild_lock:
                        _raise_if_study_upload_cancelled()
                        relation_error = _rebuild_all_study_recall_relations()
                    _raise_if_study_upload_cancelled()
                    final_message = (
                        f"重點卡已完成；{relation_error}"
                        if relation_error
                        else "重點卡已完成，所有新舊卡片的關聯與聯想也已更新。"
                    )
                processing_warnings = [
                    str(warning).strip()
                    for warning in analysis.get("processing_warnings") or []
                    if str(warning).strip()
                ]
                if processing_warnings:
                    final_message = f"{final_message} {' '.join(processing_warnings)}"
                _set_study_upload_job(
                    job_id,
                    status="success",
                    progress=100,
                    message=final_message,
                    session_id=recall_id,
                )
                remove_staging_after_run = True
                record_ui_event(
                    "study_recall_note_analyzed",
                    meta={"username": username, "session_id": recall_id, "subject": subject, "image_count": len(images)},
                )
            except _StudyUploadCancelled:
                cleanup_partial_upload()
                remove_staging_after_run = True
                _set_study_upload_job(
                    job_id,
                    status="cancelled",
                    message="已取消這次筆記處理，可以立即上傳下一份筆記。",
                )
                record_ui_event("study_recall_note_analyzed", "cancelled", {"username": username})
            except Exception as exc:  # pragma: no cover - guarded by route-level integration tests
                cleanup_partial_upload()
                message = str(exc).strip() or "筆記背景處理失敗，請稍後重試。"
                _set_study_upload_job(job_id, status="error", message=message[:240])
                if staging_directory is not None and staging_directory.is_dir():
                    with study_upload_staging_lock:
                        manifest, _directory = _read_study_upload_manifest(staging_upload_id, username)
                        if manifest is not None:
                            manifest.update(
                                {
                                    "job_id": job_id,
                                    "status": "error",
                                    "last_error": message[:500],
                                    "updated_at": time.time(),
                                }
                            )
                            _write_study_upload_manifest(staging_directory, manifest)
                record_ui_event("study_recall_note_analyzed", "error", {"username": username, "reason": message[:160]})
            finally:
                if remove_staging_after_run and staging_directory is not None and staging_directory.is_dir():
                    try:
                        shutil.rmtree(staging_directory)
                    except OSError:
                        pass
                if hasattr(study_upload_context, "cancel_event"):
                    del study_upload_context.cancel_event
                if hasattr(study_upload_context, "job_id"):
                    del study_upload_context.job_id

        threading.Thread(target=_run_study_upload, daemon=True).start()
        return job_id

    @app.post("/admin/study-recall/upload")
    @admin_required
    def admin_study_recall_upload():
        user = current_user() or {}
        username = str(user.get("username") or "")
        active_job_id = _active_study_upload_job(username)
        if active_job_id:
            if _is_study_upload_request():
                return {
                    "ok": True,
                    "background": True,
                    "job_id": active_job_id,
                    "message": "已有一份筆記正在背景整理。",
                }, 202
            flash("已有一份筆記正在背景整理，可先使用其他頁面。", "info")
            return redirect(url_for("admin_study_recall"))
        study_date = (request.form.get("study_date") or _study_plan_business_date().isoformat()).strip()
        try:
            date.fromisoformat(study_date)
        except ValueError:
            return _study_upload_error("請輸入有效的筆記日期。")
        subject = (request.form.get("subject") or "").strip()
        if subject not in STUDY_PLAN_SUBJECTS:
            return _study_upload_error("請選擇科目。")
        requested_title = (request.form.get("title") or "").strip()[:120]
        allow_corrections = str(request.form.get("allow_corrections") or "").strip().lower() in {"1", "true", "yes", "on"}
        skip_relation_rebuild = (
            request.headers.get("X-E3-Study-Reprocess") == "1"
            and request.headers.get("X-E3-Skip-Relation-Rebuild") == "1"
        )
        images: List[Tuple[str, bytes, str]] = []
        staging_directory: Optional[Path] = None
        staging_upload_id = str(request.form.get("upload_id") or "").strip()
        if staging_upload_id:
            manifest, staging_directory = _read_study_upload_manifest(staging_upload_id, username)
            if manifest is None or staging_directory is None:
                return _study_upload_error("這次暫存上傳已失效，請重新選擇照片。", 404)
            expected_count = int(manifest.get("expected_count") or 0)
            files = manifest.get("files") if isinstance(manifest.get("files"), dict) else {}
            if expected_count < 1 or len(files) != expected_count:
                return _study_upload_error(f"照片尚未傳完（{len(files)}/{expected_count} 張），請稍後再試。")
            for image_index in range(1, expected_count + 1):
                metadata = files.get(str(image_index))
                if not isinstance(metadata, dict):
                    return _study_upload_error(f"第 {image_index} 張照片尚未完成上傳。")
                stored_name = secure_filename(str(metadata.get("stored_name") or ""))
                image_path = (staging_directory / stored_name).resolve()
                if image_path.parent != staging_directory.resolve() or not image_path.is_file():
                    return _study_upload_error(f"第 {image_index} 張暫存照片已遺失，請重新上傳。")
                image_size = image_path.stat().st_size
                if image_size < 1 or image_size > STUDY_NOTE_MAX_IMAGE_BYTES:
                    return _study_upload_error(f"第 {image_index} 張照片大小不正確，請重新上傳。")
                images.append(
                    (
                        secure_filename(str(metadata.get("original_name") or "")) or f"note-{image_index}",
                        image_path,
                        str(metadata.get("mime_type") or "image/jpeg"),
                    )
                )
        else:
            incoming_files = [item for item in request.files.getlist("note_images") if item and item.filename]
            if not incoming_files:
                return _study_upload_error("請至少上傳 1 張筆記照片。")
            for item in incoming_files:
                filename = secure_filename(item.filename) or "note-image"
                extension = Path(filename).suffix.lower()
                mime_type = _NOTE_IMAGE_MIME_TYPES.get(extension)
                if not mime_type:
                    return _study_upload_error("筆記僅支援 JPG、PNG、WEBP 或 GIF 圖片。")
                image_bytes = item.stream.read(STUDY_NOTE_MAX_IMAGE_BYTES + 1)
                if not image_bytes or len(image_bytes) > STUDY_NOTE_MAX_IMAGE_BYTES:
                    return _study_upload_error("每張筆記照片壓縮後必須小於 2MB。")
                images.append((filename, image_bytes, mime_type))
        job_id = _launch_study_note_upload_job(
            username=username,
            study_date=study_date,
            subject=subject,
            requested_title=requested_title,
            allow_corrections=allow_corrections,
            skip_relation_rebuild=skip_relation_rebuild,
            images=images,
            staging_directory=staging_directory,
            staging_upload_id=staging_upload_id,
        )
        if _is_study_upload_request():
            return {
                "ok": True,
                "background": True,
                "job_id": job_id,
                "message": "已開始背景整理，可自由前往其他頁面。",
            }, 202
        flash("已開始背景整理筆記，可自由前往其他頁面並從右下角查看進度。", "success")
        return redirect(url_for("admin_study_recall"))

    @app.get("/admin/study-recall/upload-jobs/<job_id>")
    @admin_required
    def admin_study_recall_upload_job(job_id: str):
        user = current_user() or {}
        with study_upload_jobs_lock:
            stored_job = study_upload_jobs.get(job_id)
            job = dict(stored_job) if stored_job else None
        if job is None:
            job = storage.get_study_note_upload_job(job_id)
        if not job or job.get("username") != user.get("username"):
            return {"ok": False, "error": "找不到這次筆記處理工作。"}, 404
        payload = {
            "ok": True,
            "status": job.get("status") or "running",
            "progress": int(job.get("progress") or 0),
            "message": str(job.get("message") or "正在處理筆記。"),
        }
        if job.get("status") == "success" and job.get("session_id"):
            payload["session_id"] = int(job["session_id"])
            payload["redirect_url"] = url_for("admin_study_recall", session_id=int(job["session_id"]))
        elif job.get("status") == "error":
            payload["can_resume"] = _study_upload_job_can_resume(
                job_id,
                str(user.get("username") or ""),
            )
        return payload

    @app.get("/admin/study-recall/upload-jobs/current")
    @admin_required
    def admin_study_recall_current_upload_job():
        user = current_user() or {}
        username = str(user.get("username") or "")
        job = storage.get_current_study_note_upload_job(username)
        if not job:
            return {"ok": True, "job": None}
        payload: Dict[str, Any] = {
            "ok": True,
            "job_id": str(job.get("job_id") or ""),
            "status": str(job.get("status") or "running"),
            "progress": int(job.get("progress") or 0),
            "message": str(job.get("message") or "正在處理筆記。"),
        }
        if job.get("status") == "success" and job.get("session_id"):
            payload["session_id"] = int(job["session_id"])
            payload["redirect_url"] = url_for(
                "admin_study_recall",
                session_id=int(job["session_id"]),
            )
        elif job.get("status") == "error":
            payload["can_resume"] = _study_upload_job_can_resume(
                str(job.get("job_id") or ""),
                username,
            )
        return payload

    @app.post("/admin/study-recall/upload-jobs/<job_id>/resume")
    @admin_required
    def admin_study_recall_resume_upload_job(job_id: str):
        user = current_user() or {}
        username = str(user.get("username") or "")
        job = storage.get_study_note_upload_job(job_id)
        if not job or str(job.get("username") or "") != username:
            return {"ok": False, "error": "找不到這次筆記處理工作。"}, 404
        if str(job.get("status") or "") != "error":
            return {"ok": False, "error": "只有失敗的筆記工作可以從中斷處繼續。"}, 409
        active_job_id = _active_study_upload_job(username)
        if active_job_id:
            return {"ok": False, "error": "已有一份筆記正在背景整理。"}, 409

        with study_upload_staging_lock:
            manifest, staging_directory = _find_study_upload_staging_for_job(job_id, username)
            if manifest is None or staging_directory is None:
                return {"ok": False, "error": "可續跑資料已過期，請重新上傳原圖。"}, 410
            expected_count = int(manifest.get("expected_count") or 0)
            files = manifest.get("files") if isinstance(manifest.get("files"), dict) else {}
            if expected_count < 1 or len(files) != expected_count:
                return {"ok": False, "error": "原始圖片暫存不完整，請重新上傳。"}, 410
            images: List[Tuple[str, Any, str]] = []
            for image_index in range(1, expected_count + 1):
                metadata = files.get(str(image_index))
                if not isinstance(metadata, dict):
                    return {"ok": False, "error": "原始圖片暫存不完整，請重新上傳。"}, 410
                stored_name = secure_filename(str(metadata.get("stored_name") or ""))
                image_path = (staging_directory / stored_name).resolve()
                if image_path.parent != staging_directory.resolve() or not image_path.is_file():
                    return {"ok": False, "error": "原始圖片暫存已遺失，請重新上傳。"}, 410
                images.append(
                    (
                        secure_filename(str(metadata.get("original_name") or "")) or f"note-{image_index}",
                        image_path,
                        str(metadata.get("mime_type") or "image/jpeg"),
                    )
                )

        study_date = str(manifest.get("study_date") or "")
        subject = str(manifest.get("subject") or "")
        if subject not in STUDY_PLAN_SUBJECTS:
            return {"ok": False, "error": "原工作科目資料不正確，請重新上傳。"}, 410
        try:
            date.fromisoformat(study_date)
        except ValueError:
            return {"ok": False, "error": "原工作日期資料不正確，請重新上傳。"}, 410

        resumed_job_id = _launch_study_note_upload_job(
            username=username,
            study_date=study_date,
            subject=subject,
            requested_title=str(manifest.get("requested_title") or "")[:120],
            allow_corrections=bool(manifest.get("allow_corrections")),
            skip_relation_rebuild=bool(manifest.get("skip_relation_rebuild")),
            images=images,
            staging_directory=staging_directory,
            staging_upload_id=staging_directory.name,
            existing_job_id=job_id,
        )
        return {
            "ok": True,
            "background": True,
            "job_id": resumed_job_id,
            "message": "已從最近完成的批次繼續整理。",
        }, 202

    @app.post("/admin/study-recall/upload-jobs/<job_id>/cancel")
    @admin_required
    def admin_study_recall_cancel_upload_job(job_id: str):
        user = current_user() or {}
        with study_upload_jobs_lock:
            memory_job = study_upload_jobs.get(job_id)
            job = dict(memory_job) if memory_job else None
        if job is None:
            job = storage.get_study_note_upload_job(job_id)
        if not job or job.get("username") != user.get("username"):
            return {"ok": False, "error": "找不到這次筆記處理工作。"}, 404
        status = str(job.get("status") or "running")
        if status == "success":
            return {"ok": False, "error": "筆記已完成，無法取消。", "status": status}, 409
        if status == "running":
            if memory_job is not None:
                cancel_event = memory_job.get("cancel_event")
                if isinstance(cancel_event, threading.Event):
                    cancel_event.set()
                _set_study_upload_job(
                    job_id,
                    status="cancelled",
                    message="已取消這次筆記處理，可以立即上傳下一份筆記。",
                )
                job = storage.get_study_note_upload_job(job_id) or job
            else:
                updated_at = time.time()
                storage.save_study_note_upload_job(
                    job_id=job_id,
                    username=str(job.get("username") or ""),
                    status="cancelled",
                    progress=int(job.get("progress") or 0),
                    message="已取消這次筆記處理，可以立即上傳下一份筆記。",
                    session_id=(int(job["session_id"]) if job.get("session_id") is not None else None),
                    created_at=float(job.get("created_at") or updated_at),
                    updated_at=updated_at,
                )
                job.update(
                    status="cancelled",
                    message="已取消這次筆記處理，可以立即上傳下一份筆記。",
                )
        return {
            "ok": True,
            "status": "cancelled" if status in {"running", "cancelled"} else status,
            "progress": int(job.get("progress") or 0),
            "message": str(job.get("message") or "已取消這次筆記處理。"),
        }

    @app.post("/admin/study-recall/<int:session_id>/rate-cards")
    @admin_required
    def admin_study_recall_rate_cards(session_id: int):
        is_async_rating = request.headers.get("X-E3-Recall-Rating") == "1"
        return_to = (request.form.get("return_to") or "").strip()
        if return_to not in {"admin_study_home", "admin_study_plan", "public_study_progress"}:
            return_to = ""

        def recall_redirect():
            return redirect(url_for(return_to) if return_to else url_for("admin_study_recall", session_id=session_id))

        def rating_error(message: str, status_code: int = 400):
            if is_async_rating:
                return {"ok": False, "error": message}, status_code
            flash(message, "error")
            return recall_redirect()

        recall_session = storage.get_study_recall_session(session_id)
        if not recall_session:
            return rating_error("找不到這份回想紀錄。", 404)
        ratings: Dict[int, int] = {}
        for index, _concept in enumerate(recall_session.get("key_concepts") or []):
            raw_rating = (request.form.get(f"rating_{index}") or "").strip()
            if not raw_rating:
                continue
            try:
                rating = int(raw_rating)
            except (TypeError, ValueError):
                rating = 0
            if rating not in {1, 2, 3, 4, 5}:
                return rating_error("印象分必須是 1 至 5 分。")
            ratings[index] = rating
        if not ratings:
            return rating_error("請至少為一張重點卡填寫印象分。")
        if storage.record_study_recall_card_ratings(
            session_id=session_id,
            ratings=ratings,
            review_date=_study_plan_business_date().isoformat(),
        ):
            next_session = storage.get_study_recall_session(session_id) or {}
            next_review_at = next_session.get("next_review_at") or "待安排"
            record_ui_event(
                "study_recall_cards_rated",
                meta={"session_id": session_id, "card_count": len(ratings), "next_review_at": next_review_at},
            )
            if is_async_rating:
                return {"ok": True, "remaining_due_count": _build_recall_widget_context()["due_count"]}
            flash(f"已記錄每張重點卡的印象分；最早的下一次複習是 {next_review_at}。", "success")
        elif is_async_rating:
            return {"ok": False, "error": "印象分暫時無法儲存，請再試一次。"}, 500
        else:
            flash("印象分暫時無法儲存，請再試一次。", "error")
        return recall_redirect()

    return (
        admin_study_recall_delete,
        admin_study_recall_rename,
        admin_study_recall_upload_staging,
        admin_study_recall_cancel_upload_staging,
        _launch_study_note_upload_job,
        admin_study_recall_upload,
        admin_study_recall_upload_job,
        admin_study_recall_current_upload_job,
        admin_study_recall_resume_upload_job,
        admin_study_recall_cancel_upload_job,
        admin_study_recall_rate_cards,
    )
