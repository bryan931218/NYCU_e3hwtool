"""Study plan routes and their feature helpers."""

from ...services.study_progress import _study_plan_business_date, _study_plan_nonnegative_number, _study_plan_video_completion, _study_plan_default_video, _parse_youtube_url, _study_plan_total_is_complete, _study_plan_progress_week, _study_plan_week_start
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from flask import flash, redirect, render_template_string, request, url_for
from ...services.youtube_playlists import (
    KNOWN_YOUTUBE_PLAYLISTS,
    YoutubePlaylistSyncBusyError,
    sync_known_youtube_playlists,
)


def register_study_plan_routes(*,
    STUDY_MARKERS_TEMPLATE,
    STUDY_PLAN_END,
    STUDY_PLAN_START,
    STUDY_PLAN_SUBJECTS,
    STUDY_PLAN_TEMPLATE,
    STUDY_SETTINGS_TEMPLATE,
    _build_recall_widget_context,
    _invalidate_study_progress_context,
    _repair_legacy_study_assistant_time_moves,
    _start_youtube_storyboard_index,
    _study_plan_business_date,
    _study_plan_default_video,
    _study_plan_minutes,
    _study_plan_nonnegative_number,
    _study_plan_progress_week,
    _study_plan_replan_preview,
    _study_plan_schedule_definitions,
    _study_plan_today_task_videos,
    _study_plan_total_is_complete,
    _study_plan_video_completion,
    _study_plan_week_rows,
    _study_plan_week_start,
    admin_required,
    app,
    current_user,
    openai_api_key,
    record_ui_event,
    storage,
):
    @app.route("/admin/study-settings", methods=["GET", "POST"])
    @admin_required
    def admin_study_settings():
        user = current_user()
        selected_subject = (request.args.get("subject") or request.form.get("subject") or "").strip()
        if selected_subject not in STUDY_PLAN_SUBJECTS:
            selected_subject = ""
        if request.method == "POST":
            try:
                video_id = int(request.form.get("video_id") or 0)
            except (TypeError, ValueError):
                video_id = 0
            parsed = _parse_youtube_url("") if request.form.get("clear") else _parse_youtube_url(request.form.get("youtube_url"))
            if not video_id:
                flash("找不到要設定的影片。", "error")
            elif parsed is None:
                flash("請輸入有效的 YouTube 影片連結，例如 https://www.youtube.com/watch?v=xxxxxxxxxxx。", "error")
            elif storage.update_study_plan_video_youtube(
                video_id=video_id,
                youtube_video_id=parsed["video_id"],
                youtube_playlist_id=parsed["playlist_id"],
                youtube_url=parsed["url"],
            ):
                if parsed["video_id"]:
                    _start_youtube_storyboard_index(
                        storage,
                        [parsed["video_id"]],
                        app.logger,
                    )
                record_ui_event(
                    "study_plan_video_youtube_updated",
                    meta={"video_id": video_id, "youtube_video_id": parsed["video_id"]},
                )
                flash("影片 YouTube 連結已更新。", "success")
            else:
                flash("找不到要設定的影片。", "error")
            return redirect(url_for("admin_study_settings", subject=selected_subject) if selected_subject else url_for("admin_study_settings"))

        videos = storage.list_study_plan_videos_with_records()
        if selected_subject:
            videos = [video for video in videos if video["subject"] == selected_subject]
        return render_template_string(
            STUDY_SETTINGS_TEMPLATE,
            admin_user=user,
            subjects=STUDY_PLAN_SUBJECTS,
            selected_subject=selected_subject,
            videos=videos,
            synced_playlist_subjects=[item["subject"] for item in KNOWN_YOUTUBE_PLAYLISTS],
        )

    @app.post("/admin/study-settings/youtube-sync")
    @admin_required
    def admin_study_settings_youtube_sync():
        wants_json = request.accept_mimetypes.best == "application/json"
        try:
            result = sync_known_youtube_playlists(storage)
        except YoutubePlaylistSyncBusyError as exc:
            if wants_json:
                return {"ok": False, "error": str(exc)}, 409
            flash(str(exc), "error")
            return redirect(url_for("admin_study_settings"))
        except Exception:
            app.logger.exception("YouTube playlist synchronization failed")
            message = "YouTube 播放清單同步失敗，請稍後再試。"
            if wants_json:
                return {"ok": False, "error": message}, 502
            flash(message, "error")
            return redirect(url_for("admin_study_settings"))

        if not result.get("ok"):
            message = "目前無法讀取已設定的 YouTube 播放清單。"
            if wants_json:
                return {"ok": False, "error": message, "result": result}, 502
            flash(message, "error")
            return redirect(url_for("admin_study_settings"))

        result["storyboard_indexing"] = _start_youtube_storyboard_index(
            storage,
            result.get("youtube_video_ids") or [],
            app.logger,
        )
        current_videos = storage.list_study_plan_videos_with_records()
        payload = {
            "ok": True,
            "result": result,
            "videos": [
                {"id": video["id"], "youtube_url": video["youtube_url"]}
                for video in current_videos
            ],
        }
        record_ui_event(
            "study_plan_youtube_playlists_synced",
            meta={
                "updated": result["updated"],
                "matched": result["matched"],
                "empty_subjects": result["empty_subjects"],
                "failed_subjects": [item["subject"] for item in result["errors"]],
            },
        )
        if wants_json:
            return payload

        message = f"YouTube 同步完成：更新 {result['updated']} 支，{result['unchanged']} 支無變更。"
        if result["errors"]:
            message += f"另有 {len(result['errors'])} 個播放清單讀取失敗。"
        if result["empty_subjects"]:
            message += f"目前空白：{'、'.join(result['empty_subjects'])}。"
        flash(message, "success")
        return redirect(url_for("admin_study_settings"))

    def _study_plan_marker_library(
        videos: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        marker_videos = videos or storage.list_study_plan_videos_with_records()
        video_lookup = {int(video["id"]): video for video in marker_videos}
        marker_library = []
        for marker in storage.list_study_plan_video_markers():
            marker_video = video_lookup.get(int(marker.get("video_id") or 0))
            if not marker_video:
                continue
            marker_seconds = max(0.0, float(marker.get("playback_seconds") or 0))
            marker_minutes, marker_remainder = divmod(int(round(marker_seconds)), 60)
            marker_library.append(
                {
                    **marker,
                    "subject": str(marker_video.get("subject") or ""),
                    "sequence": int(marker_video.get("sequence") or 0),
                    "video_title": str(marker_video.get("title") or ""),
                    "time_label": f"{marker_minutes}:{marker_remainder:02d}",
                }
            )
        marker_library.sort(
            key=lambda item: (str(item.get("updated_at") or ""), int(item.get("id") or 0)),
            reverse=True,
        )
        return marker_library

    @app.get("/admin/study-plan/markers")
    @admin_required
    def admin_study_plan_markers():
        marker_library = _study_plan_marker_library()
        ready_count = sum(
            1 for marker in marker_library if marker.get("summary_status") == "ready"
        )
        return render_template_string(
            STUDY_MARKERS_TEMPLATE,
            admin_user=current_user(),
            markers=marker_library,
            subjects=STUDY_PLAN_SUBJECTS,
            openai_ready=bool(openai_api_key),
            marker_stats={
                "total": len(marker_library),
                "ready": ready_count,
                "pending": len(marker_library) - ready_count,
                "subjects": len(
                    {
                        str(marker.get("subject") or "")
                        for marker in marker_library
                        if marker.get("subject")
                    }
                ),
            },
        )

    @app.route("/admin/study-plan", methods=["GET", "POST"])
    @admin_required
    def admin_study_plan():
        user = current_user()
        if user:
            _repair_legacy_study_assistant_time_moves(str(user.get("username") or ""))
        if request.method == "POST":
            action = (request.form.get("action") or "save_video").strip()
            selected_subject = (request.form.get("subject") or "").strip()
            if selected_subject not in STUDY_PLAN_SUBJECTS:
                selected_subject = STUDY_PLAN_SUBJECTS[0]
            try:
                video_id = int(request.form.get("video_id") or 0)
            except (TypeError, ValueError):
                video_id = 0
            if action == "delete_video":
                if video_id and storage.delete_study_plan_video_record(video_id):
                    _invalidate_study_progress_context()
                    record_ui_event("study_plan_video_record_deleted", meta={"video_id": video_id})
            else:
                watched_minutes = _study_plan_minutes(request.form.get("watched_minutes"))
                notes = (request.form.get("notes") or "").strip()[:2000]
                if video_id and storage.upsert_study_plan_video_record(
                    video_id=video_id,
                    watched_seconds=watched_minutes * 60,
                    notes=notes,
                ):
                    _invalidate_study_progress_context()
                    record_ui_event(
                        "study_plan_video_record_saved",
                        meta={"video_id": video_id, "watched_minutes": watched_minutes},
                    )
            return redirect(url_for("admin_study_plan", subject=selected_subject))

        videos = storage.list_study_plan_videos_with_records()
        week_rows, _calendar_week, summary = _study_plan_week_rows(videos)
        current_week = _study_plan_progress_week(week_rows)
        videos_by_subject: Dict[str, List[Dict[str, Any]]] = {subject: [] for subject in STUDY_PLAN_SUBJECTS}
        for video in videos:
            video["duration_minutes"] = round(float(video["duration_seconds"]) / 60, 1)
            video["watched_minutes"] = round(
                min(float(video["watched_seconds"]), float(video["duration_seconds"])) / 60,
                1,
            )
            video["completion"] = _study_plan_video_completion(video["duration_seconds"], video["watched_seconds"])
            videos_by_subject.setdefault(video["subject"], []).append(video)

        try:
            requested_video_id = int(request.args.get("video_id") or 0)
        except (TypeError, ValueError):
            requested_video_id = 0
        requested_video = next(
            (video for video in videos if int(video.get("id") or 0) == requested_video_id),
            None,
        )
        activity_events = storage.list_study_plan_activity_events(
            start_day=STUDY_PLAN_START,
            end_day=_study_plan_business_date().isoformat(),
        )
        last_watched_event = next(
            (
                event
                for event in reversed(activity_events)
                if float(event.get("delta_seconds") or 0) > 0
            ),
            None,
        )
        last_watched_video_id = int((last_watched_event or {}).get("video_id") or 0)
        last_watched_subject = str((last_watched_event or {}).get("subject") or "").strip()
        current_subjects = current_week.get("subjects") or [current_week.get("subject")]
        plan_default_subject = next(
            (subject for subject in current_subjects if subject in STUDY_PLAN_SUBJECTS),
            STUDY_PLAN_SUBJECTS[0],
        )
        default_subject = (
            last_watched_subject
            if last_watched_subject in STUDY_PLAN_SUBJECTS
            else plan_default_subject
        )
        requested_subject = str(request.args.get("subject") or "").strip()
        if requested_subject in STUDY_PLAN_SUBJECTS:
            selected_subject = requested_subject
        elif requested_video and requested_video.get("subject") in STUDY_PLAN_SUBJECTS:
            selected_subject = str(requested_video["subject"])
        else:
            selected_subject = default_subject

        today_task_videos = _study_plan_today_task_videos(
            videos_by_subject,
            week_rows,
            current_week,
        )
        visible_videos = videos_by_subject.get(selected_subject, [])
        selected_video = _study_plan_default_video(
            visible_videos,
            last_watched_video_id=last_watched_video_id,
            requested_video_id=requested_video_id,
        )
        selected_video_id = int((selected_video or {}).get("id") or 0)
        visible_video_ids = [int(video["id"]) for video in visible_videos]
        video_markers = storage.list_study_plan_video_markers(video_ids=visible_video_ids)
        try:
            requested_marker_id = int(request.args.get("marker_id") or 0)
        except (TypeError, ValueError):
            requested_marker_id = 0
        requested_marker = (
            storage.get_study_plan_video_marker(requested_marker_id)
            if requested_marker_id > 0
            else None
        )
        if requested_marker and int(requested_marker.get("video_id") or 0) != selected_video_id:
            requested_marker = None
        replan_settings = storage.get_study_plan_replan_settings()
        rest_days = storage.list_study_plan_rest_days()
        replan_preview = _study_plan_replan_preview(replan_settings, rest_days)
        today_iso = _study_plan_business_date().isoformat()
        next_week_start = _study_plan_week_start(_study_plan_business_date()) + timedelta(days=7)
        effective_plan_end = str((replan_settings or {}).get("end_date") or STUDY_PLAN_END)
        study_time_today = storage.get_study_time_summary(
            day=_study_plan_business_date().isoformat()
        )
        study_time_sessions = storage.list_study_time_sessions(
            day=_study_plan_business_date().isoformat()
        )
        return render_template_string(
            STUDY_PLAN_TEMPLATE,
            admin_user=user,
            week_rows=week_rows,
            current_week=current_week,
            summary=summary,
            subjects=STUDY_PLAN_SUBJECTS,
            selected_subject=selected_subject,
            selected_video_id=selected_video_id,
            videos=visible_videos,
            today_task_videos=today_task_videos,
            video_count=len(visible_videos),
            plan_total_weeks=len(week_rows),
            plan_start=STUDY_PLAN_START,
            plan_end=effective_plan_end,
            recall_widget=_build_recall_widget_context(),
            video_markers=video_markers,
            requested_marker=requested_marker,
            replan_settings=replan_settings,
            replan_preview=replan_preview,
            today_iso=today_iso,
            study_time_today=study_time_today,
            study_time_sessions=study_time_sessions,
            openai_ready=bool(openai_api_key),
            replan_defaults={
                "start_date": next_week_start.isoformat(),
                "end_date": effective_plan_end,
                "weekday_hours": round(float((replan_settings or {}).get("weekday_minutes") or 180) / 60, 1),
                "weekend_hours": round(float((replan_settings or {}).get("weekend_minutes") or 120) / 60, 1),
            },
        )

    @app.post("/admin/study-plan/rest-day")
    @admin_required
    def admin_study_plan_rest_day():
        today = _study_plan_business_date()
        today_iso = today.isoformat()
        requested_date = str(
            request.args.get("study_date")
            or request.form.get("study_date")
            or today_iso
        ).strip()
        try:
            selected_day = datetime.strptime(requested_date, "%Y-%m-%d").date()
        except ValueError:
            flash("休息日日期格式不正確。", "error")
            return redirect(url_for("admin_study_plan") + "#current-focus")
        selected_day_iso = selected_day.isoformat()
        action = str(request.form.get("action") or "skip").strip()
        selected_subject = str(
            request.args.get("subject")
            or request.form.get("subject")
            or ""
        ).strip()
        if selected_subject not in STUDY_PLAN_SUBJECTS:
            selected_subject = STUDY_PLAN_SUBJECTS[0]
        return_url = (
            url_for("admin_study_plan", subject=selected_subject)
            + f"#plan-day-{selected_day_iso}"
        )

        if action == "restore":
            if storage.delete_study_plan_rest_day(selected_day_iso):
                _invalidate_study_progress_context()
                record_ui_event("study_plan_rest_day_restored", meta={"study_date": selected_day_iso})
                flash(f"已恢復 {selected_day_iso} 的原定進度。", "success")
            return redirect(return_url)

        existing_rest_days = storage.list_study_plan_rest_days()
        if selected_day_iso in existing_rest_days:
            flash(f"{selected_day_iso} 已經是休息日。", "info")
            return redirect(return_url)

        videos = storage.list_study_plan_videos_with_records()
        settings = storage.get_study_plan_replan_settings()
        before_weeks, _, _ = _study_plan_week_rows(videos)
        before_day = next(
            (
                day
                for week in before_weeks
                for day in week.get("daily_recommendations", [])
                if day["date"] == selected_day_iso
            ),
            None,
        )
        original_seconds = float((before_day or {}).get("target_seconds") or 0)
        if original_seconds <= 0.001:
            flash("該日沒有可移動的計畫時數。", "info")
            return redirect(return_url)
        if _study_plan_total_is_complete(
            original_seconds,
            float((before_day or {}).get("credited_seconds") or 0),
        ):
            flash("該日進度已完成，不需要改設為休息日。", "info")
            return redirect(return_url)
        if not before_day or not before_day.get("can_be_rest_day"):
            flash("該日之後沒有同一排程區段可承接進度，無法設為休息日。", "error")
            return redirect(return_url)

        proposed_weeks = _study_plan_schedule_definitions(
            videos,
            settings,
            [*existing_rest_days, selected_day_iso],
        )
        proposed_day = next(
            (
                day
                for week in proposed_weeks
                for day in week.get("daily_targets", [])
                if day["date"] == selected_day
            ),
            None,
        )
        if not proposed_day or not proposed_day.get("is_rest_day"):
            flash("截止日前沒有其他日期可承接該日進度，無法設為休息日。", "error")
            return redirect(return_url)

        if storage.add_study_plan_rest_day(selected_day_iso):
            _invalidate_study_progress_context()
            moved_hours = original_seconds / 3600
            redistributed_days = int(proposed_day.get("redistributed_day_count") or 0)
            record_ui_event(
                "study_plan_rest_day_added",
                meta={
                    "study_date": selected_day_iso,
                    "moved_hours": round(moved_hours, 2),
                    "redistributed_days": redistributed_days,
                },
            )
            flash(
                f"{selected_day_iso} 已設為休息日；原定 {moved_hours:.2f} 小時已分攤到後續 {redistributed_days} 天。",
                "success",
            )
        return redirect(return_url)

    @app.post("/admin/study-plan/replan")
    @admin_required
    def admin_study_plan_replan():
        action = str(request.form.get("action") or "save").strip()
        selected_subject = str(request.form.get("subject") or "").strip()
        if selected_subject not in STUDY_PLAN_SUBJECTS:
            selected_subject = STUDY_PLAN_SUBJECTS[0]
        if action == "reset":
            if storage.delete_study_plan_replan_settings():
                _invalidate_study_progress_context()
                record_ui_event("study_plan_replan_reset")
                flash("已恢復原始讀書計畫。", "success")
            return redirect(url_for("admin_study_plan", subject=selected_subject))

        start_day = _study_plan_week_start(_study_plan_business_date()) + timedelta(days=7)
        try:
            end_day = datetime.strptime(str(request.form.get("end_date") or ""), "%Y-%m-%d").date()
            weekday_hours = float(request.form.get("weekday_hours") or 0)
            weekend_hours = float(request.form.get("weekend_hours") or 0)
        except (TypeError, ValueError):
            flash("請輸入有效的日期與每日可讀時數。", "error")
            return redirect(url_for("admin_study_plan", subject=selected_subject) + "#smart-replan")
        if end_day < start_day + timedelta(days=6):
            flash(f"目標日期至少需要晚於 {start_day.isoformat()} 一週。", "error")
            return redirect(url_for("admin_study_plan", subject=selected_subject) + "#smart-replan")
        if end_day > start_day + timedelta(days=366):
            flash("智慧計畫最長可安排一年。", "error")
            return redirect(url_for("admin_study_plan", subject=selected_subject) + "#smart-replan")
        if not (0.25 <= weekday_hours <= 12 and 0.25 <= weekend_hours <= 12):
            flash("平日與假日可讀時數需介於 0.25 至 12 小時。", "error")
            return redirect(url_for("admin_study_plan", subject=selected_subject) + "#smart-replan")

        videos = storage.list_study_plan_videos_with_records()
        baseline_by_subject: Dict[str, float] = {}
        subject_targets: Dict[str, float] = {}
        for subject in STUDY_PLAN_SUBJECTS:
            subject_videos = [video for video in videos if str(video.get("subject") or "") == subject]
            total_seconds = sum(_study_plan_nonnegative_number(video.get("duration_seconds")) for video in subject_videos)
            watched_seconds = sum(
                min(
                    _study_plan_nonnegative_number(video.get("watched_seconds")),
                    _study_plan_nonnegative_number(video.get("duration_seconds")),
                )
                for video in subject_videos
            )
            baseline_by_subject[subject] = watched_seconds
            remaining_seconds = max(0.0, total_seconds - watched_seconds)
            if remaining_seconds > 0.001:
                subject_targets[subject] = remaining_seconds
        if not subject_targets:
            flash("所有影片都已完成，目前不需要重新安排。", "success")
            return redirect(url_for("admin_study_plan", subject=selected_subject))
        storage.save_study_plan_replan_settings(
            start_date=start_day.isoformat(),
            end_date=end_day.isoformat(),
            weekday_minutes=weekday_hours * 60,
            weekend_minutes=weekend_hours * 60,
            baseline_by_subject=baseline_by_subject,
            subject_targets=subject_targets,
        )
        _invalidate_study_progress_context()
        record_ui_event(
            "study_plan_replanned",
            meta={
                "start_date": start_day.isoformat(),
                "end_date": end_day.isoformat(),
                "weekday_hours": weekday_hours,
                "weekend_hours": weekend_hours,
            },
        )
        flash("智慧重排已套用，既有觀看紀錄都已保留。", "success")
        return redirect(url_for("admin_study_plan", subject=selected_subject) + "#smart-replan")

    return (
        admin_study_settings,
        admin_study_settings_youtube_sync,
        _study_plan_marker_library,
        admin_study_plan_markers,
        admin_study_plan,
        admin_study_plan_rest_day,
        admin_study_plan_replan,
    )
