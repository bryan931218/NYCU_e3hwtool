"""Assistant actions routes and their feature helpers."""

import math
from typing import Any, Dict, List, Optional, Tuple
from flask import url_for


def register_assistant_actions_routes(*,
    _invalidate_study_progress_context,
    _study_assistant_time_label,
    admin_required,
    app,
    current_user,
    record_ui_event,
    storage,
):
    def _study_assistant_restore_action_status(
        action_id: str,
        username: str,
        *,
        current: str,
        restored: str,
    ) -> None:
        storage.transition_study_assistant_action(
            action_id,
            username=username,
            expected_status=current,
            next_status=restored,
        )

    def _study_assistant_activity_seconds(*, video_id: int, day: str) -> float:
        return sum(
            max(0.0, float(item.get("delta_seconds") or 0))
            for item in storage.list_study_plan_activity_events(day=day)
            if int(item.get("video_id") or 0) == int(video_id or 0)
        )

    def _repair_legacy_study_assistant_time_moves(username: str) -> None:
        """Finish confirmed time moves created before calendar activity was included.

        The original confirmation already authorised these exact dates, video and
        seconds. Recovery is compare-and-swap guarded and only runs while the
        calendar's source value still matches the uncorrected state.
        """
        normalized_user = str(username or "").strip()
        if not normalized_user:
            return
        actions = storage.list_study_assistant_actions(
            username=normalized_user,
            action_type="move_study_time_between_days",
            status="applied",
            limit=20,
        )
        videos = storage.list_study_plan_videos_with_records()
        repaired_any = False
        for action in actions:
            after = action.get("after_state") if isinstance(action.get("after_state"), dict) else {}
            # New actions already contain the calendar mutation. A legacy action
            # contains the moved study-time session directly at the top level.
            if isinstance(after.get("activity"), dict):
                continue
            payload = action.get("action_payload") if isinstance(action.get("action_payload"), dict) else {}
            app.logger.info(
                "Checking legacy calendar move %s (after keys: %s)",
                str(action.get("action_id") or ""),
                ",".join(sorted(str(key) for key in after)),
            )
            try:
                moved_seconds = float(payload.get("target_seconds") or 0)
                sequence = int(payload.get("video_sequence") or 0)
            except (TypeError, ValueError):
                continue
            source_day = str(payload.get("source_day") or "")
            target_day = str(payload.get("target_day") or "")
            subject = str(payload.get("subject") or "")
            video = next((
                item for item in videos
                if str(item.get("subject") or "") == subject
                and int(item.get("sequence") or 0) == sequence
            ), None)
            if not video or moved_seconds <= 0 or not source_day or not target_day:
                continue
            video_id = int(video.get("id") or 0)
            source_seconds = _study_assistant_activity_seconds(video_id=video_id, day=source_day)
            target_seconds = _study_assistant_activity_seconds(video_id=video_id, day=target_day)
            if source_seconds + 0.5 < moved_seconds:
                continue
            action_id = str(action.get("action_id") or "")
            if not storage.transition_study_assistant_action(
                action_id,
                username=normalized_user,
                expected_status="applied",
                next_status="repairing",
            ):
                continue
            activity_result: Optional[Dict[str, Any]] = None
            try:
                activity_result = storage.move_study_plan_activity_between_days(
                    video_id=video_id,
                    source_day=source_day,
                    target_day=target_day,
                    seconds=moved_seconds,
                    expected_source_seconds=source_seconds,
                    expected_target_seconds=target_seconds,
                )
                if not activity_result or activity_result.get("stale"):
                    raise ValueError("legacy calendar activity changed before repair")
                repaired_after = {
                    "activity": dict(activity_result),
                    "study_time": dict(after),
                    "legacy_repaired": True,
                }
                if not storage.transition_study_assistant_action(
                    action_id,
                    username=normalized_user,
                    expected_status="repairing",
                    next_status="applied",
                    after_state=repaired_after,
                ):
                    raise RuntimeError("legacy action audit transition failed")
                repaired_any = True
                app.logger.info(
                    "Repaired legacy calendar move %s for %s video %s",
                    action_id,
                    subject,
                    sequence,
                )
            except Exception:
                if activity_result and not activity_result.get("stale"):
                    storage.undo_move_study_plan_activity_between_days(
                        original_rows=list(activity_result.get("original_rows") or []),
                        generated_ids=list(activity_result.get("generated_ids") or []),
                    )
                _study_assistant_restore_action_status(
                    action_id,
                    normalized_user,
                    current="repairing",
                    restored="applied",
                )
                app.logger.exception("Failed to repair legacy calendar move %s", action_id)
        if repaired_any:
            _invalidate_study_progress_context()

    def _study_assistant_numbers_match(left: Any, right: Any, tolerance: float = 0.05) -> bool:
        try:
            left_number = float(left)
            right_number = float(right)
        except (TypeError, ValueError):
            return False
        return math.isfinite(left_number) and math.isfinite(right_number) and abs(left_number - right_number) <= tolerance

    def _study_assistant_mappings_match(left: Any, right: Any) -> bool:
        left_values = left if isinstance(left, dict) else {}
        right_values = right if isinstance(right, dict) else {}
        keys = set(left_values) | set(right_values)
        return all(
            _study_assistant_numbers_match(left_values.get(key, 0), right_values.get(key, 0))
            for key in keys
        )

    def _study_assistant_verify_applied_action(
        action_type: str,
        payload: Dict[str, Any],
        after: Dict[str, Any],
    ) -> Tuple[bool, Dict[str, Any], Dict[str, Any]]:
        """Read storage again; returned data, not the model or write response, is authoritative."""
        verified_state: Dict[str, Any] = {}
        details: List[Dict[str, str]] = []
        if action_type == "set_video_progress":
            video_id = int(payload.get("video_id") or 0)
            current = next(
                (
                    item for item in storage.list_study_plan_videos_with_records()
                    if int(item.get("id") or 0) == video_id
                ),
                None,
            )
            if current:
                verified_state = {
                    "video_id": video_id,
                    "watched_seconds": float(current.get("watched_seconds") or 0),
                    "progress_version": int(current.get("progress_version") or 0),
                }
                details.append({
                    "label": "影片進度",
                    "value": _study_assistant_time_label(verified_state["watched_seconds"]),
                })
            matches = bool(
                current
                and _study_assistant_numbers_match(
                    current.get("watched_seconds"), payload.get("target_seconds"), tolerance=0.5
                )
                and int(current.get("progress_version") or 0)
                == int(after.get("progress_version") or -1)
            )
        elif action_type == "set_study_time_session":
            current = storage.get_study_time_session(str(payload.get("session_id") or ""))
            if current:
                verified_state = dict(current)
                details.append({
                    "label": "學習時間",
                    "value": _study_assistant_time_label(current.get("elapsed_seconds")),
                })
            matches = bool(
                current
                and _study_assistant_numbers_match(
                    current.get("elapsed_seconds"), payload.get("target_seconds"), tolerance=0.5
                )
                and str(current.get("updated_at") or "") == str(after.get("updated_at") or "")
            )
        elif action_type == "move_study_time_between_days":
            video_id = int(payload.get("video_id") or 0)
            source_seconds = sum(
                max(0.0, float(item.get("delta_seconds") or 0))
                for item in storage.list_study_plan_activity_events(day=str(payload.get("source_day") or ""))
                if int(item.get("video_id") or 0) == video_id
            )
            target_seconds = sum(
                max(0.0, float(item.get("delta_seconds") or 0))
                for item in storage.list_study_plan_activity_events(day=str(payload.get("target_day") or ""))
                if int(item.get("video_id") or 0) == video_id
            )
            activity_after = after.get("activity") if isinstance(after.get("activity"), dict) else {}
            activity_matches = bool(
                _study_assistant_numbers_match(source_seconds, activity_after.get("source_seconds"), tolerance=0.5)
                and _study_assistant_numbers_match(target_seconds, activity_after.get("target_seconds"), tolerance=0.5)
            )
            study_time_after = after.get("study_time") if isinstance(after.get("study_time"), dict) else {}
            study_time_matches = True
            observed_study_time: Dict[str, Any] = {}
            if payload.get("moves"):
                target = storage.get_study_time_session(str(payload.get("target_session_id") or ""))
                source_checks = []
                for source in study_time_after.get("source_sessions") or []:
                    current_source = storage.get_study_time_session(str(source.get("session_id") or ""))
                    source_checks.append(bool(
                        current_source
                        and _study_assistant_numbers_match(
                            current_source.get("elapsed_seconds"), source.get("elapsed_seconds"), tolerance=0.5
                        )
                        and str(current_source.get("updated_at") or "") == str(source.get("updated_at") or "")
                    ))
                study_time_matches = bool(
                    target
                    and str(target.get("day") or "") == str(payload.get("target_day") or "")
                    and _study_assistant_numbers_match(
                        target.get("elapsed_seconds"), payload.get("target_seconds"), tolerance=0.5
                    )
                    and str(target.get("updated_at") or "")
                    == str((study_time_after.get("target_session") or {}).get("updated_at") or "")
                    and source_checks
                    and all(source_checks)
                )
                observed_study_time = {
                    "source_sessions": list(study_time_after.get("source_sessions") or []),
                    "target_session": dict(target or {}),
                }
            verified_state = {"activity": activity_after, "study_time": observed_study_time}
            details.append({
                "label": "月曆影片時間",
                "value": f"{payload.get('target_day')} · {_study_assistant_time_label(target_seconds)}",
            })
            matches = activity_matches and study_time_matches
        elif action_type == "update_study_plan":
            current = storage.get_study_plan_replan_settings()
            if current:
                verified_state = {"settings": dict(current)}
                details.extend([
                    {"label": "計畫日期", "value": f"{current.get('start_date')} 至 {current.get('end_date')}"},
                    {
                        "label": "每日時數",
                        "value": (
                            f"平日 {float(current.get('weekday_minutes') or 0) / 60:g} 小時 · "
                            f"假日 {float(current.get('weekend_minutes') or 0) / 60:g} 小時"
                        ),
                    },
                ])
            matches = bool(
                current
                and str(current.get("start_date") or "") == str(payload.get("start_date") or "")
                and str(current.get("end_date") or "") == str(payload.get("end_date") or "")
                and _study_assistant_numbers_match(
                    current.get("weekday_minutes"), payload.get("weekday_minutes")
                )
                and _study_assistant_numbers_match(
                    current.get("weekend_minutes"), payload.get("weekend_minutes")
                )
                and _study_assistant_mappings_match(
                    current.get("baseline_by_subject"), payload.get("baseline_by_subject")
                )
                and _study_assistant_mappings_match(
                    current.get("subject_targets"), payload.get("subject_targets")
                )
            )
        else:
            matches = False
        return matches, verified_state, {
            "verified": matches,
            "label": "已回讀資料庫確認" if matches else "資料庫驗證未通過",
            "details": details,
        }

    def _study_assistant_rollback_unverified_action(
        action_type: str,
        before: Dict[str, Any],
        observed: Dict[str, Any],
    ) -> bool:
        """Best-effort compensation; never overwrite data changed after our own write."""
        try:
            if action_type == "set_video_progress":
                result = storage.update_study_plan_video_progress(
                    video_id=int(before.get("video_id") or 0),
                    watched_seconds=float(before.get("watched_seconds") or 0),
                    expected_version=int(observed.get("progress_version") or -1),
                )
                return bool(result and not result.get("stale"))
            if action_type == "set_study_time_session":
                result = storage.update_study_time_session_elapsed(
                    session_key=str(before.get("session_id") or ""),
                    elapsed_seconds=float(before.get("elapsed_seconds") or 0),
                    expected_updated_at=str(observed.get("updated_at") or ""),
                )
                return bool(result and not result.get("stale"))
            if action_type == "move_study_time_between_days":
                activity = observed.get("activity") if isinstance(observed.get("activity"), dict) else {}
                study_time = observed.get("study_time") if isinstance(observed.get("study_time"), dict) else {}
                target = study_time.get("target_session") if isinstance(study_time.get("target_session"), dict) else {}
                before_sources = before.get("source_sessions") if isinstance(before.get("source_sessions"), list) else []
                observed_sources = {
                    str(item.get("session_id") or ""): item
                    for item in (study_time.get("source_sessions") or [])
                    if isinstance(item, dict)
                }
                study_time_restored = True
                if before_sources:
                    study_time_restored = storage.undo_move_study_time_between_days(
                        source_sessions=[{
                            "session_id": str(item.get("session_id") or ""),
                            "restore_seconds": float(item.get("elapsed_seconds") or 0),
                            "expected_updated_at": str(observed_sources.get(str(item.get("session_id") or ""), {}).get("updated_at") or ""),
                        } for item in before_sources],
                        target_session_key=str(target.get("session_id") or ""),
                        expected_target_updated_at=str(target.get("updated_at") or ""),
                    )
                activity_restored = storage.undo_move_study_plan_activity_between_days(
                    original_rows=list(activity.get("original_rows") or []),
                    generated_ids=list(activity.get("generated_ids") or []),
                )
                return bool(study_time_restored and activity_restored)
            if action_type == "update_study_plan":
                observed_settings = observed.get("settings") if isinstance(observed.get("settings"), dict) else {}
                current = storage.get_study_plan_replan_settings()
                if not current or str(current.get("updated_at") or "") != str(observed_settings.get("updated_at") or ""):
                    return False
                old_settings = before.get("settings") if isinstance(before.get("settings"), dict) else {}
                if before.get("exists"):
                    storage.save_study_plan_replan_settings(
                        start_date=str(old_settings.get("start_date") or ""),
                        end_date=str(old_settings.get("end_date") or ""),
                        weekday_minutes=float(old_settings.get("weekday_minutes") or 0),
                        weekend_minutes=float(old_settings.get("weekend_minutes") or 0),
                        baseline_by_subject=dict(old_settings.get("baseline_by_subject") or {}),
                        subject_targets=dict(old_settings.get("subject_targets") or {}),
                    )
                else:
                    storage.delete_study_plan_replan_settings()
                return True
        except Exception:
            app.logger.exception("Failed to roll back unverified study assistant action")
        return False

    def _study_assistant_verify_reverted_action(
        action_type: str,
        before: Dict[str, Any],
        after: Dict[str, Any],
    ) -> Dict[str, Any]:
        verified = False
        if action_type == "set_video_progress":
            current = next((
                item for item in storage.list_study_plan_videos_with_records()
                if int(item.get("id") or 0) == int(before.get("video_id") or 0)
            ), None)
            verified = bool(current and _study_assistant_numbers_match(
                current.get("watched_seconds"), before.get("watched_seconds"), tolerance=0.5
            ))
        elif action_type == "set_study_time_session":
            current = storage.get_study_time_session(str(before.get("session_id") or ""))
            verified = bool(current and _study_assistant_numbers_match(
                current.get("elapsed_seconds"), before.get("elapsed_seconds"), tolerance=0.5
            ))
        elif action_type == "move_study_time_between_days":
            activity = after.get("activity") if isinstance(after.get("activity"), dict) else {}
            study_time = after.get("study_time") if isinstance(after.get("study_time"), dict) else after
            sources = before.get("source_sessions") if isinstance(before.get("source_sessions"), list) else []
            source_matches = []
            for source in sources:
                current = storage.get_study_time_session(str(source.get("session_id") or ""))
                source_matches.append(bool(current and _study_assistant_numbers_match(
                    current.get("elapsed_seconds"), source.get("elapsed_seconds"), tolerance=0.5
                )))
            target = study_time.get("target_session") if isinstance(study_time.get("target_session"), dict) else {}
            study_time_verified = True
            if sources:
                study_time_verified = bool(
                    source_matches
                    and all(source_matches)
                    and target
                    and not storage.get_study_time_session(str(target.get("session_id") or ""))
                )
            activity_verified = True
            if activity:
                video_id = int(activity.get("video_id") or 0)
                source_seconds = _study_assistant_activity_seconds(
                    video_id=video_id,
                    day=str(before.get("source_day") or ""),
                )
                target_seconds = _study_assistant_activity_seconds(
                    video_id=video_id,
                    day=str(before.get("target_day") or ""),
                )
                activity_verified = bool(
                    _study_assistant_numbers_match(
                        source_seconds,
                        before.get("source_activity_seconds", activity.get("before_source_seconds")),
                        tolerance=0.5,
                    )
                    and _study_assistant_numbers_match(
                        target_seconds,
                        before.get("target_activity_seconds", activity.get("before_target_seconds")),
                        tolerance=0.5,
                    )
                )
            verified = bool(study_time_verified and activity_verified and (sources or activity))
        elif action_type == "update_study_plan":
            current = storage.get_study_plan_replan_settings()
            old = before.get("settings") if isinstance(before.get("settings"), dict) else {}
            if not before.get("exists"):
                verified = current is None
            else:
                verified = bool(
                    current
                    and str(current.get("start_date") or "") == str(old.get("start_date") or "")
                    and str(current.get("end_date") or "") == str(old.get("end_date") or "")
                    and _study_assistant_numbers_match(current.get("weekday_minutes"), old.get("weekday_minutes"))
                    and _study_assistant_numbers_match(current.get("weekend_minutes"), old.get("weekend_minutes"))
                    and _study_assistant_mappings_match(current.get("baseline_by_subject"), old.get("baseline_by_subject"))
                    and _study_assistant_mappings_match(current.get("subject_targets"), old.get("subject_targets"))
                )
        return {
            "verified": verified,
            "label": "已回讀資料庫確認復原" if verified else "復原後驗證未通過",
            "details": [],
        }

    @app.post("/admin/study-recall/assistant/actions/<action_id>/apply")
    @admin_required
    def admin_study_assistant_action_apply(action_id: str):
        username = str((current_user() or {}).get("username") or "")
        action = storage.get_study_assistant_action(action_id, username=username)
        if not action:
            return {"ok": False, "error": "找不到這次變更提案。"}, 404
        if action.get("status") != "pending":
            return {"ok": False, "error": "這次變更已處理，不能重複套用。"}, 409
        if not storage.transition_study_assistant_action(
            action_id,
            username=username,
            expected_status="pending",
            next_status="applying",
        ):
            return {"ok": False, "error": "這次變更正在處理或已經完成。"}, 409

        action_type = str(action.get("action_type") or "")
        payload = dict(action.get("action_payload") or {})
        before = dict(action.get("before_state") or {})
        after: Dict[str, Any] = {}
        conflict_message = "資料已在其他頁面更新，請重新向 AI 提出修改，以免覆蓋較新的紀錄。"
        try:
            if action_type == "set_video_progress":
                result = storage.update_study_plan_video_progress(
                    video_id=int(payload.get("video_id") or 0),
                    watched_seconds=float(payload.get("target_seconds") or 0),
                    expected_version=int(payload.get("expected_version") or 0),
                )
                if not result:
                    raise LookupError("找不到要修改的影片。")
                if result.get("stale"):
                    _study_assistant_restore_action_status(
                        action_id, username, current="applying", restored="failed"
                    )
                    return {"ok": False, "error": conflict_message}, 409
                after = {
                    "video_id": int(result["video_id"]),
                    "watched_seconds": float(result["watched_seconds"]),
                    "progress_version": int(result["progress_version"]),
                }
            elif action_type == "set_study_time_session":
                result = storage.update_study_time_session_elapsed(
                    session_key=str(payload.get("session_id") or ""),
                    elapsed_seconds=float(payload.get("target_seconds") or 0),
                    expected_updated_at=str(payload.get("expected_updated_at") or ""),
                )
                if not result:
                    raise LookupError("找不到要修改的學習紀錄。")
                if result.get("stale"):
                    _study_assistant_restore_action_status(
                        action_id, username, current="applying", restored="failed"
                    )
                    return {"ok": False, "error": conflict_message}, 409
                after = dict(result)
            elif action_type == "move_study_time_between_days":
                activity_result = storage.move_study_plan_activity_between_days(
                    video_id=int(payload.get("video_id") or 0),
                    source_day=str(payload.get("source_day") or ""),
                    target_day=str(payload.get("target_day") or ""),
                    seconds=float(payload.get("target_seconds") or 0),
                    expected_source_seconds=float(payload.get("expected_source_activity_seconds") or 0),
                    expected_target_seconds=float(payload.get("expected_target_activity_seconds") or 0),
                )
                if not activity_result:
                    raise LookupError("找不到可移轉的影片觀看活動。")
                if activity_result.get("stale"):
                    _study_assistant_restore_action_status(
                        action_id, username, current="applying", restored="failed"
                    )
                    return {"ok": False, "error": conflict_message}, 409
                after = {"activity": dict(activity_result)}
                moves = list(payload.get("moves") or [])
                if moves:
                    result = storage.move_study_time_between_days(
                        moves=moves,
                        source_day=str(payload.get("source_day") or ""),
                        target_day=str(payload.get("target_day") or ""),
                        target_session_key=str(payload.get("target_session_id") or ""),
                    )
                    if not result or result.get("stale"):
                        storage.undo_move_study_plan_activity_between_days(
                            original_rows=list(activity_result.get("original_rows") or []),
                            generated_ids=list(activity_result.get("generated_ids") or []),
                        )
                        _study_assistant_restore_action_status(
                            action_id, username, current="applying", restored="failed"
                        )
                        return {"ok": False, "error": conflict_message}, 409
                    after["study_time"] = dict(result)
            elif action_type == "update_study_plan":
                current = storage.get_study_plan_replan_settings()
                expected_exists = bool(before.get("exists"))
                expected_settings = before.get("settings") if isinstance(before.get("settings"), dict) else {}
                if expected_exists != bool(current) or (
                    expected_exists
                    and str((current or {}).get("updated_at") or "")
                    != str(expected_settings.get("updated_at") or "")
                ):
                    _study_assistant_restore_action_status(
                        action_id, username, current="applying", restored="failed"
                    )
                    return {"ok": False, "error": conflict_message}, 409
                storage.save_study_plan_replan_settings(
                    start_date=str(payload.get("start_date") or ""),
                    end_date=str(payload.get("end_date") or ""),
                    weekday_minutes=float(payload.get("weekday_minutes") or 0),
                    weekend_minutes=float(payload.get("weekend_minutes") or 0),
                    baseline_by_subject=dict(payload.get("baseline_by_subject") or {}),
                    subject_targets=dict(payload.get("subject_targets") or {}),
                )
                after = {"settings": storage.get_study_plan_replan_settings() or {}}
            else:
                raise ValueError("不支援的資料修改類型。")
        except (LookupError, ValueError, TypeError) as exc:
            _study_assistant_restore_action_status(
                action_id, username, current="applying", restored="failed"
            )
            return {"ok": False, "error": str(exc) or "資料修改失敗。"}, 400
        except Exception:
            app.logger.exception("Failed to apply study assistant action %s", action_id)
            _study_assistant_restore_action_status(
                action_id, username, current="applying", restored="failed"
            )
            return {"ok": False, "error": "資料修改失敗，原資料未被標記為完成。"}, 500

        verified, verified_state, verification = _study_assistant_verify_applied_action(
            action_type, payload, after
        )
        if not verified:
            rolled_back = _study_assistant_rollback_unverified_action(
                action_type, before, verified_state or after
            )
            _study_assistant_restore_action_status(
                action_id, username, current="applying", restored="failed"
            )
            _invalidate_study_progress_context()
            record_ui_event(
                "study_assistant_action_verification_failed",
                meta={"action_id": action_id, "action_type": action_type, "rolled_back": rolled_back},
            )
            error = (
                "寫入後的資料庫驗證未通過，系統已自動恢復原資料；這次變更沒有完成。"
                if rolled_back
                else "寫入後的資料庫驗證未通過，且無法安全覆蓋後續變更；請重新整理並檢查資料。"
            )
            return {"ok": False, "error": error, "verification": verification}, 500

        if not storage.transition_study_assistant_action(
            action_id,
            username=username,
            expected_status="applying",
            next_status="applied",
            after_state=after,
        ):
            app.logger.error("Study assistant action applied but audit transition failed: %s", action_id)
            rolled_back = _study_assistant_rollback_unverified_action(
                action_type, before, verified_state
            )
            _study_assistant_restore_action_status(
                action_id, username, current="applying", restored="failed"
            )
            return {
                "ok": False,
                "error": (
                    "稽核紀錄建立失敗，系統已自動恢復原資料；這次變更沒有完成。"
                    if rolled_back
                    else "稽核紀錄建立失敗，請重新整理並檢查目前資料。"
                ),
            }, 500
        _invalidate_study_progress_context()
        record_ui_event(
            "study_assistant_action_applied",
            meta={"action_id": action_id, "action_type": action_type},
        )
        return {
            "ok": True,
            "message": f"已套用並回讀確認：{action.get('summary')}",
            "verification": verification,
            "undo": {
                "label": "復原這次變更",
                "url": url_for("admin_study_assistant_action_undo", action_id=action_id),
            },
        }

    @app.post("/admin/study-recall/assistant/actions/<action_id>/undo")
    @admin_required
    def admin_study_assistant_action_undo(action_id: str):
        username = str((current_user() or {}).get("username") or "")
        action = storage.get_study_assistant_action(action_id, username=username)
        if not action:
            return {"ok": False, "error": "找不到這次變更紀錄。"}, 404
        if action.get("status") != "applied":
            return {"ok": False, "error": "這次變更目前無法復原。"}, 409
        if not storage.transition_study_assistant_action(
            action_id,
            username=username,
            expected_status="applied",
            next_status="reverting",
        ):
            return {"ok": False, "error": "這次變更正在處理或已復原。"}, 409

        action_type = str(action.get("action_type") or "")
        before = dict(action.get("before_state") or {})
        after = dict(action.get("after_state") or {})
        conflict_message = "資料套用後又有新的變更，為避免覆蓋最新紀錄，這次無法自動復原。"
        try:
            if action_type == "set_video_progress":
                result = storage.update_study_plan_video_progress(
                    video_id=int(before.get("video_id") or 0),
                    watched_seconds=float(before.get("watched_seconds") or 0),
                    expected_version=int(after.get("progress_version") or -1),
                )
                if not result:
                    raise LookupError("找不到原本的影片紀錄。")
                if result.get("stale"):
                    _study_assistant_restore_action_status(
                        action_id, username, current="reverting", restored="applied"
                    )
                    return {"ok": False, "error": conflict_message}, 409
            elif action_type == "set_study_time_session":
                result = storage.update_study_time_session_elapsed(
                    session_key=str(before.get("session_id") or ""),
                    elapsed_seconds=float(before.get("elapsed_seconds") or 0),
                    expected_updated_at=str(after.get("updated_at") or ""),
                )
                if not result:
                    raise LookupError("找不到原本的學習紀錄。")
                if result.get("stale"):
                    _study_assistant_restore_action_status(
                        action_id, username, current="reverting", restored="applied"
                    )
                    return {"ok": False, "error": conflict_message}, 409
            elif action_type == "move_study_time_between_days":
                activity = after.get("activity") if isinstance(after.get("activity"), dict) else {}
                study_time = after.get("study_time") if isinstance(after.get("study_time"), dict) else after
                target = study_time.get("target_session") if isinstance(study_time.get("target_session"), dict) else {}
                source_after = {
                    str(item.get("session_id") or ""): item
                    for item in (study_time.get("source_sessions") or [])
                    if isinstance(item, dict)
                }
                source_before = before.get("source_sessions") if isinstance(before.get("source_sessions"), list) else []
                study_time_restored = True
                if source_before:
                    study_time_restored = storage.undo_move_study_time_between_days(
                        source_sessions=[{
                            "session_id": str(item.get("session_id") or ""),
                            "restore_seconds": float(item.get("elapsed_seconds") or 0),
                            "expected_updated_at": str(source_after.get(str(item.get("session_id") or ""), {}).get("updated_at") or ""),
                        } for item in source_before],
                        target_session_key=str(target.get("session_id") or ""),
                        expected_target_updated_at=str(target.get("updated_at") or ""),
                    )
                activity_restored = True
                if activity:
                    activity_restored = storage.undo_move_study_plan_activity_between_days(
                        original_rows=list(activity.get("original_rows") or []),
                        generated_ids=list(activity.get("generated_ids") or []),
                    )
                if not study_time_restored or not activity_restored or not (source_before or activity):
                    _study_assistant_restore_action_status(
                        action_id, username, current="reverting", restored="applied"
                    )
                    return {"ok": False, "error": conflict_message}, 409
            elif action_type == "update_study_plan":
                current = storage.get_study_plan_replan_settings()
                after_settings = after.get("settings") if isinstance(after.get("settings"), dict) else {}
                if (
                    not current
                    or str(current.get("updated_at") or "") != str(after_settings.get("updated_at") or "")
                ):
                    _study_assistant_restore_action_status(
                        action_id, username, current="reverting", restored="applied"
                    )
                    return {"ok": False, "error": conflict_message}, 409
                old_settings = before.get("settings") if isinstance(before.get("settings"), dict) else {}
                if before.get("exists"):
                    storage.save_study_plan_replan_settings(
                        start_date=str(old_settings.get("start_date") or ""),
                        end_date=str(old_settings.get("end_date") or ""),
                        weekday_minutes=float(old_settings.get("weekday_minutes") or 0),
                        weekend_minutes=float(old_settings.get("weekend_minutes") or 0),
                        baseline_by_subject=dict(old_settings.get("baseline_by_subject") or {}),
                        subject_targets=dict(old_settings.get("subject_targets") or {}),
                    )
                else:
                    storage.delete_study_plan_replan_settings()
            else:
                raise ValueError("不支援的資料修改類型。")
        except (LookupError, ValueError, TypeError) as exc:
            _study_assistant_restore_action_status(
                action_id, username, current="reverting", restored="applied"
            )
            return {"ok": False, "error": str(exc) or "無法復原資料。"}, 400
        except Exception:
            app.logger.exception("Failed to undo study assistant action %s", action_id)
            _study_assistant_restore_action_status(
                action_id, username, current="reverting", restored="applied"
            )
            return {"ok": False, "error": "復原失敗，請重新檢查目前資料。"}, 500

        verification = _study_assistant_verify_reverted_action(action_type, before, after)
        if not verification["verified"]:
            _study_assistant_restore_action_status(
                action_id, username, current="reverting", restored="applied"
            )
            return {
                "ok": False,
                "error": "復原後的資料庫驗證未通過，請重新整理並檢查目前資料。",
                "verification": verification,
            }, 500

        if not storage.transition_study_assistant_action(
            action_id,
            username=username,
            expected_status="reverting",
            next_status="reverted",
        ):
            return {"ok": False, "error": "資料已復原，但稽核狀態更新失敗。"}, 500
        _invalidate_study_progress_context()
        record_ui_event(
            "study_assistant_action_reverted",
            meta={"action_id": action_id, "action_type": action_type},
        )
        return {"ok": True, "message": "已復原並回讀資料庫確認。", "verification": verification}

    return (
        _study_assistant_restore_action_status,
        _study_assistant_activity_seconds,
        _repair_legacy_study_assistant_time_moves,
        _study_assistant_numbers_match,
        _study_assistant_mappings_match,
        _study_assistant_verify_applied_action,
        _study_assistant_rollback_unverified_action,
        _study_assistant_verify_reverted_action,
        admin_study_assistant_action_apply,
        admin_study_assistant_action_undo,
    )
