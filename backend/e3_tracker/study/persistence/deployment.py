"""Study-owned player persistence, synchronization and historical data repairs."""

import hashlib
import hmac
import json
from datetime import datetime
from typing import Any, Dict, List, Mapping
from sqlalchemy import text
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.study.domain.player_settings import (
    PLAYER_SETTINGS_DEFAULTS,
    normalize_player_settings,
)
from e3_tracker.study.services.discord_presence import DISCORD_APPLICATION_ID_PATTERN

_YOUTUBE_FIELDS = (
    "youtube_video_id",
    "youtube_playlist_id",
    "youtube_url",
)


class StudyDeploymentStorage:
    """Keep deployment data and player preferences authoritative across restarts."""

    def _repair_remove_misclassified_computer_org_videos_and_replan(self) -> None:
        """Remove the logic-design tail and rebuild the remaining whole-course plan."""
        repair_key = "remove-computer-org-70-78-and-replan-2026-11-19"
        repair_day = "2026-11-19"
        start_date = "2026-11-20"
        end_date = "2026-11-30"
        subject_order = (
            "線性代數",
            "離散數學",
            "資料結構",
            "作業系統",
            "計算機組織",
            "演算法",
        )
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs "
                    "WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return

            wrong_videos = (
                conn.execute(
                    text(
                        "SELECT id, sequence, youtube_video_id "
                        "FROM study_plan_videos "
                        "WHERE subject = :subject AND sequence BETWEEN 70 AND 78 "
                        "ORDER BY sequence"
                    ),
                    {"subject": "計算機組織"},
                )
                .mappings()
                .all()
            )
            # Do not install a dated global replan on a new or unrelated database.
            if not wrong_videos:
                return

            wrong_ids = [int(row["id"]) for row in wrong_videos]
            bind_names = [f"video_{index}" for index in range(len(wrong_ids))]
            placeholders = ", ".join(f":{name}" for name in bind_names)
            id_params = dict(zip(bind_names, wrong_ids))

            removed_activity_seconds = float(
                conn.execute(
                    text(
                        "SELECT COALESCE(SUM(delta_seconds), 0) "
                        f"FROM study_plan_activity_events WHERE video_id IN ({placeholders})"
                    ),
                    id_params,
                ).scalar_one()
                or 0
            )
            removed_session_seconds = float(
                conn.execute(
                    text(
                        "SELECT COALESCE(SUM(elapsed_seconds), 0) "
                        f"FROM study_time_sessions WHERE video_id IN ({placeholders})"
                    ),
                    id_params,
                ).scalar_one()
                or 0
            )
            deleted_counts: Dict[str, int] = {}
            for table_name in (
                "study_plan_activity_events",
                "study_time_sessions",
                "study_plan_video_markers",
                "study_plan_video_records",
                "study_plan_video_overrides",
            ):
                result = conn.execute(
                    text(
                        f"DELETE FROM {table_name} WHERE video_id IN ({placeholders})"
                    ),
                    id_params,
                )
                deleted_counts[table_name] = max(0, int(result.rowcount or 0))

            deleted_video_count = conn.execute(
                text(f"DELETE FROM study_plan_videos WHERE id IN ({placeholders})"),
                id_params,
            ).rowcount

            # Snapshots at and after the correction can no longer be trusted, and
            # rest days would conflict with the requested seven-day schedule.
            conn.execute(
                text("DELETE FROM study_plan_daily_snapshots WHERE day >= :repair_day"),
                {"repair_day": repair_day},
            )
            conn.execute(
                text(
                    "DELETE FROM study_plan_rest_days "
                    "WHERE study_date >= :start_date AND study_date <= :end_date"
                ),
                {"start_date": start_date, "end_date": end_date},
            )

            progress_rows = (
                conn.execute(
                    text(
                        "SELECT v.subject, v.duration_seconds, "
                        "COALESCE(r.watched_seconds, 0) AS watched_seconds "
                        "FROM study_plan_videos v "
                        "LEFT JOIN study_plan_video_records r ON r.video_id = v.id"
                    )
                )
                .mappings()
                .all()
            )
            baseline_by_subject = {subject: 0.0 for subject in subject_order}
            total_by_subject = {subject: 0.0 for subject in subject_order}
            for row in progress_rows:
                subject = str(row["subject"] or "")
                if subject not in baseline_by_subject:
                    continue
                duration = max(0.0, float(row["duration_seconds"] or 0))
                watched = min(duration, max(0.0, float(row["watched_seconds"] or 0)))
                total_by_subject[subject] += duration
                baseline_by_subject[subject] += watched
            subject_targets = {
                subject: max(0.0, total_by_subject[subject] - baseline_by_subject[subject])
                for subject in subject_order
                if total_by_subject[subject] - baseline_by_subject[subject] > 0.001
            }

            now = datetime.utcnow().isoformat()
            replan_values = {
                "start_date": start_date,
                "end_date": end_date,
                # Weekends retain one third of a weekday's weight.  The schedule
                # engine scales these weights to the actual remaining duration.
                "weekday_minutes": 720.0,
                "weekend_minutes": 240.0,
                "baseline_by_subject": json.dumps(
                    baseline_by_subject, ensure_ascii=False, sort_keys=True
                ),
                "subject_targets": json.dumps(
                    subject_targets, ensure_ascii=False, sort_keys=True
                ),
                "updated_at": now,
            }
            if conn.execute(
                text("SELECT id FROM study_plan_replan_settings WHERE id = 1")
            ).first():
                conn.execute(
                    text(
                        "UPDATE study_plan_replan_settings SET "
                        "start_date = :start_date, end_date = :end_date, "
                        "weekday_minutes = :weekday_minutes, "
                        "weekend_minutes = :weekend_minutes, "
                        "baseline_by_subject = :baseline_by_subject, "
                        "subject_targets = :subject_targets, updated_at = :updated_at "
                        "WHERE id = 1"
                    ),
                    replan_values,
                )
            else:
                conn.execute(
                    text(
                        "INSERT INTO study_plan_replan_settings ("
                        "id, start_date, end_date, weekday_minutes, weekend_minutes, "
                        "baseline_by_subject, subject_targets, created_at, updated_at"
                        ") VALUES ("
                        "1, :start_date, :end_date, :weekday_minutes, :weekend_minutes, "
                        ":baseline_by_subject, :subject_targets, :updated_at, :updated_at"
                        ")"
                    ),
                    replan_values,
                )

            details = json.dumps(
                {
                    "removed_sequences": [int(row["sequence"]) for row in wrong_videos],
                    "deleted_videos": max(0, int(deleted_video_count or 0)),
                    "deleted_counts": deleted_counts,
                    "removed_activity_seconds": removed_activity_seconds,
                    "removed_session_seconds": removed_session_seconds,
                    "replan_start": start_date,
                    "replan_end": end_date,
                    "remaining_hours": round(sum(subject_targets.values()) / 3600, 2),
                    "subjects": list(subject_targets),
                },
                ensure_ascii=False,
            )
            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {"repair_key": repair_key, "details": details, "applied_at": now},
            )

    def _repair_replan_only_final_three_subjects(self) -> None:
        """Replan only OS, computer organization and algorithms from live progress."""
        repair_key = "replan-final-three-subjects-2026-11-19"
        prerequisite_key = "remove-computer-org-70-78-and-replan-2026-11-19"
        start_date = "2026-11-20"
        end_date = "2026-11-30"
        final_subjects = ("作業系統", "計算機組織", "演算法")
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs "
                    "WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return
            # This is a follow-up to the production cleanup above.  Requiring
            # that marker prevents a dated plan from appearing in fresh installs.
            if not conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs "
                    "WHERE repair_key = :repair_key"
                ),
                {"repair_key": prerequisite_key},
            ).first():
                return

            rows = (
                conn.execute(
                    text(
                        "SELECT v.subject, v.duration_seconds, "
                        "COALESCE(r.watched_seconds, 0) AS watched_seconds "
                        "FROM study_plan_videos v "
                        "LEFT JOIN study_plan_video_records r ON r.video_id = v.id "
                        "WHERE v.subject IN (:os_subject, :co_subject, :algo_subject)"
                    ),
                    {
                        "os_subject": final_subjects[0],
                        "co_subject": final_subjects[1],
                        "algo_subject": final_subjects[2],
                    },
                )
                .mappings()
                .all()
            )
            present_subjects = {str(row["subject"] or "") for row in rows}
            if not all(subject in present_subjects for subject in final_subjects):
                return

            baseline_by_subject = {subject: 0.0 for subject in final_subjects}
            subject_targets = {subject: 0.0 for subject in final_subjects}
            for row in rows:
                subject = str(row["subject"] or "")
                duration = max(0.0, float(row["duration_seconds"] or 0))
                watched = min(duration, max(0.0, float(row["watched_seconds"] or 0)))
                # Match the UI's completion rule.  Without this tolerance, the
                # final 0.1-5 seconds of completed videos become fake 0h tasks.
                complete = duration > 0 and (
                    watched >= duration - 5.0
                    or watched / duration >= 0.995
                )
                credited = duration if complete else watched
                baseline_by_subject[subject] += credited
                subject_targets[subject] += max(0.0, duration - credited)
            subject_targets = {
                subject: seconds
                for subject, seconds in subject_targets.items()
                if seconds > 0.001
            }

            now = datetime.utcnow().isoformat()
            values = {
                "start_date": start_date,
                "end_date": end_date,
                "weekday_minutes": 720.0,
                "weekend_minutes": 240.0,
                "baseline_by_subject": json.dumps(
                    baseline_by_subject, ensure_ascii=False, sort_keys=True
                ),
                "subject_targets": json.dumps(
                    subject_targets, ensure_ascii=False, sort_keys=True
                ),
                "updated_at": now,
            }
            conn.execute(
                text(
                    "UPDATE study_plan_replan_settings SET "
                    "start_date = :start_date, end_date = :end_date, "
                    "weekday_minutes = :weekday_minutes, "
                    "weekend_minutes = :weekend_minutes, "
                    "baseline_by_subject = :baseline_by_subject, "
                    "subject_targets = :subject_targets, updated_at = :updated_at "
                    "WHERE id = 1"
                ),
                values,
            )
            conn.execute(
                text(
                    "DELETE FROM study_plan_rest_days "
                    "WHERE study_date >= :start_date AND study_date <= :end_date"
                ),
                {"start_date": start_date, "end_date": end_date},
            )
            details = json.dumps(
                {
                    "scope": list(final_subjects),
                    "scheduled_subjects": list(subject_targets),
                    "completed_subjects": [
                        subject
                        for subject in final_subjects
                        if subject not in subject_targets
                    ],
                    "remaining_hours": {
                        subject: round(seconds / 3600, 2)
                        for subject, seconds in subject_targets.items()
                    },
                    "replan_start": start_date,
                    "replan_end": end_date,
                },
                ensure_ascii=False,
            )
            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {"repair_key": repair_key, "details": details, "applied_at": now},
            )

    def _repair_finalize_computer_org_and_replan_os_algorithm(self) -> None:
        """End computer organization today and schedule only OS then algorithms."""
        repair_key = "finalize-computer-org-replan-os-algorithm-2026-11-19-v2"
        start_date = "2026-11-20"
        end_date = "2026-11-30"
        scheduled_subjects = ("作業系統", "演算法")
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs "
                    "WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return

            counts = {
                str(row["subject"]): int(row["video_count"] or 0)
                for row in conn.execute(
                    text(
                        "SELECT subject, COUNT(*) AS video_count "
                        "FROM study_plan_videos "
                        "WHERE subject IN (:os_subject, :co_subject, :algo_subject) "
                        "GROUP BY subject"
                    ),
                    {
                        "os_subject": "作業系統",
                        "co_subject": "計算機組織",
                        "algo_subject": "演算法",
                    },
                ).mappings()
            }
            # Match the complete production inventory and avoid imposing this
            # dated repair on small development databases.
            if (
                counts.get("作業系統", 0) < 58
                or counts.get("計算機組織", 0) < 69
                or counts.get("演算法", 0) < 16
            ):
                return

            rows = (
                conn.execute(
                    text(
                        "SELECT v.subject, v.duration_seconds, "
                        "COALESCE(r.watched_seconds, 0) AS watched_seconds "
                        "FROM study_plan_videos v "
                        "LEFT JOIN study_plan_video_records r ON r.video_id = v.id "
                        "WHERE v.subject IN (:os_subject, :algo_subject)"
                    ),
                    {
                        "os_subject": scheduled_subjects[0],
                        "algo_subject": scheduled_subjects[1],
                    },
                )
                .mappings()
                .all()
            )
            baseline_by_subject = {subject: 0.0 for subject in scheduled_subjects}
            subject_targets = {subject: 0.0 for subject in scheduled_subjects}
            for row in rows:
                subject = str(row["subject"] or "")
                duration = max(0.0, float(row["duration_seconds"] or 0))
                watched = min(duration, max(0.0, float(row["watched_seconds"] or 0)))
                complete = duration > 0 and (
                    watched >= duration - 5.0
                    or watched / duration >= 0.995
                )
                credited = duration if complete else watched
                baseline_by_subject[subject] += credited
                subject_targets[subject] += max(0.0, duration - credited)
            subject_targets = {
                subject: seconds
                for subject, seconds in subject_targets.items()
                if seconds > 0.001
            }
            now = datetime.utcnow().isoformat()
            values = {
                "start_date": start_date,
                "end_date": end_date,
                "weekday_minutes": 720.0,
                "weekend_minutes": 240.0,
                "baseline_by_subject": json.dumps(
                    baseline_by_subject, ensure_ascii=False, sort_keys=True
                ),
                "subject_targets": json.dumps(
                    subject_targets, ensure_ascii=False, sort_keys=True
                ),
                "updated_at": now,
            }
            if conn.execute(
                text("SELECT id FROM study_plan_replan_settings WHERE id = 1")
            ).first():
                conn.execute(
                    text(
                        "UPDATE study_plan_replan_settings SET "
                        "start_date = :start_date, end_date = :end_date, "
                        "weekday_minutes = :weekday_minutes, "
                        "weekend_minutes = :weekend_minutes, "
                        "baseline_by_subject = :baseline_by_subject, "
                        "subject_targets = :subject_targets, updated_at = :updated_at "
                        "WHERE id = 1"
                    ),
                    values,
                )
            else:
                conn.execute(
                    text(
                        "INSERT INTO study_plan_replan_settings ("
                        "id, start_date, end_date, weekday_minutes, weekend_minutes, "
                        "baseline_by_subject, subject_targets, created_at, updated_at"
                        ") VALUES ("
                        "1, :start_date, :end_date, :weekday_minutes, :weekend_minutes, "
                        ":baseline_by_subject, :subject_targets, :updated_at, :updated_at"
                        ")"
                    ),
                    values,
                )
            conn.execute(
                text(
                    "DELETE FROM study_plan_rest_days "
                    "WHERE study_date >= :start_date AND study_date <= :end_date"
                ),
                {"start_date": start_date, "end_date": end_date},
            )
            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {
                    "repair_key": repair_key,
                    "details": json.dumps(
                        {
                            "computer_organization_end": "2026-11-19",
                            "scheduled_subjects": list(subject_targets),
                            "remaining_hours": {
                                subject: round(seconds / 3600, 2)
                                for subject, seconds in subject_targets.items()
                            },
                            "replan_start": start_date,
                            "replan_end": end_date,
                        },
                        ensure_ascii=False,
                    ),
                    "applied_at": now,
                },
            )

    def _repair_even_daily_weight_for_remaining_plan(self) -> None:
        """Use the same daily weight while preserving separate subject blocks."""
        repair_key = "even-daily-os-algorithm-plan-2026-11-19"
        prerequisite_key = "finalize-computer-org-replan-os-algorithm-2026-11-19-v2"
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs "
                    "WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return
            if not conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs "
                    "WHERE repair_key = :repair_key"
                ),
                {"repair_key": prerequisite_key},
            ).first():
                return
            settings = conn.execute(
                text(
                    "SELECT subject_targets FROM study_plan_replan_settings WHERE id = 1"
                )
            ).first()
            if not settings:
                return
            targets = json.loads(str(settings[0] or "{}"))
            if any(subject not in {"作業系統", "演算法"} for subject in targets):
                return

            now = datetime.utcnow().isoformat()
            conn.execute(
                text(
                    "UPDATE study_plan_replan_settings SET "
                    "weekday_minutes = 240, weekend_minutes = 240, "
                    "updated_at = :updated_at WHERE id = 1"
                ),
                {"updated_at": now},
            )
            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {
                    "repair_key": repair_key,
                    "details": json.dumps(
                        {
                            "weekday_minutes": 240,
                            "weekend_minutes": 240,
                            "subjects_remain_separate": True,
                        },
                        ensure_ascii=False,
                    ),
                    "applied_at": now,
                },
            )

    def _ensure_current_os_algorithm_plan(self) -> None:
        """Recover the active plan if a restart drops or restores stale settings."""
        today = datetime.now(TAIPEI_TZ).date().isoformat()
        if today < "2026-11-19" or today > "2026-11-30":
            return
        start_date = "2026-11-20"
        end_date = "2026-11-30"
        subjects = ("作業系統", "演算法")
        with self._lock, self._engine.begin() as conn:
            saved = conn.execute(
                text("SELECT * FROM study_plan_replan_settings WHERE id = 1")
            ).mappings().first()
            if saved:
                try:
                    saved_targets = json.loads(str(saved["subject_targets"] or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    saved_targets = None
                if (
                    str(saved["start_date"] or "") == start_date
                    and str(saved["end_date"] or "") == end_date
                    and abs(float(saved["weekday_minutes"] or 0) - 240.0) < 0.001
                    and abs(float(saved["weekend_minutes"] or 0) - 240.0) < 0.001
                    and isinstance(saved_targets, dict)
                    and set(saved_targets).issubset(set(subjects))
                ):
                    return

            counts = {
                str(row["subject"]): int(row["video_count"] or 0)
                for row in conn.execute(
                    text(
                        "SELECT subject, COUNT(*) AS video_count "
                        "FROM study_plan_videos "
                        "WHERE subject IN (:os_subject, :co_subject, :algo_subject) "
                        "GROUP BY subject"
                    ),
                    {
                        "os_subject": subjects[0],
                        "co_subject": "計算機組織",
                        "algo_subject": subjects[1],
                    },
                ).mappings()
            }
            if (
                counts.get("作業系統", 0) < 58
                or counts.get("計算機組織", 0) < 69
                or counts.get("演算法", 0) < 16
            ):
                return
            rows = conn.execute(
                text(
                    "SELECT v.subject, v.duration_seconds, "
                    "COALESCE(r.watched_seconds, 0) AS watched_seconds "
                    "FROM study_plan_videos v "
                    "LEFT JOIN study_plan_video_records r ON r.video_id = v.id "
                    "WHERE v.subject IN (:os_subject, :algo_subject)"
                ),
                {"os_subject": subjects[0], "algo_subject": subjects[1]},
            ).mappings().all()
            baselines = {subject: 0.0 for subject in subjects}
            targets = {subject: 0.0 for subject in subjects}
            for row in rows:
                subject = str(row["subject"] or "")
                duration = max(0.0, float(row["duration_seconds"] or 0))
                watched = min(duration, max(0.0, float(row["watched_seconds"] or 0)))
                complete = duration > 0 and (
                    watched >= duration - 5.0 or watched / duration >= 0.995
                )
                credited = duration if complete else watched
                baselines[subject] += credited
                targets[subject] += max(0.0, duration - credited)
            targets = {
                subject: seconds
                for subject, seconds in targets.items()
                if seconds > 0.001
            }
            now = datetime.utcnow().isoformat()
            values = {
                "start_date": start_date,
                "end_date": end_date,
                "weekday_minutes": 240.0,
                "weekend_minutes": 240.0,
                "baseline_by_subject": json.dumps(
                    baselines, ensure_ascii=False, sort_keys=True
                ),
                "subject_targets": json.dumps(
                    targets, ensure_ascii=False, sort_keys=True
                ),
                "updated_at": now,
            }
            if saved:
                conn.execute(
                    text(
                        "UPDATE study_plan_replan_settings SET "
                        "start_date = :start_date, end_date = :end_date, "
                        "weekday_minutes = :weekday_minutes, "
                        "weekend_minutes = :weekend_minutes, "
                        "baseline_by_subject = :baseline_by_subject, "
                        "subject_targets = :subject_targets, updated_at = :updated_at "
                        "WHERE id = 1"
                    ),
                    values,
                )
            else:
                conn.execute(
                    text(
                        "INSERT INTO study_plan_replan_settings ("
                        "id, start_date, end_date, weekday_minutes, weekend_minutes, "
                        "baseline_by_subject, subject_targets, created_at, updated_at"
                        ") VALUES ("
                        "1, :start_date, :end_date, :weekday_minutes, :weekend_minutes, "
                        ":baseline_by_subject, :subject_targets, :updated_at, :updated_at"
                        ")"
                    ),
                    values,
                )

    def _repair_discrete_video_11_12_history(self) -> None:
        repair_key = "discrete-video-11-to-12-2026-08-26"
        repair_day = "2026-08-26"
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return

            videos = (
                conn.execute(
                    text(
                        "SELECT id, sequence, title FROM study_plan_videos "
                        "WHERE subject = :subject AND sequence IN (11, 12)"
                    ),
                    {"subject": "離散數學"},
                )
                .mappings()
                .all()
            )
            video_by_sequence = {int(row["sequence"]): row for row in videos}
            source = video_by_sequence.get(11)
            target = video_by_sequence.get(12)
            if source is None or target is None:
                return

            source_id = int(source["id"])
            target_id = int(target["id"])
            events = (
                conn.execute(
                    text(
                        "SELECT id, video_id, watched_seconds, updated_at "
                        "FROM study_plan_activity_events "
                        "WHERE video_id IN (:source_id, :target_id) "
                        "ORDER BY updated_at, id"
                    ),
                    {"source_id": source_id, "target_id": target_id},
                )
                .mappings()
                .all()
            )
            moved_event_ids = [
                int(row["id"])
                for row in events
                if int(row["video_id"]) == source_id
                and self._study_plan_business_day_from_timestamp(
                    str(row["updated_at"] or "")
                )
                == repair_day
            ]
            if not moved_event_ids:
                return

            for event_id in moved_event_ids:
                conn.execute(
                    text(
                        "UPDATE study_plan_activity_events SET video_id = :target_id "
                        "WHERE id = :event_id"
                    ),
                    {"target_id": target_id, "event_id": event_id},
                )

            repaired_events = (
                conn.execute(
                    text(
                        "SELECT id, video_id, watched_seconds FROM study_plan_activity_events "
                        "WHERE video_id IN (:source_id, :target_id) "
                        "ORDER BY updated_at, id"
                    ),
                    {"source_id": source_id, "target_id": target_id},
                )
                .mappings()
                .all()
            )
            previous_by_video = {source_id: 0.0, target_id: 0.0}
            for row in repaired_events:
                video_id = int(row["video_id"])
                previous = previous_by_video[video_id]
                watched = max(0.0, float(row["watched_seconds"] or 0))
                conn.execute(
                    text(
                        "UPDATE study_plan_activity_events SET "
                        "previous_watched_seconds = :previous, delta_seconds = :delta "
                        "WHERE id = :event_id"
                    ),
                    {
                        "previous": previous,
                        "delta": watched - previous,
                        "event_id": int(row["id"]),
                    },
                )
                previous_by_video[video_id] = watched

            moved_sessions = conn.execute(
                text(
                    "UPDATE study_time_sessions SET video_id = :target_id, label = :target_title "
                    "WHERE video_id = :source_id AND day = :repair_day"
                ),
                {
                    "source_id": source_id,
                    "target_id": target_id,
                    "target_title": str(target["title"] or "離散數學第 12 支"),
                    "repair_day": repair_day,
                },
            ).rowcount
            details = json.dumps(
                {
                    "moved_events": len(moved_event_ids),
                    "moved_sessions": max(0, int(moved_sessions or 0)),
                },
                ensure_ascii=False,
            )
            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {
                    "repair_key": repair_key,
                    "details": details,
                    "applied_at": datetime.utcnow().isoformat(),
                },
            )

    def _repair_confirmed_discrete_14_calendar_move(self) -> None:
        """Finish the confirmed 23-minute Sep 1 -> Sep 2 calendar correction."""
        repair_key = "confirmed-discrete-14-calendar-move-2026-09-01-to-02"
        source_day = "2026-09-01"
        target_day = "2026-09-02"
        moved_seconds = 23 * 60
        with self._lock, self._engine.connect() as conn:
            if conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return
            video = (
                conn.execute(
                    text(
                        "SELECT id FROM study_plan_videos "
                        "WHERE subject = :subject AND sequence = :sequence"
                    ),
                    {"subject": "離散數學", "sequence": 14},
                )
                .mappings()
                .first()
            )
        if not video:
            return

        video_id = int(video["id"])
        source_seconds = sum(
            max(0.0, float(item.get("delta_seconds") or 0))
            for item in self.list_study_plan_activity_events(day=source_day)
            if int(item.get("video_id") or 0) == video_id
        )
        target_seconds = sum(
            max(0.0, float(item.get("delta_seconds") or 0))
            for item in self.list_study_plan_activity_events(day=target_day)
            if int(item.get("video_id") or 0) == video_id
        )
        # The UI rounds the stored seconds to ten minutes (the actual value is
        # about 9m32s for this video). Match only values that render as 10m; if
        # anything has changed beyond that narrow interval, fail closed.
        renders_as_ten_minutes = 9.5 * 60 <= target_seconds < 10.5 * 60
        if source_seconds + 0.5 < moved_seconds or not renders_as_ten_minutes:
            return
        result = self.move_study_plan_activity_between_days(
            video_id=video_id,
            source_day=source_day,
            target_day=target_day,
            seconds=moved_seconds,
            expected_source_seconds=source_seconds,
            expected_target_seconds=target_seconds,
        )
        if not result or result.get("stale"):
            return

        verified_source = sum(
            max(0.0, float(item.get("delta_seconds") or 0))
            for item in self.list_study_plan_activity_events(day=source_day)
            if int(item.get("video_id") or 0) == video_id
        )
        verified_target = sum(
            max(0.0, float(item.get("delta_seconds") or 0))
            for item in self.list_study_plan_activity_events(day=target_day)
            if int(item.get("video_id") or 0) == video_id
        )
        if (
            abs(verified_source - (source_seconds - moved_seconds)) > 0.5
            or abs(verified_target - (target_seconds + moved_seconds)) > 0.5
        ):
            self.undo_move_study_plan_activity_between_days(
                original_rows=list(result.get("original_rows") or []),
                generated_ids=list(result.get("generated_ids") or []),
            )
            return
        with self._lock, self._engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {
                    "repair_key": repair_key,
                    "details": json.dumps(
                        {
                            "video_id": video_id,
                            "source_day": source_day,
                            "target_day": target_day,
                            "moved_seconds": moved_seconds,
                            "source_seconds": verified_source,
                            "target_seconds": verified_target,
                        },
                        ensure_ascii=False,
                    ),
                    "applied_at": datetime.utcnow().isoformat(),
                },
            )

    def _repair_discrete_video_11_progress_offset(self) -> None:
        repair_key = "discrete-video-11-progress-offset-2026-08-27"
        repair_day = "2026-08-26"
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return

            videos = (
                conn.execute(
                    text(
                        "SELECT id, sequence FROM study_plan_videos "
                        "WHERE subject = :subject AND sequence IN (11, 12)"
                    ),
                    {"subject": "離散數學"},
                )
                .mappings()
                .all()
            )
            video_id_by_sequence = {
                int(row["sequence"]): int(row["id"]) for row in videos
            }
            source_id = video_id_by_sequence.get(11)
            target_id = video_id_by_sequence.get(12)
            if source_id is None or target_id is None:
                return

            target_events = (
                conn.execute(
                    text(
                        "SELECT watched_seconds, updated_at FROM study_plan_activity_events "
                        "WHERE video_id = :target_id ORDER BY updated_at, id"
                    ),
                    {"target_id": target_id},
                )
                .mappings()
                .all()
            )
            repaired_day_positions = [
                max(0.0, float(row["watched_seconds"] or 0))
                for row in target_events
                if self._study_plan_business_day_from_timestamp(
                    str(row["updated_at"] or "")
                )
                == repair_day
            ]
            baseline = repaired_day_positions[-1] if repaired_day_positions else 0.0
            if baseline <= 0:
                return

            source_events = (
                conn.execute(
                    text(
                        "SELECT id, watched_seconds, updated_at FROM study_plan_activity_events "
                        "WHERE video_id = :source_id ORDER BY updated_at, id"
                    ),
                    {"source_id": source_id},
                )
                .mappings()
                .all()
            )
            adjusted_event_count = 0
            for row in source_events:
                event_day = self._study_plan_business_day_from_timestamp(
                    str(row["updated_at"] or "")
                )
                if event_day <= repair_day:
                    continue
                corrected_watched = max(
                    0.0, float(row["watched_seconds"] or 0) - baseline
                )
                conn.execute(
                    text(
                        "UPDATE study_plan_activity_events SET watched_seconds = :watched "
                        "WHERE id = :event_id"
                    ),
                    {"watched": corrected_watched, "event_id": int(row["id"])},
                )
                adjusted_event_count += 1

            running_position = 0.0
            corrected_source_events = (
                conn.execute(
                    text(
                        "SELECT id, watched_seconds FROM study_plan_activity_events "
                        "WHERE video_id = :source_id ORDER BY updated_at, id"
                    ),
                    {"source_id": source_id},
                )
                .mappings()
                .all()
            )
            for row in corrected_source_events:
                watched = max(0.0, float(row["watched_seconds"] or 0))
                conn.execute(
                    text(
                        "UPDATE study_plan_activity_events SET "
                        "previous_watched_seconds = :previous, delta_seconds = :delta "
                        "WHERE id = :event_id"
                    ),
                    {
                        "previous": running_position,
                        "delta": watched - running_position,
                        "event_id": int(row["id"]),
                    },
                )
                running_position = watched

            record = (
                conn.execute(
                    text(
                        "SELECT watched_seconds, playback_seconds FROM study_plan_video_records "
                        "WHERE video_id = :source_id"
                    ),
                    {"source_id": source_id},
                )
                .mappings()
                .first()
            )
            corrected_record = None
            now = datetime.utcnow().isoformat()
            if record is not None:
                corrected_record = max(
                    0.0, float(record["watched_seconds"] or 0) - baseline
                )
                corrected_playback = max(
                    0.0, float(record["playback_seconds"] or 0) - baseline
                )
                conn.execute(
                    text(
                        "UPDATE study_plan_video_records SET watched_seconds = :watched, "
                        "playback_seconds = :playback, progress_version = progress_version + 1, "
                        "updated_at = :updated_at WHERE video_id = :source_id"
                    ),
                    {
                        "watched": corrected_record,
                        "playback": corrected_playback,
                        "updated_at": now,
                        "source_id": source_id,
                    },
                )
                self._record_study_plan_daily_snapshot_locked(conn, now=now)

            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {
                    "repair_key": repair_key,
                    "details": json.dumps(
                        {
                            "baseline_seconds": baseline,
                            "adjusted_events": adjusted_event_count,
                            "corrected_record_seconds": corrected_record,
                        },
                        ensure_ascii=False,
                    ),
                    "applied_at": now,
                },
            )

    def _restore_discrete_video_11_fresh_progress(self) -> None:
        repair_key = "discrete-video-11-fresh-progress-2026-08-27"
        offset_repair_key = "discrete-video-11-progress-offset-2026-08-27"
        repair_day = "2026-08-26"
        with self._lock, self._engine.begin() as conn:
            if conn.execute(
                text(
                    "SELECT repair_key FROM e3_data_repairs WHERE repair_key = :repair_key"
                ),
                {"repair_key": repair_key},
            ).first():
                return

            offset_repair = (
                conn.execute(
                    text(
                        "SELECT details FROM e3_data_repairs WHERE repair_key = :repair_key"
                    ),
                    {"repair_key": offset_repair_key},
                )
                .mappings()
                .first()
            )
            if offset_repair is None:
                return

            try:
                baseline = max(
                    0.0,
                    float(
                        json.loads(str(offset_repair["details"] or "{}"))[
                            "baseline_seconds"
                        ]
                    ),
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                return
            if baseline <= 0:
                return

            source = (
                conn.execute(
                    text(
                        "SELECT id FROM study_plan_videos "
                        "WHERE subject = :subject AND sequence = 11"
                    ),
                    {"subject": "離散數學"},
                )
                .mappings()
                .first()
            )
            if source is None:
                return
            source_id = int(source["id"])

            source_events = (
                conn.execute(
                    text(
                        "SELECT id, watched_seconds, updated_at FROM study_plan_activity_events "
                        "WHERE video_id = :source_id ORDER BY updated_at, id"
                    ),
                    {"source_id": source_id},
                )
                .mappings()
                .all()
            )
            restored_event_count = 0
            running_position = 0.0
            for row in source_events:
                watched = max(0.0, float(row["watched_seconds"] or 0))
                event_day = self._study_plan_business_day_from_timestamp(
                    str(row["updated_at"] or "")
                )
                if event_day > repair_day:
                    watched += baseline
                    restored_event_count += 1
                conn.execute(
                    text(
                        "UPDATE study_plan_activity_events SET watched_seconds = :watched, "
                        "previous_watched_seconds = :previous, delta_seconds = :delta "
                        "WHERE id = :event_id"
                    ),
                    {
                        "watched": watched,
                        "previous": running_position,
                        "delta": watched - running_position,
                        "event_id": int(row["id"]),
                    },
                )
                running_position = watched

            record = (
                conn.execute(
                    text(
                        "SELECT watched_seconds, playback_seconds FROM study_plan_video_records "
                        "WHERE video_id = :source_id"
                    ),
                    {"source_id": source_id},
                )
                .mappings()
                .first()
            )
            restored_record = None
            now = datetime.utcnow().isoformat()
            if record is not None:
                restored_record = (
                    max(0.0, float(record["watched_seconds"] or 0)) + baseline
                )
                restored_playback = (
                    max(0.0, float(record["playback_seconds"] or 0)) + baseline
                )
                conn.execute(
                    text(
                        "UPDATE study_plan_video_records SET watched_seconds = :watched, "
                        "playback_seconds = :playback, progress_version = progress_version + 1, "
                        "updated_at = :updated_at WHERE video_id = :source_id"
                    ),
                    {
                        "watched": restored_record,
                        "playback": restored_playback,
                        "updated_at": now,
                        "source_id": source_id,
                    },
                )
                self._record_study_plan_daily_snapshot_locked(conn, now=now)

            conn.execute(
                text(
                    "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                    "VALUES (:repair_key, :details, :applied_at)"
                ),
                {
                    "repair_key": repair_key,
                    "details": json.dumps(
                        {
                            "restored_seconds": baseline,
                            "restored_events": restored_event_count,
                            "restored_record_seconds": restored_record,
                        },
                        ensure_ascii=False,
                    ),
                    "applied_at": now,
                },
            )

    def load_discord_presence_settings(self) -> Dict[str, Any]:
        with self._lock, self._engine.connect() as conn:
            row = (
                conn.execute(
                    text(
                        "SELECT application_id, token_hash, enabled, updated_at "
                        "FROM discord_presence_settings WHERE id = 1"
                    )
                )
                .mappings()
                .first()
            )
        if not row:
            return {
                "application_id": "",
                "has_token": False,
                "enabled": False,
                "updated_at": "",
            }
        return {
            "application_id": str(row.get("application_id") or ""),
            "has_token": bool(str(row.get("token_hash") or "")),
            "enabled": bool(row.get("enabled")),
            "updated_at": str(row.get("updated_at") or ""),
        }

    def save_discord_presence_settings(
        self,
        *,
        application_id: str,
        token: str | None = None,
        enabled: bool,
    ) -> Dict[str, Any]:
        normalized_application_id = str(application_id or "").strip()
        token_hash = (
            hashlib.sha256(str(token).encode("utf-8")).hexdigest() if token else None
        )
        now = datetime.utcnow().isoformat()
        with self._lock, self._engine.begin() as conn:
            existing = (
                conn.execute(
                    text(
                        "SELECT token_hash FROM discord_presence_settings WHERE id = 1"
                    )
                )
                .mappings()
                .first()
            )
            resolved_hash = (
                token_hash
                if token_hash is not None
                else str((existing or {}).get("token_hash") or "")
            )
            params = {
                "application_id": normalized_application_id,
                "token_hash": resolved_hash,
                "enabled": (
                    1 if enabled and resolved_hash and normalized_application_id else 0
                ),
                "updated_at": now,
            }
            if existing:
                conn.execute(
                    text(
                        "UPDATE discord_presence_settings SET "
                        "application_id = :application_id, token_hash = :token_hash, "
                        "enabled = :enabled, updated_at = :updated_at WHERE id = 1"
                    ),
                    params,
                )
            else:
                conn.execute(
                    text(
                        "INSERT INTO discord_presence_settings "
                        "(id, application_id, token_hash, enabled, updated_at) VALUES "
                        "(1, :application_id, :token_hash, :enabled, :updated_at)"
                    ),
                    params,
                )
        return self.load_discord_presence_settings()

    def revoke_discord_presence_token(self) -> None:
        current = self.load_discord_presence_settings()
        with self._lock, self._engine.begin() as conn:
            conn.execute(text("DELETE FROM discord_presence_settings WHERE id = 1"))
        if current.get("application_id"):
            self.save_discord_presence_settings(
                application_id=str(current["application_id"]),
                enabled=False,
            )

    def verify_discord_presence_token(self, token: str) -> bool:
        candidate = str(token or "").strip()
        if not candidate:
            return False
        with self._lock, self._engine.connect() as conn:
            row = (
                conn.execute(
                    text(
                        "SELECT token_hash, enabled FROM discord_presence_settings WHERE id = 1"
                    )
                )
                .mappings()
                .first()
            )
        expected = str((row or {}).get("token_hash") or "")
        actual = hashlib.sha256(candidate.encode("utf-8")).hexdigest()
        return bool(
            row
            and row.get("enabled")
            and expected
            and hmac.compare_digest(expected, actual)
        )

    def load_study_player_settings(self) -> Dict[str, Any]:
        with self._lock, self._engine.connect() as conn:
            row = (
                conn.execute(
                    text(
                        "SELECT default_playback_rate, hold_space_rate, hold_delay_ms, "
                        "seek_back_seconds, seek_forward_seconds, seek_repeat_ms, "
                        "playback_rate_step, volume_step, controls_hide_ms, "
                        "center_click_toggle, pause_on_marker, show_speed_presets, "
                        "show_shortcut_hint, hint_duration_ms "
                        "FROM study_player_settings WHERE id = 1"
                    )
                )
                .mappings()
                .first()
            )
        if not row:
            return dict(PLAYER_SETTINGS_DEFAULTS)
        return normalize_player_settings(dict(row))

    def save_study_player_settings(self, values: Mapping[str, Any]) -> Dict[str, Any]:
        settings = normalize_player_settings(values)
        params = {
            **settings,
            "center_click_toggle": 1 if settings["center_click_toggle"] else 0,
            "pause_on_marker": 1 if settings["pause_on_marker"] else 0,
            "show_speed_presets": 1 if settings["show_speed_presets"] else 0,
            "show_shortcut_hint": 1 if settings["show_shortcut_hint"] else 0,
            "updated_at": datetime.utcnow().isoformat(),
        }
        with self._lock, self._engine.begin() as conn:
            exists = conn.execute(
                text("SELECT id FROM study_player_settings WHERE id = 1")
            ).first()
            if exists:
                conn.execute(
                    text(
                        "UPDATE study_player_settings SET "
                        "default_playback_rate = :default_playback_rate, "
                        "hold_space_rate = :hold_space_rate, "
                        "hold_delay_ms = :hold_delay_ms, "
                        "seek_back_seconds = :seek_back_seconds, "
                        "seek_forward_seconds = :seek_forward_seconds, "
                        "seek_repeat_ms = :seek_repeat_ms, "
                        "playback_rate_step = :playback_rate_step, "
                        "volume_step = :volume_step, "
                        "controls_hide_ms = :controls_hide_ms, "
                        "center_click_toggle = :center_click_toggle, "
                        "pause_on_marker = :pause_on_marker, "
                        "show_speed_presets = :show_speed_presets, "
                        "show_shortcut_hint = :show_shortcut_hint, "
                        "hint_duration_ms = :hint_duration_ms, "
                        "updated_at = :updated_at WHERE id = 1"
                    ),
                    params,
                )
            else:
                conn.execute(
                    text(
                        "INSERT INTO study_player_settings ("
                        "id, default_playback_rate, hold_space_rate, hold_delay_ms, "
                        "seek_back_seconds, seek_forward_seconds, seek_repeat_ms, "
                        "playback_rate_step, volume_step, controls_hide_ms, "
                        "center_click_toggle, pause_on_marker, show_speed_presets, "
                        "show_shortcut_hint, hint_duration_ms, updated_at"
                        ") VALUES ("
                        "1, :default_playback_rate, :hold_space_rate, :hold_delay_ms, "
                        ":seek_back_seconds, :seek_forward_seconds, :seek_repeat_ms, "
                        ":playback_rate_step, :volume_step, :controls_hide_ms, "
                        ":center_click_toggle, :pause_on_marker, :show_speed_presets, "
                        ":show_shortcut_hint, "
                        ":hint_duration_ms, :updated_at"
                        ")"
                    ),
                    params,
                )
        return settings

    def sync_study_plan_videos(self, videos: List[Dict[str, Any]]) -> None:
        existing: Dict[tuple[str, int], Dict[str, str]] = {}
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT subject, sequence, youtube_video_id, "
                    "youtube_playlist_id, youtube_url FROM study_plan_videos"
                )
            ).mappings()
            for row in rows:
                key = (str(row.get("subject") or ""), int(row.get("sequence") or 0))
                existing[key] = {
                    field: str(row.get(field) or "").strip()
                    for field in _YOUTUBE_FIELDS
                }

        merged: List[Dict[str, Any]] = []
        for source in videos:
            item = dict(source)
            try:
                key = (
                    str(item.get("subject") or "").strip(),
                    int(item.get("sequence") or 0),
                )
            except (TypeError, ValueError):
                merged.append(item)
                continue
            saved = existing.get(key)
            if saved:
                for field, value in saved.items():
                    if value:
                        item[field] = value
            merged.append(item)

        super().sync_study_plan_videos(merged)
        self._repair_discrete_video_11_12_history()
        self._repair_confirmed_discrete_14_calendar_move()
        self._restore_discrete_video_11_fresh_progress()
        self._repair_remove_misclassified_computer_org_videos_and_replan()
        self._repair_replan_only_final_three_subjects()
        self._repair_finalize_computer_org_and_replan_os_algorithm()
        self._repair_even_daily_weight_for_remaining_plan()
        self._ensure_current_os_algorithm_plan()
