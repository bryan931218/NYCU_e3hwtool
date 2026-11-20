import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import text

from e3_tracker.platform.config import load_env_defaults
from e3_tracker.platform.deployment_runtime import DeploymentSafeStorage
from e3_tracker.study.domain.november_plan_checkpoint import uses_november_checkpoint


class DeploymentPersistenceTests(unittest.TestCase):
    def test_november_recovery_does_not_reapply_after_an_intentional_reset(self):
        for prior_update in (None, "2026-11-20T05:49:26", "2026-11-20T06:00:00"):
            with self.subTest(prior_update=prior_update), tempfile.TemporaryDirectory() as temp_dir:
                storage = DeploymentSafeStorage(str(Path(temp_dir) / "recovery.sqlite3"))
                try:
                    with storage._engine.begin() as conn:
                        conn.execute(text(
                            "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                            "VALUES ('finalize-computer-org-replan-os-algorithm-2026-11-19-v2', '{}', 'x')"
                        ))
                        if prior_update is not None:
                            conn.execute(text(
                                "INSERT INTO study_plan_replan_settings "
                                "(id, start_date, end_date, weekday_minutes, weekend_minutes, "
                                "baseline_by_subject, subject_targets, created_at, updated_at) "
                                "VALUES (1, '2026-11-23', '2026-11-30', 60, 60, '{}', '{}', 'x', :updated)"
                            ), {"updated": prior_update})
                    with patch("e3_tracker.study.persistence.deployment.datetime", wraps=datetime) as clock:
                        clock.now.return_value = datetime(2026, 11, 20, 15)
                        storage._restore_november_plan_checkpoint()
                        settings = storage.get_study_plan_replan_settings()
                        if prior_update == "2026-11-20T06:00:00":
                            self.assertEqual(settings["start_date"], "2026-11-23")
                        else:
                            self.assertTrue(uses_november_checkpoint(settings))
                            self.assertEqual(settings["weekday_minutes"], settings["weekend_minutes"])
                            storage.delete_study_plan_replan_settings()
                            storage._restore_november_plan_checkpoint()
                            self.assertIsNone(storage.get_study_plan_replan_settings())
                finally:
                    storage._engine.dispose()

    def test_misclassified_computer_org_tail_is_removed_and_all_subjects_replanned(self):
        subjects = (
            "線性代數",
            "離散數學",
            "資料結構",
            "作業系統",
            "計算機組織",
            "演算法",
        )
        inventory = [
            {
                "subject": subject,
                "sequence": 69 if subject == "計算機組織" else 1,
                "title": f"{subject}保留影片",
                "duration_seconds": 3600,
            }
            for subject in subjects
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = DeploymentSafeStorage(str(Path(temp_dir) / "whole-plan.sqlite3"))
            storage.sync_study_plan_videos(inventory)
            linear_id = next(
                int(item["id"])
                for item in storage.list_study_plan_videos_with_records()
                if item["subject"] == "線性代數"
            )
            with storage._lock, storage._engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO study_plan_videos "
                        "(subject, sequence, title, duration_seconds, created_at, updated_at) "
                        "VALUES "
                        "('計算機組織', 70, '邏輯設計 70', 7200, 'x', 'x'), "
                        "('計算機組織', 78, '邏輯設計 78', 7200, 'x', 'x')"
                    )
                )
                wrong_ids = [
                    int(row[0])
                    for row in conn.execute(
                        text(
                            "SELECT id FROM study_plan_videos "
                            "WHERE subject = '計算機組織' AND sequence IN (70, 78)"
                        )
                    ).all()
                ]
                conn.execute(
                    text(
                        "INSERT INTO study_plan_video_records "
                        "(video_id, watched_seconds, playback_seconds, progress_version, notes, updated_at) "
                        "VALUES (:linear_id, 600, 600, 1, '', 'x'), "
                        "(:wrong_id, 1800, 1800, 1, '', 'x')"
                    ),
                    {"linear_id": linear_id, "wrong_id": wrong_ids[0]},
                )
                conn.execute(
                    text(
                        "INSERT INTO study_plan_activity_events "
                        "(day, video_id, previous_watched_seconds, watched_seconds, delta_seconds, updated_at) "
                        "VALUES ('2026-11-19', :wrong_id, 0, 1800, 1800, '2026-11-19T10:00:00')"
                    ),
                    {"wrong_id": wrong_ids[0]},
                )
                conn.execute(
                    text(
                        "INSERT INTO study_time_sessions "
                        "(session_key, day, kind, video_id, label, elapsed_seconds, completed, started_at, updated_at) "
                        "VALUES ('wrong-session', '2026-11-19', 'video', :wrong_id, "
                        "'邏輯設計 70', 1800, 0, 'x', 'x')"
                    ),
                    {"wrong_id": wrong_ids[0]},
                )
                conn.execute(
                    text(
                        "INSERT INTO study_plan_daily_snapshots "
                        "(day, total_watched_seconds, updated_at) "
                        "VALUES ('2026-11-19', 2400, 'x')"
                    )
                )
                conn.execute(
                    text(
                        "INSERT INTO study_plan_rest_days (study_date, created_at) "
                        "VALUES ('2026-11-22', 'x')"
                    )
                )

            storage.sync_study_plan_videos(inventory)

            videos = storage.list_study_plan_videos_with_records()
            computer_sequences = {
                int(item["sequence"])
                for item in videos
                if item["subject"] == "計算機組織"
            }
            self.assertEqual(computer_sequences, {69})
            with storage._lock, storage._engine.connect() as conn:
                self.assertEqual(
                    conn.execute(text("SELECT COUNT(*) FROM study_plan_activity_events")).scalar_one(),
                    0,
                )
                self.assertEqual(
                    conn.execute(text("SELECT COUNT(*) FROM study_time_sessions")).scalar_one(),
                    0,
                )
                self.assertEqual(
                    conn.execute(text("SELECT COUNT(*) FROM study_plan_daily_snapshots")).scalar_one(),
                    0,
                )
                self.assertEqual(
                    conn.execute(text("SELECT COUNT(*) FROM study_plan_rest_days")).scalar_one(),
                    0,
                )
                settings = conn.execute(
                    text("SELECT * FROM study_plan_replan_settings WHERE id = 1")
                ).mappings().one()
                repair_count = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM e3_data_repairs "
                        "WHERE repair_key = "
                        "'remove-computer-org-70-78-and-replan-2026-11-19'"
                    )
                ).scalar_one()
            self.assertEqual(settings["start_date"], "2026-11-20")
            self.assertEqual(settings["end_date"], "2026-11-30")
            targets = json.loads(settings["subject_targets"])
            self.assertEqual(set(targets), {"作業系統", "計算機組織", "演算法"})
            self.assertEqual(targets["作業系統"], 3600)
            self.assertEqual(repair_count, 1)

            storage.sync_study_plan_videos(inventory)
            with storage._lock, storage._engine.connect() as conn:
                self.assertEqual(
                    conn.execute(
                        text(
                            "SELECT COUNT(*) FROM e3_data_repairs "
                            "WHERE repair_key = "
                            "'remove-computer-org-70-78-and-replan-2026-11-19'"
                        )
                    ).scalar_one(),
                    1,
                )
            storage._engine.dispose()

    def test_final_three_replan_ignores_completed_video_tail_seconds(self):
        inventory = [
            {
                "subject": subject,
                "sequence": sequence,
                "title": f"{subject}影片",
                "duration_seconds": 3600,
            }
            for subject, sequence in (
                ("作業系統", 1),
                ("計算機組織", 69),
                ("演算法", 1),
            )
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = DeploymentSafeStorage(str(Path(temp_dir) / "final-three.sqlite3"))
            storage.sync_study_plan_videos(inventory)
            videos = {
                item["subject"]: int(item["id"])
                for item in storage.list_study_plan_videos_with_records()
            }
            with storage._lock, storage._engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO study_plan_videos "
                        "(subject, sequence, title, duration_seconds, created_at, updated_at) "
                        "VALUES ('計算機組織', 70, '誤放影片', 7200, 'x', 'x')"
                    )
                )
                conn.execute(
                    text(
                        "INSERT INTO study_plan_video_records "
                        "(video_id, watched_seconds, playback_seconds, progress_version, notes, updated_at) "
                        "VALUES "
                        "(:os_id, 1800, 1800, 1, '', 'x'), "
                        "(:co_id, 3598, 3598, 1, '', 'x'), "
                        "(:algo_id, 900, 900, 1, '', 'x')"
                    ),
                    {
                        "os_id": videos["作業系統"],
                        "co_id": videos["計算機組織"],
                        "algo_id": videos["演算法"],
                    },
                )

            storage.sync_study_plan_videos(inventory)
            settings = storage.get_study_plan_replan_settings()
            self.assertIsNotNone(settings)
            self.assertEqual(
                set(settings["subject_targets"]),
                {"作業系統", "演算法"},
            )
            self.assertEqual(settings["subject_targets"]["作業系統"], 1800)
            self.assertEqual(settings["subject_targets"]["演算法"], 2700)
            self.assertNotIn("計算機組織", settings["subject_targets"])
            with storage._lock, storage._engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO e3_data_repairs (repair_key, details, applied_at) "
                        "VALUES ("
                        "'finalize-computer-org-replan-os-algorithm-2026-11-19-v2', "
                        "'{}', 'x')"
                    )
                )
            storage._repair_even_daily_weight_for_remaining_plan()
            even_settings = storage.get_study_plan_replan_settings()
            self.assertEqual(even_settings["weekday_minutes"], 240)
            self.assertEqual(even_settings["weekend_minutes"], 240)
            storage._engine.dispose()

    def test_database_url_uses_standard_railway_postgres_variable(self):
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgres://user:pass@postgres.internal:5432/app",
            },
            clear=True,
        ):
            defaults = load_env_defaults()
        self.assertEqual(
            defaults["database_url"],
            "postgres://user:pass@postgres.internal:5432/app",
        )

    def test_railway_volume_is_used_for_sqlite_and_uploads(self):
        with patch.dict(
            os.environ,
            {"RAILWAY_VOLUME_MOUNT_PATH": "/data"},
            clear=True,
        ):
            defaults = load_env_defaults()
        self.assertEqual(defaults["database_url"], "/data/e3_tracker.sqlite3")
        self.assertEqual(defaults["cache_dir"], "/data")
        self.assertEqual(defaults["study_upload_dir"], "/data/study_note_images")

    def test_restart_inventory_sync_preserves_database_synced_youtube_link(self):
        inventory = [
            {
                "subject": "測試科目",
                "sequence": 1,
                "title": "測試影片",
                "duration_seconds": 600,
                "youtube_video_id": "STATIC00001",
                "youtube_playlist_id": "STATICLIST",
                "youtube_url": "https://www.youtube.com/watch?v=STATIC00001",
            }
        ]
        synced_url = "https://www.youtube.com/watch?v=SYNCVIDEO01&list=SYNCLIST01"

        with tempfile.TemporaryDirectory() as temp_dir:
            storage = DeploymentSafeStorage(str(Path(temp_dir) / "persistent.sqlite3"))
            storage.sync_study_plan_videos(inventory)
            result = storage.sync_study_plan_youtube_links(
                [
                    {
                        "subject": "測試科目",
                        "sequence": 1,
                        "youtube_video_id": "SYNCVIDEO01",
                        "youtube_playlist_id": "SYNCLIST01",
                        "youtube_url": synced_url,
                    }
                ]
            )
            self.assertEqual(result["updated"], 1)

            # Simulate the app startup inventory sync after a redeploy.
            storage.sync_study_plan_videos(inventory)
            video = storage.list_study_plan_videos_with_records()[0]
            self.assertEqual(video["youtube_url"], synced_url)
            self.assertEqual(video["youtube_video_id"], "SYNCVIDEO01")
            storage._engine.dispose()

    def test_discrete_video_history_repair_moves_only_the_misattributed_day(self):
        inventory = [
            {
                "subject": "離散數學",
                "sequence": sequence,
                "title": f"離散數學第 {sequence} 支",
                "duration_seconds": 10000,
            }
            for sequence in (11, 12)
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = DeploymentSafeStorage(str(Path(temp_dir) / "repair.sqlite3"))
            storage.sync_study_plan_videos(inventory)
            videos = {
                item["sequence"]: item
                for item in storage.list_study_plan_videos_with_records()
            }
            video_11_id = int(videos[11]["id"])
            video_12_id = int(videos[12]["id"])
            with storage._lock, storage._engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO study_plan_activity_events "
                        "(day, video_id, previous_watched_seconds, watched_seconds, delta_seconds, updated_at) "
                        "VALUES "
                        "('2026-08-26', :video_11, 0, 1054, 1054, '2026-08-26T15:00:00'), "
                        "('2026-08-27', :video_12, 0, 1860, 1860, '2026-08-27T02:00:00'), "
                        "('2026-08-27', :video_11, 1054, 1274, 220, '2026-08-27T02:01:00')"
                    ),
                    {"video_11": video_11_id, "video_12": video_12_id},
                )
                conn.execute(
                    text(
                        "INSERT INTO study_time_sessions "
                        "(session_key, day, kind, video_id, label, elapsed_seconds, completed, started_at, updated_at) "
                        "VALUES "
                        "('old-video-session', '2026-08-26', 'video', :video_11, '舊標籤', 900, 1, 'x', 'x'), "
                        "('new-video-session', '2026-08-27', 'video', :video_11, '新標籤', 30, 0, 'x', 'x')"
                    ),
                    {"video_11": video_11_id},
                )
                conn.execute(
                    text(
                        "INSERT INTO study_plan_video_records "
                        "(video_id, watched_seconds, playback_seconds, progress_version, notes, updated_at) "
                        "VALUES "
                        "(:video_11, 1274, 1274, 3, '', '2026-08-27T02:01:00'), "
                        "(:video_12, 1860, 1860, 2, '', '2026-08-27T02:00:00')"
                    ),
                    {"video_11": video_11_id, "video_12": video_12_id},
                )

            storage.sync_study_plan_videos(inventory)
            repaired = storage.list_study_plan_activity_events(
                start_day="2026-08-26",
                end_day="2026-08-27",
            )
            repaired_by_day_sequence = {
                (item["day"], item["sequence"]): item
                for item in repaired
            }
            self.assertEqual(repaired_by_day_sequence[("2026-08-26", 12)]["delta_seconds"], 1054)
            self.assertEqual(repaired_by_day_sequence[("2026-08-27", 12)]["delta_seconds"], 806)
            self.assertEqual(repaired_by_day_sequence[("2026-08-27", 11)]["delta_seconds"], 1274)
            repaired_videos = {
                item["sequence"]: item
                for item in storage.list_study_plan_videos_with_records()
            }
            self.assertEqual(repaired_videos[11]["watched_seconds"], 1274)
            self.assertEqual(repaired_videos[11]["playback_seconds"], 1274)
            self.assertEqual(repaired_videos[12]["watched_seconds"], 1860)
            with storage._lock, storage._engine.connect() as conn:
                sessions = conn.execute(
                    text(
                        "SELECT session_key, video_id, label FROM study_time_sessions "
                        "ORDER BY session_key"
                    )
                ).mappings().all()
                repairs = conn.execute(text("SELECT repair_key FROM e3_data_repairs")).all()
            session_by_key = {row["session_key"]: row for row in sessions}
            self.assertEqual(session_by_key["old-video-session"]["video_id"], video_12_id)
            self.assertEqual(session_by_key["old-video-session"]["label"], "離散數學第 12 支")
            self.assertEqual(session_by_key["new-video-session"]["video_id"], video_11_id)
            self.assertEqual(len(repairs), 1)

            storage.sync_study_plan_videos(inventory)
            self.assertEqual(
                len(storage.list_study_plan_activity_events(start_day="2026-08-26", end_day="2026-08-27")),
                3,
            )
            storage._engine.dispose()

    def test_confirmed_discrete_14_calendar_move_is_applied_once_and_verified(self):
        inventory = [{
            "subject": "離散數學",
            "sequence": 14,
            "title": "離散數學第 14 支",
            "duration_seconds": 20000,
        }]
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = DeploymentSafeStorage(str(Path(temp_dir) / "confirmed-move.sqlite3"))
            storage.sync_study_plan_videos(inventory)
            video_id = int(storage.list_study_plan_videos_with_records()[0]["id"])
            with storage._lock, storage._engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO study_plan_activity_events "
                    "(day, video_id, previous_watched_seconds, watched_seconds, delta_seconds, updated_at) "
                    "VALUES ('2026-09-01', :video_id, 0, 11520, 11520, '2026-09-01T12:00:00')"
                ), {"video_id": video_id})
                conn.execute(text(
                    "INSERT INTO study_plan_activity_events "
                    "(day, video_id, previous_watched_seconds, watched_seconds, delta_seconds, updated_at) "
                    "VALUES ('2026-09-02', :video_id, 11520, 12092, 572, '2026-09-02T12:00:00')"
                ), {"video_id": video_id})

            storage.sync_study_plan_videos(inventory)
            source = next(
                item for item in storage.list_study_plan_activity_events(day="2026-09-01")
                if item["video_id"] == video_id
            )
            target = next(
                item for item in storage.list_study_plan_activity_events(day="2026-09-02")
                if item["video_id"] == video_id
            )
            self.assertEqual(source["delta_seconds"], 169 * 60)
            self.assertEqual(target["delta_seconds"], 572 + 23 * 60)

            # A later startup must leave the verified result unchanged.
            storage.sync_study_plan_videos(inventory)
            target_again = next(
                item for item in storage.list_study_plan_activity_events(day="2026-09-02")
                if item["video_id"] == video_id
            )
            self.assertEqual(target_again["delta_seconds"], 572 + 23 * 60)
            with storage._lock, storage._engine.connect() as conn:
                repair_count = conn.execute(text(
                    "SELECT COUNT(*) FROM e3_data_repairs "
                    "WHERE repair_key = 'confirmed-discrete-14-calendar-move-2026-09-01-to-02'"
                )).scalar_one()
            self.assertEqual(repair_count, 1)
            storage._engine.dispose()

    def test_discrete_video_history_repair_restores_fresh_video_11_progress(self):
        inventory = [
            {
                "subject": "離散數學",
                "sequence": sequence,
                "title": f"離散數學第 {sequence} 支",
                "duration_seconds": 10000,
            }
            for sequence in (11, 12)
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = DeploymentSafeStorage(str(Path(temp_dir) / "restore.sqlite3"))
            storage.sync_study_plan_videos(inventory)
            videos = {
                item["sequence"]: item
                for item in storage.list_study_plan_videos_with_records()
            }
            video_11_id = int(videos[11]["id"])
            video_12_id = int(videos[12]["id"])
            with storage._lock, storage._engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO study_plan_activity_events "
                        "(day, video_id, previous_watched_seconds, watched_seconds, delta_seconds, updated_at) "
                        "VALUES "
                        "('2026-08-26', :video_11, 0, 1054, 1054, '2026-08-26T15:00:00'), "
                        "('2026-08-27', :video_12, 0, 1860, 1860, '2026-08-27T02:00:00'), "
                        "('2026-08-27', :video_11, 1054, 1274, 220, '2026-08-27T02:01:00')"
                    ),
                    {"video_11": video_11_id, "video_12": video_12_id},
                )
                conn.execute(
                    text(
                        "INSERT INTO study_plan_video_records "
                        "(video_id, watched_seconds, playback_seconds, progress_version, notes, updated_at) "
                        "VALUES (:video_11, 1274, 1274, 3, '', '2026-08-27T02:01:00')"
                    ),
                    {"video_11": video_11_id},
                )

            storage._repair_discrete_video_11_12_history()
            storage._repair_discrete_video_11_progress_offset()
            storage._restore_discrete_video_11_fresh_progress()

            repaired = storage.list_study_plan_activity_events(
                start_day="2026-08-26",
                end_day="2026-08-27",
            )
            repaired_by_day_sequence = {
                (item["day"], item["sequence"]): item
                for item in repaired
            }
            self.assertEqual(repaired_by_day_sequence[("2026-08-26", 12)]["delta_seconds"], 1054)
            self.assertEqual(repaired_by_day_sequence[("2026-08-27", 12)]["delta_seconds"], 806)
            self.assertEqual(repaired_by_day_sequence[("2026-08-27", 11)]["delta_seconds"], 1274)
            repaired_videos = {
                item["sequence"]: item
                for item in storage.list_study_plan_videos_with_records()
            }
            self.assertEqual(repaired_videos[11]["watched_seconds"], 1274)
            self.assertEqual(repaired_videos[11]["playback_seconds"], 1274)
            with storage._lock, storage._engine.connect() as conn:
                repairs = conn.execute(text("SELECT repair_key FROM e3_data_repairs")).all()
            self.assertEqual(len(repairs), 3)
            storage._engine.dispose()


if __name__ == "__main__":
    unittest.main()
