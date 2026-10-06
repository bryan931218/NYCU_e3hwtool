import tempfile
import json
import sqlite3
import subprocess
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, inspect, text
from e3_tracker.platform.persistence import migrations


class SchemaMigrationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.engine = create_engine('sqlite:///' + (Path(self.directory.name) / 'test.sqlite3').as_posix())

    def tearDown(self):
        self.engine.dispose()
        self.directory.cleanup()

    def test_fresh_schema_and_rerun_are_idempotent(self):
        self.assertTrue(all(not row['applied'] for row in migrations.migration_status(self.engine)))
        self.assertNotIn('e3_schema_migrations', inspect(self.engine).get_table_names())
        self.assertEqual(migrations.run_migrations(self.engine), [version for version, _ in migrations.MIGRATIONS])
        self.assertEqual(migrations.run_migrations(self.engine), [])
        self.assertTrue(all(row['applied'] for row in migrations.migration_status(self.engine)))
        for table in ('study_recall_favorites', 'study_player_settings', 'discord_presence_settings', 'e3_data_repairs'):
            self.assertIn(table, inspect(self.engine).get_table_names())

    def test_legacy_progress_is_backfilled_only_once(self):
        with self.engine.begin() as conn:
            conn.execute(text('CREATE TABLE study_plan_video_records (video_id INTEGER PRIMARY KEY, watched_seconds FLOAT, updated_at TEXT)'))
            conn.execute(text('INSERT INTO study_plan_video_records VALUES (12, 123.5, "original")'))
        migrations.run_migrations(self.engine)
        with self.engine.begin() as conn:
            record = conn.execute(text('SELECT * FROM study_plan_video_records')).mappings().one()
            self.assertEqual(record['playback_seconds'], 123.5)
            self.assertEqual(record['progress_version'], 0)
            self.assertEqual(record['updated_at'], 'original')
            conn.execute(text('UPDATE study_plan_video_records SET playback_seconds = 18'))
        migrations.run_migrations(self.engine)
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text('SELECT playback_seconds FROM study_plan_video_records')).scalar(), 18)

    def test_grading_notification_upgrade_preserves_existing_seen_assignments(self):
        with self.engine.begin() as conn:
            conn.execute(text('CREATE TABLE assignment_notification_seen (user_id INTEGER NOT NULL, uid_hash VARCHAR(64) NOT NULL, PRIMARY KEY (user_id, uid_hash))'))
            conn.execute(text("INSERT INTO assignment_notification_seen VALUES (7, 'existing-assignment')"))
            conn.execute(text('CREATE TABLE assignments (id INTEGER PRIMARY KEY, course_id INTEGER, title TEXT, grade_text TEXT, due_ts INTEGER)'))
            conn.execute(text("INSERT INTO assignments (id, course_id, title, grade_text) VALUES (12, 1, 'existing homework', '85')"))
        with patch.object(migrations, 'MIGRATIONS', migrations.MIGRATIONS[:-1]):
            migrations.run_migrations(self.engine)
        self.assertEqual(migrations.run_migrations(self.engine), ['0016_grading_notifications'])
        with self.engine.begin() as conn:
            row = conn.execute(text('SELECT * FROM assignment_notification_seen')).mappings().one()
            self.assertEqual(dict(row), {'user_id': 7, 'uid_hash': 'existing-assignment', 'graded_observed': 0})
            conn.execute(text('UPDATE assignment_notification_seen SET graded_observed = 1'))
            grade = conn.execute(text('SELECT grade_text, feedback_text FROM assignments')).one()
            self.assertEqual(tuple(grade), ('85', None))
        self.assertEqual(migrations.run_migrations(self.engine), [])
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text('SELECT graded_observed FROM assignment_notification_seen')).scalar(), 1)

    def test_existing_settings_are_preserved(self):
        with self.engine.begin() as conn:
            conn.execute(text('CREATE TABLE study_player_settings (id INTEGER PRIMARY KEY, hold_space_rate FLOAT, hold_delay_ms INTEGER, center_click_toggle INTEGER, show_shortcut_hint INTEGER, hint_duration_ms INTEGER, updated_at TEXT)'))
            conn.execute(text('INSERT INTO study_player_settings VALUES (1, 2.5, 400, 1, 0, 2000, "original")'))
        migrations.run_migrations(self.engine)
        with self.engine.connect() as conn:
            row = conn.execute(text('SELECT * FROM study_player_settings')).mappings().one()
            self.assertEqual(row['hold_space_rate'], 2.5)
            self.assertEqual(row['default_playback_rate'], 1.0)
            self.assertEqual(row['updated_at'], 'original')

    def test_failure_is_not_recorded_and_can_retry(self):
        def broken(conn):
            conn.execute(text('CREATE TABLE retry_probe (id INTEGER)'))
            raise RuntimeError('injected migration failure')
        with patch.object(migrations, 'MIGRATIONS', (('failure_probe', broken),)):
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                migrations.run_migrations(self.engine)
        self.assertNotIn('retry_probe', inspect(self.engine).get_table_names())
        self.assertEqual(len(migrations.run_migrations(self.engine)), len(migrations.MIGRATIONS))

    def test_newer_database_refuses_older_application(self):
        migrations.run_migrations(self.engine)
        with self.engine.begin() as conn:
            conn.execute(text("INSERT INTO e3_schema_migrations VALUES ('9999_future', 'original')"))
        with self.assertRaisesRegex(RuntimeError, 'newer application'):
            migrations.run_migrations(self.engine)
        self.assertFalse(migrations.migration_status(self.engine)[-1]['known'])

    def test_separate_connections_apply_each_version_once(self):
        with ThreadPoolExecutor(max_workers=3) as workers:
            results = list(workers.map(lambda _: migrations.run_migrations(self.engine), range(3)))
        self.assertEqual(sum(len(result) for result in results), len(migrations.MIGRATIONS))

    def test_separate_workers_apply_each_version_once(self):
        code = (
            "import json; from sqlalchemy import create_engine; "
            "from e3_tracker.platform.persistence.migrations import run_migrations; "
            f"engine = create_engine({str(self.engine.url)!r}); "
            "print(json.dumps(run_migrations(engine))); engine.dispose()"
        )
        workers = [subprocess.Popen([sys.executable, '-c', code], cwd=Path(__file__).resolve().parents[1],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(3)]
        results = []
        for worker in workers:
            try:
                stdout, stderr = worker.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.communicate()
                self.fail('migration worker timed out')
            self.assertEqual(worker.returncode, 0, stderr)
            results.extend(json.loads(stdout))
        self.assertEqual(sorted(results), [version for version, _ in migrations.MIGRATIONS])

    def test_status_cli_does_not_create_or_upgrade_database(self):
        target = Path(self.directory.name) / 'status.sqlite3'
        script = Path(__file__).resolve().parents[1] / 'tools/migrate_database.py'
        command = [sys.executable, str(script), '--database', str(target), '--status']
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(target.exists())
        with sqlite3.connect(target) as conn:
            conn.execute('CREATE TABLE preserved (id INTEGER)')
        before = target.read_bytes()
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('0001_core_schema: pending', result.stdout)
        self.assertEqual(target.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
