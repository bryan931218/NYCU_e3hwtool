"""Course-message alerts reuse subscriptions without leaking bodies or replaying history."""

import json
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from sqlalchemy import delete, select

from tests import test_assignment_notifications as notification_tests
from e3_tracker.assignments.domain.notifications import validate_preferences
from e3_tracker.assignments.persistence.cleanup import delete_assignment_account_data
from e3_tracker.platform.persistence.core_schema import users_table
from e3_tracker.assignments.services.collector import current_semester_key


class CourseMessageNotificationTests(unittest.TestCase):
    setUp = notification_tests.NotificationTests.setUp
    tearDown = notification_tests.NotificationTests.tearDown
    job_rows = notification_tests.NotificationTests.job_rows

    def enable(self, **overrides):
        code = self.storage.create_line_link_code('student')
        self.assertTrue(self.storage.consume_line_link_code(code, 'synthetic-line-target'))
        self.prefs = validate_preferences({**self.prefs, 'new_announcement': True,
            'new_mail': True, 'line_enabled': True, **overrides})
        self.storage.save_notification_preferences('student', self.prefs)

    def item(self, number=7, **overrides):
        return {'id': str(number), 'key': f'1:{number}', 'course_id': 1,
            'course_title': 'Test course', 'semester': current_semester_key(),
            'title': f'Private subject {number}', 'author': 'Private sender',
            'content': 'Private full body must not be delivered', 'updated_ts': time.time(),
            'url': f'https://e3p.nycu.edu.tw/local/dcpcmail/view.php?c=1&t=inbox&m={number}',
            **overrides}

    def refresh(self, kind, items, courses=(1,), semester=None, error=''):
        cache = self.service.course_message_services[kind].storage
        semester = semester or current_semester_key()
        self.attempt_time = getattr(self, 'attempt_time', time.time()-10000) + 120
        attempt = cache.claim_course_announcement_refresh('student', semester, now=self.attempt_time)
        self.assertIsNotNone(attempt)
        self.assertTrue(cache.finish_course_announcement_refresh('student', semester, attempt, items, courses, error))

    def test_existing_preferences_default_off_and_old_clients_preserve_flags(self):
        self.storage.save_notification_preferences('student', {'browser_enabled': False})
        prefs = self.storage.notification_preferences('student')['preferences']
        self.assertFalse(prefs['new_announcement']); self.assertFalse(prefs['new_mail'])
        self.enable()
        old = {key: value for key, value in self.prefs.items() if key not in ('new_mail', 'new_announcement')}
        response = self.client.post('/api/notifications/settings', json=old)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json['preferences']['new_mail'])
        self.assertTrue(response.json['preferences']['new_announcement'])

    def test_first_sync_empty_or_populated_baselines_and_sources_are_independent(self):
        self.enable()
        self.refresh('mail', [])
        self.refresh('announcements', [self.item()])
        self.assertEqual(self.job_rows(), [])
        self.refresh('mail', [self.item()])
        self.refresh('announcements', [self.item(), self.item(8)])
        self.assertEqual(len(self.job_rows()), 4)
        self.refresh('mail', [self.item(title='Edited subject')])
        self.refresh('announcements', [self.item(), self.item(8)])
        self.assertEqual(len(self.job_rows()), 4)

    def test_payload_encrypted_and_delivery_contains_only_subject_course_and_deep_link(self):
        self.enable(); self.refresh('mail', []); self.refresh('mail', [self.item()])
        for job in self.job_rows():
            self.assertTrue(job['payload'].startswith('enc:v1:'))
            self.assertNotIn('Private subject', job['payload'])
            username, prefs, payload, target = self.storage.notification_delivery(job)
            self.assertEqual(username, 'student')
            self.assertEqual(payload['body'], 'Test course\nPrivate subject 7')
            self.assertIn('tab=mail', payload['url']); self.assertIn('item=1%3A7', payload['url'])
            self.assertNotIn('Private full body', json.dumps(payload))
            self.assertNotIn('Private sender', json.dumps(payload))
            with self.assertRaises(ValueError):
                self.storage._credential_cipher.decrypt(job['payload'], 'notification:wrong-id')
        with patch.object(self.service, 'deliver') as send:
            self.service.dispatch(now=time.time()+1)
            self.service.dispatch(now=time.time()+2)
        self.assertEqual(send.call_count, 2)
        self.assertEqual({row['state'] for row in self.job_rows()}, {'sent'})

    def test_failed_course_does_not_baseline_erase_cache_or_queue(self):
        self.enable(); self.refresh('mail', [self.item()], courses=(), error='failed')
        self.assertEqual(self.job_rows(), [])
        self.refresh('mail', [self.item()])
        self.assertEqual(self.job_rows(), [])
        self.refresh('mail', [], courses=(), error='failed')
        cache = self.service.course_message_services['mail'].storage.load_course_announcements('student', current_semester_key())
        self.assertEqual(len(cache['items']), 1)
        self.refresh('mail', [self.item(), self.item(8)])
        self.assertEqual(len(self.job_rows()), 2)

    def test_disabled_pref_old_unknown_future_dates_and_archive_never_alert(self):
        self.enable(new_mail=False); self.refresh('mail', [])
        self.refresh('mail', [self.item()])
        self.enable(); self.refresh('mail', [self.item()])
        self.refresh('mail', [self.item(), self.item(8, updated_ts=time.time()-90000),
            self.item(9, updated_ts=None), self.item(10, updated_ts=time.time()+600)])
        self.refresh('mail', [], semester='113-1')
        self.refresh('mail', [self.item(11)], semester='113-1')
        self.assertEqual(self.job_rows(), [])
        self.refresh('mail', [self.item(12)])
        self.assertEqual(len(self.job_rows()), 2)

    def test_settings_save_baselines_cached_messages_before_opt_in(self):
        self.refresh('mail', [self.item()])
        self.enable(); self.service.baseline_course_messages('student')
        self.refresh('mail', [self.item()])
        self.assertEqual(self.job_rows(), [])
        self.refresh('mail', [self.item(), self.item(8)])
        self.assertEqual(len(self.job_rows()), 2)

    def test_disabled_preferences_and_removed_targets_cancel_claimed_jobs(self):
        self.enable(); self.refresh('mail', []); self.refresh('mail', [self.item()])
        claimed = self.storage.claim_notification_jobs(time.time()+1)
        self.storage.unlink_line('student')
        for job in claimed:
            if job['channel'] == 'browser': self.storage.remove_push_subscription('student', job['target_hash'])
            self.assertIsNone(self.storage.notification_delivery(job))
        self.enable(); self.refresh('mail', [self.item(), self.item(8)])
        self.storage.save_notification_preferences('student', {**self.prefs, 'new_mail': False})
        with patch.object(self.service, 'deliver') as send: self.service.dispatch(now=time.time()+1)
        send.assert_not_called()

    def test_deleted_account_is_not_recreated_by_refresh_or_notification(self):
        self.enable(); self.refresh('mail', []); self.refresh('mail', [self.item()])
        cache = self.service.course_message_services['mail'].storage
        attempt = cache.claim_course_announcement_refresh('student', current_semester_key(), now=time.time()-120)
        with self.storage._engine.begin() as conn:
            uid = conn.execute(select(users_table.c.id).where(users_table.c.username=='student')).scalar_one()
            delete_assignment_account_data(conn, [uid])
            conn.execute(delete(users_table).where(users_table.c.id==uid))
        self.assertFalse(cache.finish_course_announcement_refresh('student', current_semester_key(), attempt, [self.item(8)], [1]))
        self.storage.baseline_course_message_notifications('student', 'mail', current_semester_key(), [self.item()])
        self.assertEqual(self.job_rows(), [])

    def test_parallel_finish_is_idempotent_and_line_preserves_reader_link(self):
        self.enable(); self.refresh('mail', [])
        cache = self.service.course_message_services['mail'].storage
        attempt = cache.claim_course_announcement_refresh('student', current_semester_key(), now=time.time()-120)
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda _: cache.finish_course_announcement_refresh('student', current_semester_key(), attempt, [self.item()], [1]), range(2)))
        self.assertEqual(len(self.job_rows()), 2)
        job = next(row for row in self.job_rows() if row['channel']=='line')
        payload = self.storage.notification_delivery(job)[2]
        with patch.object(self.service, 'line_request') as send:
            self.service.deliver(job, payload, 'synthetic-line-target')
        text = send.call_args.args[1]['messages'][0]['text']
        self.assertIn('/courses/messages?tab=mail', text)
        self.assertNotIn('Private full body', text)

    def test_background_sync_only_opted_in_own_current_courses_and_shared_capacity(self):
        result = {'courses':[{'id':1,'title':'Test course','semester_key':current_semester_key()},
            {'id':2,'title':'Old course','semester_key':'113-1'}], 'all_assignments':[]}
        user = {'username':'student','moodle_session':'synthetic'}
        source = self.service.course_message_services['mail']
        def release_slot(*args): source.slots.release()
        with self.app.app_context(), patch.object(source, '_refresh', side_effect=release_slot) as refresh:
            self.service.refresh_course_messages(user, result)
            refresh.assert_not_called()
            self.enable(new_announcement=False)
            self.service.refresh_course_messages(user, result)
            refresh.assert_called_once()
            self.assertEqual([course['id'] for course in refresh.call_args.args[3]], [1])
        source.slots.acquire(); source.slots.acquire()
        try:
            with self.app.app_context(), patch.object(source, '_refresh') as refresh:
                self.service.refresh_course_messages(user, result)
                refresh.assert_not_called()
        finally: source.slots.release(); source.slots.release()


if __name__ == '__main__': unittest.main()
