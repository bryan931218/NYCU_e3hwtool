"""E3 dcpcmail parsing, encrypted inbox isolation, and independent private routes."""

import time
import unittest
from unittest.mock import Mock, patch

from sqlalchemy import delete, select

from e3_tracker.assignments.domain.course_mail import mail_content, mail_summaries, mail_url
from e3_tracker.assignments.persistence.course_mail import course_mail_cache
from e3_tracker.assignments.persistence.cleanup import delete_assignment_account_data
from e3_tracker.assignments.services.course_mail import collect_course_mail
from e3_tracker.assignments.services.course_announcements import MoodleNewsClient, AnnouncementSessionExpired
from e3_tracker.platform.persistence.core_schema import users_table
from tests import test_course_announcements as news_tests

BASE, COURSE, NEWS = news_tests.BASE, news_tests.COURSE, news_tests.NEWS


INBOX = '''<div class="mail_list"><div class="mail_item mail_unread">
<a class="mail_link" href="view.php?t=inbox&amp;c=42&amp;m=7&amp;sesskey=secret">
<span class="mail_users">課程助教</span><span class="mail_summary"><span class="mail_label mail_course">課程標籤</span>作業繳交提醒</span>
<span class="mail_date" title="2026年10月4日,09:30">今天</span></a></div></div>'''
MAIL = {**NEWS, 'title': '作業繳交提醒', 'author': '課程助教', 'e3_read': False,
        'url': BASE + '/local/dcpcmail/view.php?c=42&t=inbox&m=7'}


class MailParsingTests(unittest.TestCase):
    def test_inbox_subject_sender_date_and_course_are_independent_of_announcements(self):
        result = mail_summaries(INBOX, BASE, COURSE)
        self.assertEqual(len(result), 1)
        item = result[0]
        self.assertEqual((item['key'], item['title'], item['author']), ('42:7', '作業繳交提醒', '課程助教'))
        self.assertEqual(item['url'], MAIL['url'])
        self.assertEqual(item['updated_ts'], 1791077400)
        self.assertFalse(item['e3_read'])
        self.assertEqual(item['course_title'], COURSE['title'])

    def test_only_strict_readonly_same_origin_same_course_mail_urls(self):
        self.assertEqual(mail_url('?m=7&action=delete&sesskey=secret', BASE, 42), MAIL['url'])
        for value in ('//evil.test/local/dcpcmail/view.php?m=7', '/mod/forum/discuss.php?d=7',
                      '?m=7&m=8', '?m=0', '?c=43&m=7', '?c=42&c=43&m=7', 'https://[bad',
                      'https://user:pass@e3p.nycu.edu.tw/local/dcpcmail/view.php?m=7'):
            self.assertIsNone(mail_url(value, BASE, 42), value)
        self.assertIsNone(mail_url('?m=8', BASE, 42, 7))

    def test_known_empty_inbox_and_unknown_html_are_not_equivalent(self):
        self.assertEqual(mail_summaries('<div class="mail_list">沒有郵件</div>', BASE, COURSE), [])
        # Verified against the live E3 course inbox, not the extension's guessed markup.
        empty = '<div class="mail_list"><div class="mail_item">沒有可查看的郵件. <a href="view.php?t=inbox">顯示最近的郵件</a></div></div>'
        self.assertEqual(mail_summaries(empty, BASE, COURSE), [])
        with self.assertRaises(ValueError):
            mail_summaries(empty.replace('</div></div>', '</div><div class="mail_item">broken</div></div>'), BASE, COURSE)
        for html in ('<p>登入</p>', '<div class="mail_list"></div>', '<div class="mail_list"><div class="mail_item">broken</div></div>',
                     '<div class="mail_list"><div class="mail_item"><a class="mail_link" href="?m=7&c=43">其他課程</a></div></div>'):
            with self.assertRaises(ValueError): mail_summaries(html, BASE, COURSE)

    def test_mail_body_attachments_and_active_markup_are_sanitized(self):
        html = '''<div id="region-main">私人導覽資料<div class="mail_content"><p>第一段</p><p>第二段<br>下一行</p>
        <script>steal()</script><form>send</form><a href="/pluginfile.php/42/sheet.pdf">說明文件</a>
        <a href="javascript:alert(1)">bad</a><a href="https://example.test/?token=secret">token</a></div>
        <div class="mail_attachments"><a href="/pluginfile.php/42/guide.pdf">附件.pdf</a></div></div>'''
        result = mail_content(html, BASE)
        self.assertIn('第一段\n\n第二段\n下一行', result['content'])
        for value in ('私人導覽資料', 'steal()', 'send'): self.assertNotIn(value, result['content'])
        self.assertEqual([link['title'] for link in result['links']], ['說明文件', '附件.pdf'])
        with self.assertRaises(ValueError): mail_content('<div id="region-main">不相關的其他信件</div>', BASE)

    def test_unknown_dates_do_not_become_fresh_messages_and_read_state_is_parsed(self):
        html = INBOX.replace('mail_item mail_unread', 'mail_item').replace('2026年10月4日,09:30', 'unknown')
        item = mail_summaries(html, BASE, COURSE)[0]
        self.assertIsNone(item['updated_ts']); self.assertTrue(item['e3_read'])

    def test_collection_partial_failure_never_loads_announcement_forums(self):
        client = Mock(base_url=BASE)
        client.get.side_effect = [INBOX, ValueError('unknown page'), '<div class="mail_list">No messages</div>']
        items, successful, failed = collect_course_mail(client, [COURSE, {**COURSE, 'id':43}, {**COURSE, 'id':44}])
        self.assertEqual(successful, [42,44]); self.assertEqual(failed, [43]); self.assertEqual(len(items),1)
        self.assertEqual(client.get.call_args_list[0].args[0], BASE+'/local/dcpcmail/view.php?c=42&t=inbox')
        client.get.side_effect = AnnouncementSessionExpired()
        with self.assertRaises(AnnouncementSessionExpired): collect_course_mail(client, [COURSE])

    def test_inbox_is_bounded_and_deduplicated(self):
        row = INBOX.split('<div class="mail_list">',1)[1].rsplit('</div>',1)[0]
        html = '<div class="mail_list">' + ''.join(row.replace('m=7',f'm={number}') for number in range(1,45)) + '</div>'
        self.assertEqual(len(mail_summaries(html,BASE,COURSE)),30)
        self.assertEqual(len(mail_summaries(INBOX.replace('</div></div>', '</div>'+row+'</div>'),BASE,COURSE)),1)


class MailAccountTests(unittest.TestCase):
    login = news_tests.NewsAccountTests.login
    tearDown = news_tests.NewsAccountTests.tearDown

    def setUp(self):
        news_tests.NewsAccountTests.setUp(self)
        self.mail = self.app.extensions['e3_course_mail']
        self.cache = self.mail.storage

    def seed_mail(self, items=None):
        attempt = self.cache.claim_course_announcement_refresh('112550103', '115-1', now=time.time()-120)
        self.assertIsNotNone(attempt)
        self.cache.finish_course_announcement_refresh('112550103', '115-1', attempt, items or [MAIL], [42])

    def test_separate_pages_lists_content_and_read_markers_even_when_ids_collide(self):
        news_tests.NewsAccountTests.seed(self)
        self.seed_mail()
        html = self.client.get('/courses/mail').get_data(as_text=True)
        self.assertIn('課程信件', html); self.assertIn('"autoOpen": false', html)
        self.assertIn('/api/course-mail', html); self.assertIn('aria-current="page">課程信件', html)
        home = self.client.get('/').get_data(as_text=True)
        self.assertIn('/courses/messages', home)
        self.assertNotIn('href="/courses/mail"', home)
        self.assertNotIn('href="/courses/announcements"', home)
        self.assertEqual(self.client.get('/api/course-mail?semester=115-1').json['items'][0]['title'], MAIL['title'])
        self.assertEqual(self.client.get('/api/course-announcements?semester=115-1').json['items'][0]['title'], NEWS['title'])
        with patch.object(MoodleNewsClient, 'get', return_value='<div class="mail_content"><p>信件內文</p></div>') as get:
            response = self.client.post('/api/course-mail/item', json={'semester':'115-1','key':'42:7','read':True})
            self.assertEqual(response.status_code,200); self.assertEqual(get.call_args.args[0],MAIL['url'])
        announcement = self.storage.load_course_announcements('112550103','115-1')['items'][0]
        self.assertEqual(announcement['read_at'],0); self.assertNotIn('content',announcement)
        self.assertEqual(self.cache.load_course_announcements('112550103','115-1')['items'][0]['content'],'信件內文')

    def test_subject_and_content_are_encrypted_at_rest_bound_to_account_and_semester(self):
        self.seed_mail()
        self.cache.update_course_announcement('112550103','115-1','42:7',content={'content':'private-message', 'links':[]})
        with self.storage._engine.connect() as conn: payload = conn.execute(select(course_mail_cache.c.payload)).scalar_one()
        self.assertTrue(payload.startswith('enc:v1:'))
        self.assertNotIn('private-message',payload); self.assertNotIn(MAIL['title'],payload)
        for username, semester in [('other','115-1'),('112550103','114-2')]:
            with self.assertRaises(ValueError): self.cache._decode_message_payload(payload,username,semester)
        with self.assertRaises(ValueError): self.cache._decode_message_payload('[]','112550103','115-1')

    def test_own_account_only_including_session_login_and_guests(self):
        self.seed_mail()
        self.login('Session-synthetic')
        self.assertEqual(self.client.get('/api/course-mail?semester=115-1&username=112550103').json['items'],[])
        self.assertEqual(self.client.post('/api/course-mail/item',json={'semester':'115-1','key':'42:7','read':True}).status_code,404)
        with patch.object(self.mail,'start',return_value='running') as start:
            self.assertEqual(self.client.post('/api/course-mail/refresh',json={'semester':'115-1','username':'other'}).status_code,202)
            self.assertEqual(start.call_args.args[1]['username'],'Session-synthetic')
        self.login('訪客_test',guest=True)
        for path in ['/api/course-mail','/api/course-mail/refresh','/api/course-mail/item']:
            response = self.client.get(path) if path == '/api/course-mail' else self.client.post(path,json={})
            self.assertEqual(response.status_code,403)
        with self.client.session_transaction() as session: session.clear()
        self.assertEqual(self.client.get('/api/course-mail').status_code,302)

    def test_shared_worker_capacity_and_failed_body_preserves_unread(self):
        self.seed_mail()
        self.assertIs(self.service.slots,self.mail.slots)
        with patch.object(MoodleNewsClient,'get',return_value='<div id="region-main">unknown</div>'):
            self.assertEqual(self.client.post('/api/course-mail/item',json={'semester':'115-1','key':'42:7','read':True}).status_code,502)
        self.assertEqual(self.cache.load_course_announcements('112550103','115-1')['items'][0]['read_at'],0)
        self.service.slots.acquire(); self.service.slots.acquire()
        try:
            self.assertEqual(self.client.post('/api/course-mail/refresh',json={'semester':'115-1'}).status_code,429)
        finally: self.service.slots.release(); self.service.slots.release()

    def test_failed_inbox_preserves_cached_mail_and_deleted_users_cannot_be_resurrected(self):
        self.seed_mail()
        attempt = self.cache.claim_course_announcement_refresh('112550103','115-1')
        self.mail.slots.acquire()
        with patch.object(MoodleNewsClient,'get',return_value='<p>unexpected</p>'):
            self.mail._refresh(self.app,{'username':'112550103','moodle_session':'synthetic'},'115-1',[COURSE],attempt)
        data = self.cache.load_course_announcements('112550103','115-1')
        self.assertEqual(data['status'],'error'); self.assertEqual(len(data['items']),1)
        with self.storage._engine.begin() as conn:
            user_id = conn.execute(select(users_table.c.id).where(users_table.c.username=='112550103')).scalar_one()
            delete_assignment_account_data(conn,[user_id])
            self.assertIsNone(conn.execute(select(course_mail_cache)).first())
            conn.execute(delete(users_table).where(users_table.c.id==user_id))
        self.assertFalse(self.cache.finish_course_announcement_refresh('112550103','115-1',attempt,[MAIL],[42]))
        self.assertIsNone(self.cache.claim_course_announcement_refresh('112550103','115-1'))

    def test_csrf_validation_course_boundaries_and_cached_content(self):
        self.seed_mail()
        raw = self.app.test_client()
        with raw.session_transaction() as session: session['session_token']='test-112550103'
        self.assertEqual(raw.post('/api/course-mail/refresh',json={'semester':'115-1'}).status_code,400)
        self.assertEqual(self.client.post('/api/course-mail/refresh',json=[]).status_code,400)
        self.assertEqual(self.client.get('/api/course-mail?semester=114-2').status_code,400)
        self.assertEqual(self.client.post('/api/course-mail/item',json={'semester':'115-1','key':'42:7','read':'true'}).status_code,400)
        with patch.object(self.mail,'content',return_value={'content':'cached','links':[]}) as get:
            for _ in range(2):
                self.assertEqual(self.client.post('/api/course-mail/item',json={'semester':'115-1','key':'42:7','read':True}).status_code,200)
            self.assertEqual(get.call_count,1)

    def test_external_read_state_only_initializes_local_state_not_overrides_explicit_unread(self):
        self.seed_mail([{**MAIL,'e3_read':True}])
        self.assertGreater(self.cache.load_course_announcements('112550103','115-1')['items'][0]['read_at'],0)
        self.cache.update_course_announcement('112550103','115-1','42:7',read=False)
        attempt = self.cache.claim_course_announcement_refresh('112550103','115-1')
        self.cache.finish_course_announcement_refresh('112550103','115-1',attempt,[{**MAIL,'e3_read':True}],[42])
        self.assertEqual(self.cache.load_course_announcements('112550103','115-1')['items'][0]['read_at'],0)

    def test_additive_mail_migration_is_recorded_and_idempotent(self):
        from e3_tracker.platform.persistence.migrations import migration_status, run_migrations
        before = migration_status(self.storage._engine)
        run_migrations(self.storage._engine)
        self.assertEqual(before,migration_status(self.storage._engine))
        self.assertIn('0013_course_mail',str(before))

    def test_unread_summary_combines_sources_and_updates_after_read_unread(self):
        news_tests.NewsAccountTests.seed(self); self.seed_mail()
        summary = self.client.get('/api/course-messages/unread?semester=115-1')
        self.assertEqual(summary.status_code, 200)
        self.assertIn('no-store', summary.headers['Cache-Control'])
        self.assertEqual(summary.json, {'ok':True, 'semester':'115-1', 'announcements':1, 'mail':1, 'total':2,
                                       'unseen': {'announcements':1, 'mail':1, 'total':2}})
        self.storage.update_course_announcement('112550103','115-1','42:7',read=True)
        self.assertEqual(self.client.get('/api/course-messages/unread').json['total'], 1)
        self.cache.update_course_announcement('112550103','115-1','42:7',read=True)
        self.assertEqual(self.client.get('/api/course-messages/unread').json['total'], 0)
        self.cache.update_course_announcement('112550103','115-1','42:7',read=False)
        self.assertEqual(self.client.get('/api/course-messages/unread').json['mail'], 1)

    def test_summary_is_own_account_only_and_contains_no_private_message_data(self):
        self.seed_mail()
        with patch.object(MoodleNewsClient, 'get') as fetch:
            response = self.client.get('/api/course-messages/unread?semester=115-1')
        fetch.assert_not_called()
        self.assertNotIn(MAIL['title'], response.get_data(as_text=True))
        self.assertNotIn('items', response.json)
        self.login('Session-synthetic')
        self.assertEqual(self.client.get('/api/course-messages/unread?username=112550103').json['total'], 0)
        self.login('訪客_test', guest=True)
        self.assertEqual(self.client.get('/api/course-messages/unread').status_code, 403)
        self.assertNotIn('workspace-messages', self.client.get('/').get_data(as_text=True))
        with self.client.session_transaction() as session: session.clear()
        self.assertEqual(self.client.get('/api/course-messages/unread').status_code, 302)

    def test_summary_respects_semester_and_current_owned_course_catalog(self):
        self.seed_mail([{**MAIL, 'course_id':43, 'key':'43:7'}])
        self.assertEqual(self.client.get('/api/course-messages/unread').json['total'], 0)
        self.assertEqual(self.client.get('/api/course-messages/unread?semester=114-2').json['total'], 0)
        self.assertEqual(self.client.get('/api/course-messages/unread?semester=unknown').json['total'], 0)

    def test_visiting_entry_clears_new_badge_without_marking_items_read(self):
        news_tests.NewsAccountTests.seed(self); self.seed_mail()
        self.assertEqual(self.client.post('/api/course-messages/seen', json={'semester':'115-1'}).status_code, 200)
        summary = self.client.get('/api/course-messages/unread').json
        self.assertEqual(summary['total'], 2)
        self.assertEqual(summary['unseen']['total'], 0)
        self.cache.update_course_announcement('112550103','115-1','42:7',read=True)
        self.cache.update_course_announcement('112550103','115-1','42:7',read=False)
        self.assertEqual(self.client.get('/api/course-messages/unread').json['unseen']['total'], 0)
        self.assertGreater(self.cache.load_course_announcements('112550103','115-1')['items'][0]['seen_at'], 0)
        with self.storage._engine.connect() as conn:
            payload = conn.execute(select(course_mail_cache.c.payload)).scalar_one()
        self.assertTrue(payload.startswith('enc:v1:'))

    def test_new_arrivals_restore_badge_but_refresh_edits_and_failed_courses_do_not(self):
        self.seed_mail()
        self.client.post('/api/course-messages/seen', json={'semester':'115-1'})
        attempt = self.cache.claim_course_announcement_refresh('112550103', '115-1')
        self.cache.finish_course_announcement_refresh('112550103', '115-1', attempt,
            [{**MAIL, 'title':'Edited subject'}, {**MAIL, 'key':'42:8', 'id':'8', 'e3_read':True}], [42])
        summary = self.client.get('/api/course-messages/unread').json
        self.assertEqual(summary['unseen']['mail'], 1)
        self.assertEqual(summary['mail'], 1)
        # Partial refreshes preserve acknowledgement and unread state of old items.
        attempt = self.cache.claim_course_announcement_refresh('112550103', '115-1', now=time.time()+120)
        self.cache.finish_course_announcement_refresh('112550103', '115-1', attempt, [], [], 'failed')
        self.assertEqual(self.client.get('/api/course-messages/unread').json['unseen']['mail'], 1)
        self.client.post('/api/course-messages/seen', json={'semester':'115-1'})
        self.assertEqual(self.client.get('/api/course-messages/unread').json['unseen']['total'], 0)

    def test_legacy_read_messages_remain_seen_after_manual_unread(self):
        self.seed_mail([{**MAIL, 'e3_read':True}])
        items = self.cache.load_course_announcements('112550103', '115-1')['items']
        items[0].pop('seen_at')
        with self.storage._engine.begin() as conn:
            conn.execute(course_mail_cache.update().values(payload=self.cache._encode_message_payload(items, '112550103', '115-1')))
        self.assertEqual(self.client.get('/api/course-messages/unread').json['unseen']['total'], 0)
        self.client.post('/api/course-messages/seen', json={'semester':'115-1'})
        self.cache.update_course_announcement('112550103', '115-1', '42:7', read=False)
        self.assertEqual(self.client.get('/api/course-messages/unread').json['unseen']['total'], 0)

    def test_acknowledgement_is_csrf_protected_and_cannot_touch_other_accounts_or_courses(self):
        self.seed_mail([MAIL, {**MAIL, 'course_id':43, 'key':'43:8'}])
        raw = self.app.test_client()
        with raw.session_transaction() as session: session['session_token']='test-112550103'
        self.assertEqual(raw.post('/api/course-messages/seen', json={'semester':'115-1'}).status_code, 400)
        for payload in [[], {}, {'semester':[]}, {'semester':'unknown'}, {'semester':'114-2'}]:
            self.assertEqual(self.client.post('/api/course-messages/seen', json=payload).status_code, 400)
        self.login('Session-synthetic')
        self.assertEqual(self.client.post('/api/course-messages/seen', json={'semester':'115-1','username':'112550103'}).status_code, 200)
        self.assertEqual(self.cache.load_course_announcements('112550103','115-1')['items'][0]['seen_at'], 0)
        self.login('112550103')
        self.client.post('/api/course-messages/seen', json={'semester':'115-1'})
        unrelated = next(item for item in self.cache.load_course_announcements('112550103','115-1')['items'] if item['course_id']==43)
        self.assertEqual(unrelated['seen_at'], 0)
        self.login('訪客_test', guest=True)
        self.assertEqual(self.client.post('/api/course-messages/seen', json={'semester':'115-1'}).status_code, 403)
        with self.client.session_transaction() as session: session.clear()
        self.assertEqual(self.client.post('/api/course-messages/seen', json={'semester':'115-1'}).status_code, 302)


if __name__ == '__main__': unittest.main()
