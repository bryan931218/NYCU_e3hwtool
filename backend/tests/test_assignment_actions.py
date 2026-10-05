"""Personal deadlines, LINE action ownership/replay, work plans and calendar updates."""

import json
import base64
import hashlib
import hmac
import time
import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from sqlalchemy import select

from tests import test_assignment_notifications as fixtures
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.assignments.domain.assignment_actions import assignment_hash, deadline_dates, effective_result, match_assignment, tonight_time
from e3_tracker.assignments.domain.notifications import digest, validate_preferences
from e3_tracker.assignments.persistence.assignment_actions import personal_deadlines, deadline_proposals, work_plans, ACTION_TABLES
from e3_tracker.assignments.persistence.notification_schema import notification_jobs as jobs
from e3_tracker.assignments.services.google_calendar import upsert_assignment_action


class AssignmentActionTests(unittest.TestCase):
    setUp = fixtures.NotificationTests.setUp
    tearDown = fixtures.NotificationTests.tearDown
    item = fixtures.NotificationTests.item
    result = fixtures.NotificationTests.result

    def seed(self):
        item = self.item('HW1', days=10, due_at='E3 raw date')
        self.storage.save_user_cache('student', {'result': self.result(item), 'ts': self.now})
        self.storage.save_notification_preferences('student', validate_preferences({**self.prefs, 'deadline_changes': True}))
        return item, assignment_hash(item, self.storage.assignment_uid)

    def message(self, item):
        due = datetime.fromtimestamp(item['due_ts']+86400, TAIPEI_TZ)
        return {'key': '1:20', 'id': 20, 'course_id': 1, 'course_title': item['course_title'], 'semester': item['semester_key'],
            'title': 'HW1 延期', 'updated_ts': self.now, 'url': 'https://e3p.nycu.edu.tw/mod/forum/discuss.php?d=20',
            'content': f'HW1 截止延長至 {due:%Y/%m/%d %H:%M}。'}

    def proposal(self, kind='announcements'):
        item, key = self.seed()
        message = self.message(item)
        source = self.service.course_message_services[kind]
        attempt = source.storage.claim_course_announcement_refresh('student', item['semester_key'], now=self.now)
        self.assertTrue(source.storage.finish_course_announcement_refresh('student', item['semester_key'], attempt, [message], [1]))
        message = source.storage.update_course_announcement('student', item['semester_key'], message['key'], content={'content':message['content']})
        proposal = self.service.actions.proposals('student', kind, item['semester_key'], message, notify=True)[0]
        return item, key, message, proposal

    def test_parser_only_explicit_future_dates_and_handles_missing_time(self):
        item, _ = self.seed()
        message = self.message(item)
        self.assertEqual(len(deadline_dates(message)), 1)
        self.assertTrue(deadline_dates(message)[0]['time_explicit'])
        expected = datetime.fromtimestamp(item['due_ts']+86400, TAIPEI_TZ).replace(second=0, microsecond=0)
        self.assertEqual(deadline_dates(message)[0]['due_ts'], int(expected.timestamp()))
        message['content'] = message['content'].split(' ')[0]
        message['title'] = 'HW1 截止日期 2099/12/01'
        self.assertFalse(deadline_dates(message))
        self.assertFalse(deadline_dates({'content': '旅行時間 10/20 12:00'}))
        date = datetime.fromtimestamp(item['due_ts'], TAIPEI_TZ)
        suggestion = deadline_dates({'content': f'作業期限 {date:%Y/%m/%d}', 'updated_ts': self.now})[0]
        self.assertFalse(suggestion['time_explicit'])
        self.assertEqual(match_assignment({'content':'HW01 延期'}, [item])['title'], 'HW1')
        self.assertIsNone(match_assignment({'content':'作業延期'}, [item, {**item, 'title':'HW2'}]))

    def test_confirm_is_private_encrypted_and_does_not_modify_school_cache(self):
        item, key, message, proposal = self.proposal()
        response = self.client.post('/api/assignments/deadline-proposals', json={'id': proposal['id'], 'uid': key,
            'due_ts': proposal['due_ts'], 'original_due_ts': item['due_ts'], 'google': False})
        self.assertEqual(response.status_code, 200, response.json)
        self.assertEqual(self.storage.load_user_cache('student')['result']['all_assignments'][0]['due_ts'], item['due_ts'])
        effective = self.service.actions.result('student')['all_assignments'][0]
        self.assertEqual(effective['due_ts'], proposal['due_ts'])
        self.assertEqual(effective['original_due_ts'], item['due_ts'])
        self.assertTrue(effective['personal_due'])
        self.assertEqual(self.storage.personal_deadline_overrides('other'), {})
        with self.storage._engine.connect() as conn:
            self.assertNotIn('截止延長', conn.execute(select(deadline_proposals.c.payload)).scalar())
        self.assertEqual(self.client.post('/api/assignments/deadline-proposals', json={'id':proposal['id'], 'uid':key,
            'due_ts':proposal['due_ts'], 'original_due_ts':item['due_ts']}).status_code, 400)

    def test_stale_source_wrong_course_and_wrong_original_are_rejected(self):
        item, key, message, proposal = self.proposal()
        raw = {'id':proposal['id'], 'uid':key, 'due_ts':proposal['due_ts'], 'original_due_ts':item['due_ts']+10}
        self.assertEqual(self.client.post('/api/assignments/deadline-proposals', json=raw).status_code, 400)
        raw['original_due_ts'] = item['due_ts']; raw['uid'] = 'f'*64
        self.assertEqual(self.client.post('/api/assignments/deadline-proposals', json=raw).status_code, 400)
        source = self.service.course_message_services['announcements']
        source.storage.update_course_announcement('student', item['semester_key'], message['key'], content={'content':'新的內文'})
        raw['uid'] = key
        self.assertEqual(self.client.post('/api/assignments/deadline-proposals', json=raw).status_code, 400)
        self.assertFalse(self.storage.personal_deadline_overrides('student'))

    def test_google_failure_keeps_confirmed_personal_deadline(self):
        item, key, _, proposal = self.proposal('mail')
        with patch.object(self.service.actions, 'calendar_sync', side_effect=RuntimeError('offline')):
            response = self.client.post('/api/assignments/deadline-proposals', json={'id':proposal['id'], 'uid':key,
                'due_ts':proposal['due_ts'], 'original_due_ts':item['due_ts'], 'google':True})
        self.assertEqual(response.status_code, 200, response.json)
        self.assertTrue(response.json['calendar_error'])
        self.assertEqual(self.storage.personal_deadline_overrides('student')[key]['due_ts'], proposal['due_ts'])

    def test_deadline_notification_dedup_and_cancels_after_dismiss(self):
        item, _, message, proposal = self.proposal()
        self.service.actions.proposals('student', 'announcements', item['semester_key'], message, notify=True)
        with self.storage._engine.connect() as conn:
            self.assertEqual(len(conn.execute(select(jobs).where(jobs.c.event_key.like('deadline_change:%'))).all()), 1)
        self.client.post('/api/assignments/deadline-proposals', json={'id':proposal['id'], 'dismiss':True})
        with patch.object(self.service, 'deliver') as send:
            self.service.dispatch(now=self.now+30)
            send.assert_not_called()

    def test_plan_api_dedup_cancel_and_boundary_validation(self):
        item, key = self.seed()
        raw = {'uid':key, 'start_ts':self.now+3600, 'request_id':'a'*32, 'google':False}
        response = self.client.post('/api/assignments/plans', json=raw)
        self.assertEqual(response.status_code, 200, response.json)
        self.assertEqual(self.client.post('/api/assignments/plans', json=raw).json['id'], response.json['id'])
        self.assertEqual(len(self.client.get('/api/assignments/plans').json['items']), 1)
        self.assertEqual(self.client.post('/api/assignments/plans', json={**raw, 'uid':'f'*64}).status_code, 400)
        self.assertEqual(self.client.post('/api/assignments/plans', json={**raw, 'start_ts':item['due_ts']}).status_code, 400)
        near_deadline = self.client.post('/api/assignments/plans', json={**raw, 'request_id':'b'*32, 'start_ts':item['due_ts']-60})
        self.assertEqual(near_deadline.status_code, 200, near_deadline.json)
        self.assertNotIn('minutes', self.client.get('/api/assignments/plans').json['items'][0])
        self.client.delete('/api/assignments/plans', json={'id':near_deadline.json['id']})
        self.assertTrue(self.client.delete('/api/assignments/plans', json={'id':response.json['id']}).json['ok'])
        self.assertEqual(self.client.get('/api/assignments/plans').json['items'], [])
        with patch.object(self.service, 'deliver') as send:
            self.service.dispatch(now=self.now+3601); send.assert_not_called()

    def test_scheduled_reminders_do_not_require_automatic_due_preference(self):
        _, key = self.seed()
        self.storage.save_notification_preferences('student', validate_preferences({'browser_enabled':True}))
        self.service.actions.schedule('student', key, self.now+3600, request_id='b'*32)
        with patch.object(self.service, 'deliver') as send:
            self.service.dispatch(now=self.now+3601)
            self.assertEqual(send.call_count, 1)
            self.assertEqual(send.call_args.args[1]['kind'], 'scheduled')

    def test_line_postback_rejects_forgery_and_other_binding_and_replay_is_idempotent(self):
        item, key = self.seed()
        target = 'U'+'a'*32
        code = self.storage.create_line_link_code('student')
        self.storage.consume_line_link_code(code, target)
        self.storage.save_notification_preferences('student', validate_preferences({'new_assignment':True, 'line_enabled':True}))
        self.storage.observe_notification_assignments('student', self.result(), item['semester_key'], baseline=True)
        self.storage.observe_notification_assignments('student', self.result(item), item['semester_key'])
        with patch.object(self.service, 'line_request') as transport:
            self.service.dispatch()
            message = transport.call_args.args[1]['messages'][0]
        actions = message['quickReply']['items']
        self.assertIn(actions[0]['action']['label'], ['今晚再提醒', '明晚再提醒'])
        self.assertEqual([entry['action']['label'] for entry in actions[1:]], ['設定提醒時間', '開啟作業'])
        self.assertEqual(actions[1]['action']['type'], 'datetimepicker')
        token = actions[0]['action']['data']
        with self.assertRaises(ValueError): self.service.actions.handle_postback(token, 'U'+'b'*32)
        with self.assertRaises(ValueError): self.service.actions.handle_postback(token[:-1]+('0' if token[-1]!='0' else '1'), target)
        self.assertIn('已設定', self.service.actions.handle_postback(token, target))
        self.assertIn('已安排過', self.service.actions.handle_postback(token, target))
        self.assertEqual(len(self.storage.assignment_action_records('student', work_plans)), 1)
        self.storage.unlink_line(username='student')
        with self.assertRaises(ValueError): self.service.actions.handle_postback(token, target)

    def test_personal_deadline_api_cache_and_reset(self):
        item, key = self.seed()
        uid = self.storage.assignment_uid(item['course_id'], item['title'], item['url'])
        self.assertEqual(self.client.post('/api/assignments/personal-deadline', json={'uid':uid, 'due_ts':self.now+86400}).status_code, 200)
        data = self.client.get('/api/cache?include_cache=1').json
        self.assertTrue(data['cache']['result']['all_assignments'][0]['personal_due'])
        self.assertEqual(self.client.delete('/api/assignments/personal-deadline', json={'uid':uid}).status_code, 200)
        self.assertFalse(self.storage.personal_deadline_overrides('student'))
        self.assertEqual(self.client.post('/api/assignments/personal-deadline', json={'uid':'other', 'due_ts':self.now+1000}).status_code, 400)

    def test_cleanup_removes_all_action_data(self):
        item, key, _, _ = self.proposal()
        self.storage.set_personal_deadline('student', key, item['due_ts']+100)
        self.service.actions.schedule('student', key, self.now+3600, request_id='c'*32)
        from e3_tracker.assignments.persistence.cleanup import delete_assignment_account_data
        from e3_tracker.platform.persistence.core_schema import users_table
        with self.storage._engine.begin() as conn:
            uid = conn.execute(select(users_table.c.id).where(users_table.c.username=='student')).scalar()
            delete_assignment_account_data(conn, [uid])
            for table in ACTION_TABLES: self.assertEqual(conn.execute(select(table)).all(), [])

    def test_actions_require_login_guest_rejected_and_csrf(self):
        _, key = self.seed()
        response = self.client.get(f'/assignments/plan?uid={key}')
        self.assertEqual(response.status_code, 200)
        self.assertIn('設定提醒時間'.encode(), response.data)
        self.assertNotIn('planDuration'.encode(), response.data)
        self.assertNotIn('預留時間'.encode(), response.data)
        anonymous = self.app.test_client()
        self.assertNotEqual(anonymous.post('/api/assignments/plans', json={}).status_code, 200)
        with anonymous.session_transaction() as session: session['session_token']='notifications-test'
        self.assertEqual(anonymous.post('/api/assignments/plans', json={}).status_code, 400)

    def test_calendar_patch_uses_only_matching_owned_event(self):
        item, _ = self.seed()
        from e3_tracker.assignments.services.google_calendar import _event_id_for
        identity = _event_id_for(item)
        listed = Mock(status_code=200); listed.json.return_value={'items':[{'id':'owned', 'extendedProperties':{'private':{'e3_uid':identity}}}, {'id':'foreign'}]}
        changed = Mock(status_code=200); changed.json.return_value={}
        with patch('e3_tracker.assignments.services.google_calendar.requests.get', return_value=listed), patch(
            'e3_tracker.assignments.services.google_calendar.requests.patch', return_value=changed) as write, patch(
            'e3_tracker.assignments.services.google_calendar.requests.post') as create:
            upsert_assignment_action(item, access_token='synthetic', calendar_id='primary')
            create.assert_not_called(); self.assertEqual(write.call_count, 1)
            self.assertTrue(write.call_args.args[0].endswith('/owned'))
            self.assertEqual(set(write.call_args.kwargs['json']), {'start', 'end'})

    def test_calendar_reminder_has_stable_separate_identity_and_no_busy_duration(self):
        item, _ = self.seed()
        listed=Mock(status_code=200); listed.json.return_value={'items':[]}
        created=Mock(status_code=200); created.json.return_value={}
        plan={'id':'a'*64, 'start_ts':self.now+3600}
        with patch('e3_tracker.assignments.services.google_calendar.requests.get', return_value=listed), patch(
            'e3_tracker.assignments.services.google_calendar.requests.post', return_value=created) as write:
            upsert_assignment_action(item, access_token='synthetic', calendar_id='primary', plan=plan)
            body = write.call_args.kwargs['json']
            self.assertRegex(body['id'], '^[a-v0-9]+$')
            self.assertEqual(body['extendedProperties']['private']['e3_uid'], 'e3-plan-'+'a'*64)
            self.assertEqual(body['summary'], '作業提醒｜HW1')
            self.assertEqual(body['transparency'], 'transparent')
            self.assertEqual(body['reminders']['overrides'][0]['minutes'], 0)
            self.assertEqual((datetime.fromisoformat(body['end']['dateTime'])-datetime.fromisoformat(body['start']['dateTime'])).total_seconds(), 1)

    def test_signed_line_webhook_handles_action_and_redelivery(self):
        item, _ = self.seed()
        target='U'+'a'*32
        self.storage.consume_line_link_code(self.storage.create_line_link_code('student'), target)
        self.storage.save_notification_preferences('student', validate_preferences({'new_assignment':True,'line_enabled':True}))
        self.storage.observe_notification_assignments('student', self.result(), item['semester_key'], baseline=True)
        self.storage.observe_notification_assignments('student', self.result(item), item['semester_key'])
        with patch.object(self.service, 'line_request') as transport:
            self.service.dispatch()
        data=transport.call_args.args[1]['messages'][0]['quickReply']['items'][0]['action']['data']
        event={'type':'postback','source':{'type':'user','userId':target},'postback':{'data':data},'replyToken':'synthetic-reply'}
        def send(event):
            body=json.dumps({'events':[event]}).encode()
            signature=base64.b64encode(hmac.new(self.service.line_secret.encode(),body,hashlib.sha256).digest()).decode()
            return self.client.post('/api/notifications/line/webhook',data=body,content_type='application/json',headers={'X-Line-Signature':signature})
        with patch.object(self.service,'line_request') as reply:
            self.assertEqual(send(event).status_code,200)
            self.assertEqual(reply.call_count,1)
            self.assertEqual(send({**event,'deliveryContext':{'isRedelivery':True}}).status_code,200)
            self.assertEqual(reply.call_count,1)
        self.assertEqual(len(self.storage.assignment_action_records('student',work_plans)),1)
        self.assertEqual(self.client.post('/api/notifications/line/webhook',json={'events':[event]}).status_code,403)

    def test_calendar_existing_work_block_becomes_non_blocking_reminder_on_sync(self):
        item,_=self.seed()
        plan={'id':'a'*64,'start_ts':self.now+3600,'minutes':120}
        listed=Mock(status_code=200)
        listed.json.return_value={'items':[{'id':'owned','extendedProperties':{'private':{'e3_uid':'e3-plan-'+plan['id']}}}]}
        changed=Mock(status_code=200); changed.json.return_value={}
        with patch('e3_tracker.assignments.services.google_calendar.requests.get',return_value=listed), patch(
            'e3_tracker.assignments.services.google_calendar.requests.patch',return_value=changed) as write:
            upsert_assignment_action(item,access_token='synthetic',calendar_id='primary',plan=plan)
            body=write.call_args.kwargs['json']
            self.assertEqual(body['summary'],'作業提醒｜HW1')
            self.assertEqual(body['transparency'],'transparent')
            self.assertEqual(body['reminders']['overrides'][0]['minutes'],0)
            self.assertEqual((datetime.fromisoformat(body['end']['dateTime'])-datetime.fromisoformat(body['start']['dateTime'])).total_seconds(),1)

    def test_another_account_cannot_confirm_cancel_or_list_own_actions(self):
        item,key,_,proposal=self.proposal()
        plan=self.service.actions.schedule('student',key,self.now+3600,request_id='e'*32)
        self.storage.save_web_session('other-test','other',moodle_session='synthetic')
        with self.client.session_transaction() as session: session['session_token']='other-test'
        self.assertEqual(self.client.get('/api/assignments/plans').json['items'],[])
        self.assertFalse(self.client.delete('/api/assignments/plans',json={'id':plan['id']}).json['ok'])
        response=self.client.post('/api/assignments/deadline-proposals',json={'id':proposal['id'],'uid':key,'due_ts':proposal['due_ts'],'original_due_ts':item['due_ts']})
        self.assertEqual(response.status_code,400)
        self.assertEqual(self.client.get(f'/assignments/plan?uid={key}').status_code,404)

    def test_guest_cannot_use_action_routes(self):
        self.storage.save_web_session('guest-test','guest_actions',is_guest=True)
        with self.client.session_transaction() as session: session['session_token']='guest-test'
        for path in ('/api/assignments/plans','/api/assignments/deadline-proposals','/api/assignments/personal-deadline'):
            self.assertEqual(self.client.post(path,json={}).status_code,403)

    def test_completed_assignment_cancels_scheduled_reminder(self):
        item,key=self.seed()
        self.service.actions.schedule('student',key,self.now+3600,request_id='f'*32)
        self.storage.save_user_cache('student',{'ts':self.now,'result':self.result({**item,'completed':True})})
        with patch.object(self.service,'deliver') as send:
            self.service.dispatch(now=self.now+3601); send.assert_not_called()

    def test_personal_deadline_reschedules_automatic_reminders(self):
        item,key=self.seed()
        self.storage.observe_notification_assignments('student',self.result(item),item['semester_key'],baseline=True)
        self.storage.set_personal_deadline('student',key,self.now+2*86400)
        self.storage.observe_notification_assignments('student',self.result(item),item['semester_key'],now=self.now)
        with patch.object(self.service,'deliver') as send:
            self.service.dispatch(now=self.now+1)
            self.assertEqual(send.call_count,1)
            self.assertEqual(send.call_args.args[1]['due_ts'],self.now+2*86400)

    def test_calendar_synced_marker_and_cancelled_plan_cannot_be_replayed(self):
        _,key=self.seed()
        with patch.object(self.service.actions,'calendar_sync'):
            plan=self.service.actions.schedule('student',key,self.now+3600,request_id='d'*32,google=True)
        records=self.storage.assignment_action_records('student',work_plans)
        self.assertTrue(records[plan['id']]['google_synced'])
        self.storage.cancel_assignment_plan('student',plan['id'])
        with self.assertRaises(ValueError):
            self.service.actions.schedule('student',key,self.now+3600,request_id='d'*32)

    def test_old_settings_tabs_preserve_new_deadline_preference(self):
        self.seed()
        response=self.client.post('/api/notifications/settings',json={key:value for key,value in self.prefs.items() if key!='deadline_changes'})
        self.assertEqual(response.status_code,200,response.json)
        self.assertTrue(response.json['preferences']['deadline_changes'])

    def test_manual_sync_uses_same_upsert_and_ignores_undated_items(self):
        from e3_tracker.assignments.services.google_calendar import sync_assignments_to_google_calendar
        item,_=self.seed()
        with patch('e3_tracker.assignments.services.google_calendar.upsert_assignment_action') as upsert:
            count=sync_assignments_to_google_calendar([item,{**item,'due_ts':None}],access_token='synthetic',calendar_id='primary')
            self.assertEqual(count,1)
            self.assertEqual(upsert.call_count,1)

    def test_calendar_duplicate_insert_is_retried_only_for_owned_identity(self):
        item,_=self.seed()
        listed=Mock(status_code=200); listed.json.return_value={'items':[]}
        conflict=Mock(status_code=409)
        foreign=Mock(status_code=200); foreign.json.return_value={'extendedProperties':{'private':{'e3_uid':'not-ours'}}}
        with patch('e3_tracker.assignments.services.google_calendar.requests.get',side_effect=[listed,foreign]), patch(
            'e3_tracker.assignments.services.google_calendar.requests.post',return_value=conflict), patch(
            'e3_tracker.assignments.services.google_calendar.requests.patch') as write:
            with self.assertRaises(ValueError): upsert_assignment_action(item,access_token='synthetic',calendar_id='primary')
            write.assert_not_called()

    def test_parser_skips_explicit_original_date_and_supports_roc(self):
        item,_=self.seed()
        old=datetime.fromtimestamp(item['due_ts'],TAIPEI_TZ)
        new=datetime.fromtimestamp(item['due_ts']+86400,TAIPEI_TZ)
        message={'title':'HW1 期限異動','content':f'原期限：{old:%Y/%m/%d %H:%M} 改至 {new.year-1911}年 {new.month}月 {new.day} 日 {new:%H:%M}。','updated_ts':self.now}
        dates=deadline_dates(message)
        self.assertEqual(len(dates),1)
        self.assertEqual(dates[0]['due_ts'],int(new.replace(second=0,microsecond=0).timestamp()))

    def test_source_update_between_check_and_commit_is_rejected_atomically(self):
        item,key,message,proposal=self.proposal('mail')
        original=self.storage.confirm_personal_deadline
        def concurrent_update(*args,**kwargs):
            self.service.course_message_services['mail'].storage.update_course_announcement('student',item['semester_key'],message['key'],content={'content':'老師已重新修改日期'})
            return original(*args,**kwargs)
        with patch.object(self.storage,'confirm_personal_deadline',side_effect=concurrent_update):
            response=self.client.post('/api/assignments/deadline-proposals',json={'id':proposal['id'],'uid':key,
                'due_ts':proposal['due_ts'],'original_due_ts':item['due_ts']})
        self.assertEqual(response.status_code,400)
        self.assertFalse(self.storage.personal_deadline_overrides('student'))

    def test_relative_extension_does_not_compound_after_confirming(self):
        item,key,message,_=self.proposal()
        message=self.service.course_message_services['announcements'].storage.update_course_announcement('student',item['semester_key'],message['key'],content={'content':'HW1 deadline extended by 3 days'})
        proposal=self.service.actions.proposals('student','announcements',item['semester_key'],message)[0]
        self.assertEqual(proposal['due_ts'],item['due_ts']+3*86400)
        self.service.actions.confirm('student',proposal['id'],key,proposal['due_ts'],item['due_ts'])
        self.assertEqual(self.service.actions.proposals('student','announcements',item['semester_key'],message),[])

    def test_assignment_matching_handles_chinese_numbers_and_avoids_prefix_collisions(self):
        item,_=self.seed()
        self.assertEqual(match_assignment({'content':'第一份作業延期'},[item])['title'],'HW1')
        self.assertEqual(match_assignment({'content':'HW１ 延期'},[item])['title'],'HW1')
        self.assertIsNone(match_assignment({'content':'HW10 延期'},[item]))
        self.assertIsNone(match_assignment({'content':'HW1-2 延期'},[item]))


if __name__ == '__main__': unittest.main()
