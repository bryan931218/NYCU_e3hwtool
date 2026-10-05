"""LINE picker -> reminder, without trusting clients or duplicating writes."""
import base64
import hashlib
import hmac
import json
import unittest
from datetime import datetime
from unittest.mock import patch
from sqlalchemy import select
from tests import test_assignment_actions as action_fixtures, test_assignment_notifications as fixtures
from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.assignments.domain.notifications import validate_preferences
from e3_tracker.assignments.persistence.notification_schema import notification_jobs as jobs
from e3_tracker.assignments.persistence.assignment_actions import work_plans


class LinePlanningTests(unittest.TestCase):
    setUp = fixtures.NotificationTests.setUp
    tearDown = fixtures.NotificationTests.tearDown
    item = fixtures.NotificationTests.item
    result = fixtures.NotificationTests.result
    seed = action_fixtures.AssignmentActionTests.seed

    def line_job(self):
        item, key = self.seed()
        target = 'U'+'a'*32
        self.storage.consume_line_link_code(self.storage.create_line_link_code('student'), target)
        self.storage.save_notification_preferences('student', validate_preferences({'new_assignment':True, 'line_enabled':True}))
        self.storage.observe_notification_assignments('student', self.result(), item['semester_key'], baseline=True)
        self.storage.observe_notification_assignments('student', self.result(item), item['semester_key'])
        with patch.object(self.service, 'line_request') as transport:
            self.service.dispatch()
        with self.storage._engine.connect() as conn:
            job = dict(conn.execute(select(jobs).where(jobs.c.channel == 'line')).mappings().first())
        return item, key, target, job, transport.call_args.args[1]['messages'][0]

    def select(self, job, target, start=None):
        start = ((self.now+3600)//60)*60 if start is None else start
        selected = datetime.fromtimestamp(start, TAIPEI_TZ).strftime('%Y-%m-%dT%H:%M')
        response = self.service.actions.handle_line_postback(self.service.actions.action_data(job, 'schedule'), target, {'datetime':selected})
        return start, response

    def test_picker_bounds_direct_link_and_selection_creates_reminder(self):
        item, _, target, job, pushed = self.line_job()
        self.assertIn(item['url'], pushed['text'])
        picker = next(entry['action'] for entry in pushed['quickReply']['items'] if entry['action']['type']=='datetimepicker')
        self.assertEqual(picker['mode'], 'datetime')
        self.assertLessEqual(picker['min'], picker['initial'])
        self.assertLessEqual(picker['initial'], picker['max'])
        self.assertLess(len(picker['data']),300)
        _, response = self.select(job, target)
        self.assertIn('台灣時間', response['text'])
        self.assertEqual(len(self.storage.assignment_action_records('student',work_plans)),1)
        self.assertEqual([entry['action']['label'] for entry in response['quickReply']['items']], ['開啟作業', '取消提醒'])
        self.assertNotIn('預留', response['text'])

    def test_selected_time_and_redelivery_create_one_plan(self):
        _, _, target, job, _ = self.line_job()
        start, response = self.select(job,target)
        self.assertIn('已設定',response['text'])
        self.assertIn('已安排過',self.select(job,target,start)[1]['text'])
        # Old duration buttons now keep only the reminder time and remain idempotent.
        for minutes in (15, 60):
            token = self.service.actions.action_data(job,'plan',start=start,minutes=minutes)
            self.assertIn('已安排過',self.service.actions.handle_line_postback(token,target)['text'])
        records = list(self.storage.assignment_action_records('student',work_plans).values())
        self.assertEqual(len(records),1)
        self.assertEqual(records[0]['start_ts'],start)
        self.assertNotIn('minutes',records[0])

    def test_signed_fields_owner_expiry_and_invalid_picker_are_rejected(self):
        _, _, target, job, _ = self.line_job()
        _, response = self.select(job,target)
        token = response['quickReply']['items'][1]['action']['data']
        invalid = [token.replace(':0:',':120:'), self.service.actions.action_data(job,'plan',expires=self.now-1,start=self.now+3600,minutes=15)]
        for data in invalid:
            with self.assertRaises(ValueError): self.service.actions.handle_line_postback(data,target)
        with self.assertRaises(ValueError): self.service.actions.handle_line_postback(token,'U'+'b'*32)
        schedule = self.service.actions.action_data(job,'schedule')
        for value in ['2026-10-09T12:00+08:00','2026-13-09T12:00','not-a-date',None,self.now]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.service.actions.handle_line_postback(schedule,target,{'datetime':value})
        self.storage.unlink_line(username='student')
        with self.assertRaises(ValueError): self.service.actions.handle_line_postback(token,target)

    def test_reminder_can_be_close_to_deadline_but_not_at_or_after_it(self):
        item, key, target, job, _ = self.line_job()
        start = ((self.now+3600)//60)*60
        self.storage.set_personal_deadline('student',key,start+60)
        _, response = self.select(job,target,start)
        self.assertIn('已設定',response['text'])
        self.assertIsNotNone(self.service.actions.line_picker(job,self.now+300))
        self.assertIsNone(self.service.actions.line_picker(job,self.now+30))
        for due in (start, start-60):
            self.storage.set_personal_deadline('student',key,due)
            with self.assertRaises(ValueError): self.select(job,target,start+60)
        with patch.object(self.service,'deliver') as send:
            self.service.dispatch(now=start+1)
            send.assert_not_called()

    def test_completion_channel_disabled_and_cancelled_plan_are_rechecked(self):
        item, _, target, job, _ = self.line_job()
        _, response = self.select(job,target)
        token=self.service.actions.action_data(job,'plan',start=((self.now+3600)//60)*60,minutes=15)
        key=next(iter(self.storage.assignment_action_records('student',work_plans)))
        self.storage.cancel_assignment_plan('student',key)
        self.assertIn('已取消',self.service.actions.handle_line_postback(token,target)['text'])
        self.storage.save_user_cache('student',{'ts':self.now,'result':self.result({**item,'completed':True})})
        with self.assertRaises(ValueError): self.service.actions.handle_line_postback(token,target)
        self.storage.save_user_cache('student',{'ts':self.now,'result':self.result(item)})
        self.storage.save_notification_preferences('student',validate_preferences({}))
        with self.assertRaises(ValueError): self.service.actions.handle_line_postback(token,target)

    def test_webhook_creates_reminder_on_selection_and_handles_duplicates(self):
        _, _, target, job, _ = self.line_job()
        selected=datetime.fromtimestamp(self.now+3600,TAIPEI_TZ).strftime('%Y-%m-%dT%H:%M')
        def send(data,params=None,redelivery=False):
            event={'type':'postback','source':{'type':'user','userId':target},'replyToken':'synthetic',
                   'postback':{'data':data,'params':params},'deliveryContext':{'isRedelivery':redelivery}}
            body=json.dumps({'events':[event]}).encode()
            signature=base64.b64encode(hmac.new(self.service.line_secret.encode(),body,hashlib.sha256).digest()).decode()
            return self.client.post('/api/notifications/line/webhook',data=body,content_type='application/json',headers={'X-Line-Signature':signature})
        with patch.object(self.service,'line_request') as reply:
            data=self.service.actions.action_data(job,'schedule')
            self.assertEqual(send(data,{'datetime':selected}).status_code,200)
            message=reply.call_args.args[1]['messages'][0]
            self.assertIn('已設定',message['text'])
            self.assertEqual(send(data,{'datetime':selected}).status_code,200)
            self.assertEqual(send(data,{'datetime':selected},redelivery=True).status_code,200)
            self.assertEqual(reply.call_count,2)
        self.assertEqual(len(self.storage.assignment_action_records('student',work_plans)),1)

    def test_line_can_cancel_only_its_own_confirmed_plan_and_replay_is_safe(self):
        _,_,target,job,_=self.line_job()
        _,response=self.select(job,target)
        cancel=next(entry['action']['data'] for entry in response['quickReply']['items'] if entry['action']['label']=='取消提醒')
        with self.assertRaises(ValueError): self.service.actions.handle_line_postback(cancel,'U'+'b'*32)
        for _ in range(2): self.assertIn('已取消',self.service.actions.handle_line_postback(cancel,target)['text'])
        plan=next(iter(self.storage.assignment_action_records('student',work_plans).values()))
        self.assertEqual(plan['state'],'cancelled')
        with self.storage._engine.connect() as conn:
            self.assertTrue(all(row.state=='cancelled' for row in conn.execute(select(jobs.c.state).where(jobs.c.event_key.like('plan:%')))))

    def test_legacy_duration_button_creates_time_only_and_legacy_cancel_still_works(self):
        _,key,target,job,_=self.line_job()
        start=((self.now+3600)//60)*60
        self.storage.set_personal_deadline('student',key,start+60)
        legacy=self.service.actions.action_data(job,'plan',start=start,minutes=120)
        response=self.service.actions.handle_line_postback(legacy,target)
        self.assertIn('已設定',response['text'])
        records=self.storage.assignment_action_records('student',work_plans)
        plan_id,plan=next(iter(records.items()))
        self.assertNotIn('minutes',plan)
        # Existing encrypted records still deliver even if their old duration crosses the deadline.
        with self.storage._engine.begin() as conn:
            uid=self.storage._notification_user(conn,'student')
            self.storage._write_action(conn,work_plans,uid,plan_id,{**plan,'minutes':120})
        with patch.object(self.service,'deliver') as send:
            self.service.dispatch(now=start+1)
            self.assertEqual(send.call_count,1)
        cancel=self.service.actions.action_data(job,'cancel',start=start,minutes=120)
        self.assertIn('已取消',self.service.actions.handle_line_postback(cancel,target)['text'])


if __name__ == '__main__': unittest.main()
