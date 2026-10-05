"""LINE picker -> duration -> reminder, without trusting clients or duplicating writes."""
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

    def test_picker_bounds_direct_link_and_no_write_before_duration(self):
        item, _, target, job, pushed = self.line_job()
        self.assertIn(item['url'], pushed['text'])
        picker = next(entry['action'] for entry in pushed['quickReply']['items'] if entry['action']['type']=='datetimepicker')
        self.assertEqual(picker['mode'], 'datetime')
        self.assertLessEqual(picker['min'], picker['initial'])
        self.assertLessEqual(picker['initial'], picker['max'])
        self.assertLess(len(picker['data']),300)
        _, response = self.select(job, target)
        self.assertIn('台灣時間', response['text'])
        self.assertEqual(self.storage.assignment_action_records('student',work_plans),{})
        self.assertEqual(len(response['quickReply']['items']),6)

    def test_selected_duration_and_redelivery_create_one_plan(self):
        _, _, target, job, _ = self.line_job()
        start, response = self.select(job,target)
        token = response['quickReply']['items'][3]['action']['data']
        result = self.service.actions.handle_line_postback(token,target)
        self.assertIn('預留 60 分鐘',result['text'])
        self.assertIn('已安排過',self.service.actions.handle_line_postback(token,target)['text'])
        # Choosing a different duration for the same picker does not create another plan.
        alternate = response['quickReply']['items'][0]['action']['data']
        self.assertIn('預留 60 分鐘',self.service.actions.handle_line_postback(alternate,target)['text'])
        records = list(self.storage.assignment_action_records('student',work_plans).values())
        self.assertEqual(len(records),1)
        self.assertEqual((records[0]['start_ts'],records[0]['minutes']),(start,60))

    def test_signed_fields_owner_expiry_and_invalid_picker_are_rejected(self):
        _, _, target, job, _ = self.line_job()
        _, response = self.select(job,target)
        token = response['quickReply']['items'][0]['action']['data']
        invalid = [token.replace(':15:',':120:'), self.service.actions.action_data(job,'plan',expires=self.now-1,start=self.now+3600,minutes=15)]
        for data in invalid:
            with self.assertRaises(ValueError): self.service.actions.handle_line_postback(data,target)
        with self.assertRaises(ValueError): self.service.actions.handle_line_postback(token,'U'+'b'*32)
        schedule = self.service.actions.action_data(job,'schedule')
        for value in ['2026-10-09T12:00+08:00','2026-13-09T12:00','not-a-date',None,self.now]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.service.actions.handle_line_postback(schedule,target,{'datetime':value})
        self.storage.unlink_line(username='student')
        with self.assertRaises(ValueError): self.service.actions.handle_line_postback(token,target)

    def test_short_deadline_limits_durations_and_rechecks_changed_deadline(self):
        item, key, target, job, _ = self.line_job()
        start = ((self.now+3600)//60)*60
        self.storage.set_personal_deadline('student',key,start+30*60)
        _, response = self.select(job,target,start)
        self.assertEqual([i['action']['label'] for i in response['quickReply']['items']],['15 分鐘','30 分鐘'])
        self.storage.set_personal_deadline('student',key,start+5*60)
        with self.assertRaises(ValueError): self.service.actions.handle_line_postback(response['quickReply']['items'][0]['action']['data'],target)
        self.assertEqual(self.storage.assignment_action_records('student',work_plans),{})
        self.assertIsNone(self.service.actions.line_picker(job,self.now+300))

    def test_completion_channel_disabled_and_cancelled_plan_are_rechecked(self):
        item, _, target, job, _ = self.line_job()
        _, response = self.select(job,target)
        token=response['quickReply']['items'][0]['action']['data']
        self.service.actions.handle_line_postback(token,target)
        key=next(iter(self.storage.assignment_action_records('student',work_plans)))
        self.storage.cancel_assignment_plan('student',key)
        self.assertIn('已取消',self.service.actions.handle_line_postback(token,target)['text'])
        self.storage.save_user_cache('student',{'ts':self.now,'result':self.result({**item,'completed':True})})
        with self.assertRaises(ValueError): self.service.actions.handle_line_postback(token,target)
        self.storage.save_user_cache('student',{'ts':self.now,'result':self.result(item)})
        self.storage.save_notification_preferences('student',validate_preferences({}))
        with self.assertRaises(ValueError): self.service.actions.handle_line_postback(token,target)

    def test_webhook_returns_picker_choices_and_handles_duplicates(self):
        _, _, target, job, _ = self.line_job()
        selected=datetime.fromtimestamp(self.now+3600,TAIPEI_TZ).strftime('%Y-%m-%dT%H:%M')
        def send(data,params=None,redelivery=False):
            event={'type':'postback','source':{'type':'user','userId':target},'replyToken':'synthetic',
                   'postback':{'data':data,'params':params},'deliveryContext':{'isRedelivery':redelivery}}
            body=json.dumps({'events':[event]}).encode()
            signature=base64.b64encode(hmac.new(self.service.line_secret.encode(),body,hashlib.sha256).digest()).decode()
            return self.client.post('/api/notifications/line/webhook',data=body,content_type='application/json',headers={'X-Line-Signature':signature})
        with patch.object(self.service,'line_request') as reply:
            self.assertEqual(send(self.service.actions.action_data(job,'schedule'),{'datetime':selected}).status_code,200)
            message=reply.call_args.args[1]['messages'][0]
            data=message['quickReply']['items'][0]['action']['data']
            self.assertEqual(send(data).status_code,200)
            self.assertEqual(send(data,redelivery=True).status_code,200)
            self.assertEqual(reply.call_count,2)
        self.assertEqual(len(self.storage.assignment_action_records('student',work_plans)),1)

    def test_line_can_cancel_only_its_own_confirmed_plan_and_replay_is_safe(self):
        _,_,target,job,_=self.line_job()
        _,choice=self.select(job,target)
        response=self.service.actions.handle_line_postback(choice['quickReply']['items'][0]['action']['data'],target)
        cancel=next(entry['action']['data'] for entry in response['quickReply']['items'] if entry['action']['label']=='取消提醒')
        with self.assertRaises(ValueError): self.service.actions.handle_line_postback(cancel,'U'+'b'*32)
        for _ in range(2): self.assertIn('已取消',self.service.actions.handle_line_postback(cancel,target)['text'])
        plan=next(iter(self.storage.assignment_action_records('student',work_plans).values()))
        self.assertEqual(plan['state'],'cancelled')
        with self.storage._engine.connect() as conn:
            self.assertTrue(all(row.state=='cancelled' for row in conn.execute(select(jobs.c.state).where(jobs.c.event_key.like('plan:%')))))


if __name__ == '__main__': unittest.main()
