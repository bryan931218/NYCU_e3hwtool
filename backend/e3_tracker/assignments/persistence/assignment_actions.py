"""Encrypted account-owned deadline confirmations and durable work plans."""

import json
import time
from sqlalchemy import Column, Integer, String, Table, Text, delete, insert, select, update
from e3_tracker.platform.persistence.metadata import metadata
from e3_tracker.platform.persistence.core_schema import users_table
from e3_tracker.assignments.domain.notifications import digest, notification_payload, DEFAULT_NOTIFICATION_PREFERENCES
from .notification_schema import notification_jobs as jobs, notification_settings as settings, line_bindings, push_subscriptions


def action_table(name):
    return Table(name, metadata, Column('user_id', Integer, primary_key=True),
                 Column('id', String(64), primary_key=True), Column('payload', Text, nullable=False),
                 Column('state', String(16), nullable=False, default='pending'))


personal_deadlines = action_table('assignment_personal_deadlines')
deadline_proposals = action_table('assignment_deadline_proposals')
work_plans = action_table('assignment_work_plans')
ACTION_TABLES = (personal_deadlines, deadline_proposals, work_plans)


class AssignmentActionStorage:
    def _action_payload(self, table, uid, key, raw):
        return json.loads(self._credential_cipher.decrypt(raw, f'{table.name}:{uid}:{key}'))

    def assignment_action_records(self, username, table):
        with self._lock, self._engine.connect() as conn:
            uid = self._notification_user(conn, username)
            rows = conn.execute(select(table).where(table.c.user_id == uid)).mappings().all()
        return {row['id']: {**self._action_payload(table, uid, row['id'], row['payload']), 'state': row['state']} for row in rows}

    def personal_deadline_overrides(self, username):
        return self.assignment_action_records(username, personal_deadlines)

    def set_personal_deadline(self, username, key, due):
        with self._lock, self._engine.begin() as conn:
            uid = self._announcement_user(conn, username, write=True)
            if not uid:
                raise ValueError('請登入 E3 帳號。')
            if due is None:
                conn.execute(delete(personal_deadlines).where(personal_deadlines.c.user_id == uid, personal_deadlines.c.id == key))
            else:
                self._write_action(conn, personal_deadlines, uid, key, {'due_ts': due}, 'applied')
            self._cancel_assignment_jobs(conn, uid, key)

    def _write_action(self, conn, table, uid, key, payload, state='pending'):
        values = {'payload': self._credential_cipher.encrypt(json.dumps(payload, ensure_ascii=False), f'{table.name}:{uid}:{key}'), 'state': state}
        if not conn.execute(update(table).where(table.c.user_id == uid, table.c.id == key).values(**values)).rowcount:
            conn.execute(insert(table).values(user_id=uid, id=key, **values))

    def save_deadline_proposal(self, username, key, payload, *, notify=False):
        with self._lock, self._engine.begin() as conn:
            uid = self._announcement_user(conn, username, write=True)
            if not uid:
                return
            if conn.execute(select(deadline_proposals.c.id).where(deadline_proposals.c.user_id == uid, deadline_proposals.c.id == key)).first():
                return
            self._write_action(conn, deadline_proposals, uid, key, payload)
            if notify:
                prefs, targets = self._action_targets(conn, uid)
                if prefs.get('deadline_changes'):
                    self._queue_notification(conn, uid, f'deadline_change:{key}', targets, {
                        'kind': 'deadline_change', 'proposal_id': key, 'title': 'E3｜可能有期限異動',
                        'body': f"課程：{payload['course_title'][:100]}\n{payload['source_title'][:160]}\n請確認後再更新個人期限。", 'url': payload['source_path'],
                    }, time.time(), time.time()+86400)

    def confirm_personal_deadline(self, username, key, item_hash, due_ts, *, expected_source_version, original_due):
        with self._lock, self._engine.begin() as conn:
            uid = self._announcement_user(conn, username, write=True)
            row = conn.execute(select(deadline_proposals).where(deadline_proposals.c.user_id == uid, deadline_proposals.c.id == key)).mappings().first()
            if not row or row['state'] != 'pending':
                raise ValueError('這項異動已處理，請重新讀取。')
            proposal = self._action_payload(deadline_proposals, uid, key, row['payload'])
            if proposal['source_version'] != expected_source_version:
                raise ValueError('來源訊息已更新，請重新讀取。')
            from .course_mail import CourseMailStorage
            from e3_tracker.assignments.domain.assignment_actions import source_version
            source = CourseMailStorage(self) if proposal['kind'] == 'mail' else self
            table = source.message_cache_table
            raw = conn.execute(select(table.c.payload).where(table.c.user_id == uid, table.c.semester_key == proposal['semester'])).scalar()
            messages = source._decode_message_payload(raw, username, proposal['semester']) if raw else []
            if not any(item['key'] == proposal['message_key'] and source_version(item) == expected_source_version for item in messages):
                raise ValueError('來源訊息已更新，請重新讀取。')
            old = conn.execute(select(personal_deadlines.c.payload).where(personal_deadlines.c.user_id == uid, personal_deadlines.c.id == item_hash)).scalar()
            if old and self._action_payload(personal_deadlines, uid, item_hash, old)['due_ts'] != original_due:
                raise ValueError('個人期限已變更，請重新讀取。')
            self._write_action(conn, personal_deadlines, uid, item_hash, {'due_ts': due_ts, 'proposal_id': key}, 'applied')
            conn.execute(update(deadline_proposals).where(deadline_proposals.c.user_id == uid, deadline_proposals.c.id == key).values(state='applied'))
            # Automatic reminders are tied to a deadline; explicit work times stay fixed.
            self._cancel_assignment_jobs(conn, uid, item_hash)

    def _cancel_assignment_jobs(self, conn, uid, item_hash):
        rows = conn.execute(select(jobs).where(jobs.c.user_id == uid, jobs.c.state == 'pending')).mappings().all()
        for row in rows:
            raw = row['payload']
            if raw.startswith(self._credential_cipher.PREFIX):
                raw = self._credential_cipher.decrypt(raw, f"notification:{row['id']}")
            payload = json.loads(raw)
            if payload.get('uid_hash') == item_hash and payload.get('kind') == 'due':
                conn.execute(update(jobs).where(jobs.c.id == row['id']).values(state='cancelled'))

    def dismiss_deadline_proposal(self, username, key):
        with self._lock, self._engine.begin() as conn:
            uid = self._notification_user(conn, username)
            return bool(conn.execute(update(deadline_proposals).where(deadline_proposals.c.user_id == uid, deadline_proposals.c.id == key,
                deadline_proposals.c.state == 'pending').values(state='dismissed')).rowcount)

    def _action_targets(self, conn, uid):
        raw = conn.execute(select(settings.c.preferences).where(settings.c.user_id == uid)).scalar()
        prefs = {**DEFAULT_NOTIFICATION_PREFERENCES, **json.loads(raw or '{}')}
        targets = []
        if prefs['line_enabled']:
            targets.extend(('line', key) for key in conn.execute(select(line_bindings.c.target_hash).where(line_bindings.c.user_id == uid)).scalars())
        if prefs['browser_enabled']:
            targets.extend(('browser', key) for key in conn.execute(select(push_subscriptions.c.endpoint_hash).where(push_subscriptions.c.user_id == uid)).scalars())
        return prefs, targets

    def schedule_assignment_plan(self, username, key, item, item_hash, start, *, line_job=None):
        with self._lock, self._engine.begin() as conn:
            uid = self._announcement_user(conn, username, write=True)
            if not uid:
                raise ValueError('請登入 E3 帳號。')
            if line_job:
                owner = conn.execute(select(jobs.c.id).join(line_bindings, line_bindings.c.user_id == jobs.c.user_id).where(
                    jobs.c.id == line_job['id'], jobs.c.user_id == uid, line_bindings.c.target_hash == line_job['target_hash'],
                    jobs.c.channel == 'line', jobs.c.state.in_(['sending', 'sent']))).scalar()
                if not owner:
                    raise ValueError('此通知已失效。')
            old = conn.execute(select(work_plans).where(work_plans.c.user_id == uid, work_plans.c.id == key)).mappings().first()
            if old:
                if old['state'] == 'cancelled':
                    raise ValueError('這項提醒已取消，請重新安排。')
                return {**self._action_payload(work_plans, uid, key, old['payload']), 'duplicate': True}
            prefs, targets = self._action_targets(conn, uid)
            if not targets:
                raise ValueError('請先在通知設定啟用 LINE 或瀏覽器通知。')
            plan = {'uid_hash': item_hash, 'title': item['title'], 'course_title': item.get('course_title', ''),
                    'start_ts': start, 'due_ts': item.get('due_ts'), 'created_at': time.time()}
            self._write_action(conn, work_plans, uid, key, plan)
            payload = {**notification_payload(item, 'due'), 'title': 'E3｜你安排的作業提醒', 'kind': 'scheduled',
                       'uid_hash': item_hash, 'due_ts': item.get('due_ts'), 'plan_id': key, 'url': f'/assignments/plan?uid={item_hash}'}
            self._queue_notification(conn, uid, f'plan:{key}', targets, payload, start, min(start+3600, item.get('due_ts') or start+3600))
            return plan

    def mark_plan_calendar_synced(self, username, key):
        with self._lock, self._engine.begin() as conn:
            uid = self._announcement_user(conn, username, write=True)
            row = conn.execute(select(work_plans).where(work_plans.c.user_id == uid, work_plans.c.id == key)).mappings().first()
            if row:
                payload = self._action_payload(work_plans, uid, key, row['payload'])
                self._write_action(conn, work_plans, uid, key, {**payload, 'google_synced': True}, row['state'])

    def cancel_assignment_plan(self, username, key):
        with self._lock, self._engine.begin() as conn:
            uid = self._notification_user(conn, username)
            changed = conn.execute(update(work_plans).where(work_plans.c.user_id == uid, work_plans.c.id == key).values(state='cancelled')).rowcount
            conn.execute(update(jobs).where(jobs.c.user_id == uid, jobs.c.event_key == f'plan:{key}', jobs.c.state == 'pending').values(state='cancelled'))
            return bool(changed)

    def line_action_job(self, identifier, target_hash):
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(select(jobs).join(line_bindings, line_bindings.c.user_id == jobs.c.user_id).where(
                jobs.c.id == identifier, jobs.c.channel == 'line', jobs.c.target_hash == target_hash,
                line_bindings.c.target_hash == target_hash, jobs.c.state.in_(['sent', 'sending']))).mappings().first()
            username = conn.execute(select(users_table.c.username).where(users_table.c.id == row['user_id'])).scalar() if row else None
        return (username, dict(row)) if row and username else None
