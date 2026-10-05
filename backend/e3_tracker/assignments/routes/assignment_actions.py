"""Authenticated, CSRF-protected actions on the caller's own assignments."""

import re
import secrets
import time
from flask import render_template, request
from e3_tracker.assignments.persistence.assignment_actions import personal_deadlines, work_plans
from e3_tracker.assignments.domain.notifications import digest
from e3_tracker.assignments.domain.assignment_actions import assignment_hash


def register_assignment_action_routes(app, storage, account_only, actions):
    @app.get('/assignments/plan')
    @account_only
    def assignment_plan_page(username):
        try:
            key = request.args.get('uid', '')
            item = actions.item(username, key)
        except ValueError as error:
            return render_template('assignments/pages/assignment_plan.html', item=None, error=str(error), plan_config={}), 404
        return render_template('assignments/pages/assignment_plan.html', item=item, error='', plan_config={
            'uid': key, 'request_id': secrets.token_hex(16), 'google_linked': bool(storage.load_google_tokens(username))})

    @app.route('/api/assignments/plans', methods=['GET', 'POST', 'DELETE'])
    @account_only
    def assignment_plans_api(username):
        try:
            if request.method == 'GET':
                records = storage.assignment_action_records(username, work_plans)
                return {'ok': True, 'items': [{**item, 'id': key} for key, item in records.items()
                    if item['state'] == 'pending' and item['start_ts'] > time.time()]}
            raw = request.get_json(silent=True)
            if not isinstance(raw, dict):
                raise ValueError('安排資料無效。')
            if request.method == 'DELETE':
                if not isinstance(raw.get('id'), str) or not re.fullmatch('[0-9a-f]{64}', raw['id']):
                    raise ValueError('安排資料無效。')
                return {'ok': storage.cancel_assignment_plan(username, raw['id'])}
            if not re.fullmatch('[0-9a-f]{32}', str(raw.get('request_id', ''))) or type(raw.get('google', False)) is not bool:
                raise ValueError('安排資料無效。')
            if not storage.consume_security_limit(f'assignment-plan:{username}', 30, 600):
                return {'ok': False, 'error': '操作過於頻繁，請稍後再試。'}, 429
            return actions.schedule(username, raw.get('uid'), raw.get('start_ts'),
                request_id=raw['request_id'], google=raw.get('google', False))
        except ValueError as error:
            return {'ok': False, 'error': str(error)}, 400

    @app.post('/api/assignments/deadline-proposals')
    @account_only
    def deadline_proposal_api(username):
        raw = request.get_json(silent=True)
        try:
            if (not isinstance(raw, dict) or not re.fullmatch('[0-9a-f]{64}', str(raw.get('id', '')))
                    or type(raw.get('google', False)) is not bool):
                raise ValueError('異動資料無效。')
            if raw.get('dismiss') is True:
                return {'ok': storage.dismiss_deadline_proposal(username, raw['id'])}
            if raw.get('retry_calendar') is True:
                record = storage.assignment_action_records(username, personal_deadlines).get(raw.get('uid'))
                if not record or record.get('proposal_id') != raw['id']:
                    raise ValueError('請先確認個人期限。')
                item = actions.items(username).get(raw.get('uid'))
                if not item:
                    raise ValueError('作業不存在。')
                error = actions.sync_calendar(username, item)
                return {'ok': not bool(error), 'error': error}
            return actions.confirm(username, raw['id'], raw.get('uid'), raw.get('due_ts'), raw.get('original_due_ts'), google=raw.get('google', False))
        except ValueError as error:
            return {'ok': False, 'error': str(error)}, 400

    @app.route('/api/assignments/personal-deadline', methods=['POST', 'DELETE'])
    @account_only
    def personal_deadline_api(username):
        raw = request.get_json(silent=True)
        try:
            if not isinstance(raw, dict) or not isinstance(raw.get('uid'), str) or len(raw['uid']) > 4096:
                raise ValueError('作業資料無效。')
            key = digest(raw['uid'])
            if not any(assignment_hash(item, storage.assignment_uid) == key for item in actions.result(username).get('all_assignments', [])):
                raise ValueError('找不到可修改的作業。')
            if request.method == 'POST':
                due = raw.get('due_ts')
                if type(due) is not int or not 946684800 <= due <= 4102444800:
                    raise ValueError('請選擇有效的期限。')
            storage.set_personal_deadline(username, key, raw.get('due_ts') if request.method == 'POST' else None)
            actions.notifications.observe(username, actions.result(username))
            return {'ok': True}
        except ValueError as error:
            return {'ok': False, 'error': str(error)}, 400
