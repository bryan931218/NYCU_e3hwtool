"""Private course announcements; never accept an account or remote URL from the browser."""

import time
import re

from flask import render_template, request, url_for

from e3_tracker.assignments.services.collector import annotate_result_semesters, normalize_semester_selection
from e3_tracker.assignments.services.course_announcements import AnnouncementSessionExpired


def register_course_message_entry(app, login_required, current_user, load_cache, get_preferences, sources):
    @app.get('/courses/messages')
    @login_required
    def course_messages_page():
        endpoint = 'course_mail_page' if request.args.get('tab') == 'mail' else 'course_announcements_page'
        return app.view_functions[endpoint]()

    @app.get('/api/course-messages/unread')
    @login_required
    def course_messages_unread():
        user = current_user()
        if user.get('is_guest'):
            return {'ok': False, 'error': '訪客模式無法讀取課程訊息。'}, 403
        result = (load_cache(user['username']) or {}).get('result') or {}
        annotate_result_semesters(result, selected_keys=get_preferences(user['username']).get('semester_filter'))
        semester = request.args.get('semester') or (result.get('selected_semesters') or [''])[0]
        courses = {int(course['id']) for course in result.get('courses', []) if course.get('semester_key') == semester}
        counts = {kind: sum(not item.get('read_at') for item in source.storage.load_course_announcements(user['username'], semester)['items']
                           if item['course_id'] in courses) if courses else 0 for kind, source in sources.items()}
        return {'ok': True, 'semester': semester, **counts, 'total': sum(counts.values())}, 200, {'Cache-Control': 'no-store'}


def register_course_announcement_routes(app, storage, current_user, login_required, load_cache, get_preferences, service, record_activity, *, kind='announcements'):
    is_mail = kind == 'mail'
    noun = '信件' if is_mail else '公告'
    prefix = 'course_mail' if is_mail else 'course_announcements'
    item_endpoint = 'course_mail_item' if is_mail else 'course_announcement_item'
    api_path = '/api/course-mail' if is_mail else '/api/course-announcements'
    cache_storage = service.storage
    def catalog(user):
        result = (load_cache(user['username']) or {}).get('result') or {}
        annotate_result_semesters(result, selected_keys=get_preferences(user['username']).get('semester_filter'))
        return result

    def selected_courses(user, requested):
        if requested is not None and (not isinstance(requested, str) or len(requested) > 16):
            return None, []
        result = catalog(user)
        semesters = normalize_semester_selection([requested]) if requested else result.get('selected_semesters') or []
        semester = semesters[0] if semesters else ''
        courses = [course for course in result.get('courses', []) if course.get('semester_key') == semester]
        if not semester or not courses or len(courses) > 30:
            return None, []
        return semester, courses

    @app.get('/courses/mail' if is_mail else '/courses/announcements', endpoint=f'{prefix}_page')
    @login_required
    def course_announcements_page():
        user = current_user()
        result = catalog(user)
        semesters = result.get('available_semesters') or []
        selected = request.args.get('semester')
        if selected not in {semester['key'] for semester in semesters}:
            selected = (result.get('selected_semesters') or [''])[0]
        item_key = request.args.get('item', '')
        if not re.fullmatch(r'[1-9][0-9]{0,9}:[1-9][0-9]{0,9}', item_key):
            item_key = ''
        return render_template('assignments/pages/course_announcements.html', user=user, message_noun=noun, message_kind=kind,
            semesters=semesters, selected_semester=selected,
            announcement_config={'dataUrl': url_for(f'{prefix}_data'), 'unreadUrl': url_for('course_messages_unread'), 'refreshUrl': url_for(f'{prefix}_refresh'),
                'itemUrl': url_for(item_endpoint), 'kind': kind, 'noun': noun, 'itemKey': item_key, 'autoOpen': not is_mail, 'guest': bool(user.get('is_guest'))})

    @app.get(api_path, endpoint=f'{prefix}_data')
    @login_required
    def course_announcements_data():
        user = current_user()
        if user.get('is_guest'):
            return {'ok': False, 'error': f'訪客模式無法連線至 E3 {noun}。'}, 403
        semester, courses = selected_courses(user, request.args.get('semester'))
        if not semester:
            return {'ok': False, 'error': '請先更新作業以取得課程。'}, 400
        cache = cache_storage.load_course_announcements(user['username'], semester)
        valid_ids = {int(course['id']) for course in courses}
        items = [item for item in cache['items'] if item['course_id'] in valid_ids]
        running = cache['status'] == 'running' and time.time() - cache['attempt'] < 90
        return {'ok': True, 'semester': semester, 'courses': [{'id': course['id'], 'title': course['title']} for course in courses],
            'items': items, 'fetched_at': cache['fetched_at'], 'running': running,
            'error': cache['error'] or ('更新逾時，請重試。' if cache['status'] == 'running' and not running else ''),
            'stale': time.time() - cache['fetched_at'] > 600}

    @app.post(api_path + '/refresh', endpoint=f'{prefix}_refresh')
    @login_required
    def course_announcements_refresh():
        user = current_user()
        if user.get('is_guest'):
            return {'ok': False, 'error': f'訪客模式無法連線至 E3 {noun}。'}, 403
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return {'ok': False, 'error': '請選擇學期。'}, 400
        semester, courses = selected_courses(user, payload.get('semester'))
        if not semester:
            return {'ok': False, 'error': '找不到該學期課程，請先更新作業。'}, 400
        if not storage.consume_security_limit(f"{prefix}-refresh:{user['username']}", 12, 600):
            return {'ok': False, 'error': '更新過於頻繁，請稍後再試。'}, 429
        result = service.start(app, user, semester, courses)
        if result in {'busy', 'session_expired'}:
            return {'ok': False, 'error': '更新人數較多，請稍後再試。' if result == 'busy' else '請重新登入 E3。'}, 429 if result == 'busy' else 409
        if result == 'running':
            try:
                record_activity(f'{prefix}_refresh', status='success', metadata={'username': user['username'], 'site': 'assignments'})
            except Exception:
                app.logger.warning('Course %s activity unavailable', kind)
        return {'ok': True, 'status': result}, 202 if result == 'running' else 200

    @app.post(api_path + '/item', endpoint=item_endpoint)
    @login_required
    def course_announcement_item():
        user = current_user()
        if user.get('is_guest'):
            return {'ok': False, 'error': f'訪客模式無法讀取課程{noun}。'}, 403
        payload = request.get_json(silent=True)
        if (not isinstance(payload, dict) or not isinstance(payload.get('key'), str)
            or not re.fullmatch(r'[1-9][0-9]{0,9}:[1-9][0-9]{0,9}', payload['key'])
            or type(payload.get('read')) is not bool or type(payload.get('load_content', True)) is not bool):
            return {'ok': False, 'error': f'{noun}資料無效。'}, 400
        semester, courses = selected_courses(user, payload.get('semester'))
        if not semester:
            return {'ok': False, 'error': f'{noun}不存在。'}, 404
        valid_ids = {int(course['id']) for course in courses}
        item = next((item for item in cache_storage.load_course_announcements(user['username'], semester)['items']
                     if item['key'] == payload['key'] and item['course_id'] in valid_ids), None)
        if not item:
            return {'ok': False, 'error': f'{noun}不存在。'}, 404
        content = None
        if payload['read'] and payload.get('load_content', True) and 'content' not in item:
            if not storage.consume_security_limit(f"{prefix}-read:{user['username']}", 60, 600):
                return {'ok': False, 'error': '讀取過於頻繁，請稍後再試。'}, 429
            try:
                content = service.content(user, item)
            except AnnouncementSessionExpired:
                return {'ok': False, 'error': 'E3 登入已失效，請重新登入。'}, 409
            except BlockingIOError:
                return {'ok': False, 'error': f'{noun}讀取中，請稍後再試。'}, 429
            except Exception as error:
                app.logger.warning('Course %s content unavailable (%s)', kind, type(error).__name__)
                return {'ok': False, 'error': '內文暫時無法讀取，請重新讀取或前往 E3。'}, 502
        updated = cache_storage.update_course_announcement(user['username'], semester, item['key'], content=content, read=payload['read'],
            expected_version=(item.get('title'), item.get('updated_ts')) if content is not None else None)
        if not updated:
            return {'ok': False, 'error': f'{noun}已更新，請重新整理列表。'}, 409
        return {'ok': True, 'item': updated}
