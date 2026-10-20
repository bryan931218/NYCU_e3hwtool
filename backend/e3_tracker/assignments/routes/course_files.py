"""Own-account file library. Browser input cannot select another account or fetch URL."""

import time

from flask import render_template, request, url_for

from e3_tracker.assignments.domain.course_files import file_index
from e3_tracker.assignments.services.collector import annotate_result_semesters


def register_course_file_routes(app, storage, current_user, login_required, load_cache, get_preferences, service, message_services, record_activity):
    def catalog(user):
        result = (load_cache(user['username']) or {}).get('result') or {}
        annotate_result_semesters(result, selected_keys=get_preferences(user['username']).get('semester_filter'))
        return result

    def scope(user, selected):
        result = catalog(user)
        if selected is None:
            selected = (result.get('selected_semesters') or [''])[0]
        if not isinstance(selected, str) or selected not in {semester['key'] for semester in result.get('available_semesters', [])}:
            return None, []
        courses = [course for course in result.get('courses', []) if course.get('semester_key') == selected]
        return (selected, courses) if courses and len(courses) <= 30 else (None, [])

    def messages(user, semester, courses):
        valid = {int(course['id']) for course in courses}
        return {kind: [item for item in source.storage.load_course_announcements(user['username'], semester)['items']
                       if item['course_id'] in valid] for kind, source in message_services.items()}

    def response(body, status=200):
        return body, status, {'Cache-Control': 'no-store'}

    @app.get('/courses/files')
    @login_required
    def course_files_page():
        user = current_user()
        result = catalog(user)
        selected, _ = scope(user, request.args.get('semester'))
        selected = selected or (result.get('selected_semesters') or [''])[0]
        return render_template('assignments/pages/course_files.html', user=user,
            semesters=result.get('available_semesters') or [], selected_semester=selected,
            files_config={'dataUrl': url_for('course_files_data'), 'refreshUrl': url_for('course_files_refresh'),
                          'guest': bool(user.get('is_guest'))})

    @app.get('/api/course-files')
    @login_required
    def course_files_data():
        user = current_user()
        if user.get('is_guest'):
            return response({'ok': False, 'error': '訪客模式無法讀取 E3 附件。'}, 403)
        semester, courses = scope(user, request.args.get('semester'))
        if not semester:
            return response({'ok': False, 'error': '請先更新作業以取得該學期課程。'}, 400)
        cache = service.storage.load(user['username'], semester)
        running = cache['status'] == 'running' and time.time() - cache['attempt'] < 90
        items = file_index(cache['sources'], courses, messages(user, semester, courses), service.base_url, limit=3001)
        return response({'ok': True, 'semester': semester,
            'courses': [{'id': course['id'], 'title': course['title']} for course in courses],
            'items': items[:3000], 'limited': len(items) > 3000,
            'fetched_at': cache['fetched_at'], 'running': running, 'remaining': len(cache['pending']),
            'initialized': bool(cache['attempt']),
            'error': cache['error'] or ('整理逾時，請重新更新。' if cache['status'] == 'running' and not running else '')})

    @app.post('/api/course-files/refresh')
    @login_required
    def course_files_refresh():
        user = current_user()
        if user.get('is_guest'):
            return response({'ok': False, 'error': '訪客模式無法更新 E3 附件。'}, 403)
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict) or not isinstance(payload.get('semester'), str):
            return response({'ok': False, 'error': '請選擇學期。'}, 400)
        semester, courses = scope(user, payload['semester'])
        if not semester:
            return response({'ok': False, 'error': '找不到該學期課程，請先更新作業。'}, 400)
        if not storage.consume_security_limit(f"course-files:{user['username']}", 12, 600):
            return response({'ok': False, 'error': '更新過於頻繁，請稍後再試。'}, 429)
        status = service.start(app, user, semester, courses, messages(user, semester, courses))
        if status in {'busy', 'session_expired'}:
            return response({'ok': False, 'error': '整理人數較多，請稍後再試。' if status == 'busy' else 'E3 登入已失效，請重新登入。'}, 429 if status == 'busy' else 409)
        if status == 'running':
            try:
                record_activity('course_files_refresh', status='success', metadata={'username': user['username'], 'site': 'assignments'})
            except Exception:
                app.logger.warning('Course attachment activity unavailable')
        return response({'ok': True, 'status': status}, 202 if status == 'running' else 200)
