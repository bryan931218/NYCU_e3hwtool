"""Own-account file library. Browser input cannot select another account or fetch URL."""

import re

from flask import render_template, request, url_for

def register_course_file_routes(app, storage, current_user, login_required, load_cache, service, record_activity):
    def catalog(user):
        return service.catalog(user['username'], (load_cache(user['username']) or {}).get('result') or {})

    def response(body, status=200):
        return body, status, {'Cache-Control': 'no-store'}

    @app.get('/courses/files')
    @login_required
    def course_files_page():
        user = current_user()
        result = catalog(user)
        selected = request.args.get('semester')
        if selected not in {semester['key'] for semester in result.get('available_semesters', [])}:
            selected = (result.get('selected_semesters') or [''])[0]
        course = request.args.get('course', '')
        if not re.fullmatch(r'[1-9][0-9]{0,9}', course):
            course = ''
        return render_template('assignments/pages/course_files.html', user=user,
            semesters=result.get('available_semesters') or [], selected_semester=selected,
            files_config={'dataUrl': url_for('course_files_data'), 'refreshUrl': url_for('course_files_refresh'),
                          'course': course, 'query': request.args.get('q', '')[:120], 'guest': bool(user.get('is_guest'))})

    @app.get('/api/course-files')
    @login_required
    def course_files_data():
        user = current_user()
        if user.get('is_guest'):
            return response({'ok': False, 'error': '訪客模式無法讀取 E3 附件。'}, 403)
        try:
            return response(service.library(user['username'], request.args.get('semester'), result=catalog(user)))
        except ValueError as error:
            return response({'ok': False, 'error': str(error)}, 400)

    @app.post('/api/course-files/refresh')
    @login_required
    def course_files_refresh():
        user = current_user()
        if user.get('is_guest'):
            return response({'ok': False, 'error': '訪客模式無法更新 E3 附件。'}, 403)
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict) or not isinstance(payload.get('semester'), str):
            return response({'ok': False, 'error': '請選擇學期。'}, 400)
        try:
            semester, courses, _ = service.scope(user['username'], payload['semester'], result=catalog(user))
        except ValueError:
            return response({'ok': False, 'error': '找不到該學期課程，請先更新作業。'}, 400)
        if not storage.consume_security_limit(f"course-files:{user['username']}", 12, 600):
            return response({'ok': False, 'error': '更新過於頻繁，請稍後再試。'}, 429)
        status = service.start(app, user, semester, courses, service.messages(user['username'], semester, courses))
        if status in {'busy', 'session_expired'}:
            return response({'ok': False, 'error': '整理人數較多，請稍後再試。' if status == 'busy' else 'E3 登入已失效，請重新登入。'}, 429 if status == 'busy' else 409)
        if status == 'running':
            try:
                record_activity('course_files_refresh', status='success', metadata={'username': user['username'], 'site': 'assignments'})
            except Exception:
                app.logger.warning('Course attachment activity unavailable')
        return response({'ok': True, 'status': status}, 202 if status == 'running' else 200)
