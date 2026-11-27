import hashlib
import re
import time

from flask import abort, redirect, render_template, request, url_for

from e3_tracker.assignments.domain.course_files import course_display_name
from e3_tracker.assignments.services.course_insights import CourseAIError


ERRORS = {
    'key': 'Gemini 拒絕這個金鑰。請確認專案已開放 Gemini API 且能使用 Flash-Lite。',
    'quota': 'Gemini 用量已達上限，稍後會再試。',
    'session_expired': '請重新登入 E3 以繼續整理課程。',
    'source': '部分課程來源暫時讀不到，已保留先前的整理結果。',
    'provider': 'Gemini 暫時無法使用，請稍後再試。',
    'output': 'Gemini 回傳格式有誤，請稍後再試。',
    'unconfigured': '請先設定自己的 Gemini API 金鑰。',
}


def overview_version(courses, snapshots):
    fields = ('analyzed', 'partial', 'document')
    state = [(course['id'], course['title'], course.get('semester_key'),
              tuple(snapshots.get(course['id'], {}).get(field) for field in fields)) for course in courses]
    return hashlib.sha256(repr(state).encode()).hexdigest()[:20]


def register_course_insight_routes(app, current_user, login_required, service):
    def username():
        user = current_user()
        if not user or user.get('is_guest'):
            abort(403)
        return user['username']

    def scope(name, semester):
        try:
            return service.files.scope(name, semester or None)
        except ValueError:
            return '', [], []

    def response(data, status=200):
        return data, status, {'Cache-Control': 'no-store'}

    def overview_view(name, semester, courses, semesters, settings, *, error=''):
        course = request.values.get('course', '')
        selected = next((row for row in courses if str(row['id']) == course), None)
        snapshots = service.storage.snapshots(name, semester, [row['id'] for row in courses])
        return render_template('assignments/pages/course_overviews.html', semester=semester,
            semesters=semesters, courses=courses, selected_course=course if selected else '',
            selected_name=course_display_name(selected['title']) if selected else '全部課程', configured=bool(settings),
            snapshots=snapshots, course_name=course_display_name,
            overview_version=overview_version(courses, snapshots),
            ready=sum(bool(row.get('analyzed')) for row in snapshots.values()),
            setup_requested=request.args.get('setup') == '1' or bool(error),
            setup_error=error, error=ERRORS.get((settings or {}).get('error'), ''),
            error_code=(settings or {}).get('error'), initial_query=request.args.get('q', '')[:300])

    @app.route('/settings/course-ai', methods=['GET', 'POST'])
    @login_required
    def course_ai_settings():
        name = username()
        if request.method == 'POST':
            semester, courses, semesters = scope(name, request.form.get('semester'))
            selected = request.form.get('course', '')
            return_args = {'semester': semester} if semester else {}
            if selected in {str(course['id']) for course in courses}:
                return_args['course'] = selected
            action = request.form.get('action')
            key = request.form.get('api_key', '').strip()
            if action == 'disconnect':
                service.storage.configure(name, None)
                return redirect(url_for('course_overviews', **return_args))
            if (action == 'connect' and request.form.get('consent') == 'yes'
                    and re.fullmatch(r'[A-Za-z0-9_.=-]{24,512}', key)):
                if service.storage.configure(name, key):
                    service.start_worker(app)
                    return redirect(url_for('course_overviews', **return_args))
            error = '請輸入有效的 Gemini API 金鑰，並確認課程資料的使用方式。'
            return overview_view(name, semester, courses, semesters, service.storage.settings(name), error=error), 400, {'Cache-Control': 'no-store'}
        return redirect(url_for('course_overviews', setup=1, semester=request.args.get('semester'),
                                course=request.args.get('course'), _anchor='courseSetup'))

    @app.get('/courses/overview')
    @login_required
    def course_overviews():
        name = username()
        semester, courses, semesters = scope(name, request.args.get('semester'))
        if not semester and request.args.get('semester'):
            semester, courses, semesters = scope(name, None)
        settings = service.storage.settings(name)
        return overview_view(name, semester, courses, semesters, settings), 200, {'Cache-Control': 'no-store'}

    @app.get('/api/course-overview')
    @login_required
    def course_overview_data():
        name = username()
        settings = service.storage.settings(name)
        if not settings:
            return response({'configured': False})
        semester, courses, _ = scope(name, request.args.get('semester'))
        data = service.storage.snapshots(name, semester, [course['id'] for course in courses])
        courses_by_id = {course['id']: course['title'] for course in courses}
        events = []
        for course_id, snapshot in data.items():
            for item in snapshot.get('events', {}).values():
                if not item['cancelled']:
                    events.append({**item, 'course_id': course_id, 'course': courses_by_id[course_id]})
        overview = version = None
        if request.args.get('include') == 'overview':
            version = overview_version(courses, data)
        if version and request.args.get('overview_version') != version:
            overview = render_template('assignments/components/course-overview-cards.html',
                courses=courses, snapshots=data, configured=True, selected_course='',
                course_name=course_display_name)
        return response({'configured': True, 'semester': semester, 'events': sorted(events, key=lambda row: row['date']),
            'running': settings['lease_until'] > time.time(), 'retry_at': settings['retry_at'],
            'ready': sum(bool(snapshot.get('analyzed')) for snapshot in data.values()), 'count': len(courses),
            'error': ERRORS.get(settings['error'], ''), 'error_code': settings['error'],
            'overview': overview, 'overview_version': version,
            'courses_ready': [str(cid) for cid, row in data.items() if row.get('analyzed')]})

    @app.post('/api/course-overview/refresh')
    @login_required
    def course_overview_refresh():
        name = username()
        if not service.storage.settings(name):
            return response({'configured': False})
        data = request.get_json(silent=True)
        if not isinstance(data, dict) or not isinstance(data.get('semester', ''), str):
            return response({'error': '請選擇學期。'}, 400)
        semester, courses, _ = scope(name, data.get('semester'))
        if not courses:
            return response({'status': 'empty'})
        if not service.storage.owner.consume_security_limit(f'course-overview:{name}', 18, 900):
            return response({'error': '更新過於頻繁，請稍後再試。'}, 429)
        status = service.start(app, current_user(), semester, courses, manual=data.get('manual') is True)
        return response({'status': status, 'error': ERRORS.get(status, '')}, 202 if status == 'running' else 200)

    @app.post('/api/course-overview/search')
    @login_required
    def course_overview_search():
        name = username()
        data = request.get_json(silent=True)
        if not isinstance(data, dict) or not isinstance(data.get('query'), str) or not isinstance(data.get('semester'), str):
            return response({'error': '請輸入課程問題。'}, 400)
        query = data['query'].strip()
        if not 2 <= len(query) <= 300:
            return response({'error': '問題須在 2–300 字內。'}, 400)
        if not service.storage.owner.consume_security_limit(f'course-search:{name}', 8, 300):
            return response({'error': '查詢過於頻繁，請稍後再試。'}, 429)
        semester, courses, _ = scope(name, data['semester'])
        titles = {course['id']: course_display_name(course['title']) for course in courses}
        course_id = data.get('course_id')
        if course_id is not None:
            if type(course_id) is not int or course_id not in titles:
                return response({'error': '找不到該課程。'}, 400)
            titles = {course_id: titles[course_id]}
        snapshots = service.storage.snapshots(name, semester, list(titles), include_sources=True)
        try:
            result = service.search(name, query, snapshots, titles)
        except CourseAIError as exc:
            return response({'error': ERRORS.get(str(exc), ERRORS['provider'])}, 409)
        if result.get('choices'):
            result['choices'] = [{'id': choice, 'title': titles[choice]} for choice in result['choices']]
        result['partial'] = len(snapshots) < len(titles) or any(state.get('partial') for state in snapshots.values())
        return response(result)
