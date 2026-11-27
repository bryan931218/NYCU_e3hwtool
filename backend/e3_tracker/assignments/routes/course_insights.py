import hashlib
import re
import time

import requests
from flask import abort, flash, redirect, render_template, request, url_for

from e3_tracker.assignments.domain.course_files import course_display_name
from e3_tracker.assignments.domain.course_insights import grading_fields
from e3_tracker.assignments.services.collector import _semester_from_key, current_semester_key, normalize_semester_selection
from e3_tracker.assignments.services.course_announcements import AnnouncementSessionExpired
from e3_tracker.assignments.services.course_insights import ANALYSIS_REVISION
from e3_tracker.assignments.services.http import E3LoginError


ERRORS = {
    'key': 'Gemini 拒絕這個金鑰。請確認專案已開放 Gemini API 且能使用 Flash-Lite。',
    'quota': 'Gemini 用量已達上限，稍後會再試。',
    'session_expired': '請重新登入 E3 以繼續整理課程。',
    'source': '部分課程來源暫時讀不到，已保留先前的整理結果。',
    'provider': 'Gemini 暫時無法使用，請稍後再試。',
    'output': 'Gemini 回傳格式有誤，請稍後再試。',
}


def overview_version(courses, snapshots):
    fields = ('analyzed', 'partial', 'document', 'attempt_revision')
    state = [(course['id'], course['title'], course.get('semester_key'),
              tuple(snapshots.get(course['id'], {}).get(field) for field in fields),
              bool(snapshots.get(course['id'], {}).get('material_progress')),
              bool(snapshots.get(course['id'], {}).get('analysis_pending'))) for course in courses]
    return hashlib.sha256(repr(state).encode()).hexdigest()[:20]


def course_states(snapshots):
    return {str(cid): 'pending' if row.get('material_progress') else 'partial' if row.get('partial') else 'ready'
            for cid, row in snapshots.items() if row.get('analyzed') or row.get('partial') or row.get('material_progress')}


def register_course_insight_routes(app, current_user, login_required, service):
    def username():
        user = current_user()
        if not user or user.get('is_guest'):
            abort(403)
        return user['username']

    def scope(name, semester):
        try:
            return service.files.scope(name, semester or None, max_courses=None)
        except ValueError:
            return '', [], []

    def response(data, status=200):
        return data, status, {'Cache-Control': 'no-store'}

    def overview_view(name, semester, courses, semesters, settings, *, error=''):
        course = request.values.get('course', '')
        selected = next((row for row in courses if str(row['id']) == course), None)
        snapshots = service.storage.snapshots(name, semester, [row['id'] for row in courses])
        term = _semester_from_key(semester)
        historical = term['key'] != 'other' and term['sort_key'] < _semester_from_key(current_semester_key())['sort_key']
        e3_course_base = service.files.base_url + '/course/view.php?id='
        return render_template('assignments/pages/course_overviews.html', semester=semester,
            semesters=semesters, courses=courses, selected_course=course if selected else '',
            selected_name=course_display_name(selected['title']) if selected else '全部課程', configured=bool(settings),
            snapshots=snapshots, course_name=course_display_name, grading_fields=grading_fields,
            verification_revision=ANALYSIS_REVISION,
            course_states=course_states(snapshots), historical=historical,
            e3_course_base=e3_course_base,
            overview_version=overview_version(courses, snapshots),
            ready=sum(bool(row.get('analyzed')) for row in snapshots.values()),
            setup_requested=request.args.get('setup') == '1' or bool(error),
            setup_error=error, error=ERRORS.get((settings or {}).get('error'), ''),
            error_code=(settings or {}).get('error'))

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

    @app.post('/courses/overview/semesters')
    @login_required
    def course_overview_semesters():
        name = username()
        semester = normalize_semester_selection(request.form.get('semester', ''))
        course = request.form.get('course', '')
        target = url_for('course_overviews', semester=semester[0] if semester else None,
                         course=course if re.fullmatch(r'[1-9][0-9]{0,9}', course) else None)
        owner = service.storage.owner
        if not owner.consume_security_limit(f'course-semesters:{name}', 1, 30):
            flash('剛更新過學期，請稍候再試。', 'semester-error')
            return redirect(target, code=303)
        user = current_user()
        if not (user.get('moodle_session') or user.get('moodle_token')):
            user = {**user, **(owner.load_session_identity_user(name, time.time()) or {})}
        previous = {entry['key'] for entry in service.files.catalog(name)['available_semesters']}
        try:
            courses = service.files.refresh_catalog(user)
        except AnnouncementSessionExpired:
            flash('E3 登入已失效，請重新登入後更新學期。', 'semester-login')
        except E3LoginError as exc:
            if exc.reason in {'api_token_invalid', 'identity_mismatch'}:
                flash('E3 登入已失效，請重新登入後更新學期。', 'semester-login')
            else:
                flash('暫時無法讀取 E3 學期清單，請稍後再試。', 'semester-error')
        except (requests.RequestException, TimeoutError, ValueError, RuntimeError):
            flash('暫時無法讀取 E3 學期清單，請稍後再試。', 'semester-error')
        else:
            if courses is None:
                flash('目前有其他學期正在更新，請稍後再試。', 'semester-error')
            elif not courses:
                flash('E3 目前沒有回傳修課清單。', 'semester-error')
            else:
                added = len({course['semester_key'] for course in courses} - previous - {'other'})
                flash(f'學期清單已更新，新增 {added} 個學期。' if added else '學期清單已更新。', 'semester-success')
                if semester and service.storage.settings(name):
                    selected, scoped_courses, _ = scope(name, semester[0])
                    if scoped_courses:
                        service.storage.request_semester(name, selected)
        return redirect(target, code=303)

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
                course_name=course_display_name, grading_fields=grading_fields, verification_revision=ANALYSIS_REVISION)
        running = settings['lease_until'] > time.time()
        states = course_states(data)
        needs_scan = any(data.get(cid, {}).get('attempt_revision') != ANALYSIS_REVISION or
                         data.get(cid, {}).get('material_progress') or data.get(cid, {}).get('retry_once') or
                         (data.get(cid, {}).get('analysis_pending') and
                          data[cid]['analysis_pending'].get('failures', 0) < 3)
                         for cid in courses_by_id)
        return response({'configured': True, 'semester': semester, 'events': sorted(events, key=lambda row: row['date']),
            'running': running, 'running_here': running and settings['working_semester'] == semester,
            'queued_here': bool(semester and settings['requested_semester'] == semester), 'needs_scan': needs_scan,
            'retry_at': settings['retry_at'],
            'ready': sum(bool(snapshot.get('analyzed')) for snapshot in data.values()), 'count': len(courses),
            'error': ERRORS.get(settings['error'], ''), 'error_code': settings['error'],
            'overview': overview, 'overview_version': version,
            'course_states': states,
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
        manual = data.get('manual') is True
        queued = manual or data.get('queue') is True
        user = current_user()
        if queued and (user.get('moodle_session') or user.get('moodle_token')):
            service.storage.request_semester(name, semester)
        status = service.start(app, user, semester, courses, manual=manual)
        if queued and status in {'waiting', 'busy'}:
            status = 'queued'
        return response({'status': status, 'error': ERRORS.get(status, '')}, 202 if status == 'running' else 200)
