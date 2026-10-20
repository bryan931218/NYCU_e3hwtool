"""Synthetic library markup and data for browser checks, without any real sessions."""

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import g, render_template
from e3_tracker.platform.application import create_app

COURSES = [{'id': 42, 'title': '【115上】作業系統 Operating Systems'},
           {'id': 43, 'title': '【115上】計算機網路 Computer Networks'},
           {'id': 44, 'title': '【115上】線性代數 Linear Algebra'}]
FILES = [
    {'title': 'HW 2 作業規格', 'filename': 'HW2_specification.pdf', 'type': 'pdf', 'extension': 'PDF', 'course': 42,
     'origins': [{'kind': 'assignment', 'title': 'HW 2：CPU 排程模擬', 'url': 'https://e3p.nycu.edu.tw/mod/assign/view.php?id=9'},
                 {'kind': 'announcements', 'title': '作業二規格修正與注意事項', 'url': '/courses/messages?semester=115-1&item=42:7'}]},
    {'title': 'Chapter 3 — Processes', 'filename': '03_Processes.pptx', 'type': 'slides', 'extension': 'PPTX', 'course': 42,
     'origins': [{'kind': 'course', 'title': '課程教材', 'url': 'https://e3p.nycu.edu.tw/course/view.php?id=42'}]},
    {'title': '期中報告格式與繳交注意事項', 'filename': '期中報告格式.docx', 'type': 'document', 'extension': 'DOCX', 'course': 43,
     'origins': [{'kind': 'mail', 'title': '期中報告說明', 'url': '/courses/messages?tab=mail&semester=115-1&item=43:8'}]},
    {'title': 'Lab 1 起始程式碼', 'filename': 'lab1_starter.zip', 'type': 'archive', 'extension': 'ZIP', 'course': 43,
     'origins': [{'kind': 'assignment', 'title': 'Lab 1：Socket Programming', 'url': 'https://e3p.nycu.edu.tw/mod/assign/view.php?id=10'}]},
    {'title': '課堂參考資料', 'filename': '', 'type': 'link', 'extension': '', 'course': 44,
     'origins': [{'kind': 'announcements', 'title': '參考教材與練習題', 'url': '/courses/messages?semester=115-1&item=44:1'}]},
    {'title': '習題解答與課堂範例', 'filename': 'Chapter_2_Exercise_Solutions_and_Examples.pdf', 'type': 'pdf', 'extension': 'PDF', 'course': 44,
     'origins': [{'kind': 'course', 'title': '第二章教材', 'url': 'https://e3p.nycu.edu.tw/course/view.php?id=44'}]},
]
ITEMS = [{**item, 'id': str(index), 'course_id': item['course'],
          'course_title': next(course['title'] for course in COURSES if course['id'] == item['course']),
          'url': f'https://e3p.nycu.edu.tw/pluginfile.php/42/example-{index}.pdf', 'updated_ts': 0}
         for index, item in enumerate(FILES)]

if '--data' in sys.argv:
    print(json.dumps({'ok': True, 'semester': '115-1', 'courses': COURSES, 'items': ITEMS,
                      'initialized': True, 'fetched_at': 1792497600, 'running': False, 'remaining': 0, 'error': ''}, ensure_ascii=False))
else:
    with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {
        'E3_CACHE_DIR': folder, 'E3_DATABASE_URL': '', 'DATABASE_URL': '', 'E3_CANONICAL_HOST': '',
        'E3_ENV': 'development', 'RAILWAY_ENVIRONMENT_ID': '', 'RAILWAY_ENVIRONMENT_NAME': '',
        'E3_NOTIFICATIONS_WORKER': '0', 'E3_YOUTUBE_AUTO_SYNC_ENABLED': '0', 'E3_SESSION_PROFILE_WORKER': '0',
        'E3_GA4_MEASUREMENT_ID': '', 'E3_GA4_PROPERTY_ID': '', 'E3_SESSION_COOKIE_SECURE': '0',
    }):
        app = create_app()
        try:
            with app.test_request_context('/courses/files'):
                g.csp_nonce = 'files-fixture'
                print(render_template('assignments/pages/course_files.html', user={'is_guest': False},
                    semesters=[{'key': '115-1', 'label': '115 上學期'}, {'key': '114-2', 'label': '114 下學期'}],
                    selected_semester='115-1', files_config={'dataUrl': '/api/course-files', 'refreshUrl': '/api/course-files/refresh', 'guest': False}))
        finally:
            app.extensions['e3_storage']._engine.dispose()
