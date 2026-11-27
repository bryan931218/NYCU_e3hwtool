"""Read the public NYCU course outline without using a Moodle credential."""

import re
import time
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup

from e3_tracker.assignments.domain.course_insights import compact_text
from e3_tracker.platform.constants import HEADERS


TIMETABLE = 'https://timetable.nycu.edu.tw/'
COURSE_CODE = re.compile(r'^\s*(?:【(\d{3})([上下])】\s*|(\d{3})([12])[.\s_-]+)(\d{6})(?!\d)')


def outline_sources(base, course, previous, deadline):
    if urlsplit(base).hostname not in {'e3p.nycu.edu.tw', 'e3.nycu.edu.tw'}:
        return [], False
    match = COURSE_CODE.match(course['title'])
    if not match:
        return [], False
    year, term = match[1] or match[3], {'上': '1', '下': '2'}.get(match[2], match[4])
    if course.get('semester_key') != f'{year}-{term}':
        return [], False
    code = match[5]
    key = f'outline:{year}:{term}:{code}'
    cached = [row for row in previous if row['kind'] == 'syllabus' and row['key'].startswith(key + ':')]
    if cached and all(time.time() - row.get('retrieved_at', 0) < 12 * 3600 for row in cached):
        return cached, False

    def timeout():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Public course outline read timed out')
        return min(remaining, 5)

    params = {'r': 'main/crsoutline', 'Acy': year, 'Sem': term, 'CrsNo': code, 'lang': 'zh-tw'}
    try:
        with requests.Session() as session:
            session.headers['User-Agent'] = HEADERS['User-Agent']
            page = session.get(TIMETABLE, params=params, allow_redirects=False, timeout=timeout())
            page.raise_for_status()
            form = BeautifulSoup(page.content, 'html.parser').select_one('.crsoutline-params')
            if page.status_code != 200 or not form:
                raise ValueError('Public course outline unavailable')
            fields = {node['name']: node.get('value', '') for node in form.select('input[name]')}
            if (fields.get('acy'), fields.get('sem'), fields.get('cos_id'), fields.get('user', '')) != (year, term, code, ''):
                raise ValueError('Unexpected course outline')
            if '_token' not in fields:
                raise ValueError('Public outline form unavailable')
            response = session.post(TIMETABLE, params={'r': 'main/getCrsOutlineDescription'},
                                    data={'acy': year, 'sem': term, 'cos_id': code, 'lang': 'zh-tw',
                                          'user': '', '_token': fields['_token']},
                                    headers={'Referer': page.url, 'X-Requested-With': 'XMLHttpRequest',
                                             'Accept': 'application/json'},
                                    allow_redirects=False, timeout=timeout())
            response.raise_for_status()
            data = response.json()
            if response.status_code != 200 or not isinstance(data, dict) or 'crs_exam_score' not in data:
                raise ValueError('Course outline response unavailable')
    except (requests.RequestException, ValueError, TimeoutError):
        return cached, True

    now = time.time()
    rows = []
    for field, topic, label in (('crs_exam_score', 'grading', '評量方式'), ('crs_outline', 'overview', '課程內容')):
        soup = BeautifulSoup(str(data.get(field) or ''), 'html.parser')
        for node in soup.select('script, style'):
            node.decompose()
        for node in soup.select('br'):
            node.replace_with('\n')
        for node in soup.select('p, div, li, tr'):
            node.insert_after('\n')
        lines = [clean for line in soup.get_text().splitlines() if (clean := compact_text(line, 1200))]
        value = compact_text(' '.join(lines), 3500)
        if value:
            rows.append({'kind': 'syllabus', 'key': f'{key}:{topic}', 'topic': topic, 'title': '陽明交大課程綱要',
                         'url': page.url, 'location': label, 'text': value, 'lines': lines[:50],
                         'retrieved_at': now, 'published': 0, 'version': 0})
    return rows, False
