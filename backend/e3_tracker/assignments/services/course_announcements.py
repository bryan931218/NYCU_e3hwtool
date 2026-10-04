"""Bounded, on-demand Moodle announcement synchronization using the user's existing session."""

import threading
import time
from urllib.parse import urljoin, urlsplit

import requests
from bs4 import BeautifulSoup

from e3_tracker.assignments.domain.course_announcements import (
    announcement_forums, discussion_content, discussion_summaries, moodle_url,
)
from e3_tracker.assignments.services.http import apply_cookie
from e3_tracker.platform.constants import HEADERS


class AnnouncementSessionExpired(Exception):
    pass


class MoodleNewsClient:
    def __init__(self, base_url, cookie, *, timeout=8, deadline=None):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        apply_cookie(self.session, self.base_url, cookie)
        self.timeout = min(timeout, 8)
        self.deadline = deadline or time.monotonic() + 60

    def close(self):
        self.session.close()

    def get(self, url):
        origin = urlsplit(self.base_url)
        for _ in range(4):
            remaining = self.deadline - time.monotonic()
            target = urlsplit(url)
            if remaining <= 0:
                raise TimeoutError('Announcement synchronization timed out')
            if (target.scheme, target.netloc) != (origin.scheme, origin.netloc) or target.username or target.password:
                raise ValueError('Untrusted Moodle URL')
            if '/login/' in target.path:
                raise AnnouncementSessionExpired()
            with self.session.get(url, headers=HEADERS, timeout=min(self.timeout, remaining),
                                  allow_redirects=False, stream=True) as response:
                if response.status_code in (301, 302, 303, 307, 308):
                    url = urljoin(url, response.headers.get('Location', ''))
                    if urlsplit(url).netloc != origin.netloc:
                        raise AnnouncementSessionExpired()
                    continue
                response.raise_for_status()
                data = bytearray()
                for chunk in response.iter_content(65536):
                    data.extend(chunk)
                    if len(data) > 2 * 1024 * 1024 or time.monotonic() > self.deadline:
                        raise ValueError('Moodle response exceeded limits')
                html = data.decode('utf-8', errors='replace')
                soup = BeautifulSoup(html, 'html.parser')
                if soup.select_one('input[name="logintoken"], body#page-login-index, body#page-enrol-index'):
                    raise AnnouncementSessionExpired()
                return html
        raise ValueError('Moodle redirect limit exceeded')


def collect_course_announcements(client, courses):
    items, successful, failed = [], [], []
    for course in courses:
        course_id = int(course['id'])
        try:
            html = client.get(f'{client.base_url}/course/view.php?id={course_id}')
            summaries = {}
            for forum_url in announcement_forums(html, client.base_url):
                for item in discussion_summaries(client.get(forum_url), client.base_url):
                    summaries[item['id']] = {**item, 'key': f"{course_id}:{item['id']}",
                        'course_id': course_id, 'course_title': course['title'], 'semester': course['semester_key']}
            items.extend(list(summaries.values())[:30])
            successful.append(course_id)
        except AnnouncementSessionExpired:
            raise
        except (requests.RequestException, ValueError, TimeoutError):
            failed.append(course_id)
    return items, successful, failed


class CourseAnnouncementService:
    def __init__(self, storage, base_url, timeout=8):
        self.storage, self.base_url, self.timeout = storage, base_url.rstrip('/'), min(timeout, 8)
        self.slots = threading.BoundedSemaphore(2)

    def start(self, app, user, semester, courses):
        if not user.get('moodle_session'):
            return 'session_expired'
        if not self.slots.acquire(blocking=False):
            return 'busy'
        try:
            attempt = self.storage.claim_course_announcement_refresh(user['username'], semester)
            if attempt is None:
                self.slots.release()
                return 'cooldown'
            worker = threading.Thread(target=self._refresh, args=(app, dict(user), semester, courses, attempt), daemon=True)
            worker.start()
            return 'running'
        except Exception:
            self.slots.release()
            raise

    def _refresh(self, app, user, semester, courses, attempt):
        client = MoodleNewsClient(self.base_url, user['moodle_session'], timeout=self.timeout)
        try:
            items, successful, failed = collect_course_announcements(client, courses)
            message = '部分課程更新失敗，已保留先前公告。' if failed else ''
            self.storage.finish_course_announcement_refresh(user['username'], semester, attempt, items, successful, message)
        except AnnouncementSessionExpired:
            self.storage.finish_course_announcement_refresh(user['username'], semester, attempt, [], [], 'E3 登入已失效，請重新登入。')
        except Exception:
            app.logger.warning('Course announcement synchronization failed')
            self.storage.finish_course_announcement_refresh(user['username'], semester, attempt, [], [], '公告更新失敗，請稍後再試。')
        finally:
            client.close()
            self.slots.release()

    def content(self, user, item):
        url = moodle_url(item['url'], self.base_url, '/mod/forum/discuss.php', 'd')
        if not url:
            raise ValueError('Invalid cached announcement URL')
        if not user.get('moodle_session'):
            raise AnnouncementSessionExpired()
        if not self.slots.acquire(blocking=False):
            raise BlockingIOError()
        client = MoodleNewsClient(self.base_url, user['moodle_session'], timeout=self.timeout, deadline=time.monotonic() + 12)
        try:
            return discussion_content(client.get(url), self.base_url)
        finally:
            client.close()
            self.slots.release()
