"""Resumable attachment discovery, isolated from assignment-page startup."""

import threading
import time
from urllib.parse import parse_qs, urlsplit

import requests

from e3_tracker.assignments.domain.course_announcements import discussion_summaries, moodle_url
from e3_tracker.assignments.domain.course_files import assignment_files, course_materials, folder_files
from e3_tracker.assignments.persistence.course_files import CourseFileStorage
from .course_announcements import AnnouncementSessionExpired, MoodleNewsClient


class CourseFileService:
    def __init__(self, storage, base_url, news_service):
        self.storage = CourseFileStorage(storage)
        self.base_url = base_url.rstrip('/')
        self.news = news_service
        self.slots = news_service.slots

    def start(self, app, user, semester, courses, messages):
        if not user.get('moodle_session'):
            return 'session_expired'
        if not self.slots.acquire(blocking=False):
            return 'busy'
        try:
            attempt = self.storage.claim(user['username'], semester)
            if attempt is None:
                self.slots.release()
                return 'cooldown'
            threading.Thread(target=self._refresh, args=(app, dict(user), semester, courses, messages, attempt), daemon=True).start()
            return 'running'
        except Exception:
            self.slots.release()
            raise

    def _plan(self, previous, courses, messages):
        valid_ids = {int(course['id']) for course in courses}
        sources = {key: dict(source) for key, source in previous['sources'].items() if source['course_id'] in valid_ids}
        seeds, cached = {}, set()
        for course in courses:
            course_id = int(course['id'])
            seeds[f'course:{course_id}'] = {'kind': 'course', 'course_id': course_id, 'title': course['title'],
                'url': f'{self.base_url}/course/view.php?id={course_id}'}
            for assignment in course.get('assignments', [])[:120]:
                url = moodle_url(assignment.get('url'), self.base_url, '/mod/assign/view.php', 'id')
                if url:
                    key = 'assignment:' + parse_qs(urlsplit(url).query)['id'][0]
                    seeds[key] = {'kind': 'assignment', 'course_id': course_id, 'title': assignment['title'], 'url': url}
        for item in messages.get('announcements', []):
            if int(item['course_id']) not in valid_ids:
                continue
            url = self.news.item_url(item)
            if not url:
                continue
            key = 'announcements:' + item['key']
            seeds[key] = {**item, 'kind': 'announcements', 'url': url}
            if 'links' in item:
                cached.add(key)
        for key, source in seeds.items():
            sources[key] = {**sources.get(key, {}), **source}
        sources = {key: source for key, source in sources.items() if source['kind'] != 'announcements'
                   or key in seeds or source.get('parent') in sources}
        queue = [key for key in previous['pending'] if key in sources and key not in cached]
        if not previous['pending']:
            queue = [key for key in sources if key not in cached]
        else:
            queue.extend(key for key in seeds if key not in previous['sources'] and key not in cached and key not in queue)
        return sources, queue

    def collect(self, client, courses, messages, previous, *, max_pages=60):
        sources, queue = self._plan(previous, courses, messages)
        course_map = {int(course['id']): course for course in courses}
        attempted, failures = set(), []
        successful = 0
        def remove_source(key):
            for child in [entry for entry, value in sources.items() if value.get('parent') == key]:
                remove_source(child)
            sources.pop(key, None)

        while queue and len(attempted) < max_pages and time.monotonic() < client.deadline - 1:
            key = queue.pop(0)
            if key in attempted or key not in sources:
                continue
            attempted.add(key)
            source = sources[key]
            try:
                html = client.get(source['url'])
                children = None
                if source['kind'] == 'course':
                    links, children = course_materials(html, self.base_url, course_map[source['course_id']])
                elif source['kind'] == 'forum':
                    links = []
                    children = {f"announcements:{source['course_id']}:{item['id']}": {
                        **item, 'key': f"{source['course_id']}:{item['id']}", 'kind': 'announcements'}
                        for item in discussion_summaries(html, self.base_url)}
                elif source['kind'] == 'assignment':
                    links = assignment_files(html, source['url'])
                elif source['kind'] == 'folder':
                    links = folder_files(html, source['url'])
                else:
                    links = self.news.parse_content(html)['links']
                if children is not None:
                    # Only a successfully fetched parent can invalidate its old children.
                    for child_key in list(sources):
                        if child_key in sources and sources[child_key].get('parent') == key and child_key not in children:
                            remove_source(child_key)
                    for child_key, child in list(children.items())[:150]:
                        sources[child_key] = {**sources.get(child_key, {}), **child,
                            'course_id': source['course_id'], 'parent': key}
                        if child_key not in attempted and child_key not in queue:
                            queue.append(child_key)
                source['limited'] = len(links) > 100 or (children is not None and len(children) > 150)
                source['links'] = links[:100]
                source['fetched_at'] = time.time()
                successful += 1
            except AnnouncementSessionExpired:
                return sources, [key, *queue, *failures], 'E3 登入已失效，請重新登入後繼續整理。', successful
            except (requests.RequestException, ValueError, TimeoutError):
                failures.append(key)
        pending = list(dict.fromkeys(key for key in [*queue, *failures] if key in sources))
        error = '部分來源尚未完成，已保留先前附件，可繼續整理。' if pending else ''
        if not error and any(source.get('limited') for source in sources.values()):
            error = '部分來源的附件或活動較多，目前僅整理部分資料，完整內容請前往 E3。'
        return sources, pending, error, successful

    def _refresh(self, app, user, semester, courses, messages, attempt):
        previous = None
        client = None
        try:
            previous = self.storage.load(user['username'], semester)
            client = MoodleNewsClient(self.base_url, user['moodle_session'], deadline=time.monotonic() + 55)
            sources, pending, error, successful = self.collect(client, courses, messages, previous)
            self.storage.finish(user['username'], semester, attempt, sources, pending, error, fetched=bool(successful))
        except Exception:
            app.logger.warning('Course attachment indexing failed')
            if previous is not None:
                self.storage.finish(user['username'], semester, attempt, previous['sources'], previous['pending'],
                    '整理失敗，已保留先前附件，請稍後重試。', fetched=False)
        finally:
            if client is not None:
                client.close()
            self.slots.release()
