"""Adapt authenticated Moodle read APIs to the existing, sanitized page parsers.

The generated markup is parser input only. It is never rendered in the browser.
Only fixed read functions are called; tokens stay in server-side POST bodies.
"""
import re
import time
from html import escape
from urllib.parse import parse_qs, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup

from .http import E3LoginError
from .mobile_auth import api_call, verified_site_info


class MoodleApiNewsClient:
    def __init__(self, base_url, user, *, timeout=8, deadline=None, courses=None):
        self.base_url = base_url.rstrip('/')
        self.user = dict(user)
        self.session = requests.Session()
        self.timeout = min(timeout, 8)
        self.deadline = deadline or time.monotonic() + 60
        self.courses = {int(c['id']): c for c in courses or []}
        self.verified = False
        self.sections = {}
        self.forums = None
        self.assignments = {}

    def close(self):
        self.session.close()

    def _timeout(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Moodle API read deadline exceeded')
        return min(self.timeout, remaining)

    def _call(self, function, **params):
        from .course_announcements import AnnouncementSessionExpired
        try:
            if not self.verified:
                verified_site_info(self.session, self.base_url, self.user['moodle_token'],
                    username=self.user['username'], user_id=self.user['moodle_user_id'], timeout=self._timeout())
                self.verified = True
            return api_call(self.session, self.base_url, self.user['moodle_token'], function,
                            timeout=self._timeout(), **params)
        except E3LoginError as exc:
            if exc.reason in {'api_token_invalid', 'identity_mismatch'}:
                raise AnnouncementSessionExpired() from None
            raise ValueError('Moodle read API unavailable') from None

    def _catalog(self):
        if not self.courses:
            rows = self._call('core_enrol_get_users_courses', userid=self.user['moodle_user_id'])
            if not isinstance(rows, list):
                raise ValueError('Invalid course API response')
            self.courses = {int(c['id']): c for c in rows if type(c.get('id')) is int}
        return self.courses

    def _sections(self, course_id):
        if course_id not in self._catalog():
            raise ValueError('Course outside requested scope')
        if course_id not in self.sections:
            rows = self._call('core_course_get_contents', courseid=course_id)
            if not isinstance(rows, list):
                raise ValueError('Invalid course API response')
            self.sections[course_id] = rows
        return self.sections[course_id]

    def _forums(self):
        if self.forums is None:
            params = {f'courseids[{i}]': cid for i, cid in enumerate(self._catalog())}
            rows = self._call('mod_forum_get_forums_by_courses', **params) if params else []
            if not isinstance(rows, list) or any(row.get('course') not in self.courses for row in rows):
                raise ValueError('Invalid forum API response')
            self.forums = rows
        return self.forums

    def _module(self, module_id):
        for course_id in self._catalog():
            for section in self._sections(course_id):
                if section.get('uservisible') is False:
                    continue
                for module in section.get('modules', []):
                    if module.get('id') == module_id and module.get('uservisible') is not False:
                        return course_id, module
        raise ValueError('Module outside requested courses')

    def _file_url(self, value):
        target, base = urlsplit(str(value or '')), urlsplit(self.base_url)
        if (target.scheme, target.netloc) == (base.scheme, base.netloc) and target.path.startswith('/webservice/pluginfile.php/'):
            return urlunsplit((target.scheme, target.netloc, target.path.removeprefix('/webservice'), target.query, ''))
        return str(value or '')

    def _html(self, value):
        soup = BeautifulSoup(str(value or ''), 'html.parser')
        for node in soup.select('[href], [src]'):
            for attribute in ('href', 'src'):
                if node.get(attribute):
                    node[attribute] = self._file_url(node[attribute])
        return str(soup)

    def _link(self, url, title):
        return f'<a href="{escape(self._file_url(url), quote=True)}">{escape(str(title or "附件"))}</a>'

    def _files(self, files):
        return ''.join(self._link(f.get('fileurl'), f.get('filename')) for f in files or [] if f.get('fileurl'))

    def get(self, url):
        target, base = urlsplit(url), urlsplit(self.base_url)
        if (target.scheme, target.netloc) != (base.scheme, base.netloc) or target.username or target.password:
            raise ValueError('Untrusted Moodle URL')
        params = parse_qs(target.query, keep_blank_values=True)
        def number(key):
            values = params.get(key, [])
            if len(values) != 1 or not re.fullmatch(r'[1-9][0-9]{0,9}', values[0]):
                raise ValueError('Invalid Moodle identifier')
            return int(values[0])

        if target.path == '/course/view.php':
            cid = number('id')
            fragments = ['<div class="course-content">']
            news = {f['cmid'] for f in self._forums() if f.get('type') == 'news' and f.get('course') == cid}
            for section in self._sections(cid):
                if section.get('uservisible') is False:
                    continue
                fragments.append(self._html(section.get('summary')))
                for module in section.get('modules', []):
                    if module.get('uservisible') is False:
                        continue
                    kind, mid = module.get('modname'), module.get('id')
                    if kind == 'forum' and mid in news:
                        fragments.append('<div class="block_news_items">' + self._link(f'{self.base_url}/mod/forum/view.php?id={mid}', module.get('name')) + '</div>')
                    elif kind in {'resource', 'url', 'folder', 'assign', 'forum'}:
                        fragments.append(self._link(f'{self.base_url}/mod/{kind}/view.php?id={mid}', module.get('name')))
                    fragments.extend([self._html(module.get('description')), self._files(module.get('contents'))])
            return ''.join(fragments) + '</div>'

        if target.path == '/mod/folder/view.php':
            _, module = self._module(number('id'))
            if module.get('modname') != 'folder':
                raise ValueError('Unexpected module type')
            return '<div class="foldertree">' + self._files(module.get('contents')) + '</div>'

        if target.path == '/mod/assign/view.php':
            mid = number('id')
            cid, module = self._module(mid)
            if module.get('modname') != 'assign':
                raise ValueError('Unexpected module type')
            if cid not in self.assignments:
                listing = self._call('mod_assign_get_assignments', **{'courseids[0]': cid})
                groups = listing.get('courses', [])
                if any(g.get('id') != cid for g in groups):
                    raise ValueError('Unexpected assignment course')
                self.assignments[cid] = {a['cmid']: a for g in groups for a in g.get('assignments', [])}
            assignment = self.assignments[cid].get(mid)
            if assignment is None:
                raise ValueError('Assignment unavailable')
            return '<main id="region-main"><div id="intro">' + self._html(assignment.get('intro')) + self._files(assignment.get('introattachments')) + self._files(assignment.get('introfiles')) + '</div></main>'

        if target.path == '/mod/forum/view.php':
            mid = number('id')
            forum = next((f for f in self._forums() if f.get('cmid') == mid), None)
            if forum is None:
                raise ValueError('Forum outside requested courses')
            listing = self._call('mod_forum_get_forum_discussions', forumid=forum['id'], page=0, perpage=30)
            if listing.get('warnings'):
                raise ValueError('Forum discussions unavailable')
            rows = []
            for item in listing.get('discussions', []):
                rows.append('<tr><td>' + self._link(f"{self.base_url}/mod/forum/discuss.php?d={int(item['discussion'])}", item.get('name') or item.get('subject'))
                    + '</td><td class="author"><a>' + escape(str(item.get('userfullname') or ''))
                    + f'</a></td><td class="lastpost"><time data-timestamp="{int(item.get("timemodified") or 0)}"></time></td></tr>')
            return '<table>' + ''.join(rows) + '</table>'

        if target.path == '/mod/forum/discuss.php':
            did = number('d')
            listing = self._call('mod_forum_get_discussion_posts', discussionid=did, sortdirection='ASC')
            if listing.get('courseid') not in self._catalog() or listing.get('warnings'):
                raise ValueError('Discussion outside requested courses')
            root = next((p for p in listing.get('posts', []) if not p.get('hasparent') and p.get('discussionid') == did and not p.get('isdeleted')), None)
            if root is None:
                raise ValueError('Announcement root post unavailable')
            author = escape(str((root.get('author') or {}).get('fullname') or ''))
            created = int(root.get('timecreated') or 0)
            modified = int(root.get('timemodified') or created)
            return (f'<article class="forumpost firstpost" data-post-modified="{modified}"><div class="author"><a>'
                    + author + f'</a><time data-timestamp="{created}"></time></div><div class="post-content-container">'
                    + self._html(root.get('message')) + '</div><div class="attachments">'
                    + self._files(root.get('attachments')) + self._files(root.get('messageinlinefiles')) + '</div></article>')

        if target.path == '/local/dcpcmail/view.php':
            cid = number('c')
            if cid not in self._catalog():
                raise ValueError('Mail course outside requested scope')
            if 'm' in params:
                mid = number('m')
                message = self._call('local_dcpcmail_get_message', id=mid)
                if message.get('id') != mid or (message.get('course') or {}).get('id') != cid:
                    raise ValueError('Mail outside requested course')
                return '<div class="mail_content">' + self._html(message.get('content')) + '</div><div class="mail_attachments">' + self._files(message.get('attachments')) + '</div>'
            listing = self._call('local_dcpcmail_get_index', type='course', itemid=cid, offset=0, limit=30)
            rows = []
            for item in listing.get('messages', []):
                if (item.get('course') or {}).get('id') != cid:
                    raise ValueError('Mail outside requested course')
                classes = 'mail_item mail_unread' if item.get('unread') else 'mail_item'
                rows.append(f'<div class="{classes}">' + self._link(f"{self.base_url}/local/dcpcmail/view.php?c={cid}&t=inbox&m={int(item['id'])}", '').replace('<a ', '<a class="mail_link" ', 1)
                    + '<span class="mail_summary">' + escape(str(item.get('subject') or '')) + '</span><span class="mail_users">'
                    + escape(str((item.get('sender') or {}).get('fullname') or '')) + f'</span><span class="mail_date"><time data-timestamp="{int(item.get("time") or 0)}"></time></span></div>')
            return '<div class="mail_list">' + (''.join(rows) or '沒有任何郵件') + '</div>'

        raise ValueError('Unsupported Moodle read URL')
