"""Read bounded course sources with the current user's Moodle access."""

import hashlib
import io
import posixpath
import re
import time
import zipfile
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup

from e3_tracker.assignments.domain.course_files import safe_file_link
from e3_tracker.assignments.domain.course_announcements import _timestamp, moodle_url
from e3_tracker.assignments.domain.course_insights import compact_text
from e3_tracker.platform.constants import HEADERS
from .course_announcements import AnnouncementSessionExpired


MAX_BYTES = 8 * 1024 * 1024
MAX_PAGES = 35
EXTENSIONS = {'.pdf', '.docx', '.pptx', '.txt'}
SYLLABUS = re.compile(r'課程(?:大綱|說明|簡介|介紹)|授課|教學大綱|評量|評分|配分|syllabus|course[\s_-]*(?:outline|intro|overview|information|policy)|grading|assessment', re.I)
GRADE_CONTEXT = re.compile(r'評分|配分|成績|占比|佔比|比重|百分之|grade|grading|assessment|weight|\d\s*(?:[%％]|percent)', re.I)
FILE_PATH = re.compile(r'^/pluginfile\.php/[1-9]\d*/(?:mod_resource/content|mod_folder/content|mod_page/content|course/section)/')


def _origin(url):
    target = urlsplit(url)
    return target.scheme, target.netloc


def _extension(url):
    return posixpath.splitext(unquote(urlsplit(url).path))[1].lower()


def _file_url(url, base):
    safe = safe_file_link(url, base)
    if not safe or _origin(safe) != _origin(base):
        return None
    path = unquote(urlsplit(safe).path)
    if not FILE_PATH.match(path) or any(part in {'.', '..'} for part in path.split('/')) or '\\' in path:
        return None
    return safe if _extension(safe) in EXTENSIONS else None


def _read_file(client, user, url, base):
    token = user.get('moodle_token')
    if token:
        # The API client verifies that this bearer belongs to the signed-in account.
        if not getattr(client, 'verified', False):
            raise AnnouncementSessionExpired()
        target = urlsplit(url)
        url = urlunsplit((target.scheme, target.netloc, '/webservice' + target.path, '', ''))
    data = bytearray()
    with client.session.get(url, params={'token': token} if token else None, headers=HEADERS,
                            stream=True, allow_redirects=False,
                            timeout=max(0.5, min(8, client.deadline - time.monotonic()))) as response:
        if response.status_code != 200 or 'text/html' in response.headers.get('Content-Type', '').lower():
            return b''
        if int(response.headers.get('Content-Length') or 0) > MAX_BYTES:
            return b''
        for chunk in response.iter_content(65536):
            data.extend(chunk)
            if len(data) > MAX_BYTES or time.monotonic() >= client.deadline:
                return b''
    return bytes(data)


def document_sections(data, extension):
    if extension == '.pdf':
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(data))
        if reader.is_encrypted:
            return []
        return [(f'第 {index + 1} 頁', page.extract_text()) for index, page in enumerate(reader.pages[:MAX_PAGES])]
    if extension == '.txt':
        for encoding in ('utf-8-sig', 'big5'):
            try:
                return [('', data.decode(encoding))]
            except UnicodeDecodeError:
                continue
        return []
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        if extension == '.docx':
            names = ['word/document.xml']
        else:
            names = sorted((name for name in archive.namelist() if re.fullmatch(r'ppt/slides/slide[1-9]\d*\.xml', name)),
                           key=lambda name: int(re.search(r'(\d+)\.xml$', name)[1]))[:MAX_PAGES]
        sections = []
        total = 0
        for name in names:
            total += archive.getinfo(name).file_size
            if total > MAX_BYTES:
                break
            xml = archive.read(name)
            if b'<!DOCTYPE' in xml or b'<!ENTITY' in xml:
                continue
            root = ElementTree.fromstring(xml)
            paragraphs = [''.join(node.text or '' for node in block.iter() if node.tag.endswith('}t'))
                          for block in root.iter() if block.tag.endswith('}p')]
            location = f"第 {re.search(r'(\d+)\.xml$', name)[1]} 張投影片" if extension == '.pptx' else ''
            sections.append((location, '\n'.join(paragraphs)))
        return sections


def _file_candidates(soup, course_url, base, *, pages=False):
    seen = set()
    for anchor in soup.select('a[href]'):
        container = next((match for kind in (('folder', 'page') if pages else ('folder',))
                          if (match := moodle_url(anchor['href'], base, f'/mod/{kind}/view.php', 'id',
                                                 page_url=course_url))), None)
        url = container or safe_file_link(anchor['href'], course_url)
        if not url or url in seen or _origin(url) != _origin(base):
            continue
        if not container and not _file_url(url, base) and urlsplit(url).path != '/mod/resource/view.php':
            continue
        seen.add(url)
        yield url, anchor.get_text(' ', strip=True)[:180] or unquote(urlsplit(url).path.rsplit('/', 1)[-1])


def _resolve_file(client, url, base):
    if _file_url(url, base):
        return url
    # Moodle file resources usually redirect to their own pluginfile URL.
    with client.session.get(url, headers=HEADERS, timeout=max(.5, min(8, client.deadline - time.monotonic())),
                            stream=True, allow_redirects=False) as response:
        if response.is_redirect:
            target = urljoin(url, response.headers.get('Location', ''))
            if '/login/' in urlsplit(target).path:
                raise AnnouncementSessionExpired()
            return _file_url(target, base)
    page = BeautifulSoup(client.get(url), 'html.parser')
    return next((direct for node in page.select('a[href], iframe[src], object[data]')
                 if (direct := _file_url(urljoin(url, node.get('href') or node.get('src') or node.get('data')), base))), None)


def _row(kind, key, title, url, text, *, location='', published=0, version=0, limit=3500):
    if kind in {'announcements', 'mail'}:
        text = f'{title} {text or ""}'
    clean = compact_text(text, limit)
    if not clean:
        return None
    return {'kind': kind, 'key': key, 'title': compact_text(title, 180), 'url': url,
            'location': location, 'published': published, 'version': version, 'text': clean}


def _prioritize_candidates(candidates):
    return list(dict.fromkeys(candidates[:1] + [row for row in candidates if SYLLABUS.search(row[1])][:5] + candidates[1:14]))


def _text_sections(kind, key, title, url, location, text):
    # Preserve nearby rows for the model when a page contains a long grading table.
    clean = compact_text(text, 20000)
    offsets = [0, *range(3200, len(clean), 3200)]
    return [_row(kind, f'{key}:{offset}', title, url, clean[offset:offset + 3500], location=location)
            for offset in offsets if clean[offset:offset + 3500].strip()]


def _material_sources(client, user, main, course_url):
    base = client.base_url
    api = bool(user.get('moodle_token'))
    candidates = list(_file_candidates(main, course_url, base, pages=True))[:150]
    queue = _prioritize_candidates(candidates)
    files, visited, downloaded, any_failure = [], set(), set(), False
    while queue and len(visited) < 15 and len(downloaded) < 5:
        if time.monotonic() > client.deadline - 6:
            any_failure = True
            break
        url, title = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        path = urlsplit(url).path
        try:
            if path == '/mod/folder/view.php' or (api and path == '/mod/resource/view.php'):
                container = BeautifulSoup(client.get(url), 'html.parser').select_one(
                    '.foldertree, .foldercontent, [data-region="folder-content"], .resourcecontent')
                if container:
                    children = list(_file_candidates(container, url, base))
                    # A module name can identify the syllabus even when the filename is generic.
                    entries = [(link, name if SYLLABUS.search(name) else title + ' · ' + name)
                               for link, name in _prioritize_candidates(children)[:5]]
                    if entries:
                        queue[:] = entries[:1] + sorted(queue + entries[1:], key=lambda row: not SYLLABUS.search(row[1]))
                continue
            if path == '/mod/page/view.php':
                content = BeautifulSoup(client.get(url), 'html.parser').select_one('.mod_page-content, #region-main .generalbox, main .generalbox')
                if content:
                    for node in content.select('script, style, form, blockquote, [hidden], [aria-hidden=true]'):
                        node.decompose()
                    queue[0:0] = _prioritize_candidates(list(_file_candidates(content, url, base)))[:5]
                    files.extend(_text_sections('page', f'page:{urlsplit(url).query}', title, url, '',
                                                content.get_text(' ', strip=True)))
                else:
                    any_failure = True
                continue
            direct = _resolve_file(client, url, base)
            if not direct:
                any_failure = True
                continue
            file_path = urlsplit(direct).path
            if file_path in downloaded:
                continue
            downloaded.add(file_path)
            data = _read_file(client, user, direct, base)
            if not data:
                any_failure = True
                continue
            key = hashlib.sha256(file_path.encode()).hexdigest()[:24]
            document_rows = []
            for index, (location, content) in enumerate(document_sections(data, _extension(direct))):
                document_rows.extend(_text_sections('file', f'{key}:{index}', title, direct, location, content))
            if not document_rows:
                any_failure = True
            # Give each document a budget, prioritizing grading pages beyond the opening slides.
            files.extend(sorted(document_rows, key=lambda row: not GRADE_CONTEXT.search(row['text']))[:8])
        except AnnouncementSessionExpired:
            raise
        except Exception:
            # Parser and transport details can contain Moodle links or credentials.
            any_failure = True
    return files[:40], any_failure


def course_sources(client, user, course, services, previous_sources):
    base = client.base_url
    course_url = f"{base}/course/view.php?id={course['id']}"
    html = client.course_material_html(int(course['id'])) if user.get('moodle_token') else client.get(course_url)
    soup = BeautifulSoup(html, 'html.parser')
    main = soup.select_one('.course-content') or soup.select_one('body[id^="page-course-view-"] #region-main')
    if main is None:
        raise ValueError('Course content unavailable')
    for node in main.select('script, style, form, blockquote, [hidden], [aria-hidden=true]'):
        node.decompose()
    files, any_failure = _material_sources(client, user, main, course_url)
    overview = _text_sections('course', f"course:{course['id']}", 'E3 課程頁', course_url, '',
                              main.get_text(' ', strip=True))
    overview = sorted(overview, key=lambda row: not GRADE_CONTEXT.search(row['text']))[:4]

    cached = {row['key']: row for row in previous_sources if row['kind'] in services}
    messages = {}
    for kind, service in services.items():
        try:
            rows, _, failed = service.collect(client, [course])
            any_failure |= bool(failed)
        except AnnouncementSessionExpired:
            raise
        except (TimeoutError, ValueError, requests.RequestException):
            any_failure = True
            continue
        for item in sorted(rows, key=lambda row: row.get('updated_ts') or 0, reverse=True)[:12]:
            key = f"{kind}:{item['key']}"
            version = item.get('updated_ts') or 0
            old = cached.get(key)
            if old and version and old.get('listing_version') == version:
                messages[key] = cached[key]
                continue
            url = service.item_url(item)
            if not url or time.monotonic() > client.deadline - 2:
                any_failure = True
                continue
            try:
                html = client.get(url)
                content = service.parse_content(html).get('content')
                published = version if kind == 'mail' else 0
                event_version = version
                if kind == 'announcements':
                    post = BeautifulSoup(html, 'html.parser').select_one('.forumpost.firstpost, .forumpost')
                    if post:
                        header = post.select_one('header, .author')
                        published = _timestamp(header) or 0
                        try:
                            event_version = int(post.get('data-post-modified') or published or version)
                        except ValueError:
                            event_version = published or version
                source = _row(kind, key, item['title'], url, content,
                              published=published, version=event_version, limit=2200)
                if source:
                    if old and old['text'] == source['text'] and old['title'] == source['title']:
                        source['version'] = old['version']
                    source['listing_version'] = version
                    messages[key] = source
            except (ValueError, TimeoutError, requests.RequestException):
                any_failure = True
                if key in cached:
                    messages[key] = cached[key]
    if any_failure:
        messages = {**cached, **messages}
        if not files:
            files = [row for row in previous_sources if row['kind'] in {'file', 'page'}]
    recent = sorted(messages.values(), key=lambda row: -row['version'])[:20]
    # Leave room for both the first document and later messages in the model input.
    sources = [*files, *overview, *recent]
    return sources, any_failure, bool(files)
