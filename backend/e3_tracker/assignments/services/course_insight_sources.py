"""Read bounded course sources with the current user's Moodle access."""

import hashlib
import io
import posixpath
import re
import time
import zipfile
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qs, unquote, urljoin, urlsplit, urlunsplit
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup

from e3_tracker.assignments.domain.course_files import safe_file_link
from e3_tracker.assignments.domain.course_announcements import _timestamp, moodle_url
from e3_tracker.assignments.domain.course_insights import compact_text
from e3_tracker.platform.constants import HEADERS
from .course_announcements import AnnouncementSessionExpired


MAX_BYTES = 8 * 1024 * 1024
MAX_PAGES = 60
MAX_MATERIALS = 72
EXTENSIONS = {'.pdf', '.docx', '.pptx', '.txt'}
SYLLABUS = re.compile(r'課程(?:大綱|說明|簡介|介紹)|授課|教學大綱|評量|評分|配分|學期成績|syllabus|course[\s_-]*(?:outline|intro|overview|information|policy)|grading|assessment', re.I)
GRADE_CONTEXT = re.compile(r'評分|配分|成績|占比|佔比|比重|百分之|grade|grading|assessment|weight|\d\s*(?:[%％]|percent)', re.I)
EXAM_CONTEXT = re.compile(r'期中|期末|小考|考試|改期|延後|延期|exam|midterm|final|quiz|deadline|reschedul', re.I)
PERCENT_HEADER = re.compile(r'(?:%|％|百分比|percent(?:age)?)', re.I)
FILE_PATH = re.compile(r'^/pluginfile\.php/[1-9]\d*/(?:mod_resource/content|mod_folder/content|mod_page/content|course/section)/')
FORUM_FILE_PATH = re.compile(r'^/pluginfile\.php/[1-9]\d*/mod_forum/(?:attachment|post)/')


def _origin(url):
    target = urlsplit(url)
    return target.scheme, target.netloc


def _extension(url):
    return posixpath.splitext(unquote(urlsplit(url).path))[1].lower()


def _file_url(url, base, *, announcement=False):
    safe = safe_file_link(url, base)
    if not safe or _origin(safe) != _origin(base):
        return None
    path = unquote(urlsplit(safe).path)
    if not (FILE_PATH.match(path) or (announcement and FORUM_FILE_PATH.match(path))):
        return None
    if any(part in {'.', '..'} for part in path.split('/')) or '\\' in path:
        return None
    return safe if _extension(safe) in EXTENSIONS else None


def _read_file(client, user, url):
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
            return b'', 0
        if int(response.headers.get('Content-Length') or 0) > MAX_BYTES:
            return b'', 0
        try:
            modified = parsedate_to_datetime(response.headers.get('Last-Modified', ''))
            version = int(modified.timestamp()) if modified.tzinfo else 0
            if version <= 0 or version > time.time():
                version = 0
        except (ValueError, OverflowError, OSError):
            version = 0
        for chunk in response.iter_content(65536):
            data.extend(chunk)
            if len(data) > MAX_BYTES or time.monotonic() >= client.deadline:
                return b'', 0
    return bytes(data), version


def document_sections(data, extension, *, deadline=None, coverage=None):
    if extension == '.pdf':
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(data))
        if reader.is_encrypted:
            return []
        size = len(reader.pages)
        indices = range(size) if size <= MAX_PAGES else [*range(40), *range(size - 20, size)]
        if coverage is not None and size > MAX_PAGES:
            coverage['limited'] = True
        sections = []
        for index in indices:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError('Document extraction exceeded the read budget')
            sections.append((f'第 {index + 1} 頁', reader.pages[index].extract_text()))
        return sections
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
                           key=lambda name: int(re.search(r'(\d+)\.xml$', name)[1]))
            if len(names) > MAX_PAGES:
                if coverage is not None:
                    coverage['limited'] = True
                names = [*names[:40], *names[-20:]]
        sections = []
        total = 0
        for name in names:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError('Document extraction exceeded the read budget')
            total += archive.getinfo(name).file_size
            if total > MAX_BYTES:
                if coverage is not None:
                    coverage['limited'] = True
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


def _file_candidates(soup, course_url, base, *, pages=False, sections=False):
    seen = set()
    course_id = parse_qs(urlsplit(course_url).query).get('id')
    for anchor in soup.select('a[href]'):
        paths = ['/mod/folder/view.php']
        if pages:
            paths.append('/mod/page/view.php')
        if sections and anchor.find_parent(class_=('section', 'section-item', 'sectionname')):
            paths.append('/course/section.php')
        container = next((match for path in paths
                          if (match := moodle_url(anchor['href'], base, path, 'id', page_url=course_url))), None)
        if sections and not container:
            same_course = moodle_url(anchor['href'], base, '/course/view.php', 'id', page_url=course_url)
            if same_course and parse_qs(urlsplit(same_course).query).get('id') == course_id:
                section = parse_qs(urlsplit(anchor['href']).query).get('section', [])
                if len(section) == 1 and re.fullmatch(r'\d{1,3}', section[0]):
                    container = f'{same_course}&section={section[0]}'
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
    return list(dict.fromkeys(candidates[:1] + [row for row in candidates if SYLLABUS.search(row[1])] + candidates[1:]))


def _document_priority(row):
    if GRADE_CONTEXT.search(row['text']):
        return 0
    if EXAM_CONTEXT.search(row['text']) or SYLLABUS.search(row['title']):
        return 1
    return 2


def _course_text(node):
    # Moodle grading tables sometimes declare the unit only in the column header.
    for table in node.select('table'):
        rows = table.select('tr')
        if len(rows) < 2:
            continue
        headers = rows[0].find_all(['th', 'td'], recursive=False)
        if any(cell.has_attr('colspan') or cell.has_attr('rowspan') for cell in headers):
            continue
        columns = {index for index, header in enumerate(headers)
                   if PERCENT_HEADER.search(header.get_text(' ', strip=True))}
        for row in rows[1:]:
            cells = row.find_all(['th', 'td'], recursive=False)
            if len(cells) != len(headers) or any(cell.has_attr('colspan') or cell.has_attr('rowspan') for cell in cells):
                continue
            for index in columns:
                value = cells[index].get_text(' ', strip=True)
                if re.fullmatch(r'\d{1,3}(?:[.,]\d{1,2})?', value):
                    cells[index].string = value + '%'
    return node.get_text(' ', strip=True)


def _text_sections(kind, key, title, url, location, text):
    # Preserve nearby rows for the model when a page contains a long grading table.
    clean = compact_text(text, 60000)
    offsets = [0, *range(3200, len(clean), 3200)]
    return [_row(kind, f'{key}:{offset}', title, url, clean[offset:offset + 3500], location=location)
            for offset in offsets if clean[offset:offset + 3500].strip()]


def _document_rows(client, user, url, title, *, limit):
    data, version = _read_file(client, user, url)
    if not data:
        return [], True
    key = hashlib.sha256(urlsplit(url).path.encode()).hexdigest()[:24]
    coverage = {}
    document = document_sections(data, _extension(url), deadline=client.deadline, coverage=coverage)
    rows = []
    for index, (location, text) in enumerate(document):
        rows.extend(_text_sections('file', f'{key}:{index}', title, url, location, text))
    return [{**row, 'version': version} for row in sorted(rows, key=_document_priority)[:limit]], bool(coverage) or not rows


def _material_sources(client, user, main, course_url, progress, previous_sources):
    base = client.base_url
    api = bool(user.get('moodle_token'))
    candidates = list(_file_candidates(main, course_url, base, pages=True, sections=not api))[:150]
    roots = _prioritize_candidates(candidates)
    signature = hashlib.sha256('\n'.join(url for url, _ in roots).encode()).hexdigest()
    if progress.get('signature') != signature:
        progress.clear()
    scan_id = progress.get('scan_id') or time.time_ns()
    queue = list(progress.get('queue', roots))
    visited = set(progress.get('visited', []))
    downloaded = set(progress.get('downloaded', []))
    any_failure = progress.get('failed', False)
    files = [row for row in previous_sources if row.get('scan_id') == scan_id]
    attempts = downloads = 0
    while queue and attempts < 12 and downloads < 5 and len(visited) < MAX_MATERIALS:
        if time.monotonic() > client.deadline - 10:
            break
        url, title = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        attempts += 1
        path = urlsplit(url).path
        try:
            if path == '/mod/folder/view.php' or (api and path == '/mod/resource/view.php'):
                container = BeautifulSoup(client.get(url), 'html.parser').select_one(
                    '.foldertree, .foldercontent, [data-region="folder-content"], .resourcecontent')
                if container:
                    children = list(_file_candidates(container, url, base))
                    # A module name can identify the syllabus even when the filename is generic.
                    entries = [(link, name if SYLLABUS.search(name) else title + ' · ' + name)
                               for link, name in _prioritize_candidates(children)]
                    if entries:
                        queue[:] = entries[:1] + sorted(queue + entries[1:], key=lambda row: not SYLLABUS.search(row[1]))
                continue
            if path in {'/mod/page/view.php', '/course/section.php', '/course/view.php'}:
                selector = ('.course-content, #region-main' if path.startswith('/course/') else
                            '.mod_page-content, #region-main .generalbox, main .generalbox')
                content = BeautifulSoup(client.get(url), 'html.parser').select_one(selector)
                if content:
                    for node in content.select('script, style, form, blockquote, [hidden], [aria-hidden=true]'):
                        node.decompose()
                    queue[0:0] = _prioritize_candidates(list(_file_candidates(content, url, base, pages=True)))
                    files.extend(_text_sections('page', f'page:{path}:{urlsplit(url).query}', title, url, '',
                                                _course_text(content)))
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
            downloads += 1
            # Give each document a budget, prioritizing grading pages beyond the opening slides.
            document_rows, incomplete = _document_rows(client, user, direct, title, limit=8)
            files.extend(document_rows)
            any_failure |= incomplete
        except AnnouncementSessionExpired:
            raise
        except Exception:
            # Parser and transport details can contain Moodle links or credentials.
            any_failure = True
    queue = list(dict((url, (url, title)) for url, title in queue if url not in visited).values())
    stalls = progress.get('stalls', 0) + 1 if not attempts else 0
    progress.clear()
    if queue and len(visited) < MAX_MATERIALS and stalls < 2:
        progress.update(signature=signature, scan_id=scan_id, queue=queue[:MAX_MATERIALS],
                        visited=sorted(visited), downloaded=sorted(downloaded), failed=any_failure,
                        stalls=stalls)
    sources = [{**row, 'scan_id': scan_id} for row in sorted(files, key=_document_priority)[:40]]
    return sources, any_failure or bool(queue)


def course_sources(client, user, course, services, previous_sources, *, material_progress=None,
                   discovered_assignments=None):
    base = client.base_url
    course_url = f"{base}/course/view.php?id={course['id']}"
    html = client.course_material_html(int(course['id'])) if user.get('moodle_token') else client.get(course_url)
    soup = BeautifulSoup(html, 'html.parser')
    main = soup.select_one('.course-content') or soup.select_one('body[id^="page-course-view-"] #region-main')
    if main is None:
        raise ValueError('Course content unavailable')
    for node in main.select('script, style, form, blockquote, [hidden], [aria-hidden=true]'):
        node.decompose()
    if discovered_assignments is not None:
        for link in main.select('a[href]'):
            url = moodle_url(link['href'], base, '/mod/assign/view.php', 'id', page_url=course_url)
            if url:
                title = link.select_one('.instancename, .activityname') or link
                discovered_assignments.append({'url': url, 'title': title.get_text(' ', strip=True)})
    progress = {} if material_progress is None else material_progress
    files, any_failure = _material_sources(client, user, main, course_url, progress, previous_sources)
    overview = _text_sections('course', f"course:{course['id']}", 'E3 課程頁', course_url, '',
                              _course_text(main))
    overview = sorted(overview, key=_document_priority)[:4]

    cached = {row['key']: row for row in previous_sources if row['kind'] in services}
    cached_attachments = [row for row in previous_sources if row.get('attachment_parent')]
    if progress:
        return [*files, *overview, *cached.values(), *cached_attachments], True, bool(files)
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
        newest = sorted(rows, key=lambda row: row.get('updated_ts') or 0, reverse=True)
        # The first weeks often contain the grading rules, even in an archived course.
        important = [item for item in newest if SYLLABUS.search(item['title']) or EXAM_CONTEXT.search(item['title'])]
        selected = {item['key']: item for item in [*important[:12], *newest[:12], *newest[-3:]]}
        for item in selected.values():
            key = f"{kind}:{item['key']}"
            version = item.get('updated_ts') or 0
            old = cached.get(key)
            if old and version and old['url'] == service.item_url(item) and old.get('listing_version') == version and 'attachment_urls' in old:
                messages[key] = cached[key]
                continue
            url = service.item_url(item)
            if not url or time.monotonic() > client.deadline - 2:
                any_failure = True
                continue
            try:
                html = client.get(url)
                parsed = service.parse_content(html, page_url=url)
                content = parsed.get('content')
                published = version if kind == 'mail' or '/blocks/dcpc_news/' in url else 0
                event_version = version
                if kind == 'announcements':
                    post = BeautifulSoup(html, 'html.parser').select_one('.forumpost.firstpost, .forumpost')
                    if post:
                        header = post.select_one('header, .author')
                        published = _timestamp(header) or published
                        try:
                            event_version = int(post.get('data-post-modified') or published or version)
                        except ValueError:
                            event_version = published or version
                source = _row(kind, key, item['title'], url, content,
                              published=published, version=event_version, limit=3200)
                if source:
                    if old and old['text'] == source['text'] and old['title'] == source['title']:
                        source['version'] = old['version']
                    source['listing_version'] = version
                    source['attachment_urls'] = [
                        url for link in parsed.get('links', []) if (url := _file_url(link['url'], base, announcement=kind == 'announcements'))
                        and (SYLLABUS.search(item['title']) or SYLLABUS.search(link.get('title', '')))
                    ][:3]
                    messages[key] = source
            except (ValueError, TimeoutError, requests.RequestException):
                any_failure = True
                if key in cached:
                    messages[key] = cached[key]
    if any_failure:
        messages = {**cached, **messages}
        if not files:
            files = [row for row in previous_sources if row['kind'] in {'file', 'page'} and not row.get('attachment_parent')]
    recent = sorted(messages.values(), key=lambda row: (_document_priority(row), -row['version']))[:24]
    attachments = []
    pending = [(message, url) for message in recent for url in message.get('attachment_urls', [])][:4]
    for message, url in pending:
        previous = [row for row in cached_attachments if row['url'] == url and
                    row['attachment_parent'] == message['key'] and row.get('listing_version') == message['listing_version']]
        if previous:
            attachments.extend(previous)
            any_failure |= any(row.get('attachment_incomplete', False) for row in previous)
            continue
        try:
            if time.monotonic() > client.deadline - 1:
                any_failure = True
                break
            title = unquote(urlsplit(url).path.rsplit('/', 1)[-1])
            rows, incomplete = _document_rows(client, user, url, title, limit=4)
            attachments.extend({**row, 'key': f"{message['key']}:{row['key']}", 'attachment_parent': message['key'],
                                'version': message['version'], 'listing_version': message['listing_version'],
                                'attachment_incomplete': incomplete} for row in rows)
            any_failure |= incomplete
        except AnnouncementSessionExpired:
            raise
        except Exception:
            any_failure = True
    # Leave room for both the first document and later messages in the model input.
    sources = [*files, *overview, *recent, *attachments]
    return sources, any_failure, bool(files or attachments)
