"""Private file/link indexes; remote markup is never rendered or used as a fetch URL."""

import hashlib
import re
from urllib.parse import parse_qs, unquote, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from .course_announcements import announcement_forums, moodle_url


SOURCE_LABELS = {'course': '課程教材', 'assignment': '作業', 'folder': '課程教材',
                 'announcements': '公告', 'mail': '信件'}
FILE_TYPES = {
    'pdf': {'pdf'}, 'slides': {'ppt', 'pptx', 'odp', 'key'},
    'document': {'doc', 'docx', 'odt', 'txt', 'md', 'rtf'},
    'spreadsheet': {'xls', 'xlsx', 'csv', 'ods'},
    'archive': {'zip', 'rar', '7z', 'tar', 'gz'},
    'image': {'png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'heic'},
}
TOKEN_KEYS = {'sesskey', 'token', 'wstoken', 'moodlesession', 'access_token', 'auth', 'key'}


def safe_file_link(value, page_url):
    try:
        target = urlsplit(urljoin(page_url, str(value or '')))
        params = parse_qs(target.query, keep_blank_values=True)
        if (target.scheme != 'https' or not target.netloc or target.username or target.password
                or len(target.geturl()) > 2000 or re.search(r'[\x00-\x20\x7f]', target.geturl())
                or TOKEN_KEYS.intersection(key.casefold() for key in params)):
            return None
        if target.netloc == urlsplit(page_url).netloc:
            path = unquote(target.path)
            if '/pluginfile.php/' in path:
                if re.search(r'/(?:user|assignsubmission_[^/]+|assignfeedback_[^/]+)/', path):
                    return None
            elif path in {'/mod/resource/view.php', '/mod/url/view.php'}:
                return moodle_url(target.geturl(), page_url, path, 'id')
            else:
                return None
        return urlunsplit((target.scheme, target.netloc, target.path, target.query, ''))
    except ValueError:
        return None


def file_metadata(link):
    filename = unquote(urlsplit(link['url']).path.rsplit('/', 1)[-1])
    extension = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    extension = extension if re.fullmatch(r'[a-z0-9]{1,8}', extension) else ''
    if filename in {'view.php', 'pluginfile.php'}:
        filename, extension = '', ''
    title = re.sub(r'[\x00-\x1f\x7f]', '', str(link.get('title') or '')).strip()
    if not extension and re.search(r'\.[a-zA-Z0-9]{1,8}$', title):
        extension = title.rsplit('.', 1)[-1].lower()
    if not title or title in {'附件／圖片', '附件', '下載', 'Download'}:
        title = filename or '開啟連結'
    kind = next((key for key, suffixes in FILE_TYPES.items() if extension in suffixes), 'other' if extension else 'link')
    return {'title': title[:300], 'filename': filename[:300], 'type': kind, 'extension': extension.upper()}


def links_in(node, page_url):
    result = {}
    for link in node.select('a[href]'):
        url = safe_file_link(link.get('href'), page_url)
        if url:
            result.setdefault(url, {'url': url, 'title': link.get_text(' ', strip=True)[:300]})
    return list(result.values())[:101]


def course_materials(html, base_url, course):
    soup = BeautifulSoup(html, 'html.parser')
    main = soup.select_one('.course-content')
    if main is None:
        raise ValueError('Course content unavailable')
    for hidden in main.select('.accesshide, .sr-only'):
        hidden.decompose()
    page_url = f"{base_url}/course/view.php?id={course['id']}"
    links = links_in(main, page_url)
    children = {}
    for link in main.select('a[href]'):
        for kind, path in (('assignment', '/mod/assign/view.php'), ('folder', '/mod/folder/view.php')):
            url = moodle_url(link['href'], base_url, path, 'id', page_url=page_url)
            if not url:
                continue
            source_id = parse_qs(urlsplit(url).query)['id'][0]
            title_node = link.select_one('.instancename, .activityname') or link
            for hidden in title_node.select('.accesshide, .sr-only'):
                hidden.decompose()
            children[f'{kind}:{source_id}'] = {'kind': kind, 'url': url,
                'title': title_node.get_text(' ', strip=True)[:500] or SOURCE_LABELS[kind]}
    for url in announcement_forums(str(main), base_url):
        children['forum:' + parse_qs(urlsplit(url).query)['id'][0]] = {'kind': 'forum', 'url': url, 'title': '課程公告'}
    return links, children


def assignment_files(html, page_url):
    soup = BeautifulSoup(html, 'html.parser')
    intro = soup.select_one('#intro, [data-region="intro"], .assignment-intro, .activity-description')
    if intro is None:
        if soup.select_one('#region-main, [role="main"]'):
            return []
        raise ValueError('Assignment introduction unavailable')
    return links_in(intro, page_url)


def folder_files(html, page_url):
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.select_one('.foldertree, .filemanager, .foldercontent, [data-region="folder-content"]')
    if content is None:
        raise ValueError('Folder content unavailable')
    return links_in(content, page_url)


def file_index(sources, courses, messages, base_url, *, limit=3000):
    """One row per course/URL, retaining all origins instead of duplicate file rows."""
    catalog = {int(course['id']): course for course in courses}
    entries = {}
    live_messages = {(kind, item['key']) for kind, items in messages.items() for item in items if 'links' in item}
    source_list = [source for source in sources.values() if (source['kind'], source.get('key')) not in live_messages]
    for kind, items in messages.items():
        source_list.extend({**item, 'kind': kind, 'title': item['title'],
                            'source_key': f"{kind}:{item['key']}"} for item in items if 'links' in item)
    for source in source_list:
        course_id = int(source['course_id'])
        if course_id not in catalog:
            continue
        kind = source['kind']
        source_url = source['url']
        if kind in {'announcements', 'mail'}:
            source_url = '/courses/messages?' + ('tab=mail&' if kind == 'mail' else '') + f"semester={catalog[course_id]['semester_key']}&item={source['key']}"
        for link in source.get('links', []):
            url = safe_file_link(link.get('url'), base_url)
            if not url:
                continue
            # A force-download variant is the same Moodle file, not another attachment.
            target = urlsplit(url)
            identity = urlunsplit((target.scheme, target.netloc, target.path, '' if target.netloc == urlsplit(base_url).netloc and '/pluginfile.php/' in target.path else target.query, ''))
            key = hashlib.sha256(f'{course_id}:{identity}'.encode()).hexdigest()[:24]
            entry = entries.setdefault(key, {'id': key, 'url': url, **file_metadata({**link, 'url': url}),
                'course_id': course_id, 'course_title': catalog[course_id]['title'], 'origins': [], 'updated_ts': 0})
            origin = {'kind': 'course' if kind == 'folder' else kind,
                      'title': source['title'], 'url': source_url}
            if origin not in entry['origins']:
                entry['origins'].append(origin)
            entry['updated_ts'] = max(entry['updated_ts'], source.get('updated_ts') or 0)
    return sorted(entries.values(), key=lambda item: (-item['updated_ts'], item['course_title'], item['title']))[:limit]
