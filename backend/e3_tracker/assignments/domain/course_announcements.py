"""Parse Moodle news forums without rendering remote HTML or following arbitrary URLs."""

import re
from datetime import datetime
from urllib.parse import parse_qs, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from e3_tracker.assignments.domain.parsing import parse_due_text_to_dt


def moodle_url(value, base_url, path, parameter, *, page_url=None):
    base = urlsplit(base_url)
    try:
        target = urlsplit(urljoin(page_url or base_url.rstrip('/') + '/', str(value or '')))
    except ValueError:
        return None
    if (target.scheme, target.netloc) != (base.scheme, base.netloc) or target.username or target.password:
        return None
    if target.path != path:
        return None
    values = parse_qs(target.query).get(parameter, [])
    if len(values) != 1 or not re.fullmatch(r'[1-9][0-9]{0,9}', values[0]):
        return None
    # Strip action parameters, sesskeys, fragments, and tracking data.
    return urlunsplit((base.scheme, base.netloc, path, f'{parameter}={values[0]}', ''))


def announcement_forums(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    forums = []
    for link in soup.select('a[href]'):
        url = moodle_url(link['href'], base_url, '/mod/forum/view.php', 'id', page_url=base_url.rstrip('/') + '/course/view.php')
        if not url:
            continue
        text = link.get_text(' ', strip=True)
        # Do not treat student discussion/Q&A forums as course announcements.
        name = re.sub(r'\s+', ' ', text).casefold()
        in_news_block = link.find_parent(class_=lambda value: value and 'block_news_items' in value)
        if not (re.search(r'公告|最新消息|佈告|布告|announcements?|news forum', name) or in_news_block):
            continue
        if url not in forums:
            forums.append(url)
    return forums[:3]


def _timestamp(node):
    if not node:
        return None
    time_node = node.select_one('time[datetime], time[data-timestamp]')
    if time_node:
        try:
            if time_node.get('data-timestamp'):
                return int(time_node['data-timestamp'])
            parsed = datetime.fromisoformat(time_node['datetime'].replace('Z', '+00:00'))
            if parsed.tzinfo:
                return int(parsed.timestamp())
        except (ValueError, KeyError):
            pass
    text = node.get_text(' ', strip=True)
    # Chinese Moodle dates are not understood by dateutil without normalization.
    match = re.search(r'(20\d{2})[年/-]\s*(\d{1,2})[月/-]\s*(\d{1,2})日?[^\d]*(\d{1,2}):(\d{2})', text)
    if match:
        text = f'{match[1]}-{match[2]}-{match[3]} {match[4]}:{match[5]}'
    elif not re.search(r'20\d{2}|\d{1,2}\s+[A-Za-z]+\s+20\d{2}', text):
        return None
    parsed = parse_due_text_to_dt(text)
    return int(parsed.timestamp()) if parsed else None


def discussion_summaries(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    items = {}
    for link in soup.select('a[href]'):
        url = moodle_url(link['href'], base_url, '/mod/forum/discuss.php', 'd', page_url=base_url.rstrip('/') + '/mod/forum/view.php')
        if not url or not link.get_text(' ', strip=True):
            continue
        row = link.find_parent('tr') or link.find_parent(class_='forumpost')
        if row is None:
            continue
        subject = row.select_one('.subject, [data-region="post-subject"], [data-region-content="forum-post-core-subject"]')
        title = (subject.get_text(' ', strip=True) if subject else link.get('title') or link.get_text(' ', strip=True))[:500]
        if url in items:
            continue
        author = row.select_one('.author a, [data-region="author-name"], header a[href*="/user/view.php"], header a[href*="/user/profile.php"]')
        date = row.select_one('.lastpost') or row
        items[url] = {
            'id': parse_qs(urlsplit(url).query)['d'][0], 'title': title, 'url': url,
            'author': author.get_text(' ', strip=True)[:120] if author else '',
            'updated_ts': _timestamp(date),
        }
    return list(items.values())[:30]


def discussion_content(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    post = soup.select_one('.forumpost.firstpost') or soup.select_one('.forumpost, [data-region="post"]')
    if not post:
        raise ValueError('Announcement post unavailable')
    # The outer article can contain nested replies; limit parsing to its own core.
    post = post.select_one('.forumpost') or post
    content = post.select_one('[data-region="post-content"], .post-content-container, [id^="post-content-"], .posting, .post-content, .content .no-overflow')
    if not content:
        raise ValueError('Announcement content unavailable')
    for node in content.select('script, style, iframe, form, input, button, object, embed'):
        node.decompose()
    links = []
    attachments = post.select('.attachments a[href], [data-region="attachments"] a[href], .content-alignment-container a[href*="/pluginfile.php/"], .attachedimages img[src]')
    for link in [*content.select('a[href], img[src]'), *attachments]:
        value = link.get('href') or link.get('src')
        try:
            target = urlsplit(urljoin(base_url.rstrip('/') + '/mod/forum/discuss.php', value))
        except ValueError:
            continue
        if target.scheme != 'https' or not target.netloc or target.username or target.password:
            continue
        # Never expose Moodle action/session tokens in outbound links.
        if any(key.casefold() in {'sesskey', 'token', 'wstoken', 'moodlesession'} for key in parse_qs(target.query, keep_blank_values=True)):
            continue
        url = urlunsplit(target)
        if len(url) <= 2000 and url not in {entry['url'] for entry in links}:
            links.append({'url': url, 'title': (link.get_text(' ', strip=True) or link.get('alt') or '附件／圖片')[:200]})
    for node in content.select('br'):
        node.replace_with('\n')
    for node in content.select('p, div, li, h1, h2, h3, h4, blockquote, tr'):
        node.insert_before('\n')
        node.insert_after('\n')
    text = re.sub(r'\n[ \t]*\n(?:[ \t]*\n)+', '\n\n', content.get_text()).strip()
    author = post.select_one('[data-region="author-name"], .author a, header a[href*="/user/view.php"], header a[href*="/user/profile.php"]')
    return {
        'content': text[:20000], 'links': links[:20],
        'author': author.get_text(' ', strip=True)[:120] if author else '',
    }
