"""Parse E3 dcpcmail inboxes separately from Moodle announcement forums."""

import re
from urllib.parse import parse_qs, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from .course_announcements import _timestamp, safe_message_content


MAIL_PATH = '/local/dcpcmail/view.php'
EMPTY_INBOX = re.compile(r'沒有(?:可查看的|任何)?郵件|無郵件|no (?:messages|mail)', re.I)


def mail_url(value, base_url, course_id, message_id=None):
    base = urlsplit(base_url)
    try:
        target = urlsplit(urljoin(base_url.rstrip('/') + MAIL_PATH, str(value or '')))
        params = parse_qs(target.query, keep_blank_values=True)
    except ValueError:
        return None
    if (target.scheme, target.netloc) != (base.scheme, base.netloc) or target.username or target.password or target.path != MAIL_PATH:
        return None
    ids = params.get('m', [])
    if len(ids) != 1 or not re.fullmatch(r'[1-9][0-9]{0,9}', ids[0]):
        return None
    if message_id is not None and ids[0] != str(message_id):
        return None
    if 'c' in params and params['c'] != [str(course_id)]:
        return None
    # Rebuild a read-only inbox URL; never retain remote action/session parameters.
    return urlunsplit((base.scheme, base.netloc, MAIL_PATH, f'c={int(course_id)}&t=inbox&m={ids[0]}', ''))


def mail_summaries(html, base_url, course):
    soup = BeautifulSoup(html, 'html.parser')
    inbox = soup.select_one('.mail_list')
    if inbox is None:
        raise ValueError('E3 mail inbox unavailable')
    items = {}
    rows = inbox.select('.mail_item')
    # E3 renders its empty-inbox notice as a mail_item without a message link.
    if len(rows) == 1 and not rows[0].select_one('a.mail_link') and EMPTY_INBOX.search(rows[0].get_text(' ', strip=True)):
        return []
    if not rows and not EMPTY_INBOX.search(inbox.get_text(' ', strip=True)):
        raise ValueError('Unrecognized E3 mail inbox')
    for row in rows:
        link = row.select_one('a.mail_link[href]')
        url = mail_url(link['href'], base_url, course['id']) if link else None
        subject = row.select_one('.mail_summary')
        if not url or not subject:
            raise ValueError('Unrecognized E3 mail row')
        for label in subject.select('.mail_label, .mail_course'):
            label.decompose()
        title = subject.get_text(' ', strip=True)[:500]
        if not title:
            raise ValueError('E3 mail subject unavailable')
        sender = row.select_one('.mail_users')
        date = row.select_one('.mail_date')
        if date and date.get('title'):
            date.string = date['title']
        message_id = parse_qs(urlsplit(url).query)['m'][0]
        items[message_id] = {
            'id': message_id, 'key': f"{course['id']}:{message_id}", 'title': title, 'url': url,
            'course_id': int(course['id']), 'course_title': course['title'], 'semester': course['semester_key'],
            'author': sender.get_text(' ', strip=True)[:120] if sender else '', 'updated_ts': _timestamp(date),
            'e3_read': 'mail_unread' not in row.get('class', []),
        }
    return list(items.values())[:30]


def mail_content(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    body = soup.select_one('.mail_content, #mail_content, .message-content')
    if body is None:
        # Never fall back to the entire page, which can contain unrelated private mail.
        raise ValueError('E3 mail content unavailable')
    attachments = soup.select('.mail_attachments a[href], .mail_attachment a[href], .attachments a[href]')
    return safe_message_content(body, base_url.rstrip('/') + MAIL_PATH, attachments)
