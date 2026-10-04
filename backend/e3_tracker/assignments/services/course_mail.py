"""Bounded, own-account course mail synchronization using the existing Moodle session."""

import requests

from e3_tracker.assignments.domain.course_mail import MAIL_PATH, mail_content, mail_summaries, mail_url
from e3_tracker.assignments.persistence.course_mail import CourseMailStorage
from .course_announcements import AnnouncementSessionExpired, CourseAnnouncementService


def collect_course_mail(client, courses):
    items, successful, failed = [], [], []
    for course in courses:
        course_id = int(course['id'])
        try:
            html = client.get(f'{client.base_url}{MAIL_PATH}?c={course_id}&t=inbox')
            items.extend(mail_summaries(html, client.base_url, course))
            successful.append(course_id)
        except AnnouncementSessionExpired:
            raise
        except (requests.RequestException, ValueError, TimeoutError):
            failed.append(course_id)
    return items, successful, failed


class CourseMailService(CourseAnnouncementService):
    message_name = '信件'

    def __init__(self, storage, base_url, timeout=8, *, slots):
        super().__init__(CourseMailStorage(storage), base_url, timeout)
        self.slots = slots

    def collect(self, client, courses):
        return collect_course_mail(client, courses)

    def item_url(self, item):
        return mail_url(item['url'], self.base_url, item['course_id'], item['id'])

    def parse_content(self, html):
        return mail_content(html, self.base_url)
