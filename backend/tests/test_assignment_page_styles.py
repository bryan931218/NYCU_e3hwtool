"""Assignment subpages share theme assets without changing their form contracts."""

from html.parser import HTMLParser
import unittest

from bs4 import BeautifulSoup
from flask import Flask, render_template
from e3_tracker.platform.assets import configure_frontend


class BalancedMarkup(HTMLParser):
    VOID = {'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr'}

    def __init__(self):
        super().__init__()
        self.stack = []

    def handle_starttag(self, tag, attrs):
        if tag not in self.VOID:
            self.stack.append(tag)

    def handle_startendtag(self, tag, attrs):
        pass

    def handle_endtag(self, tag):
        if not self.stack or self.stack.pop() != tag:
            raise AssertionError(f'Mismatched closing tag: {tag}')


class AssignmentPageStyleTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = 'page-style-test'
        configure_frontend(self.app)
        self.app.jinja_env.globals.update(
            url_for=lambda endpoint, **args: '/assets/' + args['filename'] if endpoint == 'frontend_asset' else '/' + endpoint,
            csp_nonce=lambda: 'test-nonce', csrf_token=lambda: 'test-csrf',
        )
        self.context = dict(
            admin_user={'username': 'test-admin'}, stats={'online': 1, 'total': 20},
            app_home_url='/', password_login_failed=True,
            legal_entity_name='Test', effective_date='2026-10-06', google_scope='https://example.test/calendar',
            announcements=[dict(id='one', title='<script>unsafe</script>', content='公告內容', author='Test')],
            feedback_entries=[dict(id=1, username='Test', email='test@example.test', message='<script>unsafe</script>',
                                   created_label='2026-10-06', status='open')], open_count=1,
            windows=[1, 7, 30], days=7, retention_days=30, generated_at='2026-10-06',
            total_views=20, unique_visitors=5, avg_views=4, bot_views=0,
            top_page=None, top_source=None, top_pages=[], top_sources=[], devices=[], browsers=[], recent=[],
            series=[dict(date='2026-10-06', count=20, visitors=5, width=50, label='10/06')],
            item=dict(course_title='Test course', title='HW1', due_at='2026-10-10'),
            plan_config={'google_linked': False}, error='',
        )

    def render(self, path, **changes):
        with self.app.test_request_context('/'):
            source = render_template(path, **{**self.context, **changes})
        parser = BalancedMarkup()
        parser.feed(source)
        self.assertEqual(parser.stack, [])
        return BeautifulSoup(source, 'html.parser')

    def test_subpages_share_tokens_headers_and_valid_markup(self):
        paths = ('shared/admin_announcements.html', 'shared/admin_feedback.html', 'shared/admin_page_analytics.html',
                 'shared/feedback.html', 'shared/privacy.html', 'shared/terms.html', 'assignments/pages/assignment_plan.html')
        for path in paths:
            with self.subTest(path=path):
                page = self.render(path)
                self.assertIsNotNone(page.select_one('main .page-header'))
                self.assertIsNotNone(page.select_one('[data-e3-theme][type="button"]'))
                self.assertEqual(len(page.select('h1')), 1)
                self.assertIsNotNone(page.select_one('link[href="/assets/shared/css/tokens.css"]'))
                self.assertIsNotNone(page.select_one('link[href="/assets/shared/css/e3-pages.css"]'))
                self.assertIsNotNone(page.select_one('script[src="/assets/shared/js/theme-init.js"][nonce="test-nonce"]'))
                self.assertIsNone(page.select_one('style'))

    def test_admin_mutations_keep_csrf_confirmation_and_escaped_text(self):
        page = self.render('shared/admin_announcements.html')
        self.assertIsNotNone(page.select_one('input[name="title"][required]'))
        self.assertIsNotNone(page.select_one('textarea[name="content"][required]'))
        self.assertIsNotNone(page.select_one('form[data-confirm]'))
        self.assertEqual(page.select_one('.item h3').text, '<script>unsafe</script>')
        page = self.render('shared/admin_feedback.html')
        self.assertEqual(page.select_one('.feedback-content').text, '<script>unsafe</script>')
        self.assertEqual(page.select_one('.status').text, '未處理')
        self.assertIsNotNone(page.select_one('input[name="status"][value="resolved"]'))
        for form in page.select('form[method="post"]'):
            self.assertEqual(form.select_one('input[name="csrf_token"]')['value'], 'test-csrf')

    def test_login_and_plan_preserve_modes_guidance_and_empty_states(self):
        page = self.render('assignments/login.html')
        self.assertIsNotNone(page.select_one('body.login-page'))
        self.assertIsNotNone(page.select_one('link[href="/assets/assignments/css/login.css"]'))
        self.assertEqual(len(page.select('[data-mode]')), 2)
        self.assertIsNotNone(page.select_one('#passwordLoginFailure'))
        self.assertIn('不想直接輸入帳號密碼', page.select_one('#sessionUseHint').text)
        self.assertIsNotNone(page.select_one('#loginForm input[name="csrf_token"]'))
        page = self.render('assignments/pages/assignment_plan.html')
        self.assertIsNotNone(page.select_one('#planGoogle[disabled]'))
        page = self.render('assignments/pages/assignment_plan.html', item=None, error='找不到作業')
        self.assertEqual(page.select_one('[role="alert"]').text, '找不到作業')
        self.assertIsNone(page.select_one('#assignmentPlanForm'))
        self.assertIsNotNone(self.render('shared/admin_announcements.html', announcements=[]).select_one('.empty'))
