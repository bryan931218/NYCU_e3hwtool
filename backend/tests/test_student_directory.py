"""Display metadata is degree-aware, evidence-labelled and never an auth identity."""

import json
import os
import tempfile
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from bs4 import BeautifulSoup
from sqlalchemy import select, text, delete

from e3_tracker.assignments.domain.student_directory import academic_year, student_information, build_student_directory
from e3_tracker.assignments.services.profile import parse_profile_name, parse_profile_metadata, fetch_session_identity
from e3_tracker.assignments.services.admin_analytics import analytics_window, build_assignment_analytics
from e3_tracker.platform.application import create_app
from e3_tracker.platform.persistence.core_schema import users_table, web_sessions_table
from tests.security_helpers import csrf_client

NOW = datetime(2026, 10, 25)
MENU = '<div class="usermenu"><a href="/user/profile.php?id=42">關於我</a></div>'
PROFILE = '<header id="page-header"><h1>個人資料</h1></header><a href="/user/edit.php?id=42">編輯</a>'
FORM = '<form action="/user/edit.php"><input name="id" type="hidden" value="42"><input name="firstname" value="小明"><input name="lastname" value="王"></form>'


class StudentDirectoryTests(unittest.TestCase):
    def test_all_five_degrees_decode_the_same_admission_year(self):
        for prefix, degree, grade in [('1','學士','大一'),('2','學士後','學士後一'),('3','碩士','碩一'),
                                       ('4','博士','博一'),('5','碩士在職專班','在職碩一')]:
            info = student_information(prefix+'15550001', now=NOW)
            self.assertEqual((info['degree'], info['admission_year'], info['grade']), (degree,115,grade+'（推估）'))
            self.assertEqual(info['department'], '資訊工程學系')

    def test_taipei_academic_year_boundary_and_older_cohorts(self):
        self.assertEqual(academic_year(datetime(2026,7,31,15,59,tzinfo=timezone.utc)), 114)
        self.assertEqual(academic_year(datetime(2026,7,31,16,0,tzinfo=timezone.utc)), 115)
        self.assertEqual(student_information('112550001',now=NOW)['grade'], '大四（推估）')
        self.assertEqual(student_information('111550001',now=NOW)['grade'], '學士入學第 5 年（推估）')
        self.assertEqual(student_information('110101001',now=NOW)['grade'], '大六（推估）')
        self.assertEqual(student_information('116550001',now=NOW)['grade'], '116 學年度入學')
        for number in ['108550001','117550001','012550001','11255001','Session-abc','１１５５５０００１','']:
            self.assertEqual(student_information(number,now=NOW)['grade_source'], 'unknown')

    def test_unknown_codes_and_known_reorganizations_are_not_guessed(self):
        self.assertEqual(student_information('515607023',now=NOW)['department'], '未核對系所（607）')
        self.assertEqual(student_information('312555001',now=NOW)['department'], '資通安全碩士學位學程')
        self.assertEqual(student_information('313555001',now=NOW)['department'], '資訊安全研究所')
        self.assertEqual(student_information('112610001',now=NOW)['department_previous'], '奈米科學及工程學士學位學程')
        self.assertEqual(student_information('316832001',now=NOW)['department_source'], 'unknown')

    def test_current_explicit_e3_fields_override_inference_but_stale_grade_does_not(self):
        info = student_information('112550001',{'department':'電機系','grade':2,'degree_code':'1','academic_year':115},now=NOW)
        self.assertEqual((info['department'],info['grade'],info['grade_source']), ('電機工程學系','大二','e3'))
        for details in [{'grade':2,'academic_year':114}, {'grade':2,'academic_year':115,'degree_code':'3'},
                        {'grade':True,'academic_year':115}, {'grade':0,'academic_year':115}]:
            self.assertEqual(student_information('112550001',details,now=NOW)['grade'], '大四（推估）')
        unsupported = student_information('unrecognized',{'grade':2,'degree_code':'3','academic_year':115},now=NOW)
        self.assertEqual(unsupported['grade'], '碩二')
        self.assertEqual(student_information('115350001',{'department':'生科系'},now=NOW)['department'],'生科系',
                         'An ambiguous E3 abbreviation must not be expanded to the other campus department')

    def test_aliases_merge_latest_available_fields_without_relabelling_stale_grade(self):
        rows = [{'username':'112550001','profile_metadata':json.dumps({'grade':2,'academic_year':114,'department':'資工系','checked_at':1})},
                {'username':'Session-one','student_number':'112550001','profile_metadata':json.dumps({'department':'電機系','checked_at':2,'reason':'name_missing'})}]
        directory = build_student_directory(rows,now=NOW)
        self.assertEqual(list(directory), ['112550001'])
        self.assertEqual(directory['112550001']['department'], '電機工程學系')
        self.assertEqual(directory['112550001']['grade'], '大四（推估）')
        self.assertIn('姓名', directory['112550001']['sync_status'])

    def test_failed_alias_retry_does_not_make_old_academic_fields_newer(self):
        rows=[{'username':'112550001','profile_metadata':{'department':'資工系','department_checked_at':10,
                'grade':1,'academic_year':114,'grade_checked_at':10,'checked_at':30,'reason':'login_required'}},
              {'username':'Session-one','student_number':'112550001','profile_metadata':{'department':'電機系',
                'grade':2,'academic_year':115,'grade_checked_at':20,'department_checked_at':20,'checked_at':20}}]
        info=build_student_directory(rows,now=NOW)['112550001']
        self.assertEqual((info['department'],info['grade']),('電機工程學系','大二'))


class ProfileFieldTests(unittest.TestCase):
    def test_person_fields_and_unicode_names_do_not_capture_units_or_degree_labels(self):
        for value in ['𠮷小明', '歐陽阿美族名字', 'Nguyễn Văn An', '王小明 / 藥學系大一']:
            html = '<header id="page-header"><h2>'+value+'</h2></header>'
            expected = '王小明' if '/' in value else value
            self.assertEqual(parse_profile_name(html),expected)
        for value in ['藥學系大一','學士後','碩士班','研究所','王小明 李小明']:
            self.assertEqual(parse_profile_name('<header id="page-header"><h1>'+value+'</h1></header>'), '')
        self.assertEqual(parse_profile_name('<div class="userprofile"><dl><dt>姓名</dt><dd>王小明</dd></dl></div>'+PROFILE),'王小明')

    def test_metadata_only_comes_from_explicit_profile_fields(self):
        html = '<div class="profile_tree"><dl><dt>Department</dt><dd>資訊工程學系</dd><dt>年級</dt><dd>大三</dd><dt>電子郵件</dt><dd>private@example.test</dd></dl></div>'
        result = parse_profile_metadata(html)
        self.assertEqual(result, {'department':'資訊工程學系','grade':3,'academic_year':academic_year(),'degree_code':'1'})
        self.assertEqual(parse_profile_metadata('<div class="description">資工系 / 大三</div><dl><dt>年級</dt><dd>大四</dd></dl>'),{})

    def test_own_edit_names_can_be_editable_but_owner_must_be_verified(self):
        for form, expected in [(FORM,'王小明'), (FORM.replace('value="王"','value="藥學系"').replace('value="小明"','value="王小明"'),'王小明'),
                               (FORM.replace('name="id" type="hidden" value="42"','name="id" type="hidden" value="43"'),''),
                               (FORM.replace('</form>','<input name="firstname" value="李小明"></form>'),'')]:
            with patch('e3_tracker.assignments.services.profile.safe_request',side_effect=[Mock(text=html,status_code=200) for html in [MENU,PROFILE,form]]) as get:
                identity = fetch_session_identity(Mock(),'https://e3.example',include_number=False)
                self.assertEqual(identity['name'],expected)
                self.assertTrue(all(call.args[1] == 'GET' for call in get.call_args_list))
                self.assertEqual(identity['student_number'],'')


class ProfileMetadataPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {'E3_ENV':'development','E3_CACHE_DIR':self.directory.name,
            'E3_DATABASE_URL':'','DATABASE_URL':'','E3_CANONICAL_HOST':'','E3_SESSION_COOKIE_SECURE':'0',
            'E3_SESSION_PROFILE_WORKER':'0','E3_NOTIFICATIONS_WORKER':'0','E3_YOUTUBE_AUTO_SYNC_ENABLED':'0',
            'E3_GA4_MEASUREMENT_ID':'','E3_GA4_PROPERTY_ID':'','E3_GA4_SERVICE_ACCOUNT_JSON':''})
        self.environment.start()
        self.app = create_app(default_base_url='https://e3.example')
        self.storage = self.app.extensions['e3_storage']
        self.sync = self.app.extensions['e3_session_identity']
        self.client = csrf_client(self.app)
        self.storage.save_web_session('directory-token','115550001',moodle_session='fixture-cookie',is_admin=True)
        self.storage.save_user_profile('115550001','王小明','王')
        with self.client.session_transaction() as session:
            session['session_token'] = 'directory-token'

    def tearDown(self):
        self.sync.stop.set()
        self.storage._engine.dispose()
        self.environment.stop()
        self.directory.cleanup()

    def metadata(self,username='115550001'):
        with self.storage._engine.connect() as conn:
            return json.loads(conn.execute(select(users_table.c.profile_metadata).where(users_table.c.username==username)).scalar() or '{}')

    def test_whitelist_preserves_grade_year_pair_and_ignores_guest_or_deleted_users(self):
        self.storage.save_profile_metadata('115550001',{'department':'資工系','grade':3,'academic_year':114,'reason':'success','checked_at':1,
            'html':'secret','cookie':'secret','name':'secret'})
        self.storage.save_profile_metadata('115550001',{'academic_year':115,'reason':'raw-private-error','checked_at':2})
        details = self.metadata()
        self.assertEqual(details,{'department':'資工系','grade':3,'academic_year':114,'reason':'identity_missing','checked_at':2,
                                  'department_checked_at':1,'grade_checked_at':1})
        self.storage.save_profile_metadata('deleted',{'department':'資工系'})
        self.storage.save_web_session('guest-token','訪客_fixture',is_guest=True)
        self.storage.save_profile_metadata('訪客_fixture',{'department':'資工系'})
        self.assertEqual(self.metadata('deleted'),{})
        self.assertEqual(self.metadata('訪客_fixture'),{})
        self.assertEqual(self.storage.load_user_profile('115550001'),{'name':'王小明','surname':'王'})

    def test_priority_queue_reads_latest_credentials_and_is_bounded_deduplicated(self):
        for _ in range(3):
            self.sync.schedule_user('115550001')
        self.sync.schedule_user('訪客_fixture')
        self.assertEqual(list(self.sync._pending),['115550001'])
        self.storage.save_web_session('directory-token','115550001',moodle_session='newest-fixture-cookie',is_admin=True)
        def read(session,*args,**kwargs):
            self.assertEqual(session.cookies.get('MoodleSession'),'newest-fixture-cookie')
            return {'name':'王小明','student_number':''}
        with patch('e3_tracker.assignments.services.session_identity.fetch_session_identity',side_effect=read) as fetch:
            self.sync.backfill_once()
            fetch.assert_called_once()
        self.assertEqual(self.metadata()['reason'],'success')
        self.assertEqual(list(self.sync._pending),[])
        for n in range(120):
            self.sync.schedule_user('Session-'+str(n))
        self.assertEqual(len(self.sync._pending),100)

    def test_failure_diagnostics_and_explicit_metadata_survive_without_raw_errors(self):
        def read(*args,profile_metadata,diagnostics,**kwargs):
            profile_metadata.update(department='資工系',grade=1,academic_year=academic_year(),degree_code='1')
            diagnostics['reason']='name_missing'
            return {'name':'','student_number':''}
        with patch('e3_tracker.assignments.services.session_identity.fetch_session_identity',side_effect=read):
            self.sync.refresh_user(self.storage.load_web_session('directory-token'),force=True)
        self.assertEqual(self.metadata()['reason'],'name_missing')
        self.assertEqual(self.metadata()['department'],'資工系')
        self.assertFalse(self.storage.load_web_session('directory-token')['is_guest'])

    def test_new_password_login_queues_enrichment_without_blocking_on_profile_fetch(self):
        with self.client.session_transaction() as session:
            session.clear()
        self.storage.save_user_cache('115550002',{'result':{'courses':[]},'ts':time.time()})
        def login(sess,*args,**kwargs):
            sess.cookies.set('MoodleSession','login-fixture')
        with patch('e3_tracker.assignments.routes.assignments.login_with_password',side_effect=login), \
                patch('e3_tracker.assignments.services.session_identity.fetch_session_identity') as fetch:
            response = self.client.post('/login',data={'username':'115550002','password':'fixture-password'})
        self.assertEqual(response.status_code,302)
        fetch.assert_not_called()
        self.assertIn('115550002',self.sync._pending)

    def test_admin_displays_named_distributions_and_metadata_stays_private(self):
        self.storage.save_profile_metadata('115550001',{'department':'資工系','grade':1,'academic_year':academic_year(),'reason':'success'})
        self.storage.save_user_surname('315607001','林')
        self.storage.save_profile_metadata('315607001',{'reason':'name_missing'})
        page = BeautifulSoup(self.client.get('/admin/traffic').get_data(as_text=True),'html.parser')
        self.assertNotIn('系所／年級',page.select_one('.account-usage-table').get_text())
        self.assertIn('學號／帳號',page.select_one('.account-usage-table').get_text())
        self.assertIn('E3 姓名欄位未取得',page.select_one('.account-usage-table').get_text())
        self.assertIn('資訊工程學系',page.select_one('[data-distribution="department"]').get_text())
        self.assertIn('年級分布',page.select_one('[data-distribution="grade"]').get_text())
        self.assertNotIn('315 入學',page.get_text())
        self.assertNotIn('profile_metadata',self.client.get('/api/profile').get_data(as_text=True))
        self.storage.save_web_session('directory-token','115550001',moodle_session='fixture-cookie',is_admin=False)
        self.assertEqual(self.client.get('/admin/traffic').status_code,302)

    def test_alias_stats_do_not_double_count_and_education_override_keeps_code(self):
        self.storage.save_web_session('directory-alias','Session-alias')
        self.storage.save_student_number('Session-alias','115550001')
        self.storage.save_profile_metadata('Session-alias',{'department':'電機系','grade':2,'academic_year':academic_year(),'checked_at':time.time()})
        window=analytics_window({})
        result=build_assignment_analytics(self.storage.assignment_usage_snapshot(window['start'],window['end']),window)
        self.assertEqual(result['identified'],1)
        self.assertEqual([(r['code'],r['name'],r['count']) for r in result['departments']],[('550','電機工程學系',1)])
        self.assertEqual(result['grades'][0]['e3_count'],1)
