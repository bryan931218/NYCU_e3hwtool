import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';
import { semesterCourseTitles, register as registerCourses } from '../assignments/static/js/workbench/course-filter.js';
import { register as registerFilters, initialize as initializeFilters } from '../assignments/static/js/workbench/filter-state.js';
import { register as registerAssignments } from '../assignments/static/js/workbench/local-assignments.js';

function withDocument(document, callback) {
  const previous = globalThis.document;
  globalThis.document = document;
  try { return callback(); }
  finally {
    if (previous === undefined) delete globalThis.document;
    else globalThis.document = previous;
  }
}

function filterContext(preferences = {}) {
  const ctx = { USER_PREFERENCES: { ...preferences } };
  registerFilters(ctx);
  withDocument({ querySelectorAll: () => [] }, () => initializeFilters(ctx));
  return ctx;
}

test('course options only include the selected semester, including empty courses', () => {
  const courses = [
    { semester: '115-1', courseTitle: 'Current course' },
    { semester: '114-2', courseTitle: 'Old course' },
    { semester: '115-1', courseTitle: 'Current course' },
    { semester: '115-1', courseTitle: ' Empty current course ' },
    { semester: '115-1', courseTitle: '' },
  ];
  assert.deepEqual(semesterCourseTitles(courses, ['115-1']), ['Current course', 'Empty current course']);
  assert.deepEqual(semesterCourseTitles(courses, []), []);
});

test('changing semesters clears stale course selection and safely inserts option text', () => {
  const title = '<img src=x onerror=alert(1)>';
  const select = { options: [], replaceChildren(...options) { this.options = options; } };
  const ctx = { courseFilter: select, currentCourseFilter: 'Old course', currentSemesterFilters: ['115-1'] };
  registerCourses(ctx);
  withDocument({
    querySelectorAll: () => [
      { dataset: { semester: '114-2', courseTitle: 'Old course' } },
      { dataset: { semester: '115-1', courseTitle: title } },
      { dataset: { semester: 'custom', courseTitle: 'Personal' } },
    ],
    createElement: () => ({}),
  }, () => ctx.syncCourseFilter());
  assert.equal(ctx.currentCourseFilter, '');
  assert.deepEqual(select.options.map(option => option.value), ['', title, 'custom']);
  assert.equal(select.options[1].textContent, title);
  assert.equal(select.options[1].innerHTML, undefined);
});

test('semester preferences accept one valid semester and reject malformed values', () => {
  const ctx = filterContext({ semester_filter: ['115-1', '114-2'] });
  assert.deepEqual(ctx.currentSemesterFilters, ['115-1']);
  assert.deepEqual(ctx.normalizeSemesterFilters(['bad', '114-SUMMER', 'other']), ['114-summer']);
  assert.deepEqual(ctx.normalizeSemesterFilters(null), []);
});

test('status preferences handle defaults, old JSON values, and all-status selection', () => {
  const ctx = filterContext();
  assert.deepEqual(ctx.currentStatusFilters, ['pending']);
  assert.deepEqual(ctx.normalizeStatusFilters('["graded","graded","invalid"]'), ['graded']);
  assert.deepEqual(ctx.normalizeStatusFilters('all'), ctx.STATUS_FILTER_KEYS);
  assert.equal(ctx.isAllStatusFiltersSelected(ctx.normalizeStatusFilters('all')), true);
  assert.deepEqual(ctx.normalizeStatusFilters([]), []);
});

test('assignment sorting keeps pending work first and sorts each status by deadline', () => {
  const ctx = filterContext({ status_filter: ['pending', 'graded', 'completed'] });
  const rows = [
    { id: 'completed', dataset: { primaryStatus: 'completed', dueTs: '1' } },
    { id: 'later', dataset: { primaryStatus: 'pending', dueTs: '30' } },
    { id: 'graded', dataset: { primaryStatus: 'graded', dueTs: '2' } },
    { id: 'earlier', dataset: { primaryStatus: 'overdue', dueTs: '10' } },
  ];
  const ordered = [];
  ctx.sortAssignmentTable({ querySelectorAll: () => rows, appendChild: row => ordered.push(row.id) });
  assert.deepEqual(ordered, ['earlier', 'later', 'graded', 'completed']);
});

test('local assignments escape markup, preserve final status, and round-trip deadlines', () => {
  const ctx = {};
  registerAssignments(ctx);
  assert.equal(ctx.escapeHtml('<script>"&\'</script>'), '&lt;script&gt;&quot;&amp;&#39;&lt;/script&gt;');
  assert.equal(ctx.computePrimaryStatus({ dataset: { graded: '1', completed: '1' } }, 1), 'graded');
  assert.equal(ctx.computePrimaryStatus({ dataset: { completed: '1' } }, 1), 'completed');
  assert.equal(ctx.computePrimaryStatus({ dataset: {} }, 1), 'overdue');
  const deadline = ctx.parseDatetimeLocalToTs('2026-09-27T14:30');
  assert.equal(ctx.formatDatetimeLocalFromTs(deadline), '2026-09-27T14:30');
  assert.equal(ctx.parseDatetimeLocalToTs('invalid'), null);
});

function securityContext() {
  const handlers = {};
  const requests = [];
  const values = new Map([
    ['e3_remember', JSON.stringify({ username: 'student', password: 'old-password' })],
    ['e3_remember_session', 'old-moodle-session'],
  ]);
  class Form { submit() { this.submitted = true; } }
  const context = {
    URL, Request, Headers, HTMLFormElement: Form,
    location: { href: 'https://example.test/', origin: 'https://example.test' },
    localStorage: {
      getItem: key => values.get(key) ?? null,
      setItem: (key, value) => values.set(key, value),
      removeItem: key => values.delete(key),
    },
    document: {
      querySelector: () => ({ content: 'session-csrf' }),
      querySelectorAll: () => [],
      addEventListener: (name, handler) => { handlers[name] = handler; },
      createElement: () => ({}),
    },
    window: { fetch: (...args) => { requests.push(args); return Promise.resolve(); }, confirm: () => false },
  };
  vm.runInNewContext(readFileSync(new URL('../shared/static/js/security.js', import.meta.url), 'utf8'), context);
  return { context, handlers, requests, values, Form };
}

test('CSRF wrapper protects same-origin writes without sending tokens to other sites', async () => {
  const { context, requests } = securityContext();
  await context.window.fetch('/preferences', { method: 'POST' });
  assert.equal(requests[0][1].headers.get('X-CSRFToken'), 'session-csrf');
  await context.window.fetch('https://third-party.test/endpoint', { method: 'POST' });
  assert.equal(requests[1][1].headers, undefined);
  await context.window.fetch('/api/cache');
  assert.equal(requests[2][1].headers, undefined);
  await context.window.fetch(new Request('https://example.test/preferences', { method: 'PUT' }));
  assert.equal(requests[3][1].headers.get('X-CSRFToken'), 'session-csrf');
});

test('old remembered passwords and Moodle sessions are removed while retaining the account', () => {
  const { values } = securityContext();
  assert.deepEqual(JSON.parse(values.get('e3_remember')), { username: 'student' });
  assert.equal(values.has('e3_remember_session'), false);
});

test('dynamic form submission receives CSRF protection and button confirmation can cancel', () => {
  const { handlers, Form } = securityContext();
  const form = new Form();
  form.method = 'post';
  form.action = 'https://example.test/preferences';
  form.dataset = {};
  form.querySelector = () => null;
  form.append = field => { form.field = field; };
  form.submit();
  assert.equal(form.field.name, 'csrf_token');
  assert.equal(form.field.value, 'session-csrf');
  assert.equal(form.submitted, true);
  let cancelled = false;
  handlers.submit({ target: form, submitter: { dataset: { confirm: 'Confirm removal?' } },
    preventDefault: () => { cancelled = true; }, stopImmediatePropagation() {} });
  assert.equal(cancelled, true);
});
