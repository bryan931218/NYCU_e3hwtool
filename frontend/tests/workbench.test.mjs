import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';
import { semesterCourseTitles, register as registerCourses } from '../assignments/static/js/workbench/course-filter.js';
import { register as registerFilters, initialize as initializeFilters } from '../assignments/static/js/workbench/filter-state.js';
import { register as registerAssignments } from '../assignments/static/js/workbench/local-assignments.js';
import { register as registerProfile } from '../assignments/static/js/workbench/profile.js';
import { calendarAssignments, taipeiDeadline, safeCalendarUrl, nextActionableAssignment, adjacentCalendarDay } from '../assignments/static/js/workbench/deadline-calendar.js';
import { register as registerGoogleCalendar } from '../assignments/static/js/workbench/calendar.js';
import { register as registerInteractions } from '../assignments/static/js/workbench/interactions.js';
import { register as registerWorkspaceFilters } from '../assignments/static/js/workbench/filters.js';
import { initialize as initializeAssignments } from '../assignments/static/js/workbench/local-assignments.js';
import { initialize as initializeSearch, isBrowserAutofilled } from '../assignments/static/js/workbench/search.js';
import { courseColorKey, createCourseColorRegistry, register as registerCourseColors } from '../assignments/static/js/workbench/course-colors.js';
import { initialize as initializeUsageEvents } from '../assignments/static/js/workbench/usage-events.js';

function trafficHarness(fetch, hidden = false) {
  const nodes = { online: { textContent: '1' }, total: { textContent: '20' } };
  const handlers = {};
  let interval;
  const document = { hidden, body: { dataset: { trafficStatsUrl: '/traffic/stats' } },
    addEventListener: (name, handler) => { handlers[name] = handler; },
    querySelectorAll: selector => selector.includes('online') ? [nodes.online] : [nodes.total] };
  const context = { document, fetch,
    setInterval: (callback, delay) => { assert.equal(delay, 45000); interval = callback; },
    window: { location: { reload: () => assert.fail('traffic must not reload the page') } } };
  vm.runInNewContext(readFileSync(new URL('../shared/static/js/traffic-stats.js', import.meta.url), 'utf8'), context);
  return { nodes, document, handlers, poll: () => interval() };
}

test('traffic versions update only count nodes without reloading or touching user input', async () => {
  const response = { ok: true, json: async () => ({ version: 999999, online: 8, total: 120 }) };
  const harness = trafficHarness(async (_url, options) => {
    assert.equal(options.cache, 'no-store');
    assert.equal(options.credentials, 'same-origin');
    return response;
  });
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(harness.nodes, { online: { textContent: '8' }, total: { textContent: '120' } });
  await harness.poll();
  assert.equal(harness.nodes.online.textContent, '8');
});

test('traffic polling pauses in background, avoids overlap and retains counts on network errors', async () => {
  let calls = 0;
  let resolve;
  const harness = trafficHarness(() => { calls++; return new Promise(done => { resolve = done; }); }, true);
  await harness.poll();
  assert.equal(calls, 0);
  harness.document.hidden = false;
  const pending = harness.handlers.visibilitychange();
  await harness.poll();
  assert.equal(calls, 1);
  resolve({ ok: false });
  await pending;
  assert.equal(harness.nodes.total.textContent, '20');
  const failing = trafficHarness(async () => { throw Error('offline'); });
  await new Promise(done => setImmediate(done));
  await failing.poll();
  assert.equal(failing.nodes.online.textContent, '1');
});

test('invalid traffic counts cannot replace the displayed values with markup or negative numbers', async () => {
  const harness = trafficHarness(async () => ({ ok: true, json: async () => ({ online: '<img>', total: -1 }) }));
  await new Promise(done => setImmediate(done));
  assert.deepEqual(harness.nodes, { online: { textContent: '1' }, total: { textContent: '20' } });
});

test('feature telemetry records view and controls without sending search text or assignment details', () => {
  const handlers = {};
  const calls = [];
  const ctx = { currentViewMode: 'calendar', logUiEvent: (...args) => calls.push(args) };
  withDocument({ addEventListener: (type, handler) => { handlers[type] = handler; } }, () => initializeUsageEvents(ctx));
  assert.deepEqual(calls, [['usage_calendar']]);
  handlers.click({ target: { closest: selector => selector.includes('[data-e3-assignment]') } });
  handlers.click({ target: { closest: selector => selector.includes('[data-restore-assignment]') } });
  handlers.input({ target: { matches: selector => selector === '#assignmentSearch', value: 'PRIVATE_QUERY' } });
  handlers.input({ target: { matches: selector => selector === '#assignmentSearch', value: 'PRIVATE_QUERY_NEXT_LETTER' } });
  handlers.input({ target: { matches: selector => selector === '#assignmentSearch', value: ' ' } });
  handlers.submit({ target: { id: 'customAssignmentForm' } });
  assert.deepEqual(calls.slice(1), [['usage_open_e3'], ['usage_ignore'], ['usage_search'], ['usage_custom_todo']]);
  assert.ok(!JSON.stringify(calls).includes('PRIVATE_QUERY'));
});

test('guests and admin readonly inspection do not create feature-use records', () => {
  for (const flags of [{ IS_GUEST: true }, { IS_READONLY_VIEW: true }]) {
    withDocument({ addEventListener: () => assert.fail('unexpected listener') }, () => {
      initializeUsageEvents({ ...flags, logUiEvent: () => assert.fail('unexpected telemetry') });
    });
  }
});

test('course colors stay unique beyond the base palette and deterministic across catalog order', () => {
  const keys = Array.from({ length: 80 }, (_, index) => `e3:${index + 1}`);
  const first = createCourseColorRegistry();
  const second = createCourseColorRegistry();
  first.ensure([...keys, ...keys]);
  second.ensure([...keys].reverse());
  const colors = keys.map(first.colorFor);
  assert.equal(new Set(colors).size, keys.length);
  assert.deepEqual(keys.map(second.colorFor), colors);
  first.ensure(['e3:new', ...keys.slice(10)]);
  assert.deepEqual(keys.map(first.colorFor), colors, 'a refresh or added course cannot recolor existing courses');
  assert.ok(!colors.includes(first.colorFor('e3:new')));
});

test('course IDs distinguish same-titled courses and custom labels use a separate namespace', () => {
  const row = { courseId: '1', course: 'Same course', uid: '1|Task|url', semester: '115-1' };
  assert.equal(courseColorKey(row), courseColorKey({ courseId: '1', courseTitle: 'Same course', semester: '115-1' }));
  assert.notEqual(courseColorKey(row), courseColorKey({ ...row, courseId: '2' }));
  assert.notEqual(courseColorKey(row), courseColorKey({ ...row, semester: 'custom', uid: 'custom|todo' }));
  assert.equal(courseColorKey({ ...row, courseId: undefined }), courseColorKey(row));
});

test('both assignment copies and empty courses are colored from the full catalog before filtering', () => {
  const entry = (courseId, course) => ({ dataset: { courseId, course },
    style: { value: '', setProperty(_name, value) { this.value = value; } } });
  const flat = entry('1', 'One');
  const grouped = entry('1', 'One');
  const other = entry('2', 'Two');
  const empty = entry('3', 'Empty');
  let elements = [flat, grouped, other, empty];
  const ctx = {};
  registerCourseColors(ctx);
  withDocument({ querySelectorAll: () => elements }, () => {
    ctx.syncCourseColors();
    assert.equal(flat.style.value, grouped.style.value);
    assert.equal(new Set([flat, other, empty].map(el => el.style.value)).size, 3);
    const initial = [flat, other, empty].map(el => el.style.value);
    elements = [other, empty, grouped, flat];
    ctx.syncCourseColors();
    assert.deepEqual([flat, other, empty].map(el => el.style.value), initial);
  });
});

test('search discards native autofill but preserves typing, paste and Enter without submitting', () => {
  const previousWindow = globalThis.window;
  const events = new Map();
  const windowEvents = new Map();
  const formEvents = new Map();
  let autofilled = true;
  const input = { value: '112550101', matches: () => autofilled,
    addEventListener: (name, fn) => events.set(name, fn),
    form: { addEventListener: (name, fn) => formEvents.set(name, fn) } };
  const queries = [];
  const ctx = { assignmentSearch: input, applyFilters: () => queries.push(ctx.currentAssignmentQuery) };
  globalThis.window = { addEventListener: (name, fn) => windowEvents.set(name, fn) };
  try {
    initializeSearch(ctx);
    assert.equal(input.value, '');
    assert.equal(ctx.currentAssignmentQuery, '');
    autofilled = false;
    input.value = ' Homework ABC ';
    events.get('input')();
    assert.equal(ctx.currentAssignmentQuery, 'homework abc');
    input.value = '貼上的作業名稱';
    events.get('input')();
    let prevented = false;
    formEvents.get('submit')({ preventDefault() { prevented = true; } });
    assert.equal(prevented, true);
    assert.equal(input.value, '貼上的作業名稱');
    windowEvents.get('pageshow')();
    assert.equal(input.value, '貼上的作業名稱');
    autofilled = true;
    input.value = '112550101';
    events.get('animationstart')({ animationName: 'assignment-search-autofill' });
    assert.equal(input.value, '');
    assert.ok(!queries.includes('112550101'));
    autofilled = false;
    input.value = '112550101';
    events.get('input')();
    assert.equal(input.value, '112550101', 'a deliberately typed number is a valid search');
    assert.equal(ctx.currentAssignmentQuery, '112550101');
  } finally { globalThis.window = previousWindow; }
});

test('autofill detection tolerates unsupported selectors and an absent search field', () => {
  assert.equal(isBrowserAutofilled({ matches: selector => {
    if (selector === ':autofill') throw new SyntaxError('Unsupported selector');
    return true;
  } }), true);
  assert.equal(isBrowserAutofilled({ matches() { throw new SyntaxError('Unsupported'); } }), false);
  initializeSearch({});
});

test('pending ignore applies to both lists, survives becoming overdue and can be restored', () => {
  const makeRow = (uid, primaryStatus) => {
    const classes = new Set();
    return { dataset: { uid, primaryStatus, semester: '115-1' }, style: { setProperty() {} },
      classList: { toggle: (key, on) => on ? classes.add(key) : classes.delete(key), contains: key => classes.has(key) } };
  };
  const flat = [makeRow('future', 'pending'), makeRow('past', 'overdue')];
  const course = [makeRow('future', 'pending'), makeRow('past', 'overdue')];
  const ctx = { USER_PREFERENCES: { ignored_assignment_uids: [] }, PREFERENCES_ENDPOINT: '/preferences',
    currentStatusFilters: ['pending', 'overdue'], currentSemesterFilters: ['115-1'],
    sortAssignmentTable() {}, pendingSummaryCount: {}, overdueSummaryCount: {} };
  registerWorkspaceFilters(ctx);
  ctx.updateCounts = () => ctx.updateDashboardOverview();
  ctx.renderIgnoredAssignmentsList = () => {};
  const payloads = [];
  const previousFetch = globalThis.fetch;
  globalThis.fetch = async (_url, options) => { payloads.push(JSON.parse(options.body)); };
  try {
    withDocument({
      querySelectorAll: selector => selector === '#flatTable tbody tr[data-uid]' ? flat
        : selector === '.courseTable tbody tr[data-uid]' ? course : [],
      querySelector: () => null, getElementById: () => null,
    }, () => {
      ctx.ignoreAssignment('future');
      for (const row of [flat[0], course[0]]) assert.equal(row.classList.contains('hidden'), true);
      assert.equal(ctx.pendingSummaryCount.textContent, '1');
      assert.equal(ctx.overdueSummaryCount.textContent, '1');
      assert.deepEqual(payloads[0], { ignoredAssignmentUids: ['future'] });
      flat[0].dataset.primaryStatus = course[0].dataset.primaryStatus = 'overdue';
      ctx.applyFilters();
      for (const row of [flat[0], course[0]]) assert.equal(row.classList.contains('hidden'), true);
      ctx.restoreIgnoredAssignment('future');
      for (const row of [flat[0], course[0]]) assert.equal(row.classList.contains('hidden'), false);
      ctx.ignoreAssignment('past');
      ctx.restoreAllIgnoredAssignments();
      assert.deepEqual(ctx.USER_PREFERENCES.ignored_assignment_uids, []);
      ctx.IS_READONLY_VIEW = true;
      ctx.ignoreAssignment('future');
      assert.deepEqual(ctx.USER_PREFERENCES.ignored_assignment_uids, []);
    });
  } finally { globalThis.fetch = previousFetch; }
});

test('legacy ignored preferences initialize under the generalized name without losing records', () => {
  const ctx = { USER_PREFERENCES: { ignored_overdue_uids: ['legacy'] }, loadJsonMap: () => ({}), loadJsonArray: () => [] };
  withDocument({ body: { dataset: {} } }, () => initializeAssignments(ctx));
  assert.deepEqual(ctx.USER_PREFERENCES.ignored_assignment_uids, ['legacy']);
  ctx.USER_PREFERENCES.ignored_assignment_uids = [];
  withDocument({ body: { dataset: {} } }, () => initializeAssignments(ctx));
  assert.deepEqual(ctx.USER_PREFERENCES.ignored_assignment_uids, []);
});

test('Session labels update safely after enrichment even without a surname', async () => {
  const previous = { document: globalThis.document, window: globalThis.window, fetch: globalThis.fetch };
  const avatar = { dataset: { profileUrl: '/api/profile' }, textContent: 'S' };
  const label = { textContent: 'Session-demo' };
  try {
    globalThis.document = { getElementById: id => ({ userAvatar: avatar, userAccountLabel: label })[id] };
    globalThis.window = { location: { href: 'https://e3.example/' } };
    globalThis.fetch = async () => ({ ok: true, json: async () => ({ ok: true, surname: '', account_label: '112550101（session登入）' }) });
    const ctx = {};
    registerProfile(ctx);
    await ctx.refreshUserAvatar();
    assert.equal(label.textContent, '112550101（session登入）');
    assert.equal(label.innerHTML, undefined);
    assert.equal(avatar.textContent, 'S');
    globalThis.fetch = async () => ({ ok: true, json: async () => ({ ok: true, surname: '王' }) });
    await ctx.refreshUserAvatar();
    assert.equal(label.textContent, '112550101（session登入）');
    assert.equal(avatar.textContent, '王');
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete globalThis[key];
      else globalThis[key] = value;
    }
  }
});

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

test('calendar preference survives initialization and view switching without exposing both lists', () => {
  assert.equal(filterContext({ view_mode: 'calendar' }).currentViewMode, 'calendar');
  const visible = new Set();
  const pressed = new Map();
  const view = key => ({ classList: { toggle: (_cls, hidden) => hidden ? visible.delete(key) : visible.add(key) } });
  const button = key => ({ classList: { toggle() {} }, setAttribute: (_key, value) => pressed.set(key, value) });
  let persisted;
  let filtered = 0;
  const ctx = {
    viewDue: view('due'), viewDueBtn: button('due'), viewCourse: view('course'), viewCourseBtn: button('course'),
    persistPreferences: value => { persisted = value; }, applyFilters: () => { filtered += 1; },
  };
  registerInteractions(ctx);
  withDocument({ getElementById: id => ({ viewCalendar: view('calendar'), viewCalendarBtn: button('calendar') })[id] }, () => {
    ctx.setView('calendar');
    assert.deepEqual([...visible], ['calendar']);
    assert.equal(pressed.get('calendar'), 'true');
    assert.equal(pressed.get('due'), 'false');
    assert.deepEqual(persisted, { viewMode: 'calendar' });
    ctx.setView('due', { skipPersist: true });
    assert.deepEqual([...visible], ['due']);
    assert.equal(filtered, 2);
  });
});

test('calendar deadlines consistently use Taipei including UTC day boundaries', () => {
  const seconds = Date.parse('2026-10-02T16:30:00Z') / 1000;
  assert.deepEqual(taipeiDeadline(seconds), { day: '2026-10-03', time: '00:30', iso: '2026-10-03T00:30:00+08:00' });
  assert.equal(taipeiDeadline(9999999999), null);
  assert.equal(taipeiDeadline('invalid'), null);
  assert.equal(taipeiDeadline(0), null);
});

test('calendar uses filtered rows once, preserves undated items and changed deadlines', () => {
  const row = (uid, dueTs, hidden = false, extra = {}) => ({
    dataset: { uid, dueTs, hasDue: '1', title: '<img onerror=alert(1)>', course: 'Course', ...extra },
    classList: { contains: () => hidden },
    querySelector: () => ({ getAttribute: () => 'https://e3p.nycu.edu.tw/mod/assign/view.php?id=1' }),
  });
  const later = Date.parse('2026-10-04T23:59:00+08:00') / 1000;
  const earlier = Date.parse('2026-10-03T23:59:00+08:00') / 1000;
  const rows = [row('later', later), row('earlier', earlier), row('later', later), row('hidden', earlier, true), row('undated', 9999999999, false, { hasDue: '0' })];
  const items = calendarAssignments(rows);
  assert.deepEqual(items.map(item => item.uid), ['earlier', 'later', 'undated']);
  assert.equal(items[0].deadline.day, '2026-10-03');
  assert.equal(items[2].deadline, null);
  assert.equal(items[0].title, '<img onerror=alert(1)>');
  assert.equal(items[0].courseKey, courseColorKey(rows[1].dataset));
  rows[0].dataset.dueTs = String(earlier);
  assert.equal(calendarAssignments(rows).find(item => item.uid === 'later').deadline.day, '2026-10-03');
});

test('calendar links reject script/data protocols and incomplete destinations', () => {
  const base = 'https://tracker.example/';
  for (const value of ['javascript:alert(1)', 'data:text/html,test', '#', '', 'https://[']) {
    assert.equal(safeCalendarUrl(value, base), '');
  }
  assert.equal(safeCalendarUrl('/assignments/1', base), 'https://tracker.example/assignments/1');
});

test('next deadline skips expired, completed, graded and undated tasks without reordering the source', () => {
  const item = (uid, dueTs, status = 'pending', deadline = { day: '2026-10-04' }) => ({ uid, dueTs, status, deadline, title: uid });
  const items = [item('later', 300), item('expired', 99), item('completed', 101, 'completed'),
    item('graded', 102, 'graded'), item('overdue', 103, 'overdue'), item('undated', Infinity, 'pending', null), item('next', 100)];
  assert.equal(nextActionableAssignment(items, 100).uid, 'next');
  assert.equal(items[0].uid, 'later');
  assert.equal(nextActionableAssignment(items, 301), null);
  assert.equal(nextActionableAssignment([], 100), null);
});

test('calendar keyboard navigation crosses month, year and leap-day boundaries', () => {
  assert.equal(adjacentCalendarDay('2026-10-01', -1), '2026-09-30');
  assert.equal(adjacentCalendarDay('2026-12-31', 1), '2027-01-01');
  assert.equal(adjacentCalendarDay('2028-03-01', -1), '2028-02-29');
  assert.equal(adjacentCalendarDay('2026-10-04', 7), '2026-10-11');
});

test('Google sync remains available when filtered table rows are hidden behind calendar', () => {
  const ctx = { currentViewMode: 'calendar', STATUS_FILTER_LABELS: { pending: '待處理' } };
  registerGoogleCalendar(ctx);
  const rows = [{ dataset: { uid: 'task', hasDue: '1', title: 'Task', due: '2026-10-03', primaryStatus: 'pending' }, classList: { contains: () => false }, offsetParent: null }];
  withDocument({ querySelectorAll: () => rows }, () => assert.equal(ctx.collectVisibleAssignments().length, 1));
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
