import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizeE3Target, createPendingNavigation, initialize, PENDING_KEY, PENDING_TTL_MS }
  from '../assignments/static/js/workbench/e3-navigation.js';

const baseUrl = 'https://e3p.nycu.edu.tw';
const assignment = baseUrl + '/mod/assign/view.php?id=42&action=viewsubmission#submission';
const owner = JSON.stringify(['student', 'student']);
function storageFixture() {
  const values = new Map();
  return { getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value), removeItem: (key) => values.delete(key) };
}

test('retains the precise E3 page, query and fragment including configured Moodle prefixes', () => {
  assert.equal(normalizeE3Target(assignment, baseUrl), assignment);
  assert.equal(normalizeE3Target('/mod/quiz/view.php?id=7', baseUrl), baseUrl + '/mod/quiz/view.php?id=7');
  assert.equal(normalizeE3Target('mod/assign/view.php?id=9', 'https://example.test/moodle'),
    'https://example.test/moodle/mod/assign/view.php?id=9');
  assert.equal(normalizeE3Target('/course/view.php?id=6', baseUrl), baseUrl + '/course/view.php?id=6');
});

test('rejects other origins, credentials, actions, malformed IDs and unsafe URL forms', () => {
  for (const target of [
    'https://e3p.nycu.edu.tw.attacker.test/mod/assign/view.php?id=42',
    '//attacker.test/mod/assign/view.php?id=42',
    'https://user@e3p.nycu.edu.tw/mod/assign/view.php?id=42',
    'http://e3p.nycu.edu.tw/mod/assign/view.php?id=42',
    'javascript:alert(1)', '/login/logout.php?id=42', '/mod/assign/view.php',
    '/mod/assign/view.php?id=0', '/mod/assign/view.php?id=42&id=43',
    '/mod/assign/view.php?id=x', '/mod/assign/view.php?id=42\n',
    'https:\\e3p.nycu.edu.tw/mod/assign/view.php?id=42',
  ]) assert.equal(normalizeE3Target(target, baseUrl), null, target);
  assert.equal(normalizeE3Target(assignment, 'javascript:bad'), null);
  assert.equal(normalizeE3Target(assignment, 'https://user@e3p.nycu.edu.tw'), null);
  assert.equal(normalizeE3Target('/mod/assign/view.php?id=1', baseUrl + '/moodle'), null);
});

test('restores the assignment after a page reload and replaces it on the next click', () => {
  const storage = storageFixture();
  const navigation = createPendingNavigation({ storage, owner, baseUrl });
  navigation.remember(assignment, '作業一', '課程 A');
  const restored = createPendingNavigation({ storage, owner, baseUrl });
  assert.deepEqual(restored.read(), navigation.read());
  restored.remember(baseUrl + '/mod/assign/view.php?id=43', '作業二', '課程 B');
  assert.equal(restored.read().title, '作業二');
  assert.equal(JSON.parse(storage.getItem(PENDING_KEY)).url, baseUrl + '/mod/assign/view.php?id=43');
});

test('expires pending navigation after two hours and refuses future timestamps', () => {
  for (const elapsed of [PENDING_TTL_MS, -1]) {
    let time = 1000;
    const storage = storageFixture();
    const navigation = createPendingNavigation({ storage, owner, baseUrl, now: () => time });
    navigation.remember(assignment, '作業', '課程');
    time += elapsed;
    assert.equal(navigation.read(), null);
    assert.equal(storage.getItem(PENDING_KEY), null);
  }
});

test('clears stale return links when switching accounts or admin viewed users', () => {
  for (const nextOwner of [JSON.stringify(['other', 'other']), JSON.stringify(['student', 'other'])]) {
    const storage = storageFixture();
    createPendingNavigation({ storage, owner, baseUrl }).remember(assignment, '私人的作業', '課程');
    assert.equal(createPendingNavigation({ storage, owner: nextOwner, baseUrl }).read(), null);
    assert.equal(storage.getItem(PENDING_KEY), null);
  }
});

test('malformed stored data and a tampered external target never become return links', () => {
  const storage = storageFixture();
  for (const record of ['{bad', JSON.stringify({ owner, url: 'https://other.test/mod/assign/view.php?id=42', createdAt: Date.now() }),
    JSON.stringify({ owner, url: assignment, createdAt: '123' }), 'null']) {
    storage.setItem(PENDING_KEY, record);
    assert.equal(createPendingNavigation({ storage, owner, baseUrl }).read(), null);
    assert.equal(storage.getItem(PENDING_KEY), null);
  }
});

test('disabled or full storage still allows returning in the current page', () => {
  for (const storage of [undefined, {
    getItem() { throw Error('blocked'); }, setItem() { throw Error('blocked'); }, removeItem() { throw Error('blocked'); },
  }, { getItem: () => JSON.stringify({ owner, url: assignment, createdAt: Date.now() }),
    setItem() { throw Error('full'); }, removeItem() {} }]) {
    const navigation = createPendingNavigation({ storage, owner, baseUrl });
    navigation.remember(baseUrl + '/mod/assign/view.php?id=43', '作業二', '課程');
    assert.equal(navigation.read().url, baseUrl + '/mod/assign/view.php?id=43');
    navigation.clear();
    assert.equal(navigation.read(), null);
  }
});

function pageFixture(storage) {
  const elements = new Map();
  for (const id of ['e3NavigationRecovery', 'e3NavigationTarget', 'e3NavigationResume', 'e3NavigationLogin', 'e3NavigationDismiss']) {
    elements.set(id, { hidden: true, textContent: '', events: {},
      addEventListener(type, listener) { this.events[type] = listener; },
      removeAttribute(name) { delete this[name]; } });
  }
  const events = {};
  const document = { getElementById: (id) => elements.get(id), visibilityState: 'visible',
    addEventListener: (type, listener) => { events[type] = listener; } };
  const window = { sessionStorage: storage, addEventListener() {} };
  return { elements, events, document, window };
}

test('expired external login can lose its target while our return entry survives a reload', () => {
  const storage = storageFixture();
  const oldDocument = globalThis.document;
  const oldWindow = globalThis.window;
  const config = { e3BaseUrl: baseUrl, navigationOwner: ['student', 'student'] };
  try {
    let page = pageFixture(storage);
    globalThis.document = page.document;
    globalThis.window = page.window;
    initialize({ config });
    let prevented = false;
    const link = { getAttribute: () => assignment,
      closest: () => ({ dataset: { title: '<img onerror=alert(1)>', course: '課程 A' } }) };
    page.events.click({ type: 'click', defaultPrevented: false,
      target: { closest: () => link }, preventDefault: () => { prevented = true; } });
    assert.equal(prevented, false, 'The original E3 new-tab click remains native');
    assert.equal(page.elements.get('e3NavigationRecovery').hidden, false);
    // External SSO lands on E3 home, with no reliable wantsurl. Reload our page.
    page = pageFixture(storage);
    globalThis.document = page.document;
    globalThis.window = page.window;
    initialize({ config });
    const resume = page.elements.get('e3NavigationResume');
    assert.equal(resume.href, assignment);
    assert.equal(page.elements.get('e3NavigationTarget').textContent, '課程 A ／ <img onerror=alert(1)>');
    resume.events.click({ preventDefault: () => { prevented = true; } });
    assert.equal(prevented, false);
    page.elements.get('e3NavigationDismiss').events.click();
    assert.equal(page.elements.get('e3NavigationRecovery').hidden, true);
    assert.equal(storage.getItem(PENDING_KEY), null);
  } finally {
    globalThis.document = oldDocument;
    globalThis.window = oldWindow;
  }
});
