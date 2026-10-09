import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../shared/static/js/google-analytics.js', import.meta.url), 'utf8');
function setup(consent = '', options = {}) {
  const events = {}, scripts = [], requests = [], cookies = [];
  const config = { consent, measurement: 'G-TEST1234', path: '/login', title: '登入', campaigns: ['dcard-115-1'], events: [] };
  const panel = { hidden: false, addEventListener: (key, handler) => { events[`panel-${key}`] = handler; } };
  const document = { getElementById: (id) => id === 'googleAnalyticsConfig' ? { textContent: JSON.stringify(config), nonce: 'nonce' } : id === 'analyticsConsent' ? panel : { addEventListener: (key, handler) => { events[`privacy-${key}`] = handler; } },
    querySelector: () => ({ content: 'csrf' }), createElement: () => ({}), head: { append: (node) => scripts.push(node) },
    addEventListener: (key, handler) => { events[key] = handler; }, referrer: 'https://www.dcard.tw/private-name?secret=123' };
  Object.defineProperty(document, 'cookie', { get: () => '_ga=123; _ga_TEST=456; session=keep', set: value => cookies.push(value) });
  const location = { origin: 'https://www.e3hwtool.space', hostname: 'www.e3hwtool.space', search: options.search || '?username=112550103&session=secret&utm_source=dcard&utm_medium=social&utm_campaign=dcard-115-1', reload: () => events.reloaded = true };
  const window = { requestIdleCallback: callback => callback() };
  const fetch = async (url, args) => { requests.push({ url, args }); return { ok: true }; };
  vm.runInNewContext(source, { window, document, location, fetch, URL, URLSearchParams, Date, FormData: class { constructor() {} get() { return 'session'; } }, setTimeout });
  return { events, scripts, requests, cookies, panel, window };
}

test('no Google network or events before consent, including denial', () => {
  for (const choice of ['', 'denied']) {
    const result = setup(choice);
    result.window.e3Analytics.track('feature_use', { feature: 'search' });
    assert.equal(result.scripts.length, 0);
    assert.equal(result.window.dataLayer, undefined);
  }
});
test('sanitizes URLs, title and referrer and allowlists event params', () => {
  const result = setup('granted');
  assert.equal(result.scripts.length, 1);
  assert.equal(result.scripts[0].nonce, 'nonce');
  result.window.e3Analytics.track('feature_use', { feature: 'search', query: 'private-name', student: '112550103' });
  result.window.e3Analytics.track('unknown', { password: 'secret' });
  const data = JSON.stringify(result.window.dataLayer);
  assert.equal(data.includes('112550103'), false);
  assert.equal(data.includes('secret'), false);
  assert.equal(data.includes('private-name'), false);
  assert.match(data, /utm_campaign=dcard-115-1/);
  assert.match(data, /feature_search/);
  assert.match(data, /https:\/\/www.dcard.tw\//);
  assert.doesNotMatch(data, /unknown/);
});
test('free-form campaigns and sources are not sent', () => {
  const result = setup('granted', { search: '?utm_source=112550103&utm_campaign=private-person&utm_medium=secret' });
  assert.doesNotMatch(JSON.stringify(result.window.dataLayer), /112550103|private-person|secret/);
});
test('revocation persists denial, disables Google, clears only GA cookies and reloads once', async () => {
  const result = setup('granted');
  const button = { dataset: { analyticsChoice: 'denied' } };
  await result.events['panel-click']({ target: { closest: () => button } });
  assert.equal(result.window['ga-disable-G-TEST1234'], true);
  assert.equal(result.events.reloaded, true);
  assert.equal(result.requests[0].args.headers['X-CSRFToken'], 'csrf');
  assert.match(result.requests[0].args.body, /denied/);
  assert.ok(result.cookies.every(cookie => cookie.startsWith('_ga')));
  const before = result.window.dataLayer.length;
  result.window.e3Analytics.track('login');
  assert.equal(result.window.dataLayer.length, before);
});
