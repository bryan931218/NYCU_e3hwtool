import assert from 'node:assert/strict';
import { test } from 'node:test';
import { number, format, percent, duration, sorted, sourceLabel, campaignLabel, pageLabel, eventRows, featureLabels, metrics, engagement, newVisitorSources } from '../shared/static/js/admin-ga4-model.js';

test('acquisition counts only new visitors, never returning-user sessions', () => {
  const rows = [{ newUsers: 0, sessions: 900 }, { newUsers: 2, sessions: 100 },
    { newUsers: '8' }, { activeUsers: 99 }, { newUsers: -1 }, null];
  assert.deepEqual(newVisitorSources(rows).map(row => row.newUsers), ['8', 2]);
  assert.deepEqual(newVisitorSources(null), []);
  assert.equal(rows[0].newUsers, 0);
});

test('missing values are not misrepresented as zero', () => {
  for (const value of [undefined, null, '', NaN, Infinity, -1, true, 'bad']) {
    assert.equal(number(value), null);
    assert.equal(format(value), '—');
  }
  assert.equal(format(0), '0');
  assert.equal(percent(0), '0%');
  assert.equal(percent(.745), '74.5%');
  assert.equal(percent(74), '—');
  assert.equal(duration(66), '1 分 6 秒');
  assert.equal(duration(60), '1 分');
  assert.equal(duration(null), '—');
  assert.ok(metrics({}).every(metric => metric.value === '—'));
  assert.equal(engagement({})[1].value, '—');
  assert.equal(engagement({ activeUsers: 100, userEngagementDuration: 6600 })[1].value, '1 分 6 秒');
  assert.equal(engagement({ activeUsers: 0, userEngagementDuration: 0 })[1].value, '0 秒');
});
test('rankings sort by people without modifying response data', () => {
  const rows = [{ eventName: 'feature_calendar', totalUsers: 3 }, { eventName: 'private_event', totalUsers: 99 }, { eventName: 'feature_search', totalUsers: 8 }];
  const snapshot = structuredClone(rows);
  assert.deepEqual(eventRows(rows, featureLabels, 'feature_').map(row => row.label), ['搜尋', '作業日曆']);
  assert.deepEqual(rows, snapshot);
  assert.deepEqual(sorted(null, 'sessions'), []);
});
test('source and page codes have readable labels with unknown values preserved', () => {
  assert.deepEqual(sourceLabel('(direct) / (none)'), { label: '直接造訪', detail: '' });
  assert.deepEqual(sourceLabel('dcard / social'), { label: 'Dcard', detail: '社群' });
  assert.deepEqual(sourceLabel('google / organic'), { label: 'Google', detail: '自然搜尋' });
  assert.equal(sourceLabel('custom / referral').label, 'custom');
  assert.equal(campaignLabel('(not set)'), '');
  assert.equal(campaignLabel('dcard-115-1'), 'dcard-115-1');
  assert.equal(pageLabel('/settings/notifications'), '通知設定');
  assert.equal(pageLabel('/unrecognized'), '/unrecognized');
});
