import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';
import { createPushDiagnostics } from '../assignments/static/js/browser-notifications.js';

const tag = `test:${'a'.repeat(32)}`;
const scriptURL = 'https://example.test/assignment-notifications-sw.js';

function workerBridge() {
  let listener;
  return {
    addEventListener(name, handler) { assert.equal(name, 'message'); listener = handler; },
    removeEventListener(name, handler) { assert.equal(handler, listener); listener = null; },
    emit(status, overrides = {}) {
      listener?.({ source: { scriptURL }, data: { type: 'e3-push-test-status', tag, status }, ...overrides });
    },
  };
}

test('push diagnostics correlate this test and handle receipts arriving before the HTTP response', () => {
  const worker = workerBridge();
  const reports = [];
  const diagnostics = createPushDiagnostics(worker, (...report) => reports.push(report));
  worker.emit('received');
  worker.emit('shown');
  diagnostics.track(tag);
  assert.match(reports.at(-1)[0], /已收到推播並建立通知/);
  assert.equal(reports.at(-1)[1], false);
  diagnostics.dispose();
});

test('push diagnostics ignore unknown workers, unrelated tests and malformed statuses', () => {
  const worker = workerBridge();
  const reports = [];
  const diagnostics = createPushDiagnostics(worker, (...report) => reports.push(report));
  diagnostics.track(tag);
  worker.emit('shown', { source: { scriptURL: 'https://example.test/other-worker.js' } });
  worker.emit('shown', { data: { type: 'e3-push-test-status', tag: `test:${'b'.repeat(32)}`, status: 'shown' } });
  worker.emit('bad');
  assert.equal(reports.length, 1);
  worker.emit('received');
  worker.emit('failed');
  assert.match(reports.at(-1)[0], /無法建立通知/);
  assert.equal(reports.at(-1)[1], true);
  diagnostics.dispose();
});

test('accepted push without a device receipt times out instead of claiming delivery', async () => {
  const worker = workerBridge();
  let finish;
  const complete = new Promise(resolve => { finish = resolve; });
  const diagnostics = createPushDiagnostics(worker, (message, error) => {
    if (error) finish(message);
  }, 1);
  diagnostics.track(tag);
  const message = await complete;
  assert.match(message, /尚未收到此裝置/);
  assert.doesNotMatch(message, /本機通知/);
  diagnostics.dispose();
});

test('cancelled diagnostics do not leave a pending timeout', async () => {
  const worker = workerBridge();
  const reports = [];
  const diagnostics = createPushDiagnostics(worker, message => reports.push(message), 1);
  diagnostics.track(tag);
  diagnostics.cancel();
  await new Promise(resolve => setTimeout(resolve, 5));
  assert.equal(reports.length, 1);
  diagnostics.dispose();
});

function notificationWorker(showNotification, matchAll) {
  const handlers = {};
  vm.runInNewContext(readFileSync(new URL('../assignments/static/js/notifications-sw.js', import.meta.url), 'utf8'), {
    URL,
    self: {
      addEventListener: (name, callback) => { handlers[name] = callback; },
      registration: { showNotification }, clients: { matchAll },
    },
  });
  return payload => {
    let work;
    handlers.push({ data: { json: () => payload }, waitUntil(promise) { work = promise; } });
    return work;
  };
}

test('worker reports test receipt and notification creation only to notification settings tabs', async () => {
  const messages = [];
  const client = { url: 'https://example.test/settings/notifications', postMessage: message => messages.push(message) };
  const push = notificationWorker(async () => {}, async () => [
    client, { url: 'https://example.test/', postMessage: () => assert.fail('wrong tab') },
  ]);
  await push({ title: 'Private title', body: 'Private body', tag });
  assert.deepEqual(messages.map(message => message.status), ['received', 'shown']);
  assert.equal(JSON.stringify(messages).includes('Private'), false);
  messages.length = 0;
  await push({ title: 'Assignment', tag: 'new:assignment' });
  assert.equal(messages.length, 0);
});

test('receipt reporting failures never suppress the actual notification', async () => {
  let shown = false;
  const push = notificationWorker(async () => { shown = true; }, async () => { throw new Error('no clients'); });
  await push({ tag });
  assert.equal(shown, true);
});

test('worker reports display failure without exposing exception text', async () => {
  const messages = [];
  const push = notificationWorker(async () => { throw new Error('private error'); }, async () => [
    { url: 'https://example.test/settings/notifications', postMessage: message => messages.push(message) },
  ]);
  await assert.rejects(push({ tag }), /private error/);
  assert.deepEqual(messages.map(message => message.status), ['received', 'failed']);
  assert.equal(JSON.stringify(messages).includes('private error'), false);
});
