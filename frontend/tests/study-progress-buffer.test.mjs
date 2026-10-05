import test from 'node:test';
import assert from 'node:assert/strict';
import '../study/static/js/progress-buffer.js';

function storage() {
  const values = new Map();
  return {
    get length() { return values.size; },
    key: (index) => Array.from(values.keys())[index],
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    removeItem: (key) => values.delete(key),
  };
}
const create = globalThis.E3StudyBuffer.create;
const progress = (seconds, version = 2) => ({ video_id: 1, watched_seconds: seconds, expected_version: version });

test('offline and expired-auth snapshots survive a reload and keep the latest seek position', () => {
  const store = storage();
  const buffer = create(store, 'admin');
  buffer.capture('video', 1, progress(180));
  buffer.capture('video', 1, progress(90));
  const reopened = create(store, 'admin');
  assert.equal(reopened.pending().length, 1);
  assert.equal(reopened.playback(1, 2), 90);
  assert.equal(reopened.pending()[0].payload.watched_seconds, 90);
});

test('late acknowledgment never deletes a newer local snapshot; it advances its base version', () => {
  const buffer = create(storage(), 'admin');
  const sent = buffer.capture('video', 1, progress(100));
  buffer.capture('video', 1, progress(200));
  buffer.acknowledge(sent, { ok: true, stale: false, progress_version: 3 });
  const next = buffer.pending()[0];
  assert.equal(next.payload.watched_seconds, 200);
  assert.equal(next.payload.expected_version, 3);
  buffer.acknowledge(next, { ok: true, stale: false, progress_version: 4 });
  assert.equal(buffer.pending().length, 0);
  assert.equal(buffer.playback(1, 4), 200);
});

test('new server/manual edits are not silently overwritten or restored from stale backups', () => {
  const buffer = create(storage(), 'admin');
  const sent = buffer.capture('video', 1, progress(150));
  assert.equal(buffer.playback(1, 3), null);
  buffer.acknowledge(sent, { ok: true, stale: true, progress_version: 3, playback_seconds: 10 });
  assert.equal(buffer.pending().length, 0);
  assert.equal(buffer.playback(1, 3), null);
  assert.equal(buffer.conflict(1).payload.watched_seconds, 150);
  buffer.capture('video', 1, progress(20, 3));
  assert.equal(buffer.conflict(1).payload.watched_seconds, 150);
});

test('response lost after server accepted the same position is not a conflict', () => {
  const buffer = create(storage(), 'admin');
  const sent = buffer.capture('video', 1, progress(150));
  buffer.acknowledge(sent, { ok: true, stale: true, progress_version: 3, playback_seconds: 150 });
  assert.equal(buffer.conflict(1), null);
  assert.equal(buffer.playback(1, 3), 150);
});

test('an explicit restore can use the current server version instead of an obsolete queued version', () => {
  const buffer = create(storage(), 'admin');
  buffer.capture('video', 1, progress(150));
  const restored = buffer.capture('video', 1, progress(170, 4), true);
  assert.equal(restored.payload.expected_version, 4);
});

test('failed responses never acknowledge or discard a backup', () => {
  const buffer = create(storage(), 'admin');
  const sent = buffer.capture('video', 1, progress(150));
  buffer.acknowledge(sent, { ok: false });
  assert.equal(buffer.pending().length, 1);
});

test('backups are isolated between accounts and no credentials are stored', () => {
  const store = storage();
  create(store, 'admin-a').capture('video', 1, progress(150));
  const other = create(store, 'admin-b');
  assert.equal(other.pending().length, 0);
  assert.equal(other.playback(1, 2), null);
  assert.doesNotMatch(store.getItem(store.key(0)), /csrf|session_token|password|MoodleSession/);
});

test('storage disabled or quota full falls back to memory without claiming durable persistence', () => {
  const buffer = create(null, 'admin');
  buffer.capture('video', 1, progress(150));
  assert.equal(buffer.isDurable(), false);
  assert.equal(buffer.pending().length, 1);
  assert.equal(buffer.playback(1, 2), 150);
});

test('multiple tabs observe newer stored snapshots and cannot acknowledge an outdated revision', () => {
  const store = storage();
  const first = create(store, 'admin');
  const second = create(store, 'admin');
  const sent = first.capture('video', 1, progress(150));
  second.capture('video', 1, progress(220));
  first.acknowledge(sent, { ok: true, stale: false, progress_version: 3 });
  assert.equal(second.pending()[0].payload.watched_seconds, 220);
  assert.equal(second.pending()[0].payload.expected_version, 3);
});

test('quota exhaustion never substitutes an older disk snapshot for the latest in-memory position', () => {
  const store = storage();
  const buffer = create(store, 'admin');
  buffer.capture('video', 1, progress(150));
  store.setItem = () => { throw new Error('QuotaExceededError'); };
  buffer.capture('video', 1, progress(250));
  assert.equal(buffer.isDurable(), false);
  assert.equal(buffer.pending()[0].payload.watched_seconds, 250);
  assert.equal(buffer.playback(1, 2), 250);
});

test('study-time retries preserve cumulative seconds and completion without duplicating sessions', () => {
  const buffer = create(storage(), 'admin');
  const payload = { session_id: 'video_123', kind: 'video', elapsed_seconds: 120, completed: true };
  buffer.capture('time', 'video_123', payload);
  buffer.capture('time', 'video_123', { ...payload, elapsed_seconds: 60, completed: false });
  assert.equal(buffer.pending().length, 1);
  assert.equal(buffer.pending()[0].payload.elapsed_seconds, 120);
  assert.equal(buffer.pending()[0].payload.completed, true);
});
