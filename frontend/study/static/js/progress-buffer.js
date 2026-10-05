(function (root) {
  'use strict';

  function createBuffer(storage, account, now = Date.now) {
    const prefix = 'e3-study-backup:v1:' + encodeURIComponent(account) + ':';
    const memory = new Map();
    const unsavedKeys = new Set();
    let durable = true;
    const read = (key) => {
      if (unsavedKeys.has(key)) return memory.get(key) || null;
      try {
        const raw = storage.getItem(prefix + key);
        if (raw) {
          const record = JSON.parse(raw);
          if (record && record.payload && typeof record.revision === 'string') return record;
        }
      } catch (_) { durable = false; }
      return memory.get(key) || null;
    };
    const write = (key, record) => {
      memory.set(key, record);
      try {
        storage.setItem(prefix + key, JSON.stringify(record));
        unsavedKeys.delete(key);
      } catch (_) { durable = false; unsavedKeys.add(key); }
      return record;
    };
    const capture = (kind, id, payload, forceVersion = false) => {
      const key = kind + ':' + id;
      const previous = read(key);
      const nextPayload = { ...payload };
      if (kind === 'video' && previous?.pending && !forceVersion) {
        nextPayload.expected_version = previous.payload.expected_version;
      }
      if (kind === 'time' && previous) {
        nextPayload.elapsed_seconds = Math.max(nextPayload.elapsed_seconds, previous.payload.elapsed_seconds);
        nextPayload.completed = Boolean(nextPayload.completed || previous.payload.completed);
      }
      return write(key, {
        key, kind, payload: nextPayload, pending: true,
        revision: now() + ':' + Math.random().toString(36).slice(2), savedAt: now(),
      });
    };
    const acknowledge = (sent, result) => {
      const current = read(sent.key);
      if (!current || !result?.ok) return;
      if (current.revision !== sent.revision) {
        // Rebase a newer local snapshot only after our preceding write succeeded.
        if (sent.kind === 'video' && !result.stale && current.payload.expected_version === sent.payload.expected_version) {
          current.payload.expected_version = result.progress_version;
          write(sent.key, current);
        }
        return;
      }
      current.pending = false;
      if (sent.kind === 'video') {
        current.conflict = Boolean(result.stale && Math.abs(Number(result.playback_seconds) - Number(sent.payload.watched_seconds)) >= 0.01);
        if (current.conflict) {
          write('conflict:' + sent.payload.video_id, { ...current, key: 'conflict:' + sent.payload.video_id });
        }
        current.payload.expected_version = result.progress_version;
      }
      write(sent.key, current);
    };
    const pending = () => {
      const keys = new Set(memory.keys());
      try {
        for (let i = 0; i < storage.length; i += 1) {
          const key = storage.key(i);
          if (key?.startsWith(prefix)) keys.add(key.slice(prefix.length));
        }
      } catch (_) { durable = false; }
      return Array.from(keys).map(read).filter((record) => record?.pending).sort((a, b) => a.savedAt - b.savedAt);
    };
    const playback = (id, serverVersion) => {
      const record = read('video:' + id);
      if (!record || record.conflict || record.payload.expected_version !== Number(serverVersion)) return null;
      const seconds = Number(record.payload.watched_seconds);
      return Number.isFinite(seconds) && seconds >= 0 ? seconds : null;
    };
    const forget = (key) => {
      memory.delete(key);
      try { storage.removeItem(prefix + key); unsavedKeys.delete(key); }
      catch (_) { durable = false; unsavedKeys.add(key); }
    };
    const conflict = (id) => read('conflict:' + id);
    return { capture, acknowledge, pending, playback, read, forget, conflict, isDurable: () => durable };
  }

  root.E3StudyBuffer = { create: createBuffer };
}(typeof window === 'undefined' ? globalThis : window));
