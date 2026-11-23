export const E3_ORIGIN = "https://e3p.nycu.edu.tw";
export const PORTAL_ENTRY = "https://portal.nycu.edu.tw/portal/newe3p";
export const SCHOOL_LOGIN_ENTRY = "https://portal.nycu.edu.tw/#/login?redirect=%2Fredirect%2Fnewe3p";
export const PENDING_TTL_MS = 30 * 60 * 1000;
const TRACKER_ORIGINS = new Set(["https://www.e3hwtool.space", "https://e3hwtool.space"]);

export function assignmentTarget(value) {
  if (typeof value !== "string" || /[\x00-\x20\\]/.test(value)) return null;
  try {
    const url = new URL(value);
    if (url.origin !== E3_ORIGIN || url.username || url.password) return null;
    if (!/^\/(?:mod\/[a-z0-9_]+|course)\/view\.php$/.test(url.pathname)) return null;
    const ids = url.searchParams.getAll("id");
    return ids.length === 1 && /^[1-9]\d*$/.test(ids[0]) ? url.href : null;
  } catch {
    return null;
  }
}

// A school-login transfer is tied to one login-page document and cookie store.
// Storage contains only navigation metadata, never credentials.
export function createSchoolLoginController({ storage, tabs, transfer, now = Date.now }) {
  const key = id => `e3-school-login:${id}`;
  const locks = new Set();
  const valid = pending => pending && now() >= pending.createdAt && now() - pending.createdAt < 10 * 60 * 1000;
  async function handle(message, sender) {
    if (sender.frameId !== 0 || !Number.isInteger(sender.tab?.id)) return { ok: false };
    let url;
    try { url = new URL(sender.url); } catch { return { ok: false }; }
    if (message?.type === "OPEN_E3_SCHOOL_LOGIN") {
      if (!TRACKER_ORIGINS.has(url.origin) || url.pathname !== "/login" || !sender.documentId ||
          !/^[a-f0-9-]{36}$/.test(message.requestId || "")) return { ok: false };
      const tab = await tabs.create({ url: "about:blank", active: true, windowId: sender.tab.windowId });
      await storage.set({ [key(tab.id)]: { createdAt: now(), source: {
        tabId: sender.tab.id, documentId: sender.documentId, requestId: message.requestId, origin: url.origin,
      } } });
      await tabs.update(tab.id, { url: SCHOOL_LOGIN_ENTRY });
      return { ok: true };
    }
    if (url.origin !== E3_ORIGIN) return { ok: false };
    const recordKey = key(sender.tab.id);
    const pending = (await storage.get(recordKey))[recordKey];
    if (!valid(pending)) {
      if (pending) await storage.remove(recordKey);
      return { ok: true, pending: false };
    }
    if (message?.type === "E3_SCHOOL_LOGIN_PENDING") return { ok: true, pending: true };
    if (message?.type !== "E3_SCHOOL_LOGIN_STATE" || message.status !== "authenticated") return { ok: false };
    if (locks.has(sender.tab.id)) return { ok: false };
    locks.add(sender.tab.id);
    try {
      // Consume before releasing a credential, including failed or duplicate reports.
      await storage.remove(recordKey);
      const result = await transfer(pending.source, sender.tab.id);
      if (result && TRACKER_ORIGINS.has(pending.source.origin)) {
        await tabs.update(pending.source.tabId, { active: true, url: pending.source.origin + "/" });
      }
      return { ok: Boolean(result) };
    } catch { return { ok: false }; }
    finally { locks.delete(sender.tab.id); }
  }
  return { handle, forget: id => storage.remove(key(id)) };
}

function senderOrigin(sender) {
  try { return new URL(sender.url).origin; } catch { return null; }
}

function isTargetPage(current, target) {
  const normalized = assignmentTarget(current);
  if (!normalized) return false;
  const page = new URL(normalized);
  const wanted = new URL(target);
  return page.pathname === wanted.pathname && page.searchParams.get("id") === wanted.searchParams.get("id");
}

export function pendingKey(tabId) { return `e3-navigation:${tabId}`; }

// Only navigation metadata is persisted. Session credentials stay in memory.
export function createNavigationController({ storage, tabs, restoreSession = async () => false, now = Date.now }) {
  const queues = new Map();
  function forTab(id, action) {
    const previous = queues.get(id) || Promise.resolve();
    const next = previous.catch(() => {}).then(action);
    queues.set(id, next);
    next.finally(() => { if (queues.get(id) === next) queues.delete(id); }).catch(() => {});
    return next;
  }

  async function handle(message, sender) {
    if (sender.frameId !== 0 || !Number.isInteger(sender.tab?.id)) return { ok: false };
    if (message?.type === "OPEN_E3_ASSIGNMENT") {
      const target = assignmentTarget(message.url);
      if (!target || !TRACKER_ORIGINS.has(senderOrigin(sender))) return { ok: false };
      // Record the destination before E3 loads: even an instantly cached page
      // must find its pending record when its content script reports readiness.
      const tab = await tabs.create({ url: "about:blank", active: message.background !== true,
        windowId: sender.tab.windowId });
      const key = pendingKey(tab.id);
      try {
        const source = {
          tabId: sender.tab.id, documentId: sender.documentId,
          requestId: typeof message.requestId === "string" && /^[a-f0-9-]{36}$/.test(message.requestId)
            ? message.requestId : null,
        };
        await storage.set({ [key]: { url: target, createdAt: now(), phase: "opening", source } });
      } catch {
        // A storage failure must still leave the original E3 link usable.
      }
      await tabs.update(tab.id, { url: target });
      return { ok: true };
    }
    if (message?.type === "E3_NAVIGATION_PENDING" && senderOrigin(sender) === E3_ORIGIN) {
      return forTab(sender.tab.id, async () => {
        const key = pendingKey(sender.tab.id);
        const pending = (await storage.get(key))[key];
        const age = now() - pending?.createdAt;
        const active = Boolean(assignmentTarget(pending?.url) && Number.isFinite(pending.createdAt) &&
          age >= 0 && age < PENDING_TTL_MS);
        if (pending && !active) await storage.remove(key);
        return { ok: true, pending: active };
      });
    }
    if (message?.type !== "E3_NAVIGATION_STATE" || senderOrigin(sender) !== E3_ORIGIN ||
        !["authenticated", "login-required", "unknown"].includes(message.status)) return { ok: false };
    return forTab(sender.tab.id, async () => {
      const key = pendingKey(sender.tab.id);
      const pending = (await storage.get(key))[key];
      if (!pending) return { ok: true };
      const age = now() - pending.createdAt;
      if (!assignmentTarget(pending.url) || !Number.isFinite(pending.createdAt) ||
          age < 0 || age >= PENDING_TTL_MS || !["opening", "restoring", "waiting-login", "returning"].includes(pending.phase)) {
        await storage.remove(key);
        return { ok: true };
      }
      if (message.status === "unknown") return { ok: true };
      if (message.status === "authenticated" && isTargetPage(sender.url, pending.url)) {
        await storage.remove(key);
        return { ok: true };
      }
      // Duplicate reports from the document we just navigated away from must
      // not trigger another redirect or erase the new navigation's state.
      if (pending.redirectedDocument && pending.redirectedDocument === sender.documentId) return { ok: true };
      if (pending.phase === "returning") {
        await storage.remove(key);
        return { ok: true }; // Never loop if E3 rejects the destination.
      }
      if (message.status === "authenticated") {
        await storage.set({ [key]: { ...pending, phase: "returning", redirectedDocument: sender.documentId } });
        await tabs.update(sender.tab.id, { url: pending.url });
      } else if (pending.phase === "opening") {
        let restored = false;
        if (pending.source?.requestId && pending.source.documentId) {
          try { restored = await restoreSession(pending, sender.tab.id); } catch {}
        }
        if (restored) {
          await storage.set({ [key]: { ...pending, phase: "restoring", redirectedDocument: sender.documentId } });
          await tabs.update(sender.tab.id, { url: pending.url });
          return { ok: true };
        }
        await storage.set({ [key]: { ...pending, phase: "waiting-login", redirectedDocument: sender.documentId } });
        await tabs.update(sender.tab.id, { url: PORTAL_ENTRY });
      } else if (pending.phase === "restoring") {
        // A rejected/expired session gets one school-login attempt.
        await storage.set({ [key]: { ...pending, phase: "waiting-login", redirectedDocument: sender.documentId } });
        await tabs.update(sender.tab.id, { url: PORTAL_ENTRY });
      }
      return { ok: true };
    });
  }
  function forget(tabId) { return forTab(tabId, () => storage.remove(pendingKey(tabId))); }
  return { handle, forget };
}
