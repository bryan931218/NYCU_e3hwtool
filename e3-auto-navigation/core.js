export const E3_ORIGIN = "https://e3p.nycu.edu.tw";
export const PORTAL_ENTRY = "https://portal.nycu.edu.tw/portal/newe3p";
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

// State is held in extension session storage, independently for each destination
// tab. No E3 cookie, password or page content leaves the E3 content script.
export function createNavigationController({ storage, tabs, now = Date.now }) {
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
        await storage.set({ [key]: { url: target, createdAt: now(), phase: "opening" } });
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
          age < 0 || age >= PENDING_TTL_MS || !["opening", "waiting-login", "returning"].includes(pending.phase)) {
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
        await storage.set({ [key]: { ...pending, phase: "waiting-login", redirectedDocument: sender.documentId } });
        await tabs.update(sender.tab.id, { url: PORTAL_ENTRY });
      }
      return { ok: true };
    });
  }
  function forget(tabId) { return forTab(tabId, () => storage.remove(pendingKey(tabId))); }
  return { handle, forget };
}
