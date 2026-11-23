import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";
import { installBrowser, managerAddress, copyManagerAddress } from '../assignments/static/js/e3-navigation-install.js';
import { assignmentTarget, createNavigationController, E3_ORIGIN, PORTAL_ENTRY, PENDING_TTL_MS, pendingKey }
  from "../assignments/static/e3-navigation-extension/core.js";

const target = E3_ORIGIN + "/mod/assign/view.php?id=42&action=viewsubmission#submission";

test('installation guide chooses Edge ahead of its Chrome user-agent token', () => {
  assert.equal(installBrowser('Mozilla/5.0 Chrome/130.0 Safari/537.36 Edg/130.0'),'edge');
  assert.equal(installBrowser('Mozilla/5.0 Chrome/130.0 Safari/537.36'),'chrome');
  assert.equal(managerAddress('edge'),'edge://extensions');
  assert.equal(managerAddress('chrome'),'chrome://extensions');
  assert.equal(managerAddress('invalid'),'chrome://extensions');
});

test('copying the manager address reports success only after clipboard completion', async () => {
  let value='';
  const feedback={textContent:''};
  assert.equal(await copyManagerAddress({value:'edge://extensions'},feedback,{writeText:async text=>{value=text;}}),true);
  assert.equal(value,'edge://extensions');
  assert.match(feedback.textContent,/已複製/);
});

test('unavailable or blocked clipboard selects the URL for manual copying', async () => {
  for (const clipboard of [undefined,{writeText:async()=>{throw Error('blocked');}}]) {
    const selected=[];
    const input={value:'chrome://extensions',focus:()=>selected.push('focus'),select:()=>selected.push('select')};
    const feedback={textContent:''};
    assert.equal(await copyManagerAddress(input,feedback,clipboard),false);
    assert.deepEqual(selected,['focus','select']);
    assert.match(feedback.textContent,/手動複製/);
  }
});
const tracker = { frameId: 0, tab: { id: 1, windowId: 3 }, url: "https://www.e3hwtool.space/", documentId: "tracker-document" };
const e3 = (id, url, documentId) => ({ frameId: 0, tab: { id }, url, documentId });
function fixture(restoreSession) {
  const records = {};
  const changes = [];
  let nextId = 10;
  let time = 1000;
  const storage = {
    async get(key) { return { [key]: records[key] }; },
    async set(items) { Object.assign(records, items); },
    async remove(key) { delete records[key]; },
  };
  const tabs = {
    async create(options) { const id = nextId++; changes.push({ id, ...options }); return { id }; },
    async update(id, options) { changes.push({ id, ...options }); },
  };
  const make = () => createNavigationController({ storage, tabs, restoreSession, now: () => time });
  return { records, changes, controller: make(), make, advance: (delta) => { time += delta; } };
}
const open = (controller, url = target) => controller.handle({ type: "OPEN_E3_ASSIGNMENT", url }, tracker);
const report = (controller, id, url, status, documentId = "doc") =>
  controller.handle({ type: "E3_NAVIGATION_STATE", status }, e3(id, url, documentId));
const openWithSession = controller => controller.handle({ type: "OPEN_E3_ASSIGNMENT", url: target,
  requestId: "12345678-1234-1234-1234-123456789012" }, tracker);

test("missing E3 login restores the saved session once and reaches the original assignment", async () => {
  const attempts = [];
  const f = fixture(async (pending, tabId) => { attempts.push({ pending, tabId }); return true; });
  await openWithSession(f.controller);
  await report(f.controller, 10, E3_ORIGIN + "/login/index.php", "login-required", "login");
  assert.equal(attempts.length, 1);
  assert.equal(attempts[0].pending.source.tabId, 1);
  assert.equal(attempts[0].pending.source.documentId, "tracker-document");
  assert.equal(f.changes.at(-1).url, target);
  assert.equal(f.records[pendingKey(10)].phase, "restoring");
  await report(f.controller, 10, target, "authenticated", "assignment");
  assert.equal(f.records[pendingKey(10)], undefined);
  assert.equal(f.changes.some(item => item.url === PORTAL_ENTRY), false);
});

test("expired session, unavailable bridge and duplicate reports fall back to school login without looping", async () => {
  for (const outcome of [true, false, "error"]) {
    let attempts = 0;
    const f = fixture(async () => { attempts++; if (outcome === "error") throw Error("source closed"); return outcome; });
    await openWithSession(f.controller);
    await Promise.all([1, 2].map(() => report(f.controller, 10, E3_ORIGIN + "/login/index.php", "login-required", "login")));
    if (outcome === true) {
      await report(f.controller, 10, E3_ORIGIN + "/login/index.php", "login-required", "expired");
    }
    assert.equal(attempts, 1);
    assert.equal(f.changes.filter(item => item.url === PORTAL_ENTRY).length, 1);
    await report(f.controller, 10, E3_ORIGIN + "/login/index.php", "login-required", "failed-sso");
    assert.equal(f.changes.filter(item => item.url === PORTAL_ENTRY).length, 1);
  }
});

test("authenticated E3 browsers and unknown account workflows never receive a replacement cookie", async () => {
  for (const status of ["authenticated", "unknown"]) {
    const f = fixture(async () => assert.fail("must not replace this browser session"));
    await openWithSession(f.controller);
    await report(f.controller, 10, status === "authenticated" ? target : E3_ORIGIN + "/user/edit.php", status);
    assert.equal(f.changes.length, 2);
  }
});

test("restoration writes a secure session cookie in the source and destination cookie store only", async () => {
  const source = readFileSync(new URL("../assignments/static/e3-navigation-extension/background.js", import.meta.url), "utf8");
  const writes = [];
  const calls = [];
  const chrome = {
    storage: { session: {} },
    tabs: { onRemoved: { addListener() {} }, async sendMessage(...args) {
      calls.push(args); return { ok: true, session: "synthetic-credential-1234567890" };
    } },
    runtime: { onMessage: { addListener() {} } },
    cookies: { async getAllCookieStores() { return [{ id: "regular", tabIds: [1, 10] }]; },
      async set(cookie) { writes.push(cookie); return cookie; } },
  };
  const sandbox = vm.createContext({ chrome, createNavigationController: () => ({}), E3_ORIGIN });
  vm.runInContext(source.replace(/^import [^\r\n]*\r?\n/, ""), sandbox);
  const pending = { url: target, source: { tabId: 1, documentId: "source-doc", requestId: "one-time" } };
  assert.equal(await sandbox.restoreSession(pending, 10), true);
  assert.equal(calls[0][0], 1);
  assert.equal(calls[0][2].documentId, "source-doc");
  assert.equal(writes[0].url, E3_ORIGIN + "/");
  assert.equal(writes[0].name, "MoodleSession");
  assert.equal(writes[0].httpOnly, true);
  assert.equal(writes[0].secure, true);
  assert.equal(writes[0].storeId, "regular");
  assert.equal(Object.hasOwn(writes[0], "expirationDate"), false);
  chrome.cookies.getAllCookieStores = async () => [{ id: "private", tabIds: [10] }];
  assert.equal(await sandbox.restoreSession(pending, 10), false);
  assert.equal(writes.length, 1);
});

test("the tracker releases a session only once for a trusted click and the extension worker", async () => {
  const source = readFileSync(new URL("../assignments/static/e3-navigation-extension/tracker.js", import.meta.url), "utf8");
  const handlers = {};
  const messages = [];
  const requests = [];
  let listener;
  const requestId = "12345678-1234-1234-1234-123456789012";
  const body = { dataset: { readonlyView: "0", viewUser: "admin" } };
  const sandbox = vm.createContext({ URL, AbortSignal, crypto: { randomUUID: () => requestId },
    setTimeout() {}, location: { search: "" },
    document: { body, addEventListener: (type, fn) => { handlers[type] = fn; },
      querySelector: () => ({ content: "csrf-protected" }) },
    window: { open: () => assert.fail("unexpected fallback") },
    chrome: { runtime: { id: "own-extension", onMessage: { addListener: fn => { listener = fn; } },
      async sendMessage(message) { messages.push(message); return { ok: true }; } } },
    fetch: async (url, options) => { requests.push({ url, options }); return {
      ok: true, json: async () => ({ ok: true, session: "synthetic-credential-1234567890" }),
    }; },
  });
  vm.runInContext(source, sandbox);
  const click = trusted => handlers.click({ isTrusted: trusted, type: "click", preventDefault() {},
    target: { closest: () => ({ href: target }) } });
  const getSession = (sender = { id: "own-extension" }) => new Promise(resolve => {
    const async = listener({ type: "E3_RESTORE_SESSION", requestId, url: target }, sender, resolve);
    if (async !== true) resolve({ ok: false });
  });
  click(false);
  assert.equal(messages.length, 0);
  click(true);
  assert.equal(messages[0].requestId, requestId);
  assert.equal((await getSession({ id: "other-extension" })).ok, false);
  assert.equal(requests.length, 0);
  assert.equal((await getSession()).ok, true);
  assert.equal(requests[0].url, "/api/e3-navigation/session");
  assert.equal(requests[0].options.headers["X-CSRFToken"], "csrf-protected");
  assert.equal(requests[0].options.redirect, "error");
  assert.equal((await getSession()).ok, false);
  assert.equal(requests.length, 1);
  click(true);
  body.dataset.viewUser = "changed-account";
  assert.equal((await getSession()).ok, false);
  assert.equal(requests.length, 1);
});

test("expired login opens SSO once, then automatically returns in the same tab with no second click", async () => {
  const f = fixture();
  await open(f.controller);
  await report(f.controller, 10, E3_ORIGIN + "/login/index.php", "login-required", "login");
  assert.equal(f.changes.at(-1).url, PORTAL_ENTRY);
  await report(f.controller, 10, E3_ORIGIN + "/", "authenticated", "home");
  assert.deepEqual(f.changes.at(-1), { id: 10, url: target });
  await report(f.controller, 10, target, "authenticated", "assignment");
  assert.equal(f.records[pendingKey(10)], undefined);
  assert.equal(f.changes.filter((change) => change.url === "about:blank").length, 1);
});

test("already authenticated users reach the assignment directly without an SSO round trip", async () => {
  const f = fixture();
  await open(f.controller);
  await report(f.controller, 10, target, "authenticated");
  assert.equal(f.changes.length, 2);
  assert.equal(f.changes.at(-1).url, target);
  assert.equal(f.records[pendingKey(10)], undefined);
});

test("different destination tabs keep independent assignments across worker suspension", async () => {
  const f = fixture();
  const second = E3_ORIGIN + "/mod/assign/view.php?id=43";
  await Promise.all([open(f.controller), open(f.controller, second)]);
  const restarted = f.make();
  await report(restarted, 11, E3_ORIGIN + "/my/", "authenticated", "second-home");
  await report(restarted, 10, E3_ORIGIN + "/my/", "authenticated", "first-home");
  assert.deepEqual(f.changes.slice(-2), [{ id: 11, url: second }, { id: 10, url: target }]);
});

test("duplicate readiness messages cannot race into repeated login or return redirects", async () => {
  const f = fixture();
  await open(f.controller);
  await Promise.all([1, 2].map(() => report(f.controller, 10, E3_ORIGIN + "/login/index.php", "login-required", "login")));
  assert.equal(f.changes.filter((change) => change.url === PORTAL_ENTRY).length, 1);
  await Promise.all([1, 2].map(() => report(f.controller, 10, E3_ORIGIN + "/", "authenticated", "home")));
  assert.equal(f.changes.filter((change) => change.url === target).length, 2);
  assert.equal(f.records[pendingKey(10)].phase, "returning");
});

test("a failed SSO or rejected assignment never starts a redirect loop", async () => {
  const f = fixture();
  await open(f.controller);
  await report(f.controller, 10, E3_ORIGIN + "/login/index.php", "login-required", "initial");
  await report(f.controller, 10, E3_ORIGIN + "/login/index.php", "login-required", "failed-login");
  assert.equal(f.changes.length, 3);
  await report(f.controller, 10, E3_ORIGIN + "/", "authenticated", "home");
  await report(f.controller, 10, E3_ORIGIN + "/login/index.php", "login-required", "rejected-target");
  assert.equal(f.records[pendingKey(10)], undefined);
  assert.equal(f.changes.length, 4);
});

test("expired, malformed and closed-tab records cannot redirect later unrelated visits", async () => {
  for (const expired of [PENDING_TTL_MS, -1]) {
    const f = fixture();
    await open(f.controller);
    f.advance(expired);
    await report(f.controller, 10, E3_ORIGIN + "/", "authenticated");
    assert.equal(f.changes.length, 2);
    assert.equal(f.records[pendingKey(10)], undefined);
  }
  const f = fixture();
  await open(f.controller);
  await f.controller.forget(10);
  await report(f.controller, 10, E3_ORIGIN + "/", "authenticated");
  assert.equal(f.changes.length, 2);
  f.records[pendingKey(10)] = { url: "https://other.test/", phase: "opening", createdAt: 1000 };
  await report(f.controller, 10, E3_ORIGIN + "/", "authenticated");
  assert.equal(f.records[pendingKey(10)], undefined);
});

test("only trusted tracker frames can open targets and only E3 top frames can report login", async () => {
  const f = fixture();
  for (const sender of [{ ...tracker, url: "https://other.test/" }, { ...tracker, frameId: 1 },
    { ...tracker, url: "https://www.e3hwtool.space.attacker.test/" }]) {
    assert.equal((await f.controller.handle({ type: "OPEN_E3_ASSIGNMENT", url: target }, sender)).ok, false);
  }
  await open(f.controller);
  for (const sender of [e3(10, "https://other.test/", "bad"), { ...e3(10, E3_ORIGIN + "/", "bad"), frameId: 1 }]) {
    assert.equal((await f.controller.handle({ type: "E3_NAVIGATION_STATE", status: "authenticated" }, sender)).ok, false);
  }
  assert.equal(f.changes.length, 2);
  await report(f.controller, 99, E3_ORIGIN + "/", "authenticated");
  assert.equal(f.changes.length, 2);
});

test("untrusted URLs, login actions and ambiguous IDs are not accepted as assignment targets", () => {
  assert.equal(assignmentTarget(target), target);
  for (const url of ["https://e3p.nycu.edu.tw.attacker.test/mod/assign/view.php?id=42", "/mod/assign/view.php?id=42",
    E3_ORIGIN + "/login/logout.php?id=42", E3_ORIGIN + "/mod/assign/view.php?id=42&id=43",
    E3_ORIGIN + "/mod/assign/view.php?id=0", "https://user@e3p.nycu.edu.tw/mod/assign/view.php?id=42",
    "javascript:alert(1)", target + "\n"]) assert.equal(assignmentTarget(url), null, url);
});

test("dashboard probing distinguishes expired login from authenticated Moodle and required account workflows", () => {
  const sandbox = vm.createContext({ URL });
  vm.runInContext(readFileSync(new URL("../assignments/static/e3-navigation-extension/probe.js", import.meta.url), "utf8"), sandbox);
  const detect = sandbox.E3AutoNavigationProbe.statusFromDashboard;
  const html = '<script>M.cfg = {"homeurl":{},"userId":123,"sesskey":"ignored"};</script>';
  assert.equal(detect(html, E3_ORIGIN + "/my/"), "authenticated");
  assert.equal(detect(html, E3_ORIGIN + "/my/index.php"), "authenticated");
  assert.equal(detect("<h1>登入</h1>", E3_ORIGIN + "/login/index.php"), "login-required");
  assert.equal(detect(html, E3_ORIGIN + "/login/change_password.php"), "unknown");
  assert.equal(detect(html, E3_ORIGIN + "/user/edit.php"), "unknown");
  assert.equal(detect(html, "https://portal.nycu.edu.tw/login"), "unknown");
  assert.equal(detect('<script>M.cfg = {"userId":0};</script>', E3_ORIGIN + "/my/"), "unknown");
  assert.equal(detect("<h1>維護中</h1>", E3_ORIGIN + "/my/"), "unknown");
});

test("unrelated E3 pages are not probed and an unknown login result never redirects", async () => {
  const f = fixture();
  const query = { type: "E3_NAVIGATION_PENDING" };
  assert.equal((await f.controller.handle(query, e3(10, E3_ORIGIN + "/", "home"))).pending, false);
  await open(f.controller);
  assert.equal((await f.controller.handle(query, e3(10, E3_ORIGIN + "/", "home"))).pending, true);
  await report(f.controller, 10, E3_ORIGIN + "/user/edit.php", "unknown");
  assert.equal(f.changes.length, 2);
});
