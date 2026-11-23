(() => {
  const pending = new Map();
  let schoolLogin = null;
  function schoolStatus(text) {
    const status = document.getElementById?.("schoolLoginStatus");
    if (status) status.textContent = text;
  }
  function setupSchoolLogin() {
    const button = document.getElementById?.("schoolLoginButton");
    if (!button || location.pathname !== "/login") return;
    button.disabled = false;
    button.dataset.e3SsoReady = "1";
    schoolStatus("在學校完成登入後，會自動返回原管理員帳號。");
    button.addEventListener("click", event => {
      if (!event.isTrusted || schoolLogin) return;
      const requestId = crypto.randomUUID();
      schoolLogin = { requestId, createdAt: Date.now() };
      schoolStatus("請在開啟的學校頁面完成登入，本站會自動接續驗證。");
      chrome.runtime.sendMessage({ type: "OPEN_E3_SCHOOL_LOGIN", requestId }).then(result => {
        if (!result?.ok) { schoolLogin = null; schoolStatus("無法啟動學校登入，請重新載入擴充功能後再試。"); }
      }).catch(() => { schoolLogin = null; schoolStatus("請重新載入擴充功能與此頁面。"); });
      setTimeout(() => {
        if (schoolLogin?.requestId === requestId) { schoolLogin = null; schoolStatus("登入等待已逾時，請重新開始。"); }
      }, 10 * 60 * 1000);
    });
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", setupSchoolLogin, { once: true });
  else setupSchoolLogin();
  // Remove data from the previous manual-return implementation on upgrade.
  try { sessionStorage.removeItem("e3_pending_navigation"); } catch {}
  function openAssignment(event) {
    if (!event.isTrusted || event.defaultPrevented || (event.type === "auxclick" && event.button !== 1)) return;
    const link = event.target.closest?.("a[data-e3-assignment]");
    if (!link) return;
    let url;
    try {
      url = new URL(link.href);
      if (url.origin !== "https://e3p.nycu.edu.tw" || url.username || url.password ||
          !/^\/(?:mod\/[a-z0-9_]+|course)\/view\.php$/.test(url.pathname) ||
          url.searchParams.getAll("id").length !== 1 || !/^[1-9]\d*$/.test(url.searchParams.get("id"))) return;
      const requestId = crypto.randomUUID();
      const mayRestore = document.body?.dataset.readonlyView === "0";
      if (mayRestore) {
        pending.set(requestId, { url: url.href, username: document.body.dataset.viewUser, createdAt: Date.now() });
        setTimeout(() => pending.delete(requestId), 60000);
      }
      const result = chrome.runtime.sendMessage({ type: "OPEN_E3_ASSIGNMENT", url: url.href, requestId: mayRestore ? requestId : null,
        background: event.type === "auxclick" || event.ctrlKey || event.metaKey });
      event.preventDefault();
      result.then((response) => {
        if (!response?.ok) window.open(url.href, "_blank", "noopener");
      }).catch(() => { window.open(url.href, "_blank", "noopener"); });
    } catch {
      // An unloaded extension should leave the native link click untouched.
    }
  }
  chrome.runtime.onMessage.addListener((message, sender, respond) => {
    if (message?.type === "E3_SCHOOL_LOGIN_SESSION") {
      const entry = schoolLogin;
      if (sender.id !== chrome.runtime.id || sender.tab || location.pathname !== "/login" || !entry ||
          entry.requestId !== message.requestId || Date.now() - entry.createdAt >= 10 * 60 * 1000 ||
          typeof message.session !== "string" || !/^[A-Za-z0-9,-]{16,256}$/.test(message.session)) {
        respond({ ok: false }); return;
      }
      schoolLogin = null;
      const csrf = document.querySelector('meta[name="csrf-token"]')?.content;
      if (!csrf) { respond({ ok: false }); return; }
      const fields = new URLSearchParams({ login_type: "session", admin_session: "1", moodle_session: message.session, csrf_token: csrf });
      fetch("/login", { method: "POST", credentials: "same-origin", mode: "same-origin", cache: "no-store", redirect: "follow",
        headers: { "Content-Type": "application/x-www-form-urlencoded", "X-CSRFToken": csrf },
        body: fields.toString(), signal: AbortSignal.timeout(25000),
      }).then(response => {
        const target = new URL(response.url);
        const ok = response.ok && target.origin === location.origin && target.pathname === "/";
        respond({ ok });
        if (ok) schoolStatus("管理員驗證成功，正在返回本站。");
        else schoolStatus("學校登入已完成，但本站未通過管理員本人驗證，請確認學校帳號。");
      }).catch(() => { schoolStatus("學校登入接續失敗，請重新開始。"); respond({ ok: false }); });
      return true;
    }
    if (message?.type !== "E3_RESTORE_SESSION" || sender.id !== chrome.runtime.id || sender.tab) return;
    const entry = pending.get(message.requestId);
    pending.delete(message.requestId);
    if (!entry || entry.url !== message.url || Date.now() - entry.createdAt >= 60000 ||
        document.body?.dataset.readonlyView !== "0" || entry.username !== document.body.dataset.viewUser) {
      respond({ ok: false });
      return;
    }
    const csrf = document.querySelector('meta[name="csrf-token"]')?.content;
    if (!csrf) { respond({ ok: false }); return; }
    fetch("/api/e3-navigation/session" + location.search, {
      method: "POST", credentials: "same-origin", cache: "no-store", redirect: "error",
      headers: { "Content-Type": "application/json", "X-CSRFToken": csrf },
      body: JSON.stringify({ url: entry.url, username: entry.username }), signal: AbortSignal.timeout(8000),
    }).then(async response => respond(response.ok ? await response.json() : { ok: false }))
      .catch(() => respond({ ok: false }));
    return true;
  });
  document.addEventListener("click", openAssignment, true);
  document.addEventListener("auxclick", openAssignment, true);
})();
