(() => {
  const pending = new Map();
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
