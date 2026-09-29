(() => {
  // Remove data from the previous manual-return implementation on upgrade.
  try { sessionStorage.removeItem("e3_pending_navigation"); } catch {}
  function openAssignment(event) {
    if (event.defaultPrevented || (event.type === "auxclick" && event.button !== 1)) return;
    const link = event.target.closest?.("a[data-e3-assignment]");
    if (!link) return;
    let url;
    try {
      url = new URL(link.href);
      if (url.origin !== "https://e3p.nycu.edu.tw" || url.username || url.password ||
          !/^\/(?:mod\/[a-z0-9_]+|course)\/view\.php$/.test(url.pathname) ||
          url.searchParams.getAll("id").length !== 1 || !/^[1-9]\d*$/.test(url.searchParams.get("id"))) return;
      const result = chrome.runtime.sendMessage({ type: "OPEN_E3_ASSIGNMENT", url: url.href,
        background: event.type === "auxclick" || event.ctrlKey || event.metaKey });
      event.preventDefault();
      result.then((response) => {
        if (!response?.ok) window.open(url.href, "_blank", "noopener");
      }).catch(() => { window.open(url.href, "_blank", "noopener"); });
    } catch {
      // An unloaded extension should leave the native link click untouched.
    }
  }
  document.addEventListener("click", openAssignment, true);
  document.addEventListener("auxclick", openAssignment, true);
})();
