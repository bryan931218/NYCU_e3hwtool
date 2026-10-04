(() => {
  let probe;
  async function checkLogin() {
    if (probe) return probe;
    probe = (async () => {
      const state = await chrome.runtime.sendMessage({ type: "E3_NAVIGATION_PENDING" }).catch(() => null);
      if (!state?.pending) return;
      let status = "unknown";
      const pageUrl = location.href;
      try {
        const response = await fetch(new URL("/my/", location.origin), {
          credentials: "same-origin", cache: "no-store", redirect: "follow",
          signal: AbortSignal.timeout(10000),
        });
        if (response.ok) {
          status = E3AutoNavigationProbe.statusFromDashboard(await response.text(), response.url);
        }
      } catch {}
      if (location.href === pageUrl) {
        await chrome.runtime.sendMessage({ type: "E3_NAVIGATION_STATE", status }).catch(() => {});
      }
    })().finally(() => { probe = null; });
    return probe;
  }
  void checkLogin();
})();
