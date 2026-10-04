(function () {
  const endpoint = document.body.dataset.trafficStatsUrl;
  if (!endpoint) return;
  let busy = false;
  async function poll() {
    if (document.hidden || busy) return;
    busy = true;
    try {
      const options = { cache: "no-store", credentials: "same-origin" };
      if (typeof AbortSignal !== "undefined" && AbortSignal.timeout) {
        options.signal = AbortSignal.timeout(15000);
      }
      const response = await fetch(endpoint, options);
      if (!response.ok) return;
      const data = await response.json();
      for (const key of ["online", "total"]) {
        const value = data[key];
        if (!Number.isSafeInteger(value) || value < 0) continue;
        document.querySelectorAll(`[data-traffic-stat="${key}"]`).forEach(node => {
          if (node.textContent !== String(value)) node.textContent = String(value);
        });
      }
    } catch (_) {
      // Keep the last known counts if the connection or session is unavailable.
    } finally {
      busy = false;
    }
  }
  setInterval(poll, 45000);
  document.addEventListener("visibilitychange", poll);
  poll();
})();
