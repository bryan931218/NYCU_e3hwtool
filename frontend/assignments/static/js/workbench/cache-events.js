export function register(ctx) {}

export function initialize(ctx) {
  if (ctx.filterMenu) {
    document.addEventListener("click", (evt) => {
      if (window.innerWidth <= 780) return;
      if (!ctx.filterMenu.open) return;
      if (ctx.filterMenu.contains(evt.target)) return;
      ctx.filterMenu.open = false;
    });
  }

  document.querySelectorAll("[data-log-action]").forEach((el) => {
    el.addEventListener("click", () => {
      const action = el.getAttribute("data-log-action");
      if (action) {
        ctx.logUiEvent(action);
      }
    });
  });

  document.addEventListener("click", (evt) => {
    const archiveRefreshBtn = evt.target.closest("#archiveRefreshBtn");
    if (archiveRefreshBtn) {
      evt.preventDefault();
      ctx.refreshAssignments(true, ctx.currentSemesterFilters, true);
      return;
    }
    const manualRefreshBtn = evt.target.closest("#manualRefreshBtn");
    if (manualRefreshBtn) {
      evt.preventDefault();
      ctx.refreshAssignments(true);
    }
  });

  (function () {
    if (ctx.IS_GUEST) {
      return;
    }
    const HEARTBEAT_INTERVAL = 240000;
    let heartbeatTimer = null;
    let lastHeartbeat = -Infinity;
    function sendHeartbeat() {
      if (document.hidden || Date.now() - lastHeartbeat < 10000) return;
      lastHeartbeat = Date.now();
      ctx.logUiEvent("heartbeat", "info");
    }
    function scheduleHeartbeat() {
      if (heartbeatTimer) clearInterval(heartbeatTimer);
      heartbeatTimer = setInterval(sendHeartbeat, HEARTBEAT_INTERVAL);
    }
    sendHeartbeat();
    scheduleHeartbeat();
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) {
        sendHeartbeat();
        scheduleHeartbeat();
      }
    });
    window.addEventListener("focus", () => {
      sendHeartbeat();
      scheduleHeartbeat();
    });
  })();

  (function () {
    if (!ctx.CACHE_SYNC_ENDPOINT || ctx.IS_READONLY_VIEW) {
      return;
    }
    const refreshBroadcastKey = `e3_assignment_refresh_broadcast_${ctx.STORAGE_USER_KEY}`;
    let seenRefreshBroadcast = "";
    let polling = false;
    try {
      seenRefreshBroadcast = localStorage.getItem(refreshBroadcastKey) || "";
    } catch (err) {}
    async function pollCacheSync() {
      if (document.hidden || polling || ctx.refreshInFlight) return;
      polling = true;
      try {
        const options = {
          cache: "no-store",
          credentials: "same-origin",
        };
        if (typeof AbortSignal !== "undefined" && AbortSignal.timeout) options.signal = AbortSignal.timeout(30000);
        const resp = await fetch(ctx.CACHE_SYNC_ENDPOINT, options);
        if (!resp.ok) return;
        const payload = await resp.json();
        if (!payload.ok) return;
        const nextTs = Number(payload.ts || "0");
        const requestedRefresh = String(
          payload.assignment_refresh_version || "",
        );
        const requestedAfterTs = Number(
          payload.assignment_refresh_after_ts || "0",
        );
        if (
          !ctx.IS_GUEST &&
          requestedRefresh &&
          requestedRefresh !== seenRefreshBroadcast
        ) {
          seenRefreshBroadcast = requestedRefresh;
          try {
            localStorage.setItem(refreshBroadcastKey, requestedRefresh);
          } catch (err) {}
          if (
            Number.isNaN(nextTs) ||
            !requestedAfterTs ||
            nextTs < requestedAfterTs
          ) {
            await ctx.refreshAssignments(true, ctx.currentSemesterFilters);
            return;
          }
        }
        if (!Number.isNaN(nextTs) && nextTs > ctx.currentCacheTs) {
          ctx.currentCacheTs = nextTs;
          await ctx.fetchAndSwapContent();
          if (payload.preferences) {
            ctx.applyServerPreferenceState(payload.preferences);
          }
          return;
        }
        if (payload.preferences) {
          ctx.applyServerPreferenceState(payload.preferences);
        }
      } catch (err) {
        console.debug("cache sync failed", err);
      } finally {
        polling = false;
      }
    }
    setInterval(pollCacheSync, ctx.CACHE_SYNC_INTERVAL);
    setTimeout(pollCacheSync, 5000);
    document.addEventListener("visibilitychange", () => { if (!document.hidden) void pollCacheSync(); });
  })();
}
