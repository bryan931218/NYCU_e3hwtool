export function register(ctx) {}

export function initialize(ctx) {
  if (!ctx.IS_GUEST && !ctx.IS_READONLY_VIEW) {
    setTimeout(() => {
      try {
        const lastAutoRefresh = Number(
          localStorage.getItem("e3_last_auto_refresh_ts") || "0",
        );
        const now = Date.now();
        if (
          Number.isNaN(lastAutoRefresh) ||
          now - lastAutoRefresh > 15 * 60 * 1000
        ) {
          localStorage.setItem("e3_last_auto_refresh_ts", String(now));
          ctx.refreshAssignments(true);
        }
      } catch (err) {
        ctx.refreshAssignments(true);
      }
    }, 1000);
  }

  window.addEventListener("orientationchange", function () {
    setTimeout(() => {
      ctx.applyFilters();
    }, 300);
  });
}
