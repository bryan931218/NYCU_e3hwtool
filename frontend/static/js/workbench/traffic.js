export function register(ctx) {}

export function initialize(ctx) {
  (function () {
    const initialVersion = Number(ctx.config.statsVersion);
    const pollUrl = ctx.config.statsUrl;
    if (Number.isNaN(initialVersion)) {
      return;
    }
    let latest = initialVersion;
    async function poll() {
      try {
        const resp = await fetch(pollUrl, { cache: "no-store" });
        if (!resp.ok) {
          return;
        }
        const data = await resp.json();
        if (typeof data.version === "number" && data.version > latest) {
          latest = data.version;
          window.location.reload();
        }
      } catch (err) {
        console.debug("traffic poll failed", err);
      }
    }
    setInterval(poll, 120000);
  })();
}
