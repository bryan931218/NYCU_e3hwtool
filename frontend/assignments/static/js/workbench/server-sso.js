export function register() {}

export function initialize(ctx) {
  const panel = document.getElementById("serverSsoPanel");
  if (!panel) return;
  const message = document.getElementById("serverSsoMessage");
  const form = document.getElementById("serverSsoCodeForm");
  const input = document.getElementById("serverSsoCode");
  let timer = null;
  let stopped = false;
  let previousState = "";
  let inFlight = false;
  let failures = 0;

  async function poll() {
    if (stopped || inFlight || document.hidden) return;
    inFlight = true;
    try {
      const response = await fetch("/api/e3-connection", { credentials: "same-origin" });
      if (response.redirected || response.status === 401 || response.status === 403) {
        stopped = true;
        panel.hidden = true;
        return;
      }
      if (!response.ok) throw new Error();
      const data = await response.json();
      if (!data.ok || !["idle", "queued", "busy", "starting", "verifying", "otp_required", "complete", "expired", "unavailable", "cancelled"].includes(data.state)) throw new Error();
      failures = 0;
      const state = data.state;
      // Counts use the ordinary background refresh. Only a school-required
      // authentication step needs additional UI.
      panel.hidden = state !== "otp_required";
      form.hidden = state !== "otp_required";
      if (state === "otp_required") {
        message.textContent = data.attempts ? "請重新輸入校方驗證碼。" : "請完成校方二階段驗證。";
        if (previousState !== state) input.focus();
      } else if (state === "busy") {
        stopped = true;
      } else if (["expired", "unavailable", "cancelled"].includes(state)) {
        stopped = true;
      }
      if (state === "complete") {
        stopped = true;
        await ctx.fetchAndSwapContent();
      }
      if (state === "idle") stopped = true;
      previousState = state;
    } catch {
      failures += 1;
      stopped = failures >= 3;
      panel.hidden = stopped || previousState !== "otp_required";
    } finally {
      inFlight = false;
      if (!stopped) timer = window.setTimeout(poll, 2000);
    }
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    const button = form.querySelector("button");
    button.disabled = true;
    try {
      const response = await fetch("/api/e3-connection/verify", {
        method: "POST", credentials: "same-origin", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ code: input.value }),
      });
      const data = await response.json();
      if (!response.ok || !data.ok) throw new Error();
      input.value = "";
      form.hidden = true;
      message.textContent = "正在驗證…";
      window.clearTimeout(timer);
      await poll();
    } catch {
      message.textContent = "驗證碼無法送出，請稍後重試。";
    } finally { button.disabled = false; }
  });
  document.addEventListener("visibilitychange", () => {
    window.clearTimeout(timer);
    if (!document.hidden) poll();
  });
  poll();
}
