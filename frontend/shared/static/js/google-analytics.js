(() => {
  "use strict";
  const config = JSON.parse(document.getElementById("googleAnalyticsConfig").textContent);
  const panel = document.getElementById("analyticsConsent");
  let choice = config.consent;
  let loaded = false;
  const features = new Set(["due_view", "course_view", "calendar", "search", "filters", "open_e3", "ignore", "refresh", "custom_todo", "deadline_edit", "messages", "notification_settings"]);
  const simpleEvents = new Set(["landing_view", "login_view", "login_submit", "login", "line_link_success", "browser_subscribe", "notification_enable"]);
  function track(name, params = {}) {
    if (choice !== "granted" || !loaded) return;
    const safe = {};
    if (name === "feature_use" && features.has(params.feature)) name = `feature_${params.feature}`;
    else if (!simpleEvents.has(name)) return;
    if (["password", "session"].includes(params.method)) safe.method = params.method;
    if (["line", "browser"].includes(params.channel)) safe.channel = params.channel;
    window.gtag("event", name, safe);
    if (["login_submit", "login"].includes(name) && safe.method) window.gtag("event", `${name}_${safe.method}`, safe);
    if (name === "notification_enable" && safe.channel) window.gtag("event", `${safe.channel}_notification_enable`, safe);
  }
  function start() {
    if (loaded || choice !== "granted" || config.settings) return;
    loaded = true;
    window[`ga-disable-${config.measurement}`] = false;
    window.dataLayer = window.dataLayer || [];
    window.gtag = function () { window.dataLayer.push(arguments); };
    window.gtag("consent", "default", { analytics_storage: "granted", ad_storage: "denied", ad_user_data: "denied", ad_personalization: "denied" });
    // Never let Google read the real query string, document title or referrer path.
    const url = new URL(config.path, location.origin);
    const query = new URLSearchParams(location.search);
    const sources = ["dcard", "line", "google", "instagram", "facebook", "campus"];
    if (sources.includes(query.get("utm_source"))) url.searchParams.set("utm_source", query.get("utm_source"));
    if (["social", "message", "referral", "organic"].includes(query.get("utm_medium"))) url.searchParams.set("utm_medium", query.get("utm_medium"));
    if (config.campaigns.includes(query.get("utm_campaign"))) url.searchParams.set("utm_campaign", query.get("utm_campaign"));
    let referrer = "";
    try {
      const ref = new URL(document.referrer);
      if (["www.dcard.tw", "dcard.tw", "www.google.com", "www.google.com.tw", "l.facebook.com", "l.instagram.com", "line.me"].includes(ref.hostname)) referrer = ref.origin + "/";
    } catch { /* Empty or unrecognized referrals remain private. */ }
    const page = { page_location: url.href, page_title: config.title, page_referrer: referrer };
    window.gtag("js", new Date());
    window.gtag("config", config.measurement, { ...page, send_page_view: false, allow_google_signals: false, allow_ad_personalization_signals: false, cookie_flags: "SameSite=Lax;Secure" });
    window.gtag("event", "page_view", page);
    if (config.path === "/login") track("login_view");
    if (config.path === "/" && config.title === "首頁") track("landing_view");
    if (config.path.startsWith("/courses/")) track("feature_use", { feature: "messages" });
    if (config.path === "/settings/notifications") track("feature_use", { feature: "notification_settings" });
    for (const event of config.events) track("login", { method: event.method });
    config.events = [];
    if (document.dispatchEvent) document.dispatchEvent(new Event("e3-analytics-ready"));
    const script = document.createElement("script");
    script.async = true;
    script.nonce = document.getElementById("googleAnalyticsConfig").nonce;
    script.src = `https://www.googletagmanager.com/gtag/js?id=${config.measurement}`;
    document.head.append(script);
  }
  window.e3Analytics = { track };
  const status = document.getElementById("analyticsPreferenceStatus");
  panel?.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-analytics-choice]");
    if (!button) return;
    const token = document.querySelector('meta[name="csrf-token"]')?.content;
    button.disabled = true;
    try {
      const response = await fetch("/analytics/consent", { method: "POST", credentials: "same-origin", headers: { "Content-Type": "application/json", "X-CSRFToken": token || "" }, body: JSON.stringify({ choice: button.dataset.analyticsChoice }) });
      if (!response.ok) throw new Error("save failed");
      choice = button.dataset.analyticsChoice;
      if (!config.settings) panel.hidden = true;
      if (status) status.textContent = choice === "granted" ? "已啟用" : "已停用";
      if (choice === "granted") start();
      else {
        window[`ga-disable-${config.measurement}`] = true;
        for (const cookie of document.cookie.split(";")) {
          const name = cookie.trim().split("=")[0];
          if (!/^_ga(?:_|$)/.test(name)) continue;
          const domains = ["", location.hostname, `.${location.hostname}`, `.${location.hostname.split(".").slice(-2).join(".")}`];
          for (const domain of domains) document.cookie = `${name}=; Max-Age=0; path=/;${domain ? ` domain=${domain};` : ""} SameSite=Lax; Secure`;
        }
        // Unload Google's listeners as well as disabling future sends.
        if (loaded) location.reload();
      }
    } catch {
      button.title = "連線失敗，請重試";
      if (status) status.textContent = "儲存失敗，請重試";
    }
    finally { button.disabled = false; }
  });
  document.addEventListener("submit", (event) => {
    if (config.path !== "/login" || event.target.getAttribute("action")?.includes("guest")) return;
    const method = new FormData(event.target).get("login_type");
    track("login_submit", { method: method === "session" ? "session" : "password" });
  });
  if (choice === "granted" && !config.settings) {
    if ("requestIdleCallback" in window) window.requestIdleCallback(start, { timeout: 2000 });
    else setTimeout(start, 0);
  }
})();
