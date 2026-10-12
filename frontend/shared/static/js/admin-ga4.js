import { number, format, percent, metrics, engagement, sorted, sourceLabel, campaignLabel, pageLabel, eventRows, featureLabels, eventLabels } from "./admin-ga4-model.js";

const root = document.getElementById("google-analytics");
if (root) {
  const status = document.getElementById("ga4Status");
  const updated = document.getElementById("ga4Updated");
  const range = document.getElementById("ga4Range");
  const refresh = document.getElementById("ga4Refresh");
  const report = document.getElementById("ga4Report");
  const loading = document.getElementById("ga4Loading");
  const tabs = Array.from(root.querySelectorAll("[data-ga4-tab]"));
  let revision = 0, controller, timer;
  function selectTab(tab, focus = false) {
    for (const button of tabs) {
      const selected = button === tab;
      button.setAttribute("aria-selected", String(selected));
      button.tabIndex = selected ? 0 : -1;
      document.getElementById(button.getAttribute("aria-controls")).hidden = !selected;
    }
    if (focus) tab.focus();
  }
  tabs.forEach((tab, index) => {
    tab.addEventListener("click", () => selectTab(tab));
    tab.addEventListener("keydown", event => {
      const next = { ArrowRight: (index + 1) % tabs.length, ArrowLeft: (index + tabs.length - 1) % tabs.length, Home: 0, End: tabs.length - 1 }[event.key];
      if (next == null) return;
      event.preventDefault();
      selectTab(tabs[next], true);
    });
  });
  function element(tag, text, className) {
    const node = document.createElement(tag);
    if (text != null) node.textContent = text;
    if (className) node.className = className;
    return node;
  }
  function empty(container, text = "這段期間沒有紀錄") {
    container.replaceChildren(element("p", text, "analytics-empty"));
  }
  function meter(value, maximum, label) {
    const node = element("meter");
    node.min = 0; node.max = Math.max(1, maximum); node.value = number(value) ?? 0;
    node.setAttribute("aria-label", label);
    return node;
  }
  function ranking(id, rows, key, labelFor, unit, limit = 5) {
    const container = document.getElementById(`ga4-${id}`);
    const ranked = sorted(rows, key).slice(0, limit);
    if (!ranked.length) return empty(container);
    const list = element("ol", null, "ga4-ranking");
    const maximum = Math.max(...ranked.map(row => number(row[key]) ?? 0), 1);
    for (const row of ranked) {
      const label = labelFor(row), item = element("li"), line = element("div", null, "ga4-ranking-line");
      line.append(element("span", label), element("strong", `${format(row[key])} ${unit}`));
      item.append(line, meter(row[key], maximum, `${label} ${format(row[key])} ${unit}`));
      list.append(item);
    }
    container.replaceChildren(list);
  }
  function table(id, headings, rows) {
    const container = document.getElementById(`ga4-${id}`);
    if (!rows.length) return empty(container);
    const node = element("table");
    const head = node.createTHead().insertRow();
    headings.forEach(label => { const th = element("th", label); th.scope = "col"; head.append(th); });
    const body = node.createTBody();
    for (const values of rows) {
      const row = body.insertRow();
      values.forEach((value, index) => {
        const cell = index === 0 ? element("th") : element("td");
        if (index === 0) cell.scope = "row";
        if (value && typeof value === "object") {
          cell.append(element("span", value.label));
          if (value.detail) cell.append(element("small", value.detail));
        } else cell.textContent = String(value);
        row.append(cell);
      });
    }
    container.replaceChildren(node);
  }
  function funnel(id, rows) {
    const container = document.getElementById(`ga4-${id}`);
    if (!Array.isArray(rows)) return empty(container, "暫時無法提供流程資料");
    if (!rows.length) return empty(container);
    const maximum = Math.max(...rows.map(row => number(row.activeUsers) ?? 0), 1);
    const list = element("ol", null, "ga4-funnel");
    rows.forEach((row, index) => {
      const label = String(row.funnelStepName || "未命名步驟").replace(/^\d+\.\s*/, "");
      const item = element("li"), marker = element("span", index + 1, "ga4-step-index");
      const content = element("div"), line = element("div", null, "ga4-ranking-line");
      line.append(element("span", label), element("strong", `${format(row.activeUsers)} 人`));
      content.append(line, meter(row.activeUsers, maximum, `${label} ${format(row.activeUsers)} 人`));
      if (index < rows.length - 1) content.append(element("small", `下一步完成率 ${percent(row.funnelStepCompletionRate)}`));
      item.append(marker, content); list.append(item);
    });
    container.replaceChildren(list);
  }
  function render(data) {
    const reports = data.reports;
    if (!reports || typeof reports !== "object") return false;
    const summary = reports.summary?.[0] || {};
    const metricRoot = document.getElementById("ga4Metrics");
    metricRoot.replaceChildren(...metrics(summary).map(metric => {
      const node = element("article");
      const value = element("strong", metric.value);
      value.append(element("small", metric.unit));
      node.append(element("span", metric.label), value, element("p", metric.note));
      return node;
    }));
    document.getElementById("ga4Engagement").replaceChildren(...engagement(summary).map(metric => {
      const node = element("div");
      node.append(element("span", metric.label), element("strong", metric.value), element("small", metric.note));
      return node;
    }));
    const sources = sorted(reports.sources, "sessions"), pages = sorted(reports.pages, "screenPageViews");
    const features = eventRows(reports.events, featureLabels, "feature_");
    ranking("top-sources", sources, "sessions", row => sourceLabel(row.sessionSourceMedium).label, "次");
    ranking("top-features", features, "totalUsers", row => row.label, "人");
    ranking("devices", reports.devices, "activeUsers", row => ({ desktop: "電腦", mobile: "手機", tablet: "平板" }[row.deviceCategory] || "其他裝置"), "人");
    ranking("returning", reports.returning, "activeUsers", row => ({ new: "新訪客", returning: "回訪訪客" }[row.newVsReturning] || "未辨識"), "人");
    table("sources", ["來源／活動", "訪客人數", "造訪次數", "互動比例"], sources.map(row => {
      const label = sourceLabel(row.sessionSourceMedium);
      return [{ label: label.label, detail: [label.detail, campaignLabel(row.sessionCampaignName)].filter(Boolean).join(" · ") }, format(row.activeUsers), format(row.sessions), percent(row.engagementRate)];
    }));
    table("pages", ["頁面", "瀏覽次數", "訪客人數"], pages.map(row => [{ label: pageLabel(row.pagePath), detail: row.pagePath }, format(row.screenPageViews), format(row.activeUsers)]));
    table("features", ["功能", "使用人數", "操作次數"], features.map(row => [row.label, format(row.totalUsers), format(row.eventCount)]));
    table("events", ["操作", "使用人數", "操作次數"], eventRows(reports.events, eventLabels).map(row => [row.label, format(row.totalUsers), format(row.eventCount)]));
    funnel("login_funnel", reports.login_funnel);
    funnel("line_funnel", reports.line_funnel);
    report.hidden = false;
    updated.textContent = data.updated_at ? `資料更新 ${data.updated_at}` : "";
    return true;
  }
  function finish() {
    root.setAttribute("aria-busy", "false");
    loading.hidden = true;
    refresh.disabled = false;
  }
  async function load(attempt = 0, version) {
    if (version == null) {
      version = ++revision;
      clearTimeout(timer);
      controller?.abort();
      controller = new AbortController();
      report.hidden = true;
      updated.textContent = "";
      loading.hidden = false;
      root.setAttribute("aria-busy", "true");
      status.textContent = "正在讀取報表…";
    }
    refresh.disabled = true;
    const requestController = controller;
    const timeout = setTimeout(() => requestController.abort(), 15000);
    try {
      const response = await fetch(`${root.dataset.ga4Url}?days=${range.value}`, { credentials: "same-origin", signal: requestController.signal });
      if (!response.ok) throw new Error("report");
      const data = await response.json();
      if (version !== revision) return;
      const shown = render(data);
      status.dataset.state = data.status;
      status.textContent = {
        loading: shown ? "正在更新，暫時顯示先前資料" : "正在讀取報表…",
        not_configured: "尚未連接報表", unavailable: "暫時無法取得資料，請稍後重試",
        stale: "更新暫時失敗，顯示先前資料", ready: shown ? "已更新 · 今日資料尚未完整" : "暫時無法取得資料，請稍後重試",
      }[data.status] || "暫時無法取得資料，請稍後重試";
      if (data.status === "loading" && attempt < 12) {
        loading.hidden = shown;
        timer = setTimeout(() => { if (version === revision) load(attempt + 1, version); }, 5000);
      } else {
        if (data.status === "loading") status.textContent = "讀取較久，請稍後重試";
        finish();
      }
    } catch {
      if (version !== revision) return;
      status.dataset.state = "error";
      status.textContent = "暫時無法連線，請稍後重試";
      finish();
    } finally { clearTimeout(timeout); }
  }
  if (root.dataset.ready === "true") {
    range.addEventListener("change", () => load());
    refresh.addEventListener("click", () => load());
    if ("IntersectionObserver" in window) {
      const observer = new IntersectionObserver(entries => {
        if (entries.some(entry => entry.isIntersecting)) { observer.disconnect(); load(); }
      });
      observer.observe(root);
    } else load();
  }
}
