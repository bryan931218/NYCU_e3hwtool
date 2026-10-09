(() => {
  const root = document.getElementById("google-analytics");
  if (!root || root.dataset.ready !== "true") return;
  const status = document.getElementById("ga4Status");
  const range = document.getElementById("ga4Range");
  let revision = 0;
  const format = (value) => Number(value || 0).toLocaleString("zh-TW", { maximumFractionDigits: 1 });
  const events = { login_submit_password: "送出帳密登入", login_password: "帳密登入成功", login_submit_session: "送出 Session 登入", login_session: "Session 登入成功", line_link_success: "LINE 綁定完成", browser_subscribe: "瀏覽器訂閱成功", notification_enable: "通知設定啟用" };
  const features = { due_view: "到期日列表", course_view: "依課程列表", calendar: "作業日曆", search: "搜尋", filters: "篩選", open_e3: "前往 E3", ignore: "忽略／恢復", refresh: "更新作業", custom_todo: "自訂代辦", deadline_edit: "調整期限", messages: "課程訊息", notification_settings: "通知設定" };
  function table(id, headings, rows) {
    const container = document.getElementById(`ga4-${id}`);
    container.replaceChildren();
    if (!rows.length) { const text = document.createElement("p"); text.className = "analytics-empty"; text.textContent = "此區間尚無資料"; container.append(text); return; }
    const table = document.createElement("table");
    const head = table.createTHead().insertRow();
    for (const label of headings) { const th = document.createElement("th"); th.scope = "col"; th.textContent = label; head.append(th); }
    const body = table.createTBody();
    for (const row of rows) { const tr = body.insertRow(); for (const value of row) tr.insertCell().textContent = String(value); }
    container.append(table);
  }
  function render(data) {
    const reports = data.reports;
    if (!reports) return;
    const summary = reports.summary[0] || {};
    const metrics = document.getElementById("ga4Metrics"); metrics.replaceChildren();
    for (const [label, value] of [["活躍訪客", format(summary.activeUsers)], ["新訪客", format(summary.newUsers)], ["工作階段", format(summary.sessions)], ["頁面瀏覽", format(summary.screenPageViews)], ["參與率", `${format(summary.engagementRate * 100)}%`], ["平均參與時間／活躍訪客", `${format(summary.activeUsers ? summary.userEngagementDuration / summary.activeUsers : 0)} 秒`]]) {
      const article = document.createElement("article"), span = document.createElement("span"), strong = document.createElement("strong");
      span.textContent = label; strong.textContent = value; article.append(span, strong); metrics.append(article);
    }
    table("sources", ["來源／活動", "工作階段", "參與率"], reports.sources.map(r => [`${r.sessionSourceMedium} · ${r.sessionCampaignName}`, format(r.sessions), `${format(r.engagementRate * 100)}%`]));
    table("pages", ["頁面", "瀏覽", "訪客"], reports.pages.map(r => [r.pagePath, format(r.screenPageViews), format(r.activeUsers)]));
    table("devices", ["裝置", "訪客", "工作階段"], reports.devices.map(r => [{ desktop: "電腦", mobile: "手機", tablet: "平板" }[r.deviceCategory] || r.deviceCategory, format(r.activeUsers), format(r.sessions)]));
    table("returning", ["類型", "訪客", "工作階段"], reports.returning.map(r => [{ new: "新訪客", returning: "回訪訪客", "(not set)": "未識別" }[r.newVsReturning] || r.newVsReturning, format(r.activeUsers), format(r.sessions)]));
    const rows = new Map(reports.events.map(r => [r.eventName, r]));
    table("events", ["步驟", "訪客", "次數"], Object.entries(events).filter(([key]) => rows.has(key)).map(([key, label]) => [label, format(rows.get(key).totalUsers), format(rows.get(key).eventCount)]));
    table("features", ["功能", "訪客", "次數"], Object.entries(features).filter(([key]) => rows.has(`feature_${key}`)).map(([key, label]) => [label, format(rows.get(`feature_${key}`).totalUsers), format(rows.get(`feature_${key}`).eventCount)]));
    for (const key of ["login_funnel", "line_funnel"]) {
      const funnel = reports[key];
      table(key, ["步驟", "訪客", "至下一步"], (funnel || []).map((r, index) => [r.funnelStepName, format(r.activeUsers), index === funnel.length - 1 ? "—" : `${format(r.funnelStepCompletionRate * 100)}%`]));
      if (funnel == null) document.getElementById(`ga4-${key}`).textContent = "Google 漏斗 API 暫時無法提供資料";
    }
    document.getElementById("ga4Report").hidden = false;
  }
  async function load(attempt = 0, version = ++revision) {
    try {
      const response = await fetch(`${root.dataset.ga4Url}?days=${range.value}`, { credentials: "same-origin", signal: AbortSignal.timeout(15000) });
      if (!response.ok) throw new Error("report");
      const data = await response.json();
      if (version !== revision) return;
      render(data);
      status.textContent = { loading: "GA4 報表讀取中…", not_configured: "尚未連接報表", unavailable: "無法取得 GA4 報表，請檢查資源 ID、API 與檢視者權限", stale: `GA4 暫時無法更新，顯示 ${data.updated_at || "先前"} 的快取`, ready: `GA4 資料更新 ${data.updated_at}` }[data.status] || "報表尚未提供";
      if (data.status === "loading" && attempt < 12) setTimeout(() => { if (version === revision) load(attempt + 1, version); }, 5000);
      else if (data.status === "loading") status.textContent = "GA4 尚未回應，請稍後再試";
    } catch { if (version === revision) status.textContent = "GA4 暫時無法連線，本站統計不受影響"; }
  }
  range.addEventListener("change", () => { document.getElementById("ga4Report").hidden = true; load(); });
  const observer = new IntersectionObserver(entries => { if (entries.some(entry => entry.isIntersecting)) { observer.disconnect(); load(); } });
  observer.observe(root);
})();
