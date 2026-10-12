export const featureLabels = { due_view: "到期日列表", course_view: "依課程列表", calendar: "作業日曆", search: "搜尋", filters: "篩選", open_e3: "前往 E3", ignore: "忽略／恢復作業", refresh: "更新作業", custom_todo: "自訂代辦", deadline_edit: "調整作業期限", messages: "課程訊息", notification_settings: "通知設定" };
export const eventLabels = { login_submit_password: "送出帳密登入", login_password: "帳密登入成功", login_submit_session: "送出 Session 登入", login_session: "Session 登入成功", line_link_success: "LINE 綁定完成", browser_subscribe: "瀏覽器訂閱成功", notification_enable: "啟用通知設定", line_notification_enable: "啟用 LINE 通知", browser_notification_enable: "啟用瀏覽器通知" };
export function number(value) {
  if (value == null || value === "" || typeof value === "boolean") return null;
  const result = Number(value);
  return Number.isFinite(result) && result >= 0 ? result : null;
}
export function format(value) {
  const result = number(value);
  return result == null ? "—" : result.toLocaleString("zh-TW", { maximumFractionDigits: 1 });
}
export function percent(value) {
  const result = number(value);
  return result == null || result > 1 ? "—" : `${format(result * 100)}%`;
}
export function duration(value) {
  const result = number(value);
  if (result == null) return "—";
  const seconds = Math.round(result);
  return seconds < 60 ? `${seconds} 秒` : `${Math.floor(seconds / 60)} 分${seconds % 60 ? ` ${seconds % 60} 秒` : ""}`;
}
export function sorted(rows, key) {
  return (Array.isArray(rows) ? rows : []).filter(row => row && typeof row === "object").slice().sort((a, b) => (number(b[key]) ?? -1) - (number(a[key]) ?? -1));
}
export function sourceLabel(value) {
  const [source = "", medium = ""] = String(value || "").split(/\s*\/\s*/);
  const labels = { "(direct)": "直接造訪", "(not set)": "未辨識來源", dcard: "Dcard", "www.dcard.tw": "Dcard", line: "LINE", "line.me": "LINE", google: "Google", "www.google.com": "Google", instagram: "Instagram", facebook: "Facebook", campus: "校園分享" };
  const mediums = { social: "社群", message: "訊息分享", referral: "網站連結", organic: "自然搜尋", "(none)": "", "(not set)": "" };
  return { label: labels[source] || source || "未辨識來源", detail: mediums[medium] ?? medium };
}
export function campaignLabel(value) {
  return !value || ["(not set)", "(direct)", "(organic)"].includes(value) ? "" : String(value);
}
export function pageLabel(value) {
  const labels = { "/": "首頁／作業頁", "/login": "登入", "/settings/notifications": "通知設定", "/courses/messages": "課程訊息", "/courses/announcements": "課程公告", "/courses/mail": "課程信件", "/feedback": "問題回報" };
  return labels[value] || value || "未辨識頁面";
}
export function eventRows(rows, labels, prefix = "") {
  return sorted(rows, "totalUsers").filter(row => Object.hasOwn(labels, String(row.eventName).slice(prefix.length)) && String(row.eventName).startsWith(prefix)).map(row => ({ ...row, label: labels[String(row.eventName).slice(prefix.length)] }));
}
export function metrics(summary) {
  return [
    { label: "活躍訪客", value: format(summary.activeUsers), unit: "人", note: "區間內有互動的訪客" },
    { label: "新訪客", value: format(summary.newUsers), unit: "人", note: "第一次造訪網站" },
    { label: "造訪次數", value: format(summary.sessions), unit: "次", note: "同一人可有多次造訪" },
    { label: "頁面瀏覽", value: format(summary.screenPageViews), unit: "次", note: "包含重複瀏覽" },
  ];
}
export function engagement(summary) {
  const users = number(summary.activeUsers), time = number(summary.userEngagementDuration);
  return [
    { label: "互動造訪比例", value: percent(summary.engagementRate), note: "有互動的造訪占比" },
    { label: "平均互動時間", value: duration(users == null || time == null ? null : users > 0 ? time / users : 0), note: "每位活躍訪客" },
  ];
}
