import { createPushDiagnostics } from "./browser-notifications.js";

const config = JSON.parse(document.getElementById("notification-config").textContent);
const form = document.getElementById("notificationForm");
const byId = (id) => document.getElementById(id);
let state = config;
let subscription = null;
let subscriptionHash = "";
let linePoll = null;
let linkExpires = 0;
const supported = window.isSecureContext && "serviceWorker" in navigator && "PushManager" in window && "Notification" in window;
function browserMessage(text, error = false) {
  const output = byId("browserDiagnostic");
  output.hidden = false;
  output.textContent = text;
  output.dataset.error = String(error);
}
const pushDiagnostics = supported ? createPushDiagnostics(navigator.serviceWorker, browserMessage) : null;
byId("notificationTheme").addEventListener("click", () => {
  const theme = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = theme;
  try { localStorage.setItem("e3_theme", theme); } catch { /* Theme still applies if storage is unavailable. */ }
});

function message(text, error = false) {
  byId("notificationMessage").textContent = text;
  byId("notificationMessage").dataset.error = String(error);
}

async function api(path, method = "GET", body) {
  const response = await fetch(`/api/notifications/${path}`, {
    method, credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  if (response.redirected) throw new Error("登入已失效，請重新登入");
  const data = await response.json();
  if (!response.ok || !data.ok) throw new Error(data.message || "操作失敗，請稍後重試");
  return data;
}

function addDay(value) {
  const container = byId("dayInputs");
  const count = container.children.length;
  if (count >= 5) return;
  if (value === undefined) {
    const used = new Set([...container.querySelectorAll("input")].map((input) => Number(input.value)));
    value = Array.from({ length: 30 }, (_, index) => index + 1).find((day) => !used.has(day));
  }
  const row = document.createElement("div");
  row.className = "day-input";
  const input = document.createElement("input");
  input.type = "number"; input.min = "1"; input.max = "30"; input.step = "1"; input.required = true;
  input.value = String(value); input.setAttribute("aria-label", "到期前提醒天數");
  const unit = document.createElement("span"); unit.textContent = "天";
  const remove = document.createElement("button");
  remove.type = "button"; remove.textContent = "×"; remove.title = "移除此提醒";
  remove.setAttribute("aria-label", "移除此提醒");
  remove.addEventListener("click", () => {
    if (container.children.length > 1) row.remove();
    updateDayControls();
  });
  row.append(input, unit, remove); container.append(row); updateDayControls();
}

function updateDayControls() {
  byId("reminderDays").disabled = !byId("notifyDue").checked;
  byId("addDay").disabled = !byId("notifyDue").checked || byId("dayInputs").children.length >= 5;
  byId("dayInputs").querySelectorAll("button").forEach((button) => {
    button.disabled = byId("dayInputs").children.length <= 1;
  });
  [...byId("dayInputs").children].forEach((row, index) => {
    row.querySelector("input").setAttribute("aria-label", `第 ${index + 1} 個提醒的天數`);
    const label = `移除第 ${index + 1} 個提醒`;
    row.querySelector("button").setAttribute("aria-label", label);
    row.querySelector("button").title = label;
  });
}

function renderChannels() {
  const ownDevice = subscription && state.browser_endpoint_hashes.includes(subscriptionHash) &&
    subscription.options.applicationServerKey && Array.from(new Uint8Array(subscription.options.applicationServerKey)).join() === Array.from(vapidBytes(state.vapid_public_key)).join();
  byId("notifyBrowser").disabled = !state.browser_ready;
  if (!state.browser_ready) byId("notifyBrowser").checked = false;
  byId("enableBrowser").disabled = !state.browser_ready || !supported;
  byId("disableBrowser").hidden = !ownDevice;
  byId("enableBrowser").hidden = !!ownDevice;
  byId("testBrowser").disabled = !state.browser_ready || !ownDevice;
  byId("browserStatus").textContent = !state.browser_ready ? "服務尚未啟用" : !supported ? "此瀏覽器不支援推播" :
    Notification.permission === "denied" ? "已封鎖，請在瀏覽器網站設定允許通知" : ownDevice ? "此裝置已啟用" : "此裝置尚未啟用";
  byId("lineStatus").textContent = !state.line_ready ? "服務尚未啟用" : state.line_linked ? "已綁定" : "尚未綁定";
  byId("notifyLine").disabled = !state.line_ready || !state.line_linked;
  if (!state.line_ready || !state.line_linked) byId("notifyLine").checked = false;
  byId("linkLine").disabled = !state.line_ready;
  byId("linkLine").hidden = state.line_linked;
  byId("unlinkLine").hidden = !state.line_linked;
  byId("testLine").disabled = !state.line_ready || !state.line_linked;
  if (state.line_linked) {
    byId("lineLinkPanel").hidden = true;
    clearInterval(linePoll); linePoll = null;
  }
  if (state.sync_error) byId("syncNote").textContent = "目前無法更新 E3 作業，請重新登入。既有作業仍會依設定提醒。";
}

function applyPreferences(prefs) {
  byId("notifyNew").checked = prefs.new_assignment;
  byId("notifyAnnouncement").checked = !!prefs.new_announcement;
  byId("notifyMail").checked = !!prefs.new_mail;
  byId("notifyDue").checked = prefs.due_reminder;
  byId("notifyBrowser").checked = prefs.browser_enabled;
  byId("notifyLine").checked = prefs.line_enabled;
  byId("dayInputs").replaceChildren(); prefs.days_before.forEach(addDay);
  updateDayControls();
}

async function action(button, callback) {
  button.disabled = true;
  try { await callback(); } catch (error) { message(error.message, true); }
  finally { button.disabled = false; renderChannels(); updateDayControls(); }
}

function vapidBytes(value) {
  return Uint8Array.from(atob(value.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - value.length % 4) % 4)), (char) => char.charCodeAt(0));
}

async function hashSubscription() {
  subscriptionHash = subscription ? Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(subscription.endpoint))),
    (value) => value.toString(16).padStart(2, "0")).join("") : "";
}

byId("notifyDue").addEventListener("change", updateDayControls);
byId("addDay").addEventListener("click", () => addDay());
form.addEventListener("submit", async (event) => {
  event.preventDefault();
  await action(byId("saveNotifications"), async () => {
    state = await api("settings", "POST", {
      new_assignment: byId("notifyNew").checked, due_reminder: byId("notifyDue").checked,
      new_announcement: byId("notifyAnnouncement").checked, new_mail: byId("notifyMail").checked,
      browser_enabled: byId("notifyBrowser").checked, line_enabled: byId("notifyLine").checked,
      days_before: [...byId("dayInputs").querySelectorAll("input")].map((input) => Number(input.value)),
    });
    applyPreferences(state.preferences);
    message(state.preferences.browser_enabled || state.preferences.line_enabled ? "通知設定已儲存" : "設定已儲存，尚未開啟通知方式");
  });
});
byId("enableBrowser").addEventListener("click", () => action(byId("enableBrowser"), async () => {
  const permission = await Notification.requestPermission();
  if (permission !== "granted") throw new Error("尚未允許通知，請在瀏覽器網站設定開啟");
  const registration = await navigator.serviceWorker.register("/assignment-notifications-sw.js", { scope: "/" });
  await navigator.serviceWorker.ready;
  subscription = await registration.pushManager.getSubscription();
  const expectedKey = vapidBytes(state.vapid_public_key);
  if (subscription && Array.from(new Uint8Array(subscription.options.applicationServerKey || [])).join() !== Array.from(expectedKey).join()) {
    await subscription.unsubscribe(); subscription = null;
  }
  subscription = subscription || await registration.pushManager.subscribe({
    userVisibleOnly: true, applicationServerKey: vapidBytes(state.vapid_public_key),
  });
  state = await api("browser", "POST", subscription.toJSON());
  await hashSubscription();
  byId("notifyBrowser").checked = true; message("此裝置已啟用，請儲存通知設定");
}));
byId("disableBrowser").addEventListener("click", () => action(byId("disableBrowser"), async () => {
  if (subscription) {
    state = await api("browser", "DELETE", { endpoint: subscription.endpoint });
    await subscription.unsubscribe(); subscription = null;
    subscriptionHash = "";
    if (!state.browser_devices) byId("notifyBrowser").checked = false;
  }
  message("已移除此裝置");
}));
byId("linkLine").addEventListener("click", () => action(byId("linkLine"), async () => {
  const data = await api("line/link", "POST");
  byId("lineCode").value = `E3 ${data.code}`;
  byId("lineFriend").href = data.friend_url;
  byId("lineLinkPanel").hidden = false;
  linkExpires = Date.now() + data.expires_in * 1000;
  clearInterval(linePoll);
  linePoll = setInterval(async () => {
    if (Date.now() >= linkExpires) { clearInterval(linePoll); message("綁定碼已過期，請重新產生", true); return; }
    if (document.hidden) return;
    try {
      state = await api("settings"); renderChannels();
      if (state.line_linked) { byId("notifyLine").checked = true; message("LINE 已綁定，請儲存通知設定"); }
    } catch { /* Keep the pending link available during brief connection failures. */ }
  }, 5000);
}));
byId("unlinkLine").addEventListener("click", () => action(byId("unlinkLine"), async () => {
  state = await api("line/link", "DELETE"); byId("notifyLine").checked = false; message("LINE 已解除綁定");
}));
for (const [id, channel] of [["testBrowser", "browser"], ["testLine", "line"]]) {
  byId(id).addEventListener("click", () => action(byId(id), async () => {
    if (channel === "browser") pushDiagnostics?.cancel();
    const result = await api("test", "POST", { channel, ...(channel === "browser" ? { endpoint_hash: subscriptionHash } : {}) });
    message(result.message);
    if (channel === "browser") pushDiagnostics?.track(result.test_tag);
  }));
}
byId("copyCode").addEventListener("click", async () => {
  try { await navigator.clipboard.writeText(byId("lineCode").value); message("綁定碼已複製"); }
  catch { byId("lineCode").select(); message("請複製選取的綁定碼"); }
});

applyPreferences(state.preferences); renderChannels();
if (supported) {
  navigator.serviceWorker.getRegistration("/").then(async (registration) => {
    registration?.update().catch(() => {});
    subscription = await registration?.pushManager.getSubscription(); await hashSubscription(); renderChannels();
  }).catch(() => {});
}
