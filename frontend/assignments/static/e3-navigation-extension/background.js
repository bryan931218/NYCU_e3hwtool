import { createNavigationController, createSchoolLoginController, E3_ORIGIN } from "./core.js";

async function restoreSession(pending, destinationTabId) {
  const { source } = pending;
  const stores = await chrome.cookies.getAllCookieStores();
  const store = stores.find(item => item.tabIds.includes(destinationTabId));
  if (!store?.tabIds.includes(source.tabId)) return false;
  const credential = await chrome.tabs.sendMessage(source.tabId, {
    type: "E3_RESTORE_SESSION", requestId: source.requestId, url: pending.url,
  }, { documentId: source.documentId });
  if (!credential?.ok || typeof credential.session !== "string" || !/^[A-Za-z0-9,-]{16,256}$/.test(credential.session)) return false;
  const cookie = await chrome.cookies.set({
    url: E3_ORIGIN + "/", name: "MoodleSession", value: credential.session,
    path: "/", secure: true, httpOnly: true, sameSite: "lax", storeId: store.id,
  });
  return Boolean(cookie);
}

const navigation = createNavigationController({ storage: chrome.storage.session, tabs: chrome.tabs, restoreSession });
async function transferSchoolSession(source, destinationTabId) {
  const stores = await chrome.cookies.getAllCookieStores();
  const store = stores.find(item => item.tabIds.includes(destinationTabId));
  if (!store?.tabIds.includes(source.tabId)) return false;
  const cookie = await chrome.cookies.get({ url: E3_ORIGIN + "/", name: "MoodleSession", storeId: store.id });
  if (!cookie || !/^[A-Za-z0-9,-]{16,256}$/.test(cookie.value)) return false;
  const result = await chrome.tabs.sendMessage(source.tabId, {
    type: "E3_SCHOOL_LOGIN_SESSION", requestId: source.requestId, session: cookie.value,
  }, { documentId: source.documentId });
  return result?.ok === true;
}
const schoolLogin = createSchoolLoginController({ storage: chrome.storage.session, tabs: chrome.tabs, transfer: transferSchoolSession });
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  const controller = ["OPEN_E3_SCHOOL_LOGIN", "E3_SCHOOL_LOGIN_PENDING", "E3_SCHOOL_LOGIN_STATE"].includes(message?.type)
    ? schoolLogin : navigation;
  controller.handle(message, sender).then(sendResponse).catch(() => sendResponse({ ok: false }));
  return true;
});
chrome.tabs.onRemoved.addListener((tabId) => {
  navigation.forget(tabId).catch(() => {});
  schoolLogin.forget(tabId).catch(() => {});
});
