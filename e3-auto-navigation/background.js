import { createNavigationController, E3_ORIGIN } from "./core.js";

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
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  navigation.handle(message, sender).then(sendResponse).catch(() => sendResponse({ ok: false }));
  return true;
});
chrome.tabs.onRemoved.addListener((tabId) => { navigation.forget(tabId).catch(() => {}); });
