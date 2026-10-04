import { createNavigationController } from "./core.js";

const navigation = createNavigationController({ storage: chrome.storage.session, tabs: chrome.tabs });
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  navigation.handle(message, sender).then(sendResponse).catch(() => sendResponse({ ok: false }));
  return true;
});
chrome.tabs.onRemoved.addListener((tabId) => { navigation.forget(tabId).catch(() => {}); });
