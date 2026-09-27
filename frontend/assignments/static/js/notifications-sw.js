async function reportTestStatus(tag, status) {
  if (!/^test:[a-f0-9]{32}$/.test(tag)) return;
  const windows = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
  for (const client of windows) {
    if (new URL(client.url).pathname === "/settings/notifications") {
      client.postMessage({ type: "e3-push-test-status", tag, status });
    }
  }
}

self.addEventListener("push", (event) => {
  let payload;
  try { payload = event.data?.json(); } catch { payload = null; }
  if (!payload) return;
  event.waitUntil((async () => {
    // A receipt is local-only and never sends assignment data back to the server.
    await reportTestStatus(payload.tag, "received").catch(() => {});
    try {
      await self.registration.showNotification(payload.title || "E3作業追蹤系統", {
        body: payload.body || "有新的作業提醒", tag: payload.tag || "e3-assignment",
        data: { url: "/" }, renotify: false,
      });
    } catch (error) {
      await reportTestStatus(payload.tag, "failed").catch(() => {});
      throw error;
    }
    await reportTestStatus(payload.tag, "shown").catch(() => {});
  })());
});
self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  event.waitUntil((async () => {
    const url = new URL("/", self.location.origin).href;
    const windows = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
    for (const client of windows) {
      if (client.url === url) return client.focus();
    }
    return self.clients.openWindow(url);
  })());
});
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));
