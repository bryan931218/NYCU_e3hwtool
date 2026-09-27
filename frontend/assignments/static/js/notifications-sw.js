self.addEventListener("push", (event) => {
  let payload;
  try { payload = event.data?.json(); } catch { payload = null; }
  if (!payload) return;
  event.waitUntil(self.registration.showNotification(payload.title || "E3作業追蹤系統", {
    body: payload.body || "有新的作業提醒", tag: payload.tag || "e3-assignment",
    data: { url: "/" }, renotify: false,
  }));
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
