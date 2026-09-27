const TEST_TAG = /^test:[a-f0-9]{32}$/;

export function createPushDiagnostics(worker, report, timeoutMs = 20000) {
  const receipts = new Map();
  let pending = null;
  let timer = null;
  const cancel = () => {
    clearTimeout(timer);
    timer = null;
    pending = null;
  };
  const apply = (status) => {
    if (status === "received") {
      report("此裝置已收到推播，正在建立通知");
      return;
    }
    cancel();
    report(status === "shown"
      ? "此裝置已收到推播並建立通知；若沒有彈出，請查看系統通知中心與勿擾設定"
      : "此裝置已收到推播，但無法建立通知；請檢查瀏覽器通知權限", status === "failed");
  };
  const onMessage = (event) => {
    const data = event.data;
    if (!event.source?.scriptURL?.endsWith("/assignment-notifications-sw.js") ||
        data?.type !== "e3-push-test-status" || !TEST_TAG.test(data.tag) ||
        !["received", "shown", "failed"].includes(data.status)) return;
    receipts.set(data.tag, data.status);
    if (receipts.size > 10) receipts.delete(receipts.keys().next().value);
    if (data.tag === pending) apply(data.status);
  };
  worker.addEventListener("message", onMessage);
  return {
    cancel,
    track(tag) {
      cancel();
      if (!TEST_TAG.test(tag)) return;
      pending = tag;
      report("推播服務已接受，等待此裝置回報");
      timer = setTimeout(() => {
        cancel();
        report("尚未收到此裝置的推播回報；請先測試本機通知，再檢查瀏覽器連線", true);
      }, timeoutMs);
      if (receipts.has(tag)) apply(receipts.get(tag));
    },
    dispose() { cancel(); worker.removeEventListener("message", onMessage); },
  };
}

export async function testLocalNotification(registration, permission) {
  if (permission !== "granted") throw new Error("請先在瀏覽器網站設定允許通知");
  if (!registration?.active) throw new Error("請先啟用此裝置，再測試本機通知");
  await registration.showNotification("E3 裝置通知測試", {
    body: "這是本機通知測試，不經過伺服器推播。",
    tag: `e3-local-test:${Date.now()}`, data: { url: "/" },
  });
}
