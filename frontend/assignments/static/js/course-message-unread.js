export function updateUnreadIndicator(link, count, label) {
  const dot = link?.querySelector('[data-unread-dot]');
  if (!dot) return;
  const unread = Number.isFinite(count) && count > 0;
  dot.hidden = !unread;
  link.setAttribute('aria-label', unread ? `${label}，有未讀訊息` : label);
  link.title = unread ? `${label}：${count} 則未讀` : label;
}

export function createUnreadUpdater(endpoint, getSemester, render, request = fetch) {
  let revision = 0;
  let controller;
  let previousSemester;
  return async () => {
    const semester = getSemester();
    if (semester !== previousSemester) render({announcements: 0, mail: 0, total: 0});
    previousSemester = semester;
    const version = ++revision;
    controller?.abort();
    controller = new AbortController();
    const pending = controller;
    const timeout = setTimeout(() => pending.abort(), 8000);
    try {
      const response = await request(`${endpoint}?semester=${encodeURIComponent(semester)}`, {
        credentials: 'same-origin', cache: 'no-store', signal: pending.signal,
      });
      if (!response.ok || response.redirected) return;
      const data = await response.json();
      if (data.ok && version === revision && semester === getSemester()) render(data);
    } catch {
      // A failed badge refresh must not hide a previously known unread message.
    } finally { clearTimeout(timeout); }
  };
}
