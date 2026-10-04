import { createUnreadUpdater, updateUnreadIndicator } from '../course-message-unread.js';

export function register(ctx) {
  const entry = document.querySelector('.workspace-messages');
  if (!entry?.dataset.unreadUrl) return;
  const semester = () => ctx.currentSemesterFilters?.[0] || '';
  const refresh = createUnreadUpdater(entry.dataset.unreadUrl, semester, counts => updateUnreadIndicator(entry, counts.total, '課程訊息'));
  let previous;
  ctx.refreshCourseMessageUnread = (force = false) => {
    if (document.hidden || (!force && previous === semester())) return;
    previous = semester();
    const url = new URL(entry.href);
    if (previous) url.searchParams.set('semester', previous); else url.searchParams.delete('semester');
    entry.href = url.href;
    void refresh();
  };
}

export function initialize(ctx) {
  if (!ctx.refreshCourseMessageUnread) return;
  let timer;
  const resume = (force = true) => {
    clearInterval(timer);
    if (document.hidden) return;
    ctx.refreshCourseMessageUnread(force);
    timer = setInterval(() => ctx.refreshCourseMessageUnread(true), 60000);
  };
  resume(false);
  document.addEventListener('visibilitychange', () => resume());
  window.addEventListener('pageshow', event => { if (event.persisted) resume(); });
  window.addEventListener('pagehide', () => clearInterval(timer));
}
