/* Progressive organization: without JavaScript every management section remains usable. */
(() => {
  const nav = document.querySelector('.admin-view-nav');
  if (!nav) return;
  const links = [...nav.querySelectorAll('[data-admin-view]')];
  const activity = document.querySelector('[data-activity-url]');
  const activityRefresh = activity?.querySelector('[data-admin-activity-refresh]');
  const activityStatus = activity?.querySelector('[data-activity-status]');
  let activityResults = activity?.querySelector('[data-activity-results]');
  let filterActivity;
  let activityLoading = false;
  let previousView;

  async function refreshActivity() {
    if (!activityResults || activityResults.hasAttribute('data-activity-history') || activityLoading || document.hidden) return;
    activityLoading = true;
    activityResults.setAttribute('aria-busy', 'true');
    activityStatus.textContent = '更新紀錄中…';
    const url = new URL(activity.dataset.activityUrl, location.origin);
    url.search = location.search;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 12000);
    try {
      const response = await fetch(url, { cache: 'no-store', signal: controller.signal });
      if (response.redirected) {
        activityStatus.textContent = '登入已過期，請重新登入後再試。';
        return;
      }
      if (!response.ok) throw new Error();
      const html = new DOMParser().parseFromString(await response.text(), 'text/html');
      const next = html.querySelector('[data-activity-results]');
      if (!next) throw new Error();
      activityResults.replaceWith(next);
      activityResults = next;
      filterActivity();
      activityStatus.textContent = `更新於 ${next.dataset.activityUpdated}`;
    } catch (_) {
      activityStatus.textContent = '更新失敗，已保留上次讀取的紀錄。';
    } finally {
      clearTimeout(timer);
      activityResults.removeAttribute('aria-busy');
      activityLoading = false;
    }
  }

  const resolve = () => {
    let id;
    try { id = decodeURIComponent(location.hash.slice(1)); } catch (_) { id = ''; }
    return document.getElementById(id)?.closest('[data-admin-panel]')?.dataset.adminPanel || 'overview';
  };
  function show() {
    const selected = resolve();
    document.querySelectorAll('[data-admin-panel]').forEach(panel => { panel.hidden = panel.dataset.adminPanel !== selected; });
    links.forEach(link => {
      if (link.dataset.adminView === selected) link.setAttribute('aria-current', 'location');
      else link.removeAttribute('aria-current');
    });
    const refresh = document.querySelector('[data-admin-refresh]');
    if (refresh) refresh.hash = location.hash;
    if (selected === 'activity' && previousView && previousView !== selected) refreshActivity();
    previousView = selected;
    // Chart.js observes the visible container; notify it after revealing a report.
    window.dispatchEvent(new Event('resize'));
  }
  links.forEach(link => link.addEventListener('click', event => {
    if (event.button || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    if (location.hash !== link.getAttribute('href')) history.pushState(history.state, '', link.getAttribute('href'));
    show();
    nav.scrollIntoView({ block: 'start', behavior: 'instant' });
  }));
  window.addEventListener('hashchange', show);
  window.addEventListener('popstate', show);
  document.addEventListener('admin:reports-updated', show);
  const status = document.querySelector('[data-admin-status]');
  document.querySelectorAll('[data-admin-search]').forEach(input => {
    const type = input.dataset.adminSearch;
    const accountRows = type === 'accounts' ? document.querySelectorAll('.account-usage-table tbody tr') : null;
    const output = document.querySelector(`[data-admin-count="${type}"]`);
    const filter = () => {
      const query = input.value.trim().toLocaleLowerCase();
      let count = 0;
      const rows = accountRows ?? activityResults.querySelectorAll('.events > li');
      rows.forEach(row => {
        const mode = type === 'accounts' ? status?.value : 'all';
        const matches = row.textContent.toLocaleLowerCase().includes(query)
          && (mode !== 'online' || !!row.querySelector('.badge.online'))
          && (mode !== 'new' || !!row.querySelector('.new-user-badge'));
        row.hidden = !matches;
        if (matches && !row.querySelector('td[colspan]') && row.querySelector(type === 'accounts' ? '.account-label' : '.event-account')) count++;
      });
      output.textContent = `${count} ${type === 'accounts' ? '位用戶' : '筆紀錄'}${query || (status?.value !== 'all' && type === 'accounts') ? '符合條件' : ''}`;
    };
    input.addEventListener('input', filter);
    if (type === 'accounts') status?.addEventListener('change', filter);
    else filterActivity = filter;
    filter();
  });
  activityRefresh?.addEventListener('click', event => {
    if (event.button || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || activityResults.hasAttribute('data-activity-history')) return;
    event.preventDefault();
    refreshActivity();
  });
  document.addEventListener('visibilitychange', () => {
    if (previousView === 'activity') refreshActivity();
  });
  show();
})();
