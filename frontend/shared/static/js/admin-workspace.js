/* Progressive organization: without JavaScript every management section remains usable. */
(() => {
  const nav = document.querySelector('.admin-view-nav');
  if (!nav) return;
  const links = [...nav.querySelectorAll('[data-admin-view]')];
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
  show();

  const status = document.querySelector('[data-admin-status]');
  document.querySelectorAll('[data-admin-search]').forEach(input => {
    const type = input.dataset.adminSearch;
    const rows = [...document.querySelectorAll(type === 'accounts' ? '.account-usage-table tbody tr' : '.events > li')];
    const output = document.querySelector(`[data-admin-count="${type}"]`);
    const filter = () => {
      const query = input.value.trim().toLocaleLowerCase();
      let count = 0;
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
    filter();
  });
})();
