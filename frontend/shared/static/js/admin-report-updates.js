/* Fetch authorized server-rendered reports without replacing the page or executing response scripts. */
(() => {
  const selector = '#account-engagement, #feature-usage, #trafficSources, #visit-trend, #pageReports';
  if (!document.querySelector(selector)) return;
  const status = document.getElementById('adminReportStatus');
  let controller, revision = 0, appliedSearch = location.search;
  const reportFor = node => node?.closest(selector);
  function message(text, retry) {
    status.replaceChildren(document.createTextNode(text));
    status.hidden = !text;
    if (retry) {
      const button = document.createElement('button');
      button.type = 'button'; button.className = 'btn'; button.textContent = '重試';
      button.addEventListener('click', retry, { once: true });
      status.append(button);
    }
  }
  async function update(url, push = true) {
    controller?.abort();
    controller = new AbortController();
    const request = controller, current = ++revision;
    const startingHash = location.hash;
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; request.abort(); }, 20000);
    const roots = [...document.querySelectorAll(selector)];
    roots.forEach(node => { node.setAttribute('aria-busy', 'true'); node.classList.add('admin-report-pending'); });
    message('更新圖表中…');
    try {
      const response = await fetch(url, { credentials: 'same-origin', cache: 'no-store', signal: request.signal,
        headers: { Accept: 'text/html', 'X-Requested-With': 'E3AdminReports' } });
      if (response.redirected && new URL(response.url).pathname !== location.pathname) throw Error('登入已過期，請重新登入後再試。');
      if (!response.ok) throw Error('更新失敗，已保留原本資料。');
      const doc = new DOMParser().parseFromString(await response.text(), 'text/html');
      const replacements = roots.map(old => [old, doc.getElementById(old.id)]);
      if (replacements.some(([, node]) => !node)) throw Error('無法讀取報表，已保留原本資料。');
      if (current !== revision) return;
      const focusId = document.activeElement?.id;
      const position = { top: window.scrollY, left: window.scrollX, behavior: 'instant' };
      // Prepare everything before mutation, so failures never leave half a report.
      replacements.forEach(([old, node]) => {
        node.hidden = old.hidden;
        old.querySelectorAll('details').forEach((detail, index) => {
          if (!detail.open) return;
          const next = detail.id ? node.querySelector('#' + CSS.escape(detail.id)) : node.querySelectorAll('details')[index];
          if (next && !next.classList.contains('trend-date-picker')) next.open = true;
        });
      });
      replacements.forEach(([old, node]) => old.replaceWith(node));
      if (push) {
        const nextUrl = new URL(url);
        if (location.hash !== startingHash) nextUrl.hash = location.hash;
        history.pushState(history.state, '', nextUrl);
      }
      appliedSearch = url.search;
      const freshRefresh = doc.querySelector('[data-admin-refresh]');
      const refresh = document.querySelector('[data-admin-refresh]');
      if (freshRefresh && refresh) { refresh.href = freshRefresh.href; refresh.hash = location.hash; }
      document.dispatchEvent(new Event('admin:reports-updated'));
      if (focusId) document.getElementById(focusId)?.focus({ preventScroll: true });
      window.scrollTo(position);
      message('');
    } catch (error) {
      if (current !== revision) return;
      if (request.signal.aborted && !timedOut) return;
      if (!push) history.replaceState(history.state, '', location.pathname + appliedSearch + location.hash);
      message(timedOut ? '讀取逾時，已保留原本資料。' : (error.message || '更新失敗，已保留原本資料。'), () => update(url, push));
    } finally {
      clearTimeout(timer);
      if (current === revision) document.querySelectorAll(selector).forEach(node => {
        node.removeAttribute('aria-busy'); node.classList.remove('admin-report-pending');
      });
    }
  }
  document.addEventListener('click', event => {
    const link = event.target.closest('a[href]');
    if (!link || !reportFor(link) || event.defaultPrevented || event.button || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || link.target || link.download) return;
    const url = new URL(link.href);
    if (url.origin !== location.origin || url.pathname !== location.pathname || !url.search) return;
    event.preventDefault();
    update(url);
  });
  document.addEventListener('submit', event => {
    const form = event.target;
    if (event.defaultPrevented || !(form instanceof HTMLFormElement) || !reportFor(form) || form.method.toLowerCase() !== 'get') return;
    const url = new URL(form.action);
    if (url.origin !== location.origin || url.pathname !== location.pathname) return;
    event.preventDefault();
    url.search = new URLSearchParams(new FormData(form)).toString();
    update(url);
  });
  document.addEventListener('change', event => {
    if (event.target.matches('select[name="usage_range"]') && reportFor(event.target)) event.target.form?.requestSubmit();
  });
  window.addEventListener('popstate', () => {
    if (location.search !== appliedSearch) update(new URL(location.href), false);
    else {
      controller?.abort();
      revision++;
      message('');
      document.querySelectorAll(selector).forEach(node => { node.removeAttribute('aria-busy'); node.classList.remove('admin-report-pending'); });
    }
  });
})();
