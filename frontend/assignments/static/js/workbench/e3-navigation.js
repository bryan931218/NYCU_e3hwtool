export const PENDING_KEY = "e3_pending_navigation";
export const PENDING_TTL_MS = 2 * 60 * 60 * 1000;

function e3Base(value) {
  try {
    const base = new URL(value);
    if (!['https:', 'http:'].includes(base.protocol) || base.username || base.password) {
      return null;
    }
    base.search = '';
    base.hash = '';
    base.pathname = base.pathname.replace(/\/$/, '') + '/';
    return base;
  } catch {
    return null;
  }
}

// Only retain course/activity pages on the configured E3 site, never login actions
// or imported links to other hosts. The original anchor remains usable either way.
export function normalizeE3Target(value, baseUrl) {
  const base = e3Base(baseUrl);
  if (!base || typeof value !== 'string' || /[\x00-\x20\\]/.test(value)) {
    return null;
  }
  try {
    const target = new URL(value, base);
    if (target.origin !== base.origin || target.username || target.password) {
      return null;
    }
    if (!target.pathname.startsWith(base.pathname)) return null;
    const path = target.pathname.slice(base.pathname.length);
    if (!/^(?:mod\/[a-z0-9_]+|course)\/view\.php$/.test(path)) return null;
    const ids = target.searchParams.getAll('id');
    if (ids.length !== 1 || !/^[1-9]\d*$/.test(ids[0])) return null;
    return target.href;
  } catch {
    return null;
  }
}

export function createPendingNavigation({ storage, owner, baseUrl, now = Date.now }) {
  let memory = null;
  let loaded = false;
  function clear() {
    loaded = true;
    memory = null;
    try {
      storage?.removeItem(PENDING_KEY);
    } catch {}
  }
  function read() {
    let candidate = memory;
    if (!loaded) {
      loaded = true;
      try {
        const raw = storage?.getItem(PENDING_KEY);
        if (raw) candidate = JSON.parse(raw);
      } catch {
        // Storage may be disabled. Keep the return link available in this page.
      }
    }
    const url = normalizeE3Target(candidate?.url, baseUrl);
    const age = now() - candidate?.createdAt;
    if (
      !owner || candidate?.owner !== owner || !url ||
      !Number.isFinite(candidate?.createdAt) || age < 0 || age >= PENDING_TTL_MS
    ) {
      clear();
      return null;
    }
    memory = {
      ...candidate,
      url,
      title: typeof candidate.title === 'string' ? candidate.title.slice(0, 300) : 'E3 作業',
      course: typeof candidate.course === 'string' ? candidate.course.slice(0, 300) : '',
    };
    return memory;
  }
  function remember(url, title, course) {
    const target = normalizeE3Target(url, baseUrl);
    if (!target || !owner) return null;
    loaded = true;
    memory = {
      owner,
      url: target,
      title: String(title || 'E3 作業').slice(0, 300),
      course: String(course || '').slice(0, 300),
      createdAt: now(),
    };
    try {
      storage?.setItem(PENDING_KEY, JSON.stringify(memory));
    } catch {}
    return memory;
  }
  return { read, remember, clear };
}

export function register(ctx) {}

export function initialize(ctx) {
  const panel = document.getElementById('e3NavigationRecovery');
  const target = document.getElementById('e3NavigationTarget');
  const resume = document.getElementById('e3NavigationResume');
  const login = document.getElementById('e3NavigationLogin');
  const dismiss = document.getElementById('e3NavigationDismiss');
  const base = e3Base(ctx.config.e3BaseUrl);
  const owners = ctx.config.navigationOwner;
  if (
    !panel || !target || !resume || !login || !dismiss || !base ||
    !Array.isArray(owners) || owners.length !== 2 ||
    !owners.every((value) => typeof value === 'string' && value)
  ) return;
  let storage;
  try {
    storage = window.sessionStorage;
  } catch {}
  const pending = createPendingNavigation({
    storage,
    owner: JSON.stringify(owners),
    baseUrl: base.href,
  });
  login.href = new URL('login/index.php', base).href;

  function render() {
    const item = pending.read();
    panel.hidden = !item;
    if (item) {
      target.textContent = [item.course, item.title].filter(Boolean).join(' ／ ');
      target.title = target.textContent;
      resume.href = item.url;
    } else {
      target.textContent = '';
      target.removeAttribute('title');
      resume.removeAttribute('href');
    }
    return item;
  }
  function rememberClick(event) {
    if (event.defaultPrevented || (event.type === 'auxclick' && event.button !== 1)) {
      return;
    }
    const link = event.target.closest?.('a[data-e3-assignment]');
    if (!link) return;
    const row = link.closest('tr[data-uid]');
    if (pending.remember(link.getAttribute('href'), row?.dataset.title, row?.dataset.course)) {
      render();
    }
    // Keep the native new-tab click; E3 owns its login and SSO flow.
  }
  document.addEventListener('click', rememberClick);
  document.addEventListener('auxclick', rememberClick);
  resume.addEventListener('click', (event) => {
    if (!render()) {
      event.preventDefault();
      ctx.showToast?.('返回連結已到期，請重新點選作業的「前往 E3」。', 'info');
    }
  });
  dismiss.addEventListener('click', () => {
    pending.clear();
    render();
  });
  window.addEventListener('focus', render);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') render();
  });
  render();
}
