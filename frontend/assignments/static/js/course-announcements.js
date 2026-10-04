import { initialize as initializeCoursePicker } from './workbench/course-filter.js';
import { createCourseColorRegistry } from './workbench/course-colors.js';

export function filterAnnouncements(items, { course = '', query = '', unread = false } = {}) {
  const text = query.trim().toLocaleLowerCase('zh-Hant');
  return items.filter(item => (!course || String(item.course_id) === course)
    && (!unread || !item.read_at)
    && (!text || `${item.title} ${item.course_title} ${item.author || ''} ${item.content || ''}`.toLocaleLowerCase('zh-Hant').includes(text)));
}

export function safeNewsLink(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && !url.username && !url.password ? url.href : '';
  } catch { return ''; }
}

export function visibleAnnouncements(items, filters, activeKey = '') {
  const visible = new Set(filterAnnouncements(items, filters).map(item => item.key));
  // Keep the selected, newly read item until navigation, but still respect search/course filters.
  return items.filter(item => visible.has(item.key) || (item.key === activeKey
    && filterAnnouncements([item], {...filters, unread: false}).length));
}

export function announcementDate(timestamp) {
  if (!timestamp) return '';
  const date = new Date(timestamp * 1000);
  return Number.isFinite(date.getTime()) ? new Intl.DateTimeFormat('zh-TW', {
    timeZone: 'Asia/Taipei', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).format(date) : '';
}

function initialize(config) {
  const byId = id => document.getElementById(id);
  const semester = byId('newsSemester');
  const course = byId('courseFilter');
  const search = byId('newsSearch');
  const unread = byId('newsUnread');
  const list = byId('newsList');
  const reader = byId('newsReader');
  const workspace = byId('newsWorkspace');
  const refreshButton = byId('newsRefresh');
  const mobile = matchMedia('(max-width: 680px)');
  const colors = createCourseColorRegistry();
  const state = { items: [], courses: [], running: false, active: '', attempted: new Set(), generation: 0, errors: new Map(), pending: new Set() };
  const ctx = { courseFilter: course, courseAccent: colors.colorFor, coursePickerCourses: () => state.courses.map(item => ({
    dataset: { courseId: String(item.id), courseTitle: String(item.id) },
  })) };
  let pollTimer;
  let searchTimer;
  let loading = false;
  let refreshing = false;
  let itemRevision = 0;
  initializeCoursePicker(ctx);

  const element = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const icon = className => {
    const node = element('span', `news-icon ${className}`);
    node.setAttribute('aria-hidden', 'true');
    return node;
  };
  const button = (label, handler, className = 'btn') => {
    const node = element('button', className, label);
    node.type = 'button';
    node.addEventListener('click', handler);
    return node;
  };
  const message = (text = '', error = false) => {
    const node = byId('newsMessage');
    node.textContent = text;
    node.hidden = !text;
    node.dataset.error = String(error);
  };
  const counters = () => {
    byId('newsUnreadCount').textContent = state.items.filter(item => !item.read_at).length;
    byId('newsCount').textContent = state.items.length;
    refreshButton.disabled = state.running || refreshing;
    refreshButton.setAttribute('aria-busy', String(state.running || refreshing));
  };
  const fetchJson = async (url, options = {}) => {
    const response = await fetch(url, {credentials: 'same-origin', cache: 'no-store',
      signal: AbortSignal.timeout(20000), ...options});
    if (response.redirected) throw Error('登入已失效，請重新登入。');
    let result;
    try { result = await response.json(); } catch { throw Error('無法連線，請稍後再試。'); }
    if (!response.ok || !result.ok) throw Error(result.error || '公告更新失敗，請稍後再試。');
    return result;
  };
  const post = (url, payload) => fetchJson(url, {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
  const activeItem = () => state.items.find(item => item.key === state.active);
  const requestKey = key => `${state.generation}:${key}`;
  const applyUpdate = incoming => {
    itemRevision++;
    state.items = state.items.map(item => item.key === incoming.key ? incoming : item);
  };
  const returnToList = () => {
    workspace.dataset.reading = 'false';
    const key = state.active;
    if (unread.checked && activeItem()?.read_at) state.active = '';
    renderList();
    const target = [...list.children].find(node => node.dataset.key === key) || list.firstElementChild;
    target?.focus({preventScroll: !mobile.matches});
  };
  const renderReader = () => {
    const item = activeItem();
    const focusedId = reader.contains(document.activeElement) ? document.activeElement.id : '';
    reader.replaceChildren();
    reader.setAttribute('aria-busy', String(item && state.pending.has(requestKey(item.key))));
    if (!item) {
      reader.append(element('p', 'news-reader-empty', '選擇公告'));
      return;
    }
    reader.style.setProperty('--course-accent', colors.colorFor(`e3:${item.course_id}`));
    const back = button('公告列表', returnToList, 'btn news-mobile-back');
    back.id = 'newsReaderBack';
    back.prepend(icon('news-back'));
    const heading = element('header', 'news-reader-heading');
    const title = element('h2', '', item.title);
    title.tabIndex = -1;
    title.id = 'newsReaderTitle';
    heading.append(element('div', 'news-course', item.course_title), title);
    const meta = element('div', 'news-reader-meta');
    if (item.author) meta.append(element('span', '', item.author));
    if (item.updated_ts) meta.append(element('span', '', announcementDate(item.updated_ts)));
    meta.append(element('span', '', item.read_at ? '已讀' : '未讀'));
    heading.append(meta);
    const actions = element('div', 'news-reader-actions');
    const mark = button(item.read_at ? '標為未讀' : '標為已讀', () => setRead(item));
    mark.id = 'newsReaderMark';
    mark.disabled = state.pending.has(requestKey(item.key));
    actions.append(mark);
    const url = safeNewsLink(item.url);
    if (url) {
      const original = element('a', 'btn', '前往 E3');
      original.href = url; original.target = '_blank'; original.rel = 'noopener noreferrer';
      original.append(icon('news-external'));
      actions.append(original);
    }
    const error = state.errors.get(item.key);
    if (error) {
      const retry = button('重新讀取', () => openItem(item.key, true));
      retry.id = 'newsReaderRetry';
      retry.prepend(icon('news-refresh-icon'));
      actions.append(retry);
    }
    heading.append(actions);
    const body = element('p', 'news-content', item.content !== undefined ? item.content || '此公告沒有文字內文。' : error ?? '讀取內文中…');
    body.dataset.error = String(!!error && item.content === undefined);
    if (item.content === undefined && !error) body.classList.add('news-loading');
    reader.append(back, heading, body);
    if (item.links?.length) {
      const links = element('div', 'news-links');
      links.append(element('h3', '', '附件與連結'));
      for (const entry of item.links) {
        const url = safeNewsLink(entry.url);
        if (!url) continue;
        const link = element('a', '', entry.title);
        link.href = url; link.target = '_blank'; link.rel = 'noopener noreferrer';
        link.append(icon('news-external'));
        links.append(link);
      }
      reader.append(links);
    }
    if (focusedId) byId(focusedId)?.focus({preventScroll: true});
  };
  const visibleItems = () => {
    const filter = {course: course.value, query: search.value, unread: unread.checked};
    return visibleAnnouncements(state.items, filter, state.active);
  };
  const renderList = () => {
    const visible = visibleItems();
    const focused = list.contains(document.activeElement) ? document.activeElement.dataset.key : '';
    const fragment = document.createDocumentFragment();
    for (const item of visible) {
      const node = button('', () => openItem(item.key), 'news-item');
      node.dataset.key = item.key;
      node.dataset.read = String(!!item.read_at);
      node.setAttribute('aria-pressed', String(item.key === state.active));
      node.setAttribute('aria-controls', 'newsReader');
      node.style.setProperty('--course-accent', colors.colorFor(`e3:${item.course_id}`));
      node.append(element('span', 'news-course', item.course_title), element('span', 'news-subject', item.title));
      if (item.content) node.append(element('span', 'news-preview', item.content.replace(/\s+/g, ' ').slice(0, 140)));
      const bottom = element('span', 'news-item-bottom');
      bottom.append(element('span', '', announcementDate(item.updated_ts)));
      if (!item.read_at) bottom.append(element('span', 'news-unread-badge', '未讀'));
      node.append(bottom);
      fragment.append(node);
    }
    list.replaceChildren(fragment);
    if (focused) [...list.children].find(node => node.dataset.key === focused)?.focus({preventScroll: true});
    byId('newsListCount').textContent = visible.length;
    byId('newsEmpty').hidden = visible.length > 0 || loading || state.running;
    byId('newsEmpty').textContent = state.items.length ? '沒有符合篩選的公告' : '目前沒有課程公告';
    counters();
  };
  const setRead = async item => {
    const generation = state.generation;
    const key = requestKey(item.key);
    if (state.pending.has(key)) return;
    const restoreFocus = document.activeElement.id === 'newsReaderMark';
    state.pending.add(key); renderReader();
    try {
      const result = await post(config.itemUrl, {semester: semester.value, key: item.key, read: !item.read_at, load_content: false});
      if (generation !== state.generation) return;
      applyUpdate(result.item);
    } catch (error) { if (generation === state.generation) message(error.message, true); }
    finally {
      state.pending.delete(key);
      if (generation === state.generation) {
        renderList();
        if (state.active === item.key) {
          renderReader();
          if (restoreFocus && document.activeElement === document.body) byId('newsReaderMark')?.focus({preventScroll: true});
        }
      }
    }
  };
  const openItem = async (key, retry = false) => {
    const item = state.items.find(item => item.key === key);
    if (!item) return;
    state.active = key; workspace.dataset.reading = 'true';
    renderList(); renderReader();
    if (mobile.matches) byId('newsReaderTitle')?.focus({preventScroll: false});
    const pendingKey = requestKey(key);
    if (state.pending.has(pendingKey) || (!retry && (state.errors.has(key) || (item.read_at && item.content !== undefined)))) return;
    const generation = state.generation;
    state.errors.delete(key); state.pending.add(pendingKey); renderReader();
    try {
      const result = await post(config.itemUrl, {semester: semester.value, key, read: true});
      if (generation !== state.generation) return;
      applyUpdate(result.item);
    } catch (error) {
      if (generation === state.generation) state.errors.set(key, error.message);
    } finally {
      state.pending.delete(pendingKey);
      if (generation === state.generation) {
        renderList();
        if (state.active === key) renderReader();
      }
    }
  };
  const render = (autoSelect = false) => {
    const visible = visibleItems();
    if (!visible.some(item => item.key === state.active)) {
      state.active = ''; workspace.dataset.reading = 'false';
    }
    renderList(); renderReader();
    if (autoSelect && !mobile.matches && !state.active && visible.length) void openItem(visible[0].key);
  };
  list.addEventListener('keydown', event => {
    if (!['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) return;
    const nodes = [...list.children];
    const index = nodes.indexOf(document.activeElement);
    if (index < 0) return;
    event.preventDefault();
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? nodes.length - 1 : Math.min(nodes.length - 1, Math.max(0, index + (event.key === 'ArrowDown' ? 1 : -1)));
    nodes[next]?.focus();
  });
  const syncCourses = () => {
    colors.ensure(state.courses.map(item => `e3:${item.id}`));
    const entries = [{id:'', title:'全部課程'}, ...state.courses];
    const signature = JSON.stringify(entries);
    if (course.dataset.signature !== signature) {
      const value = course.value;
      course.replaceChildren(...entries.map(item => { const option = element('option', '', item.title); option.value = String(item.id); return option; }));
      if (entries.some(item => String(item.id) === value)) course.value = value;
      course.dataset.signature = signature;
    }
    ctx.syncCoursePicker?.();
  };
  const schedule = () => {
    clearTimeout(pollTimer);
    if (state.running && !document.hidden) pollTimer = setTimeout(() => load(false), 2500);
  };
  const load = async (auto = true) => {
    if (loading || !semester.value || config.guest) return;
    const generation = state.generation;
    const revision = itemRevision;
    loading = true;
    try {
      const data = await fetchJson(`${config.dataUrl}?semester=${encodeURIComponent(semester.value)}`);
      if (generation !== state.generation) return;
      const changed = JSON.stringify(state.items) !== JSON.stringify(data.items);
      if (revision === itemRevision) state.items = data.items;
      state.courses = data.courses;
      state.running = data.running || (refreshing && state.running);
      syncCourses();
      byId('newsUpdated').textContent = data.fetched_at ? `上次更新 ${announcementDate(data.fetched_at)}` : '';
      message(data.error || (state.running ? '公告更新中…' : ''), !!data.error);
      loading = false;
      if (changed || !list.children.length) render(true); else counters();
      schedule();
      if (auto && !refreshing && data.stale && !data.running && !state.attempted.has(semester.value)) {
        state.attempted.add(semester.value);
        await refresh();
      }
    } catch (error) {
      if (generation === state.generation) {
        state.running = false; counters(); message(error.message, true);
      }
    } finally {
      loading = false;
      if (generation !== state.generation) void load();
    }
  };
  const refresh = async () => {
    if (refreshing || !semester.value || config.guest) return;
    refreshing = true; counters();
    const generation = state.generation;
    try {
      const result = await post(config.refreshUrl, {semester: semester.value});
      if (generation !== state.generation) return;
      state.running = result.status === 'running';
      message(state.running ? '公告更新中…' : '請稍候再更新。');
      await load(false);
    } catch (error) { if (generation === state.generation) message(error.message, true); }
    finally {
      refreshing = false; counters(); schedule();
      if (generation !== state.generation) void load();
    }
  };
  refreshButton.addEventListener('click', refresh);
  semester.addEventListener('change', () => {
    state.generation++; state.active = ''; state.errors.clear(); state.items = []; state.courses = []; state.running = false;
    workspace.dataset.reading = 'false';
    clearTimeout(pollTimer); syncCourses(); render(); void load();
  });
  course.addEventListener('change', () => { state.active = ''; render(true); });
  unread.addEventListener('change', () => { state.active = ''; render(true); });
  search.addEventListener('input', () => { clearTimeout(searchTimer); searchTimer = setTimeout(() => render(true), 150); });
  byId('newsSearchForm').addEventListener('submit', event => { event.preventDefault(); clearTimeout(searchTimer); render(true); });
  document.addEventListener('visibilitychange', () => { if (document.hidden) clearTimeout(pollTimer); else void load(false); });
  byId('newsTheme').addEventListener('click', () => {
    const theme = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
    try { localStorage.setItem('e3_theme', theme); } catch {}
  });
  if (config.guest || !semester.value) {
    refreshButton.disabled = true;
    message(config.guest ? '訪客模式無法連線至 E3 公告。' : '請先更新作業以取得課程。');
  } else { message('讀取公告中…'); void load(); }
}

if (typeof document !== 'undefined') {
  const config = document.getElementById('course-news-config');
  if (config) initialize(JSON.parse(config.textContent));
}
