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
  const refreshButton = byId('newsRefresh');
  const colors = createCourseColorRegistry();
  const ctx = { courseFilter: course, courseAccent: colors.colorFor, coursePickerCourses: () => state.courses.map(item => ({
    dataset: { courseId: String(item.id), courseTitle: String(item.id) },
  })) };
  const state = { items: [], courses: [], running: false, opened: new Set(), attempted: new Set(), generation: 0 };
  let pollTimer;
  let searchTimer;
  let loading = false;
  let refreshing = false;
  initializeCoursePicker(ctx);

  const element = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
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

  const bodyContents = (body, item) => {
    const text = element('p', '', item.content ?? '讀取中…');
    body.replaceChildren(text);
    if (item.links?.length) {
      const links = element('div', 'news-links');
      for (const entry of item.links) {
        const url = safeNewsLink(entry.url);
        if (!url) continue;
        const link = element('a', '', entry.title);
        link.href = url; link.target = '_blank'; link.rel = 'noopener noreferrer';
        links.append(link);
      }
      body.append(links);
    }
    const actions = element('div', 'news-item-actions');
    const restore = element('button', '', item.read_at ? '標為未讀' : '標為已讀');
    restore.type = 'button';
    restore.addEventListener('click', async () => {
      restore.disabled = true;
      const generation = state.generation;
      try {
        const result = await post(config.itemUrl, {semester: semester.value, key: item.key, read: !item.read_at});
        if (generation !== state.generation) return;
        applyUpdate(item, result.item);
        updateItem(body.closest('details'), body, item);
        counters();
      } catch (error) { if (generation === state.generation) message(error.message, true); restore.disabled = false; }
    });
    const original = element('a', '', 'E3 原文');
    const url = safeNewsLink(item.url);
    if (url) {
      original.href = url; original.target = '_blank'; original.rel = 'noopener noreferrer';
      const icon = element('span', 'news-icon news-external'); icon.setAttribute('aria-hidden', 'true'); original.append(icon);
      actions.append(original);
    }
    actions.append(restore);
    body.append(actions);
  };
  const updateItem = (details, body, item) => {
    details.dataset.read = String(!!item.read_at);
    const dot = details.querySelector('.news-unread-dot');
    if (dot) dot.hidden = !!item.read_at;
    bodyContents(body, item);
  };
  const applyUpdate = (item, incoming) => {
    Object.assign(item, incoming);
    state.items = state.items.map(current => current.key === item.key ? item : current);
  };
  const render = () => {
    const filter = {course: course.value, query: search.value, unread: unread.checked};
    const filtered = filterAnnouncements(state.items, filter);
    const visible = state.items.filter(item => filtered.includes(item) || (state.opened.has(item.key)
      && filterAnnouncements([item], {...filter, unread: false}).length));
    const fragment = document.createDocumentFragment();
    for (const item of visible) {
      const details = element('details', 'news-item');
      details.dataset.key = item.key;
      details.style.setProperty('--course-accent', colors.colorFor(`e3:${item.course_id}`));
      const summary = element('summary');
      const meta = element('span', 'news-meta');
      meta.append(element('span', 'news-course', item.course_title));
      if (item.author) meta.append(element('span', '', item.author));
      if (item.updated_ts) meta.append(element('span', '', `更新 ${announcementDate(item.updated_ts)}`));
      const dot = element('span', 'news-unread-dot'); dot.title = '未讀'; dot.setAttribute('aria-label', '未讀'); meta.append(dot);
      const title = element('span', 'news-subject', item.title);
      const chevron = element('span', 'news-icon news-chevron'); chevron.setAttribute('aria-hidden', 'true');
      summary.append(meta, title, chevron);
      const body = element('div', 'news-content');
      details.append(summary, body);
      updateItem(details, body, item);
      let restored = state.opened.has(item.key);
      if (restored) details.open = true;
      details.addEventListener('toggle', async () => {
        if (restored) { restored = false; return; }
        if (!details.open) {
          state.opened.delete(item.key);
          if (unread.checked && item.read_at) render();
          return;
        }
        state.opened.add(item.key);
        if (item.read_at && item.content !== undefined) return;
        const generation = state.generation;
        summary.setAttribute('aria-busy', 'true');
        try {
          const result = await post(config.itemUrl, {semester: semester.value, key: item.key, read: true});
          if (generation !== state.generation) return;
          applyUpdate(item, result.item);
          updateItem(details, body, item); counters();
        } catch (error) {
          if (generation !== state.generation) return;
          body.querySelector('p').textContent = error.message;
        } finally { summary.removeAttribute('aria-busy'); }
      });
      fragment.append(details);
    }
    list.replaceChildren(fragment);
    byId('newsCount').textContent = visible.length;
    byId('newsEmpty').hidden = visible.length > 0 || loading || state.running;
    byId('newsEmpty').textContent = state.items.length ? '沒有符合篩選的公告' : '目前沒有課程公告';
    counters();
  };
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
    loading = true;
    try {
      const data = await fetchJson(`${config.dataUrl}?semester=${encodeURIComponent(semester.value)}`);
      if (generation !== state.generation) return;
      const changed = JSON.stringify(state.items) !== JSON.stringify(data.items);
      state.items = data.items; state.courses = data.courses;
      state.running = data.running || (refreshing && state.running);
      syncCourses();
      byId('newsUpdated').textContent = data.fetched_at ? `上次更新 ${announcementDate(data.fetched_at)}` : '';
      message(data.error || (state.running ? '公告更新中…' : ''), !!data.error);
      loading = false;
      if (changed || !list.children.length) render(); else counters();
      schedule();
      if (auto && !refreshing && data.stale && !data.running && !state.attempted.has(semester.value)) {
        state.attempted.add(semester.value);
        await refresh();
      }
    } catch (error) {
      if (generation === state.generation) {
        state.running = false; counters(); message(error.message, true);
      }
    }
    finally {
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
    state.generation++; state.opened.clear(); state.items = []; state.courses = []; state.running = false;
    clearTimeout(pollTimer); syncCourses(); render(); void load();
  });
  course.addEventListener('change', () => { state.opened.clear(); render(); });
  unread.addEventListener('change', render);
  search.addEventListener('input', () => { clearTimeout(searchTimer); searchTimer = setTimeout(render, 120); });
  byId('newsSearchForm').addEventListener('submit', event => { event.preventDefault(); clearTimeout(searchTimer); render(); });
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
