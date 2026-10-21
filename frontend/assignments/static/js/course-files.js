import { initialize as initializeCoursePicker } from './workbench/course-filter.js';
import { createCourseColorRegistry } from './workbench/course-colors.js';

const SOURCE_LABELS = {course: '課程教材', assignment: '作業附件', announcements: '公告附件', mail: '信件附件'};
const TYPE_LABELS = {pdf: 'PDF', slides: 'PPT', document: 'DOC', spreadsheet: 'XLS', archive: 'ZIP', image: 'IMG', link: 'LINK', other: 'FILE'};

export const fileSearchText = value => String(value || '').normalize('NFKC').toLocaleLowerCase('zh-Hant').replace(/[^\p{L}\p{N}]+/gu, '');

export function filterFiles(items, {course = '', source = '', type = '', query = '', sort = 'course'} = {}) {
  const words = query.trim().split(/\s+/).map(fileSearchText).filter(Boolean);
  const compare = (a, b) => a.localeCompare(b, 'zh-Hant', {numeric: true});
  return items.filter(item => (!course || String(item.course_id) === course)
    && (!type || item.type === type)
    && (!source || item.origins.some(origin => origin.kind === source))
    && words.every(word => fileSearchText(`${item.title} ${item.filename} ${item.course_title} ${item.origins.map(origin => origin.title).join(' ')}`).includes(word)))
    .sort((a, b) => (sort === 'updated' ? (b.updated_ts || 0) - (a.updated_ts || 0) : sort === 'course' ? compare(a.course_title, b.course_title) : 0)
      || compare(a.filename || a.title, b.filename || b.title) || compare(a.id, b.id));
}

export function groupFiles(items, courses) {
  return courses.map(course => ({...course, items: items.filter(item => String(item.course_id) === String(course.id))}))
    .filter(group => group.items.length);
}

export function safeLibraryLink(value, {origin = false} = {}) {
  try {
    if (origin && /^\/courses\/messages\?/.test(value)) {
      const url = new URL(value, 'https://local.invalid');
      return url.origin === 'https://local.invalid' && url.pathname === '/courses/messages' ? value : '';
    }
    const url = new URL(value);
    if (url.protocol !== 'https:' || url.username || url.password
      || [...url.searchParams.keys()].some(key => /^(sesskey|token|wstoken|moodlesession|access_token|auth|key)$/i.test(key))) return '';
    return url.href;
  } catch { return ''; }
}

function initialize(config) {
  const byId = id => document.getElementById(id);
  const semester = byId('filesSemester');
  const course = byId('courseFilter');
  const search = byId('filesSearch');
  const source = byId('filesSource');
  const type = byId('filesType');
  const sort = byId('filesSort');
  const list = byId('filesList');
  const refreshButton = byId('filesRefresh');
  const colors = createCourseColorRegistry();
  const state = {items: [], courses: [], running: false, remaining: 0, limit: 50, generation: 0, ready: false, error: '', requestedCourse: config.course || ''};
  search.value = config.query || '';
  const attempted = new Set();
  let controller;
  let poll;
  let refreshing = false;
  let rendered = '';
  let renderedCourses = '';
  const ctx = {courseFilter: course, courseAccent: colors.colorFor,
    coursePickerCourses: () => state.courses.map(item => ({dataset: {courseId: String(item.id), courseTitle: String(item.id)}}))};
  initializeCoursePicker(ctx);
  const el = (tag, className, text) => {
    const node = document.createElement(tag);
    node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const status = (text = '', error = false) => {
    byId('filesStatus').textContent = text;
    byId('filesStatus').hidden = !text;
    byId('filesStatus').dataset.error = String(error);
  };
  const controls = () => {
    refreshButton.disabled = refreshing || state.running || config.guest || !semester.value;
    refreshButton.setAttribute('aria-busy', String(refreshing || state.running));
    byId('filesRefreshLabel').textContent = state.running ? '整理中' : state.remaining ? '繼續整理' : '更新收納';
    list.setAttribute('aria-busy', String(!state.ready));
  };
  const anchor = (className, title, value, origin = false) => {
    const url = safeLibraryLink(value, {origin});
    if (!url) return el('span', className, title);
    const node = el('a', className, title);
    node.href = url;
    if (!url.startsWith('/')) { node.target = '_blank'; node.rel = 'noopener noreferrer'; }
    return node;
  };
  const originLink = origin => {
    const link = anchor('file-origin', undefined, origin.url, true);
    link.title = `${SOURCE_LABELS[origin.kind] || '來源'}：${origin.title}`;
    const label = SOURCE_LABELS[origin.kind] || '來源';
    link.append(el('span', 'file-origin-kind', label));
    if (origin.title !== label) link.append(el('span', 'file-origin-title', origin.title));
    return link;
  };
  const syncNavigation = () => {
    const counts = new Map();
    for (const item of state.items) counts.set(String(item.course_id), (counts.get(String(item.course_id)) || 0) + 1);
    const entries = [{id: '', title: '全部課程', name: '全部課程'}, ...state.courses];
    const signature = JSON.stringify([entries, [...counts], state.ready]);
    if (renderedCourses !== signature) {
      const focused = byId('filesCourses').contains(document.activeElement) ? document.activeElement.dataset.course : undefined;
      byId('filesCourses').replaceChildren(...entries.map(item => {
        const button = el('button', 'files-course'); button.type = 'button'; button.dataset.course = String(item.id);
        button.title = item.title;
        if (item.id !== '') button.style.setProperty('--course-accent', colors.colorFor(`e3:${item.id}`));
        const icon = el('span', 'files-course-icon'); icon.setAttribute('aria-hidden', 'true');
        button.append(icon, el('span', 'files-course-name', item.name || item.title),
          el('span', 'files-course-count', state.ready ? String(item.id === '' ? state.items.length : counts.get(String(item.id)) || 0) : '–'));
        button.addEventListener('click', () => { course.value = String(item.id); course.dispatchEvent(new Event('change')); });
        return button;
      }));
      renderedCourses = signature;
      if (focused !== undefined) [...byId('filesCourses').children].find(button => button.dataset.course === focused)?.focus({preventScroll: true});
    }
    for (const button of byId('filesCourses').children) button.setAttribute('aria-pressed', String(button.dataset.course === course.value));
    ctx.syncCoursePicker?.();
  };
  const render = () => {
    const visible = filterFiles(state.items, {course: course.value, source: source.value, type: type.value, query: search.value, sort: sort.value});
    const signature = JSON.stringify([visible.slice(0, state.limit), state.courses, source.value]);
    if (signature !== rendered) {
      const fragment = document.createDocumentFragment();
      for (const group of groupFiles(visible.slice(0, state.limit), state.courses)) {
        const section = el('section', 'files-group');
        section.style.setProperty('--course-accent', colors.colorFor(`e3:${group.id}`));
        const heading = el('h2', 'files-group-heading');
        const count = visible.filter(item => String(item.course_id) === String(group.id)).length;
        heading.append(el('span', '', group.name || group.title), el('small', '', `${count} 項`));
        const rows = el('ul', 'files-group-list');
        for (const item of group.items) {
          const row = el('li', 'file-row');
          const badge = el('span', 'file-badge', item.extension || TYPE_LABELS[item.type] || 'FILE');
          badge.dataset.type = item.type; badge.setAttribute('aria-hidden', 'true');
          const main = el('div', 'file-main');
          const title = item.type !== 'link' && item.filename ? item.filename : item.title;
          const link = anchor('file-title', title, item.url); link.title = item.title === title ? title : `${item.title}\n${title}`;
          main.append(link);
          const origins = el('div', 'file-origins');
          const ordered = [...item.origins].sort((a, b) => Number(b.kind === source.value) - Number(a.kind === source.value));
          if (ordered.length === 1) origins.append(originLink(ordered[0]));
          else if (ordered.length) {
            const details = el('details', '');
            details.append(el('summary', '', `${SOURCE_LABELS[ordered[0].kind] || '來源'} · ${ordered.length} 個出處`));
            details.append(...ordered.map(originLink)); origins.append(details);
          }
          main.append(origins);
          const open = anchor('icon-button file-open', undefined, item.url);
          open.title = `開啟 ${title}（新分頁）`; open.setAttribute('aria-label', open.title);
          const icon = el('span', 'news-icon news-external'); icon.setAttribute('aria-hidden', 'true'); open.append(icon);
          row.append(badge, main, open); rows.append(row);
        }
        section.append(heading, rows); fragment.append(section);
      }
      list.replaceChildren(fragment); rendered = signature;
    }
    syncNavigation();
    for (const button of document.querySelectorAll('[data-source]')) button.setAttribute('aria-pressed', String(button.dataset.source === source.value));
    byId('filesCount').textContent = state.items.length;
    byId('filesResult').textContent = state.ready ? `${visible.length} 項附件` : '讀取中…';
    byId('filesMore').hidden = visible.length <= state.limit;
    byId('filesEmpty').hidden = visible.length > 0 || !state.ready;
    const filtered = !!(search.value.trim() || source.value || type.value);
    byId('filesClear').hidden = !filtered;
    const advanced = Number(!!type.value) + Number(sort.value !== 'name');
    byId('filesFilterCount').hidden = !advanced;
    byId('filesFilterCount').textContent = advanced;
    byId('filesEmptyTitle').textContent = filtered ? '沒有符合的附件' : state.running ? '正在整理附件' : state.error ? '附件暫時無法更新' : course.value ? '這門課程尚無附件' : '尚無附件';
    byId('filesEmptyText').textContent = filtered ? '試試其他關鍵字或清除篩選。' : state.running ? '完成後會顯示在這裡。' : state.error ? '既有附件仍會保留，請稍後重試。' : '可更新附件，或先讀取課程信件。';
    controls();
  };
  const requestJson = async (url, options = {}) => {
    const response = await fetch(url, {credentials: 'same-origin', cache: 'no-store', signal: AbortSignal.timeout(20000), ...options});
    if (response.redirected) throw Error('登入已失效，請重新登入。');
    let data;
    try { data = await response.json(); } catch { throw Error('無法連線，請稍後再試。'); }
    if (!response.ok || !data.ok) throw Error(data.error || '附件暫時無法讀取。');
    return data;
  };
  const schedule = () => {
    clearTimeout(poll);
    if (state.running && !document.hidden) poll = setTimeout(() => void load(), 2500);
  };
  const load = async () => {
    if (!semester.value || config.guest) return;
    controller?.abort();
    controller = new AbortController();
    const generation = state.generation;
    try {
      const data = await requestJson(`${config.dataUrl}?semester=${encodeURIComponent(semester.value)}`, {signal: AbortSignal.any([controller.signal, AbortSignal.timeout(20000)])});
      if (generation !== state.generation) return;
      Object.assign(state, {items: data.items, courses: data.courses, running: data.running, remaining: data.remaining, ready: true, error: data.error});
      colors.ensure(state.courses.map(item => `e3:${item.id}`));
      const signature = JSON.stringify(state.courses);
      if (course.dataset.signature !== signature) {
        const selected = state.requestedCourse || course.value;
        course.replaceChildren(...[{id: '', title: '全部課程'}, ...state.courses].map(item => {
          const option = el('option', '', item.name || item.title); option.value = String(item.id); return option;
        }));
        if ([...course.options].some(option => option.value === selected)) course.value = selected;
        course.dataset.signature = signature;
        state.requestedCourse = '';
        ctx.syncCoursePicker?.();
      }
      const date = data.fetched_at ? new Intl.DateTimeFormat('zh-TW', {timeZone: 'Asia/Taipei', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23'}).format(new Date(data.fetched_at * 1000)) : '';
      byId('filesUpdated').textContent = date ? `上次整理 ${date}` : '';
      status(data.error || (data.limited ? '附件超過 3000 項，目前顯示部分結果，完整資料請前往 E3。' : data.running ? '整理中，既有附件仍可開啟。' : ''), !!data.error);
      render(); schedule();
      if (!data.initialized && !data.running && !refreshing && !attempted.has(semester.value)) {
        attempted.add(semester.value);
        void refresh();
      }
    } catch (error) {
      if (generation !== state.generation || error.name === 'AbortError') return;
      state.ready = true; state.running = false; state.error = error.message;
      status(error.message, true); render();
    }
  };
  const refresh = async () => {
    if (refreshing || state.running || config.guest || !semester.value) return;
    const generation = state.generation;
    refreshing = true; controls();
    try {
      const data = await requestJson(config.refreshUrl, {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({semester: semester.value})});
      if (generation !== state.generation) return;
      state.running = data.status === 'running';
      await load();
      if (generation === state.generation && data.status === 'cooldown') status('更新過於頻繁，請稍後再試。');
    } catch (error) { if (generation === state.generation) status(error.message, true); }
    finally { refreshing = false; controls(); schedule(); if (generation !== state.generation) void load(); }
  };
  const applyFilters = () => {
    state.limit = 50; render();
    const url = new URL(location.href);
    if (course.value) url.searchParams.set('course', course.value); else url.searchParams.delete('course');
    if (search.value.trim()) url.searchParams.set('q', search.value.trim()); else url.searchParams.delete('q');
    history.replaceState(null, '', url);
  };
  for (const control of [course, source, type, sort]) control.addEventListener('change', applyFilters);
  let searchTimer;
  search.addEventListener('input', () => { clearTimeout(searchTimer); searchTimer = setTimeout(applyFilters, 120); });
  byId('filesSearchForm').addEventListener('submit', event => { event.preventDefault(); clearTimeout(searchTimer); applyFilters(); });
  for (const button of document.querySelectorAll('[data-source]')) button.addEventListener('click', () => { source.value = button.dataset.source; applyFilters(); });
  byId('filesClear').addEventListener('click', () => { source.value = ''; type.value = ''; search.value = ''; sort.value = 'name'; applyFilters(); search.focus(); });
  byId('filesMore').addEventListener('click', () => { state.limit += 50; render(); });
  refreshButton.addEventListener('click', refresh);
  semester.addEventListener('change', () => {
    state.generation++; controller?.abort(); clearTimeout(poll);
    Object.assign(state, {items: [], courses: [], ready: false, running: false, remaining: 0, limit: 50, error: ''});
    course.value = ''; state.requestedCourse = ''; byId('filesUpdated').textContent = '';
    const url = new URL(location.href); url.searchParams.set('semester', semester.value); url.searchParams.delete('course'); history.replaceState(null, '', url);
    document.querySelectorAll('[data-semester-link]').forEach(link => { const target = new URL(link.href); target.searchParams.set('semester', semester.value); link.href = target.href; });
    status('讀取附件中…'); render(); void load();
  });
  document.addEventListener('visibilitychange', () => { if (document.hidden) clearTimeout(poll); else void load(); });
  window.addEventListener('pageshow', event => { if (event.persisted) void load(); });
  window.addEventListener('pagehide', () => { controller?.abort(); clearTimeout(poll); });
  byId('filesTheme').addEventListener('click', () => {
    const theme = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = theme; document.documentElement.style.colorScheme = theme;
    try { localStorage.setItem('e3_theme', theme); } catch {}
  });
  document.addEventListener('click', event => { if (!byId('filesFilters').contains(event.target)) byId('filesFilters').open = false; });
  byId('filesFilters').addEventListener('keydown', event => { if (event.key === 'Escape') { byId('filesFilters').open = false; byId('filesFilters').querySelector('summary').focus(); } });
  if (config.guest || !semester.value) {
    state.ready = true; render(); status(config.guest ? '訪客模式無法讀取 E3 附件。' : '請先更新作業以取得課程。');
  } else { controls(); status('讀取附件中…'); void load(); }
}

if (typeof document !== 'undefined') {
  const config = document.getElementById('course-files-config');
  if (config) initialize(JSON.parse(config.textContent));
}
