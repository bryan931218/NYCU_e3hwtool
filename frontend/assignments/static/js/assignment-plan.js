import { announcementDate } from './course-announcements.js';

const config = JSON.parse(document.getElementById('assignment-plan-config').textContent);
const form = document.getElementById('assignmentPlanForm');
if (form) {
  const start = document.getElementById('planStart');
  const status = document.getElementById('planMessage');
  const local = timestamp => new Intl.DateTimeFormat('sv-SE', {timeZone: 'Asia/Taipei', year: 'numeric', month: '2-digit',
    day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23'}).format(new Date(timestamp)).replace(' ', 'T');
  start.value = local(Date.now()+3600000);
  start.min = local(Date.now()+60000);
  const api = async (method, body) => {
    const response = await fetch('/api/assignments/plans', {method, credentials: 'same-origin', headers: {'Content-Type': 'application/json'},
      signal: AbortSignal.timeout(60000), ...(body ? {body: JSON.stringify(body)} : {})});
    if (response.redirected) throw Error('請重新登入。');
    const data = await response.json();
    if (!response.ok || !data.ok) throw Error(data.error || '操作失敗，請稍後重試。');
    return data;
  };
  const show = (text, error = false) => {status.textContent = text; status.dataset.error = String(error);};
  const load = async () => {
    const data = await api('GET');
    const list = document.getElementById('assignmentPlans');
    list.replaceChildren();
    const items = data.items.filter(item => item.uid_hash === config.uid).sort((a, b) => a.start_ts-b.start_ts);
    if (!items.length) {list.textContent = '尚未安排'; return;}
    for (const item of items) {
      const row = document.createElement('div'); row.className = 'plan-list-row';
      const text = document.createElement('span'); text.textContent = announcementDate(item.start_ts);
      const cancel = document.createElement('button'); cancel.type = 'button'; cancel.className = 'text-button'; cancel.textContent = '取消提醒';
      cancel.addEventListener('click', async () => {
        cancel.disabled = true;
        try {await api('DELETE', {id: item.id}); await load(); show('已取消提醒。' + (item.google_synced ? 'Google 日曆的提醒事件請自行刪除。' : ''));}
        catch (error) {show(error.message, true); cancel.disabled = false;}
      });
      row.append(text, cancel); list.append(row);
    }
  };
  form.addEventListener('submit', async event => {
    event.preventDefault(); const button = form.querySelector('[type="submit"]'); button.disabled = true;
    try {
      const data = await api('POST', {...config, start_ts: Math.floor(new Date(`${start.value}:00+08:00`).getTime()/1000),
        google: document.getElementById('planGoogle').checked});
      show(data.message, !!data.calendar_error);
      button.textContent = data.calendar_error ? '重試日曆同步' : '安排提醒';
      start.disabled = document.getElementById('planGoogle').disabled = !!data.calendar_error;
      if (!data.calendar_error && !config.google_linked) document.getElementById('planGoogle').disabled = true;
      if (!data.calendar_error) config.request_id = crypto.randomUUID().replaceAll('-', '');
      await load();
    } catch (error) {show(error.message, true);}
    finally {button.disabled = false;}
  });
  void load().catch(error => show(error.message, true));
}
