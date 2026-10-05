export function renderDeadlineReview(item, {element, button, post, onDismiss}) {
  const fragment = document.createDocumentFragment();
  for (const proposal of item.deadline_proposals || []) {
    const section = element('section', 'deadline-review');
    section.append(element('h3', '', '期限異動待確認'));
    const selectLabel = element('label', '', '作業');
    const select = element('select'); select.setAttribute('aria-label', '異動作業');
    const placeholder = element('option', '', '選擇作業'); placeholder.value = ''; select.append(placeholder);
    for (const candidate of proposal.candidates) {
      const option = element('option', '', candidate.title); option.value = candidate.uid;
      option.selected = candidate.uid === proposal.matched_uid; select.append(option);
    }
    selectLabel.append(select);
    const old = element('p', 'deadline-evidence');
    const format = value => value ? new Intl.DateTimeFormat('zh-TW', {timeZone: 'Asia/Taipei', year: 'numeric', month: '2-digit',
      day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23'}).format(new Date(value*1000)) : '未設定';
    const updateOld = () => {old.textContent = `目前期限：${format(proposal.candidates.find(candidate => candidate.uid === select.value)?.due_ts)}`;};
    select.addEventListener('change', updateOld); updateOld();
    const dateLabel = element('label', '', '新期限');
    const date = element('input'); date.type = 'datetime-local'; date.setAttribute('aria-label', '新期限');
    date.value = new Intl.DateTimeFormat('sv-SE', {timeZone: 'Asia/Taipei', year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', hourCycle: 'h23'}).format(new Date(proposal.due_ts*1000)).replace(' ', 'T');
    dateLabel.append(date);
    const evidence = element('p', 'deadline-evidence', `來源：${proposal.evidence}`);
    const precision = element('p', 'deadline-evidence', proposal.time_explicit ? '只更新你的個人期限，不會改動 E3。' : '來源未註明時間，請確認日期與時間。');
    const warnings = element('p', 'deadline-evidence', (proposal.warnings || []).join(' '));
    const googleLabel = element('label'); const google = element('input'); google.type = 'checkbox';
    googleLabel.append(google, document.createTextNode(' 同步 Google 日曆'));
    const status = element('p'); status.setAttribute('role', 'status'); status.setAttribute('aria-live', 'polite');
    const actions = element('div', 'deadline-actions');
    let applied = false;
    const confirm = button('確認更新', async () => {
      const candidate = proposal.candidates.find(candidate => candidate.uid === select.value);
      if (!candidate || !date.value) {status.textContent = '請選擇作業與新期限。'; status.dataset.error = 'true'; return;}
      confirm.disabled = dismiss.disabled = true;
      try {
        const result = await post('/api/assignments/deadline-proposals', {id: proposal.id, uid: select.value,
          due_ts: Math.floor(new Date(`${date.value}:00+08:00`).getTime()/1000), original_due_ts: candidate.due_ts ?? null,
          google: google.checked, ...(applied ? {retry_calendar: true} : {})});
        applied = true; select.disabled = date.disabled = google.disabled = true;
        status.textContent = result.message || 'Google 日曆已同步。'; status.dataset.error = String(!!result.calendar_error);
        if (result.calendar_error) {confirm.textContent = '重試日曆同步'; confirm.disabled = false;}
        else {confirm.textContent = '已更新'; dismiss.hidden = true;}
      } catch (error) {status.textContent = error.message; status.dataset.error = 'true'; confirm.disabled = dismiss.disabled = false;}
    }, 'btn primary');
    const dismiss = button('略過', async () => {
      dismiss.disabled = confirm.disabled = true;
      try {await post('/api/assignments/deadline-proposals', {id: proposal.id, dismiss: true}); onDismiss(proposal.id); section.remove();}
      catch (error) {status.textContent = error.message; status.dataset.error = 'true'; dismiss.disabled = confirm.disabled = false;}
    });
    actions.append(confirm, dismiss); section.append(selectLabel, old, dateLabel, evidence, precision);
    if (warnings.textContent) section.append(warnings);
    section.append(googleLabel, actions, status);
    fragment.append(section);
  }
  return fragment;
}
