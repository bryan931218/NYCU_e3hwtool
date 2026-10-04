import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { loadCalendarLibrary } from '../assignments/static/js/workbench/deadline-calendar.js';

test('calendar library is loaded once on demand, copies the CSP nonce and can retry after a failure', async () => {
  const originals = { window:globalThis.window, document:globalThis.document };
  const scripts = [];
  globalThis.window = {};
  globalThis.document = {
    createElement: () => ({remove() { this.removed = true; }}),
    getElementById: () => ({nonce:'fixture-nonce'}),
    head: {append:script=>scripts.push(script)},
  };
  try {
    const pending = loadCalendarLibrary('/calendar.js');
    assert.equal(loadCalendarLibrary('/calendar.js'), pending);
    assert.equal(scripts.length, 1);
    assert.equal(scripts[0].nonce, 'fixture-nonce');
    assert.equal(scripts[0].async, true);
    scripts[0].onerror();
    await assert.rejects(pending, /unavailable/);
    assert.equal(scripts[0].removed, true);
    const retry = loadCalendarLibrary('/calendar.js');
    assert.equal(scripts.length, 2);
    window.FullCalendar = {Calendar:class {}};
    scripts[1].onload();
    assert.equal(await retry, window.FullCalendar);
    assert.equal(await loadCalendarLibrary('/calendar.js'), window.FullCalendar);
    assert.equal(scripts.length, 2);
  } finally {
    for(const key of ['window','document']) {
      if(originals[key]===undefined) delete globalThis[key]; else globalThis[key]=originals[key];
    }
  }
});

test('workbench starts its module in the head without a blocking calendar script and renders only the preferred view', () => {
  const read=path=>readFileSync(new URL(path,import.meta.url),'utf8');
  const template=read('../assignments/templates/web.html');
  assert.match(template.split('</head>')[0], /id="workbench-script"[^>]*type="module"/);
  assert.doesNotMatch(template, /<script[^>]*fullcalendar/);
  assert.match(read('../assignments/templates/pages/web/config.html'), /calendarScriptUrl/);
  for(const mode of ['due','course']) assert.match(template,new RegExp(`initial_view != '${mode}'`));
  assert.match(read('../assignments/templates/components/deadline-calendar.html'), /initial_view != 'calendar'/);
});
