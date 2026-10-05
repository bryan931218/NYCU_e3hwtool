import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {test} from 'node:test';
import vm from 'node:vm';

test('reminders submit only a time and display legacy plans without duration', async () => {
  const calls = [];
  const handlers = {};
  const element = () => ({children: [], disabled: false, dataset: {}, textContent: '',
    append(...children) {this.children.push(...children);},
    replaceChildren() {this.children = [];}, addEventListener() {}});
  const button = element();
  const form = {...element(), querySelector: () => button,
    addEventListener: (name, handler) => {handlers[name] = handler;}};
  const config = {uid: 'a'.repeat(64), request_id: 'b'.repeat(32), google_linked: false};
  const nodes = {
    'assignment-plan-config': {textContent: JSON.stringify(config)}, assignmentPlanForm: form,
    planStart: {...element(), value: ''}, planMessage: element(),
    planGoogle: {...element(), checked: false}, assignmentPlans: element(),
  };
  const items = [{uid_hash: config.uid, start_ts: 1800000000, minutes: 120},
    {uid_hash: config.uid, start_ts: 1800003600}];
  const source = readFileSync(new URL('../assignments/static/js/assignment-plan.js', import.meta.url), 'utf8');
  const context = vm.createContext({Intl, Date, AbortSignal, crypto: {randomUUID: () => 'c'.repeat(32)},
    announcementDate: value => `date:${value}`,
    document: {getElementById(id) {assert.ok(id in nodes, `Unexpected field: ${id}`); return nodes[id];},
      createElement: element},
    fetch: async (url, options) => {
      calls.push({url, ...options});
      return {ok: true, json: async () => options.method === 'GET' ? {ok: true, items} : {ok: true, message: 'saved'}};
    },
  });
  vm.runInContext(source.replace(/^import[^\n]+\n/, ''), context);
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(nodes.assignmentPlans.children.map(row => row.children[0].textContent),
    ['date:1800000000', 'date:1800003600']);
  nodes.planStart.value = '2026-10-06T20:00';
  await handlers.submit({preventDefault() {}});
  const sent = JSON.parse(calls.find(call => call.method === 'POST').body);
  assert.equal(sent.start_ts, Date.parse('2026-10-06T20:00:00+08:00') / 1000);
  assert.equal(sent.google, false);
  assert.equal('minutes' in sent, false);
  assert.equal(button.disabled, false);
  assert.equal(nodes.planGoogle.disabled, true);
});
