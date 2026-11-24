import assert from 'node:assert/strict';
import {test} from 'node:test';
import {initialize} from '../assignments/static/js/workbench/server-sso.js';

const settle = () => new Promise(resolve => setImmediate(resolve));

async function withPage(run) {
  const original = Object.fromEntries(['window', 'document', 'fetch'].map(key => [key, globalThis[key]]));
  const timers = new Map();
  const button = {disabled: false};
  const input = {value: '', focus() { this.focused = true; }};
  const form = {hidden: true, reportValidity: () => true, querySelector: () => button,
    addEventListener(type, callback) { this[type] = callback; }};
  const panel = {hidden: true};
  const message = {};
  const nodes = {serverSsoPanel: panel, serverSsoMessage: message, serverSsoCodeForm: form, serverSsoCode: input};
  const calls = [];
  const responses = [];
  globalThis.document = {hidden: false, getElementById: id => nodes[id], addEventListener() {}};
  globalThis.window = {setTimeout(callback) { timers.set(callback, callback); return callback; }, clearTimeout(id) { timers.delete(id); }};
  globalThis.fetch = async (url, options) => {
    calls.push({url, options});
    assert.ok(responses.length, 'unexpected network request');
    const data = responses.shift();
    return {ok: true, status: 200, json: async () => data};
  };
  try { await run({panel, form, input, button, message, calls, responses, timers}); }
  finally {
    for (const [key, value] of Object.entries(original)) {
      if (value === undefined) delete globalThis[key]; else globalThis[key] = value;
    }
  }
}

test('idle login does not show a banner or keep polling', async () => withPage(async page => {
  page.responses.push({ok: true, state: 'idle'});
  initialize({fetchAndSwapContent: () => assert.fail('idle must not refresh')});
  await settle();
  assert.equal(page.panel.hidden, true);
  assert.equal(page.timers.size, 0);
}));

test('school OTP stays in its form and completed counts update without reloading', async () => withPage(async page => {
  let updates = 0;
  page.responses.push({ok: true, state: 'otp_required', attempts: 0});
  initialize({fetchAndSwapContent: async () => { updates++; }});
  await settle();
  assert.equal(page.panel.hidden, false);
  assert.equal(page.form.hidden, false);
  assert.equal(page.input.focused, true);
  page.input.value = '123456';
  page.responses.push({ok: true}, {ok: true, state: 'complete'});
  await page.form.submit({preventDefault() {}});
  assert.equal(page.calls[1].url, '/api/e3-connection/verify');
  assert.deepEqual(JSON.parse(page.calls[1].options.body), {code: '123456'});
  assert.equal(page.input.value, '');
  assert.equal(page.button.disabled, false);
  assert.equal(page.panel.hidden, true);
  assert.equal(updates, 1);
  assert.equal(page.timers.size, 0);
}));

test('unavailable school login stops polling and preserves the workbench', async () => withPage(async page => {
  page.responses.push({ok: true, state: 'unavailable'});
  initialize({fetchAndSwapContent: () => assert.fail('failure must not replace assignments')});
  await settle();
  assert.equal(page.form.hidden, true);
  assert.equal(page.panel.hidden, true);
  assert.equal(page.timers.size, 0);
}));

test('background connection states remain silent while counts follow the ordinary refresh', async () => withPage(async page => {
  page.responses.push({ok: true, state: 'queued'}, {ok: true, state: 'starting'}, {ok: true, state: 'verifying'}, {ok: true, state: 'complete'});
  let updates = 0;
  initialize({fetchAndSwapContent: async () => { updates++; }});
  await settle();
  for (let step = 0; step < 3; step++) {
    assert.equal(page.panel.hidden, true);
    const [id, callback] = page.timers.entries().next().value;
    page.timers.delete(id);
    await callback();
  }
  assert.equal(page.panel.hidden, true);
  assert.equal(page.message.textContent, undefined);
  assert.equal(updates, 1);
  assert.equal(page.timers.size, 0);
}));

test('temporary connection failures do not expose a background status banner', async () => withPage(async page => {
  const request = globalThis.fetch;
  let attempts = 0;
  globalThis.fetch = async (...args) => {
    if (++attempts < 3) throw new Error('offline');
    return request(...args);
  };
  page.responses.push({ok: true, state: 'idle'});
  initialize({});
  await settle();
  for (let step = 0; step < 2; step++) {
    assert.equal(page.panel.hidden, true);
    const [id, callback] = page.timers.entries().next().value;
    page.timers.delete(id);
    await callback();
  }
  assert.equal(page.panel.hidden, true);
  assert.equal(page.timers.size, 0);
}));
