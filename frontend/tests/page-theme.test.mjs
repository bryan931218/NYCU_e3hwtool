import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../shared/static/js/e3-page-theme.js', import.meta.url), 'utf8');
function setup({ blocked = false, buttons = true } = {}) {
  const events = {};
  const attrs = {};
  const root = { dataset: { theme: 'dark' }, style: {} };
  const button = { setAttribute: (key, value) => { attrs[key] = value; }, addEventListener: (key, fn) => { events[key] = fn; } };
  const saved = [];
  const context = { document: { documentElement: root, querySelectorAll: () => buttons ? [button] : [] },
    window: { addEventListener: (key, fn) => { events[key] = fn; } },
    localStorage: { setItem: (...args) => { if (blocked) throw Error('blocked'); saved.push(args); } } };
  vm.runInNewContext(source, context);
  return { root, events, attrs, saved };
}

test('subpage theme updates color scheme, accessible label and the existing saved preference', () => {
  const { root, events, attrs, saved } = setup();
  assert.equal(root.style.colorScheme, 'dark');
  assert.equal(attrs['aria-label'], '切換至淺色模式');
  events.click();
  assert.equal(root.dataset.theme, 'light');
  assert.equal(root.style.colorScheme, 'light');
  assert.deepEqual(saved, [['e3_theme', 'light']]);
  assert.equal(attrs['aria-label'], '切換至深色模式');
  events.storage({ key: 'e3_theme', newValue: 'dark' });
  assert.equal(root.dataset.theme, 'dark');
  events.storage({ key: 'other', newValue: 'light' });
  assert.equal(root.dataset.theme, 'dark');
});

test('subpage themes work with blocked storage and ignore pages without the control', () => {
  const result = setup({ blocked: true });
  assert.doesNotThrow(() => result.events.click());
  assert.equal(result.root.dataset.theme, 'light');
  assert.deepEqual(setup({ buttons: false }).events, {});
});
