import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { runInNewContext } from 'node:vm';
import { test } from 'node:test';
import { animateContentChange } from '../assignments/static/js/content-motion.js';
import { register } from '../assignments/static/js/workbench/interactions.js';

function motionFixture() {
  const listeners = new Set();
  const preference = { matches: false, addEventListener: (_, cb) => listeners.add(cb), removeEventListener: (_, cb) => listeners.delete(cb) };
  const animations = [];
  const element = { animate(frames, options) {
    let resolve;
    let reject;
    const animation = { frames, options, cancelled: false, finished: new Promise((yes, no) => { resolve = yes; reject = no; }),
      finish: () => resolve(), cancel() { this.cancelled = true; reject(new Error('cancelled')); } };
    animations.push(animation);
    return animation;
  } };
  return { preference, element, animations, listeners };
}

async function withPreference(preference, run) {
  const original = globalThis.matchMedia;
  globalThis.matchMedia = query => {
    assert.equal(query, '(prefers-reduced-motion: reduce)');
    return preference;
  };
  try { await run(); } finally {
    if (original === undefined) delete globalThis.matchMedia; else globalThis.matchMedia = original;
  }
}

test('content changes use a short, non-persistent animation and clean up listeners', async () => {
  const fixture = motionFixture();
  await withPreference(fixture.preference, async () => {
    animateContentChange(fixture.element);
    const animation = fixture.animations[0];
    assert.equal(animation.options.duration, 170);
    assert.equal(animation.options.fill, undefined);
    assert.deepEqual(animation.frames.at(-1), { opacity: 1, transform: 'none' });
    assert.equal(fixture.listeners.size, 1);
    animation.finish();
    await animation.finished;
    assert.equal(fixture.listeners.size, 0);
  });
});

test('rapid switching cancels the previous animation without stale cleanup cancelling the next', async () => {
  const fixture = motionFixture();
  await withPreference(fixture.preference, async () => {
    animateContentChange(fixture.element);
    animateContentChange(fixture.element);
    assert.equal(fixture.animations[0].cancelled, true);
    await Promise.resolve();
    assert.equal(fixture.listeners.size, 1);
    animateContentChange(fixture.element);
    assert.equal(fixture.animations[1].cancelled, true);
    fixture.animations[2].finish();
    await Promise.resolve();
    assert.equal(fixture.listeners.size, 0);
  });
});

test('reduced motion skips animation and changing the preference cancels an active animation', async () => {
  const fixture = motionFixture();
  await withPreference(fixture.preference, async () => {
    fixture.preference.matches = true;
    animateContentChange(fixture.element);
    assert.equal(fixture.animations.length, 0);
    fixture.preference.matches = false;
    animateContentChange(fixture.element);
    fixture.preference.matches = true;
    for (const callback of fixture.listeners) callback();
    await Promise.resolve();
    assert.equal(fixture.animations[0].cancelled, true);
    assert.equal(fixture.listeners.size, 0);
  });
  animateContentChange(null);
  animateContentChange({});
  animateContentChange({ hidden: true, animate: () => assert.fail('hidden content must not animate') });
});

test('workbench animates actual view changes only and still applies filters synchronously', async () => {
  const fixture = motionFixture();
  const original = globalThis.document;
  globalThis.document = { getElementById: () => null };
  const ctx = { currentViewMode: 'due', viewCourse: { ...fixture.element, classList: { toggle() {} } },
    persistPreferences() {}, applyFilters() { this.filtered = true; } };
  try {
    register(ctx);
    await withPreference(fixture.preference, async () => {
      ctx.setView('due', { skipPersist: true });
      assert.equal(fixture.animations.length, 0);
      ctx.setView('course');
      assert.equal(ctx.currentViewMode, 'course');
      assert.equal(ctx.filtered, true);
      assert.equal(fixture.animations.length, 1);
      ctx.setView('course');
      assert.equal(fixture.animations.length, 1);
      fixture.animations[0].finish();
      await Promise.resolve();
    });
  } finally {
    if (original === undefined) delete globalThis.document; else globalThis.document = original;
  }
});

test('page transitions are opted in by E3 templates, not study templates, and respect reduced motion', () => {
  const read = path => readFileSync(new URL(path, import.meta.url), 'utf8');
  const css = read('../shared/static/css/page-transitions.css');
  assert.match(css, /@view-transition\s*\{\s*navigation: none;/);
  assert.doesNotMatch(css, /navigation: auto/);
  assert.match(css, /body\s*\{\s*animation: e3-page-arrive 120ms/);
  assert.match(css, /prefers-reduced-motion: reduce[\s\S]*body\s*\{\s*animation: none;/);
  for (const path of ['web.html', 'home.html', 'login.html', 'settings/notifications.html', 'pages/course_announcements.html']) {
    assert.match(read(`../assignments/templates/${path}`), /include 'shared\/components\/page-transitions.html'/);
  }
  assert.doesNotMatch(read('../study/templates/admin_study_home.html'), /page-transitions/);
});

test('every continuous homepage animation can pause and respects reduced motion', () => {
  const css = readFileSync(new URL('../assignments/static/css/home.css', import.meta.url), 'utf8');
  const paused = css.slice(css.indexOf('html.motion-paused'));
  const reduced = css.slice(css.indexOf('@media (prefers-reduced-motion: reduce)'));
  for (const selector of ['.home-stats > div::after', '.stat-icon::after', '.workspace-preview', '.preview-topbar::after', '.demo-assignment::before']) {
    assert.ok(paused.includes(selector), `must pause ${selector}`);
    assert.ok(reduced.includes(selector), `must disable ${selector}`);
  }
  assert.match(paused, /animation-play-state: paused/);
  assert.match(reduced, /\.home-ambient\s*\{\s*display: none/);
  assert.match(reduced, /animation: none/);
  for (const frames of css.match(/@keyframes [^\n]+/g)) {
    assert.doesNotMatch(frames, /\b(?:width|height|top|left|margin|padding|filter|box-shadow)\s*:/);
  }
});

test('homepage pauses motion in a hidden tab and resumes without a timer loop', () => {
  const code = readFileSync(new URL('../assignments/static/js/home.js', import.meta.url), 'utf8');
  const listeners = new Map();
  const classes = new Map();
  const root = { dataset: { theme: 'dark' }, style: {}, classList: { toggle: (name, value) => classes.set(name, value) } };
  const button = { setAttribute() {}, addEventListener() {} };
  const document = { hidden: true, documentElement: root, getElementById: () => button, addEventListener: (name, callback) => listeners.set(name, callback) };
  runInNewContext(code, { document, localStorage: { getItem: () => null } });
  assert.equal(classes.get('motion-paused'), true);
  document.hidden = false;
  listeners.get('visibilitychange')();
  assert.equal(classes.get('motion-paused'), false);
  assert.equal(button.hidden, false);
});
