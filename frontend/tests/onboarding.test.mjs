import assert from 'node:assert/strict';
import {test} from 'node:test';
import {placeTourCard, tourSteps, tourSeen, rememberTour} from '../assignments/static/js/workbench/onboarding.js';

test('completion follows the account and a browser fallback never suppresses another account', () => {
  const values = new Map();
  const storage = {getItem: key => values.get(key), setItem: (key, value) => values.set(key, value)};
  const patches = [];
  const ctx = {config: {onboardingKey: 'account-a'}, USER_PREFERENCES: {}, IS_GUEST: false,
    persistPreferences: patch => patches.push(patch)};
  assert.equal(tourSeen({}, storage, 'account-a'), false);
  rememberTour(ctx, storage);
  assert.equal(tourSeen({}, storage, 'account-a'), true);
  assert.equal(tourSeen({}, storage, 'account-b'), false);
  assert.equal(tourSeen(ctx.USER_PREFERENCES, null, ''), true);
  assert.deepEqual(patches, [{onboardingCompleted: true}]);
});

test('blocked storage still saves account completion and guest completion stays local', () => {
  const storage = {getItem() {throw Error('blocked');}, setItem() {throw Error('blocked');}};
  assert.equal(tourSeen({}, storage, 'account'), false);
  const ctx = {config: {}, USER_PREFERENCES: {}, IS_GUEST: false, persistPreferences: () => {ctx.saved = true;}};
  rememberTour(ctx, storage);
  assert.equal(ctx.saved, true);
  ctx.IS_GUEST = true;
  ctx.saved = false;
  rememberTour(ctx, storage);
  assert.equal(ctx.saved, false);
});

test('guide skips unavailable features and selects the guest menu fallback', () => {
  const normal = new Set(['.workspace-actions','.toolbar-stack','.seg','.workspace-messages','.workspace-files',
    '#moreOperations > summary','#addCustomAssignmentBtn','a[aria-label="通知設定"]','#semesterSelector']);
  const doc = {querySelector: selector => normal.has(selector) ? {} : null};
  assert.equal(tourSteps(doc).length, 7);
  assert.equal(tourSteps(doc).filter(step => step.menu).length, 1);
  normal.delete('.workspace-files'); normal.delete('.workspace-messages'); normal.delete('a[aria-label="通知設定"]');
  normal.add('#guestFile');
  const guest = tourSteps(doc);
  assert.equal(guest[0].selector, '#guestFile');
  assert.equal(guest.some(step => step.selector === '#moreOperations > summary'), true);
  assert.equal(guest.some(step => step.menu), false);
});

test('popover stays within desktop, mobile and short landscape viewports', () => {
  for (const viewport of [{width:1280,height:720},{width:390,height:844},{width:667,height:375}]) {
    for (const rect of [{left:16,top:40,right:200,bottom:100,width:184},
      {left:viewport.width-150,top:viewport.height-100,right:viewport.width-16,bottom:viewport.height-30,width:134}]) {
      const point = placeTourCard(rect, viewport, {width:360,height:270});
      assert.ok(point.left >= 16 && point.top >= 16);
      assert.ok(point.left + Math.min(360, viewport.width-32) <= viewport.width-16);
      assert.ok(point.top + Math.min(270, viewport.height-32) <= viewport.height-16);
    }
  }
  const bottomTarget = {left:400,top:600,right:500,bottom:640,width:100};
  assert.ok(placeTourCard(bottomTarget,{width:1280,height:720},{width:360,height:270}).top + 270 < bottomTarget.top);
});
