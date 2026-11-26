import assert from 'node:assert/strict';
import {test} from 'node:test';
import {hasVisibleLabel, placeTourCard, tourSteps, tourSeen, rememberTour} from '../assignments/static/js/workbench/onboarding.js';

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

function label(text = '', styles = {}, hidden = false) {
  const element = {nodeType: 1, styles: {display:'block',visibility:'visible',fontSize:'14px',opacity:'1',...styles},
    matches: () => hidden, childNodes: []};
  if (text) element.childNodes.push({nodeType:3,textContent:text,parentElement:element});
  return element;
}

test('guide only teaches icon buttons and skips clearly labelled features', () => {
  const nodes = new Map([
    ['#announcementToggle',label()], ['a[aria-label="通知設定"]',label()], ['#themeToggle',label()],
    ['#moreOperations > summary',label('⋯')], ['#manualRefreshBtn',label('更新作業')],
    ['#filterMenu > summary',label('篩選')], ['.workspace-actions',label('本學期')],
    ['.toolbar-stack',label('搜尋作業或課程')], ['.seg',label('日曆')],
    ['.workspace-messages',label('課程訊息')], ['.workspace-files',label('附件收納')],
    ['#addCustomAssignmentBtn',label('新增代辦')],
  ]);
  const doc = {querySelector: selector => nodes.get(selector), defaultView:{getComputedStyle:node=>node.styles}};
  assert.deepEqual(tourSteps(doc).map(step=>step.selector), ['#announcementToggle','a[aria-label="通知設定"]','#themeToggle','#moreOperations > summary']);
  nodes.delete('a[aria-label="通知設定"]'); nodes.delete('.workspace-files'); nodes.delete('#manualRefreshBtn');
  assert.equal(tourSteps(doc).length, 3);
  assert.match(tourSteps(doc).at(-1).text, /重新開啟使用教學/);
});

test('responsive hidden labels count as icons; numeric badges and accessible names are not visible labels', () => {
  const getStyle = node => node.styles;
  assert.equal(hasVisibleLabel(label('更新作業',{fontSize:'0px'}),getStyle), false);
  const filter = label();
  filter.childNodes.push(label('篩選',{display:'none'}),label('5'));
  assert.equal(hasVisibleLabel(filter,getStyle), false);
  filter.childNodes[0].styles.display = 'block';
  assert.equal(hasVisibleLabel(filter,getStyle), true);
  const namedIcon = label(); namedIcon['aria-label'] = '通知設定';
  assert.equal(hasVisibleLabel(namedIcon,getStyle), false);
  const childLabel = label('更新作業');
  const zeroParent = label('',{fontSize:'0px'}); zeroParent.childNodes.push(childLabel);
  assert.equal(hasVisibleLabel(zeroParent,getStyle), true);
  const nodes = new Map([['#manualRefreshBtn',label('更新作業',{fontSize:'0px'})],['#filterMenu > summary',filter]]);
  const doc = {querySelector: selector=>nodes.get(selector),defaultView:{getComputedStyle:getStyle}};
  assert.deepEqual(tourSteps(doc).map(step=>step.selector),['#manualRefreshBtn']);
  filter.childNodes[0].styles.display = 'none';
  assert.deepEqual(tourSteps(doc).map(step=>step.selector),['#manualRefreshBtn','#filterMenu > summary']);
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
