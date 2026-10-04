import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createUnreadUpdater, updateUnreadIndicator } from '../assignments/static/js/course-message-unread.js';

test('unread indicator is red only for positive counts and has an accessible label', () => {
  const dot = {hidden:true};
  const attrs = {};
  const link = {querySelector:()=>dot, setAttribute:(key,value)=>{attrs[key]=value;}};
  updateUnreadIndicator(link, 2, '課程訊息');
  assert.equal(dot.hidden,false);
  assert.equal(attrs['aria-label'],'課程訊息，有新訊息');
  assert.equal(link.title,'課程訊息：2 則新訊息');
  for(const count of [0, -1, undefined, NaN]) {
    updateUnreadIndicator(link,count,'課程訊息');
    assert.equal(dot.hidden,true);
    assert.equal(attrs['aria-label'],'課程訊息');
  }
  updateUnreadIndicator(null,1,'missing');
});

test('cached unread refresh is scoped to the semester and ignores stale responses', async () => {
  let semester = '115-1';
  const renders = [];
  const pending = [];
  const request = (url, options) => new Promise(resolve=>pending.push({url, options, resolve}));
  const refresh = createUnreadUpdater('/api/course-messages/unread',()=>semester,data=>renders.push(data),request);
  const first = refresh();
  const second = refresh();
  assert.equal(pending[0].options.signal.aborted,true);
  pending[1].resolve({ok:true,json:async()=>({ok:true,total:2})});
  await second;
  pending[0].resolve({ok:true,json:async()=>({ok:true,total:9})});
  await first;
  assert.equal(renders.at(-1).total,2);
  assert.equal(pending[0].url,'/api/course-messages/unread?semester=115-1');
  const oldScope = refresh();
  semester = '114-2';
  pending[2].resolve({ok:true,json:async()=>({ok:true,total:9})});
  await oldScope;
  assert.equal(renders.at(-1).total,2);
  const nextScope = refresh();
  assert.equal(renders.at(-1).total,0);
  pending[3].resolve({ok:true,json:async()=>({ok:true,total:0})});
  await nextScope;
});

test('offline failures and login redirects do not clear a known unread badge', async () => {
  const renders = [];
  let response = {ok:true,json:async()=>({ok:true,total:1})};
  const refresh = createUnreadUpdater('/unread',()=>'',data=>renders.push(data),async()=> {
    if(response instanceof Error) throw response;
    return response;
  });
  await refresh();
  response = new Error('offline'); await refresh();
  assert.equal(renders.at(-1).total,1);
  response = {ok:true,redirected:true,json:()=>assert.fail('must not parse login')};
  await refresh();
  assert.equal(renders.at(-1).total,1);
});
