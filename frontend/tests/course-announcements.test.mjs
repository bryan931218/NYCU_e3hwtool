import assert from 'node:assert/strict';
import { test } from 'node:test';
import { filterAnnouncements, visibleAnnouncements, safeNewsLink, announcementDate } from '../assignments/static/js/course-announcements.js';
import { renderDeadlineReview } from '../assignments/static/js/deadline-review.js';

const items = [
  {key:'1:1', course_id:1, title:'期中考', course_title:'作業系統', author:'老師', read_at:0},
  {key:'2:2', course_id:2, title:'Homework ABC', course_title:'演算法', author:'助教', read_at:123, content:'Chapter 3'},
  {key:'1:3', course_id:1, title:'調課', course_title:'作業系統', author:'助教', read_at:0},
];
test('course and unread filters combine without mutating source data', () => {
  assert.deepEqual(filterAnnouncements(items, {course:'1', unread:true}).map(item => item.key), ['1:1','1:3']);
  assert.deepEqual(filterAnnouncements(items, {course:'2', unread:true}), []);
  assert.equal(items.length, 3);
});
test('search matches title, course, author and loaded body with normalized whitespace and case', () => {
  assert.deepEqual(filterAnnouncements(items, {query:'  homework abc  '}), [items[1]]);
  assert.deepEqual(filterAnnouncements(items, {query:'作業系統'}), [items[0], items[2]]);
  assert.deepEqual(filterAnnouncements(items, {query:'chapter 3'}), [items[1]]);
  assert.deepEqual(filterAnnouncements(items, {query:'助教', unread:true}), [items[2]]);
});
test('newly read selection stays in the unread inbox until navigating away', () => {
  assert.deepEqual(visibleAnnouncements(items, {unread:true}, '2:2'), items);
  assert.deepEqual(visibleAnnouncements(items, {unread:true}), [items[0],items[2]]);
  assert.deepEqual(visibleAnnouncements(items, {unread:true}, 'missing'), [items[0],items[2]]);
});
test('retaining the reader never bypasses course or search filters', () => {
  assert.deepEqual(visibleAnnouncements(items, {course:'1',unread:true}, '2:2'), [items[0],items[2]]);
  assert.deepEqual(visibleAnnouncements(items, {query:'不存在',unread:true}, '2:2'), []);
  assert.deepEqual(visibleAnnouncements(items, {query:'chapter',unread:true}, '2:2'), [items[1]]);
  assert.equal(items[1].read_at,123);
});
test('announcement and attachment links reject active URLs, insecure protocols and embedded credentials', () => {
  for (const value of ['javascript:alert(1)', 'data:text/html,<script>', 'http://evil.test', '//evil.test', 'https://user:secret@e3p.nycu.edu.tw/', '/relative']) {
    assert.equal(safeNewsLink(value), '');
  }
  assert.equal(safeNewsLink('https://e3p.nycu.edu.tw/mod/forum/discuss.php?d=7'), 'https://e3p.nycu.edu.tw/mod/forum/discuss.php?d=7');
});
test('dates use Taipei rather than browser timezone and tolerate missing dates', () => {
  assert.match(announcementDate(Date.parse('2026-10-03T16:30:00Z')/1000), /2026\/10\/04.*00:30/);
  assert.equal(announcementDate(null), '');
  assert.equal(announcementDate('bad'), '');
});

function reviewFixture(post, proposalChanges = {}) {
  class Node {
    constructor(tag, className, text) { this.tag=tag; this.className=className; this.textContent=text || ''; this.children=[]; this.dataset={}; this.attributes={}; this.listeners={}; this.checked=false; }
    append(...nodes) { this.children.push(...nodes); }
    setAttribute(key, value) { this.attributes[key]=value; }
    addEventListener(key, fn) { this.listeners[key]=fn; }
    remove() { this.removed=true; }
    get value() { return this._value ?? (this.children.find(node=>node.selected)?.value ?? this.children[0]?.value ?? ''); }
    set value(value) { this._value=value; }
  }
  const document = {createDocumentFragment:()=>new Node('fragment'), createTextNode:text=>new Node('text','',text)};
  const previous=globalThis.document; globalThis.document=document;
  const proposal={id:'proposal', due_ts:1791561540, time_explicit:true, evidence:'HW1 截止延期至 10/09 23:59',
    matched_uid:'own-assignment', candidates:[{uid:'own-assignment', title:'HW1 <script>unsafe</script>', due_ts:1791475140}], ...proposalChanges};
  let dismissed='';
  const element=(...args)=>new Node(...args);
  const button=(text, handler, className)=>{const node=element('button',className,text); node.click=handler; return node;};
  const fragment=renderDeadlineReview({deadline_proposals:[proposal]}, {element, button, post, onDismiss:id=>{dismissed=id;}});
  globalThis.document=previous;
  const all=[]; const walk=node=>{all.push(node); for(const child of node.children)walk(child);}; walk(fragment);
  return {nodes:all, dismissed:()=>dismissed, get:predicate=>all.find(predicate)};
}

test('deadline review shows literal source text and confirms the caller-selected assignment with Taipei time', async()=>{
  const calls=[];
  const fixture=reviewFixture(async(url,body)=>{calls.push({url,body}); return {ok:true,message:'已更新個人期限。'};});
  assert.equal(fixture.get(node=>node.tag==='input'&&node.type==='datetime-local').value, '2026-10-09T23:59');
  assert.ok(fixture.nodes.some(node=>node.textContent==='HW1 <script>unsafe</script>'));
  assert.ok(fixture.nodes.every(node=>node.innerHTML===undefined));
  await fixture.get(node=>node.textContent==='確認更新').click();
  assert.equal(calls.length,1);
  assert.deepEqual(calls[0].body,{id:'proposal',uid:'own-assignment',due_ts:1791561540,original_due_ts:1791475140,google:false});
  assert.equal(fixture.get(node=>node.tag==='button'&&node.textContent==='已更新').disabled,true);
});

test('deadline review needs an explicit assignment when matching is ambiguous',async()=>{
  let count=0;
  const fixture=reviewFixture(async()=>{count++;},{matched_uid:''});
  await fixture.get(node=>node.textContent==='確認更新').click();
  assert.equal(count,0);
  assert.equal(fixture.get(node=>node.attributes.role==='status').dataset.error,'true');
});

test('calendar failures retry calendar only and never reconfirm the personal deadline',async()=>{
  const calls=[];
  const fixture=reviewFixture(async(url,body)=>{calls.push(body); return calls.length===1
    ? {ok:true,calendar_error:'offline',message:'已更新個人期限。日曆同步失敗。'} : {ok:true};});
  fixture.get(node=>node.tag==='input'&&node.type==='checkbox').checked=true;
  const confirm=fixture.get(node=>node.textContent==='確認更新');
  await confirm.click();
  assert.equal(confirm.textContent,'重試日曆同步');
  assert.equal(confirm.disabled,false);
  await confirm.click();
  assert.equal(calls[1].retry_calendar,true);
  assert.equal(confirm.textContent,'已更新');
});

test('dismissed suggestions disappear only after the server acknowledges the action',async()=>{
  let body;
  const fixture=reviewFixture(async(url,payload)=>{body=payload;return {ok:true};});
  await fixture.get(node=>node.textContent==='略過').click();
  assert.deepEqual(body,{id:'proposal',dismiss:true});
  assert.equal(fixture.dismissed(),'proposal');
  assert.equal(fixture.get(node=>node.tag==='section').removed,true);
});

test('ambiguous date and timezone warnings are shown as literal text',()=>{
  const fixture=reviewFixture(async()=>({ok:true}),{warnings:['日期可能為月／日或日／月，請核對。','CST 時區可能有歧義。']});
  assert.ok(fixture.nodes.some(node=>node.textContent==='日期可能為月／日或日／月，請核對。 CST 時區可能有歧義。'));
  assert.ok(fixture.nodes.every(node=>node.innerHTML===undefined));
});
