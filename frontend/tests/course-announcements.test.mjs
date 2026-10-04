import assert from 'node:assert/strict';
import { test } from 'node:test';
import { filterAnnouncements, visibleAnnouncements, safeNewsLink, announcementDate } from '../assignments/static/js/course-announcements.js';

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
