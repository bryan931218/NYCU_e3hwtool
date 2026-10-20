import test from 'node:test';
import assert from 'node:assert/strict';
import {filterFiles, safeLibraryLink} from '../assignments/static/js/course-files.js';

const items = [
  {id: '1', title: 'HW 2', filename: 'format.pdf', course_id: 42, course_title: '作業系統', type: 'pdf', origins: [{kind: 'assignment', title: 'HW2 規格'}, {kind: 'announcements', title: '報告格式更新'}]},
  {id: '2', title: 'HW 10', filename: 'slides.pptx', course_id: 42, course_title: '作業系統', type: 'slides', origins: [{kind: 'course', title: '教材'}]},
  {id: '3', title: '參考資料', filename: '', course_id: 43, course_title: '計算機網路', type: 'link', origins: [{kind: 'mail', title: '期末參考連結'}]},
];

test('course, source, type and multi-word search combine without mutating data', () => {
  assert.deepEqual(filterFiles(items, {course: '42', source: 'announcements', type: 'pdf', query: '報告 FORMAT'}).map(item => item.id), ['1']);
  assert.deepEqual(filterFiles(items, {source: 'mail'}).map(item => item.id), ['3']);
  assert.equal(filterFiles(items, {course: '43', type: 'pdf'}).length, 0);
  assert.equal(items[0].id, '1');
});
test('numeric-aware filename sorting and whitespace-only search', () => {
  assert.deepEqual(filterFiles(items.slice(0, 2), {sort: 'name', query: '  '}).map(item => item.title), ['HW 2', 'HW 10']);
});
test('safe links reject active protocols, credential URLs and session-token queries', () => {
  for (const url of ['javascript:alert(1)', 'http://example.test/a', '//example.test/a', 'https://user:pass@example.test', 'https://[invalid', 'https://example.test/a?MoodleSession=x', 'https://example.test/a?token=']) {
    assert.equal(safeLibraryLink(url), '');
  }
  assert.equal(safeLibraryLink('https://example.test/a.pdf'), 'https://example.test/a.pdf');
});
test('only same-site course message deep links are allowed as relative source URLs', () => {
  assert.equal(safeLibraryLink('/courses/messages?semester=115-1&item=42:7', {origin: true}), '/courses/messages?semester=115-1&item=42:7');
  for (const url of ['/admin/traffic', '//evil.test/courses/messages?', '/courses/messages/../login?', '/courses/messages\\?evil']) {
    assert.equal(safeLibraryLink(url, {origin: true}), '');
  }
});
