// Fixture-only interaction and visual checks. No real account or E3 requests.
const {chromium} = require('playwright');
const {execFileSync} = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '../..');
const fixture = flag => execFileSync(process.argv[2] || 'python', [path.join(__dirname, 'course_files_fixture.py'), ...(flag ? [flag] : [])], {
  encoding: 'utf8', env: {...process.env, PYTHONUTF8: '1'}, maxBuffer: 8 * 1024 * 1024,
});
(async () => {
  const html = fixture(), scoped = fixture('--scoped'), initial = JSON.parse(fixture('--data'));
  const browser = await chromium.launch({channel: 'msedge', headless: true});
  const results = [];
  fs.mkdirSync(path.join(root, 'output/playwright'), {recursive: true});
  try {
    for (const [width, theme] of [[1440, 'dark'], [1440, 'light'], [390, 'dark'], [320, 'light']]) {
      const page = await browser.newPage({viewport: {width, height: 1000}});
      let data = structuredClone(initial), reads = 0, posts = 0;
      const errors = [], requests = [];
      page.on('pageerror', error => errors.push(error.message));
      await page.addInitScript(theme => {
        localStorage.setItem('e3_theme', theme);
        window.cspErrors = [];
        document.addEventListener('securitypolicyviolation', event => window.cspErrors.push(event.violatedDirective));
      }, theme);
      await page.route('**/*', async route => {
        const url = new URL(route.request().url());
        requests.push(url.pathname);
        if (url.origin !== 'http://files.test') return route.abort();
        const asset = url.pathname.match(/^\/assets\/(shared|assignments)\/(.+)$/);
        if (asset) return route.fulfill({body: fs.readFileSync(path.join(root, `frontend/${asset[1]}/static`, asset[2])),
          contentType: asset[2].endsWith('.css') ? 'text/css' : asset[2].endsWith('.svg') ? 'image/svg+xml' : 'text/javascript'});
        if (url.pathname === '/courses/files') return route.fulfill({body: url.searchParams.has('course') ? scoped : html, contentType: 'text/html', headers: {
          'Content-Security-Policy': "default-src 'self'; script-src 'nonce-files-fixture' 'strict-dynamic' 'self'; script-src-attr 'none'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'",
        }});
        if (url.pathname === '/api/course-files/refresh') {
          posts++;
          assert.ok(route.request().headers()['x-csrftoken'], 'CSRF header present');
          assert.deepEqual(route.request().postDataJSON(), {semester: '115-1'});
          data.running = true;
          return route.fulfill({status: 202, json: {ok: true, status: 'running'}});
        }
        if (url.pathname === '/api/course-files') {
          reads++;
          if (url.searchParams.get('semester') === '114-2') return route.fulfill({json: {...data, items: [], courses: [], running: false}});
          return route.fulfill({json: data});
        }
        return route.fulfill({json: {ok: true}});
      });
      await page.goto('http://files.test/courses/files');
      await page.locator('.file-row').last().waitFor();
      assert.equal(await page.locator('.file-row').count(), 6);
      assert.equal(posts, 0, 'Existing library never starts a background E3 scan on entry');
      assert.equal(reads, 1);
      assert.equal(await page.locator('.files-group').count(), 3, 'Files grouped by course, not mixed source/type categories');
      assert.equal(await page.locator('.files-sidebar').isVisible(), width > 740);
      assert.equal(await page.locator('.files-mobile-course').isVisible(), width <= 740);
      assert.equal(await page.locator('#filesType').isVisible(), false, 'Advanced filters start collapsed');
      assert.equal(await page.locator('.file-title').filter({hasText: 'HW2_specification.pdf'}).count(), 1);
      await page.screenshot({path: path.join(root, `output/playwright/course-files-${width}-${theme}.png`), fullPage: true});
      await page.locator('#filesSearch').fill('報告 DOCX');
      await page.waitForTimeout(180);
      assert.equal(await page.locator('.file-row').count(), 1);
      await page.locator('#filesSearch').fill('');
      await page.locator('[data-source="mail"]').click();
      await page.waitForTimeout(180);
      assert.equal(await page.locator('.file-row').count(), 1);
      await page.locator('[data-source=""]').click();
      await page.locator('#filesFilters summary').click();
      await page.locator('#filesType').selectOption('pdf');
      assert.equal(await page.locator('.file-row').count(), 2);
      await page.keyboard.press('Escape');
      assert.equal(await page.locator('#filesType').isVisible(), false);
      await page.locator('#filesFilters summary').click();
      await page.locator('#filesType').selectOption('');
      await page.locator('#filesTitle').click();
      assert.equal(await page.locator('#filesType').isVisible(), false, 'Outside click closes filter panel');
      const chooseCourse = async id => {
        if (width > 740) await page.locator(`#filesCourses [data-course="${id}"]`).click();
        else {
          await page.locator('#courseFilterToggle').click();
          await page.getByRole('option', {name: id ? '作業系統 Operating Systems' : '全部課程', exact: true}).click();
        }
      };
      await chooseCourse('42');
      assert.equal(await page.locator('.file-row').count(), 2);
      assert.equal(await page.locator('.files-group').count(), 1);
      const details = page.locator('.file-origins details');
      await details.locator('summary').click();
      assert.equal(await details.locator('a').count(), 2, 'All sources preserved without duplicate file rows');
      assert.ok((await details.locator('a').last().getAttribute('href')).startsWith('/courses/messages?'));
      await page.locator('#filesSearch').fill('<img src=x onerror=alert(1)>');
      await page.waitForTimeout(180);
      assert.equal(await page.locator('#filesEmpty').isVisible(), true);
      await page.locator('#filesClear').click();
      assert.equal(await page.locator('.file-row').count(), 2, 'Clear filters preserves selected course');
      await chooseCourse('');
      assert.equal(await page.locator('.file-row').count(), 6);
      await page.locator('#filesRefresh').click();
      await page.waitForFunction(() => document.getElementById('filesRefresh').getAttribute('aria-busy') === 'true');
      assert.equal(await page.locator('.file-row').count(), 6, 'Cached files remain interactive during scan');
      await page.locator('#filesFilters summary').click();
      await page.locator('#filesType').selectOption('document');
      await page.locator('#filesTitle').click();
      assert.equal(await page.locator('.file-row').count(), 1);
      data.running = false; data.remaining = 2; data.error = '部分來源尚未完成，已保留先前附件，可繼續整理。';
      await page.waitForFunction(() => document.getElementById('filesRefreshLabel').textContent === '繼續整理');
      assert.equal(await page.locator('.file-row').count(), 1);
      await page.locator('#filesSemester').selectOption('114-2');
      await page.waitForFunction(() => document.getElementById('filesCount').textContent === '0');
      assert.equal(await page.locator('.file-row').count(), 0, 'Previous semester files are removed immediately');
      assert.ok((await page.locator('[data-semester-link]').first().getAttribute('href')).includes('semester=114-2'));
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'No horizontal overflow');
      assert.deepEqual(errors, []);
      assert.deepEqual(await page.evaluate(() => window.cspErrors), []);
      assert.ok(!requests.some(url => /\/api\/course-(mail|announcements)\/item|\/seen/.test(url)), 'Library never marks messages read');
      data = structuredClone(initial);
      await page.goto('http://files.test/courses/files?course=42&q=HW2');
      await page.waitForFunction(() => document.querySelectorAll('.file-row').length === 1);
      assert.equal(await page.locator('#filesSearch').inputValue(), 'HW2', 'LINE link restores course and query');
      assert.equal(await page.locator('#courseFilter').inputValue(), '42');
      assert.equal(await page.locator('.files-group').count(), 1);
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'Scoped view has no overflow');
      assert.deepEqual(errors, []);
      assert.deepEqual(await page.evaluate(() => window.cspErrors), []);
      results.push({width, theme, passed: true, reads, posts});
      await page.close();
    }
    console.log(JSON.stringify(results, null, 2));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
