// Synthetic fixtures only. No personal sessions or external requests are used.
const { chromium } = require('playwright');
const { execFileSync } = require('node:child_process');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '../..');
const fixture = mode => execFileSync(process.argv[2] || 'python', [path.join(__dirname, 'workbench_fixture.py'), `--${mode}`], {
  encoding: 'utf8', maxBuffer: 16 * 1024 * 1024, env: { ...process.env, PYTHONUTF8: '1', E3_ENV: 'development', RAILWAY_ENVIRONMENT_ID: '', RAILWAY_ENVIRONMENT_NAME: '', E3_NOTIFICATIONS_WORKER: '0', E3_YOUTUBE_AUTO_SYNC_ENABLED: '0', E3_SESSION_PROFILE_WORKER: '0' },
});
(async () => {
  const browser = await chromium.launch({ channel: 'msedge', headless: true });
  const results = [];
  try {
    for (const [width, mode] of [[1440, 'due'], [390, 'due'], [1440, 'calendar'], [390, 'calendar'], [1440, 'course'], [390, 'course']]) {
      const html = fixture(mode), errors = [], assets = [];
      const page = await browser.newPage({ viewport: { width, height: 1000 } });
      await page.clock.setFixedTime(new Date('2026-10-15T12:00:00+08:00'));
      const cdp = await page.context().newCDPSession(page);
      await cdp.send('Emulation.setCPUThrottlingRate', { rate: 4 });
      await page.addInitScript(() => {
        localStorage.setItem('e3_last_auto_refresh_ts', String(Date.now()));
        window.workbenchTimings = [];
        window.longTasks = [];
        window.cspErrors = [];
        document.addEventListener('securitypolicyviolation', event => window.cspErrors.push(event.violatedDirective));
        new PerformanceObserver(list => window.longTasks.push(...list.getEntries().map(entry => Math.round(entry.duration)))).observe({ type: 'longtask', buffered: true });
      });
      page.on('pageerror', error => errors.push(error.message));
      await page.route('**/*', async route => {
        const url = new URL(route.request().url());
        if (url.origin !== 'http://workbench.test') return route.abort();
        const match = url.pathname.match(/^\/assets\/(shared|assignments)\/(.+)$/);
        if (match) {
          assets.push(url.pathname);
          let body = fs.readFileSync(path.join(root, `frontend/${match[1]}/static`, match[2]));
          if (match[2] === 'js/workbench/index.js') {
            body = body.toString().replace('features.forEach((feature) => feature.initialize(context));', `
              for (const [name, value] of Object.entries(context)) {
                if (typeof value !== 'function') continue;
                context[name] = function(...args) { const start = performance.now(); try { return value.apply(this, args); } finally { window.workbenchTimings.push([name, Math.round((performance.now() - start) * 10) / 10]); } };
              }
              features.forEach((feature) => feature.initialize(context));
              window.workbenchReady = performance.now();`);
          }
          await new Promise(resolve => setTimeout(resolve, 40));
          return route.fulfill({ body, contentType: match[2].endsWith('.css') ? 'text/css' : match[2].endsWith('.svg') ? 'image/svg+xml' : 'text/javascript' });
        }
        if (route.request().isNavigationRequest()) return route.fulfill({ body: html, contentType: 'text/html', headers: {
          'Content-Security-Policy': "default-src 'self'; script-src 'nonce-workbench-fixture' 'strict-dynamic' 'self'; script-src-attr 'none'; style-src 'self' 'unsafe-inline'; font-src 'self' data:; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'",
        } });
        return route.fulfill({ json: { ok: true, version: 0, items: [], announcements: [], ts: 0 } });
      });
      await page.goto('http://workbench.test/');
      await page.waitForFunction(() => window.workbenchReady > 0);
      if (mode === 'calendar') await page.locator('.fc-daygrid-day').first().waitFor();
      const measurements = await page.evaluate(() => ({
        ready: Math.round(window.workbenchReady),
        dom: document.querySelectorAll('*').length,
        tasks: window.longTasks,
        slow: window.workbenchTimings.filter(([, duration]) => duration > 5).sort((a, b) => b[1] - a[1]).slice(0, 8),
      }));
      const initialRequests = assets.length;
      fs.mkdirSync(path.join(root, 'output/playwright'), { recursive: true });
      await page.screenshot({ path: path.join(root, `output/playwright/workbench-loading-${width}-${mode}.png`) });
      assert.equal(await page.locator('#courseFilterToggle').isVisible(), true);
      assert.equal(await page.locator('.courseTable tbody tr[data-uid]').count(), mode === 'course' ? 200 : 0, 'Only the selected course view is rendered on startup');
      assert.equal(await page.locator('#flatTable tbody tr[data-uid]').count(), 300, 'Canonical data includes all semesters');
      assert.ok(measurements.dom < (mode === 'course' ? 21000 : 15000), 'Startup avoids rendering unused copies of the assignment list');
      if (mode === 'calendar') {
        const more = page.locator('.calendar-event-more').first();
        if (width > 600) await more.click();
        else {
          const day = await more.locator('xpath=ancestor::td[@data-date]').getAttribute('data-date');
          await page.locator(`td[data-date="${day}"] .fc-daygrid-day-number`).click();
        }
        assert.equal(await page.locator('#calendarDayList .calendar-task').count(), 8, 'All tasks remain available beyond the two-item preview');
        assert.ok(await page.locator('#assignmentCalendar .fc-daygrid-day').evaluateAll(cells => cells.every(cell => cell.querySelectorAll('.fc-event').length <= 3)));
      }
      await page.locator('#assignmentSearch').fill('Assignment 1-2');
      await page.waitForTimeout(250);
      assert.ok(await page.locator('#flatTable tbody tr[data-uid]:not(.hidden)').count() > 0);
      await page.locator('#viewCourseBtn').click();
      assert.equal(await page.locator('.courseTable tbody tr[data-uid]').count(), 200, 'Only the requested semester is hydrated');
      assert.equal(await page.locator('.courseTable tbody tr[data-uid]').first().locator('td').count(), 5);
      assert.equal(await page.locator('.courseTable tbody tr[data-uid]').first().locator('.remaining-text').count(), 1);
      const visibleUids = selector => page.locator(selector).evaluateAll(rows => rows.map(row => row.dataset.uid).sort());
      assert.deepEqual(await visibleUids('.courseTable tbody tr[data-uid]:not(.hidden)'), await visibleUids('#flatTable tbody tr[data-uid]:not(.hidden)'));
      const ignore = page.locator('.courseTable tbody tr[data-uid]:not(.hidden) [data-ignore-assignment]').first();
      const ignoredUid = await ignore.getAttribute('data-ignore-assignment');
      await ignore.click();
      assert.equal(await page.locator('tr[data-uid]').evaluateAll((rows, uid) => rows.filter(row => row.dataset.uid === uid).every(row => row.classList.contains('hidden')), ignoredUid), true);
      await page.locator('#assignmentSearch').fill('');
      await page.waitForTimeout(250);
      await page.locator('#semesterSelector summary').click();
      await page.locator('[data-semester-filter][value="114-2"]').check();
      assert.equal(await page.locator('.courseTable tbody tr[data-uid]').count(), 300);
      assert.deepEqual(await visibleUids('#viewCourse .course-card:not(.hidden) tr[data-uid]:not(.hidden)'), await visibleUids('#flatTable tbody tr[data-uid]:not(.hidden)'));
      await page.locator('#viewDueBtn').click();
      const last = page.locator('#flatTable tbody tr[data-uid]:not(.hidden)').last();
      await last.scrollIntoViewIfNeeded();
      assert.ok((await last.boundingBox()).height >= 75, 'Deferred rows render when scrolled into view');
      await page.locator('#assignmentSearch').scrollIntoViewIfNeeded();
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      assert.deepEqual(errors, []);
      assert.deepEqual(await page.evaluate(() => window.cspErrors), []);
      assert.equal(new Set(assets.filter(asset => asset.includes('/js/workbench/'))).size, assets.filter(asset => asset.includes('/js/workbench/')).length, 'Preloads do not download duplicate modules');
      fs.mkdirSync(path.join(root, 'output/playwright'), { recursive: true });
      await page.screenshot({ path: path.join(root, `output/playwright/workbench-interactions-${width}-${mode}.png`) });
      results.push({ width, mode, requests: initialRequests, ...measurements });
      await page.close();
    }
    console.log(JSON.stringify(results, null, 2));
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
