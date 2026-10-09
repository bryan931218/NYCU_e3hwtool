// Synthetic fixtures only: never contact Google or create real analytics visits.
const { chromium } = require('playwright');
const { execFileSync } = require('node:child_process');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '../..');
const fixture = (...args) => execFileSync(process.argv[2] || 'python', [path.join(__dirname, 'traffic_sources_fixture.py'), '--ga4', ...args], { encoding: 'utf8', env: { ...process.env, PYTHONUTF8: '1', E3_ENV: 'development', RAILWAY_ENVIRONMENT_ID: '', RAILWAY_ENVIRONMENT_NAME: '', E3_NOTIFICATIONS_WORKER: '0', E3_YOUTUBE_AUTO_SYNC_ENABLED: '0', E3_SESSION_PROFILE_WORKER: '0' } });
const reports = {
  summary: [{ activeUsers: 100, newUsers: 24, sessions: 130, screenPageViews: 350, engagementRate: 0.74, userEngagementDuration: 6600 }],
  sources: [{ sessionSourceMedium: 'dcard / social', sessionCampaignName: 'dcard-115-1', sessions: 60, activeUsers: 45, engagementRate: 0.8 }],
  pages: [{ pagePath: '/', screenPageViews: 200, activeUsers: 100 }],
  devices: [{ deviceCategory: 'mobile', activeUsers: 75, sessions: 90 }],
  returning: [{ newVsReturning: 'returning', activeUsers: 50, sessions: 60 }],
  events: [{ eventName: 'feature_calendar', totalUsers: 30, eventCount: 90 }, { eventName: 'login_password', totalUsers: 40, eventCount: 50 }],
  login_funnel: [{ funnelStepName: '1. 首頁', activeUsers: 90, funnelStepCompletionRate: 0.8 }, { funnelStepName: '2. 登入頁', activeUsers: 72, funnelStepCompletionRate: 0.75 }, { funnelStepName: '3. 登入成功', activeUsers: 54, funnelStepCompletionRate: 0 }],
  line_funnel: null,
};
(async () => {
  const browser = await chromium.launch({ channel: 'msedge', headless: true });
  const errors = [];
  fs.mkdirSync(path.join(root, 'output/playwright'), { recursive: true });
  try {
    const html = fixture();
    for (const width of [1440, 390]) {
      const page = await browser.newPage({ viewport: { width, height: 1000 } });
      page.on('pageerror', error => errors.push(error.message));
      let calls = 0;
      await page.route('**/*', route => {
        const url = new URL(route.request().url());
        if (url.origin !== 'http://traffic.test') return route.abort();
        if (url.pathname.startsWith('/assets/shared/')) {
          const asset = path.join(root, 'frontend/shared/static', url.pathname.slice('/assets/shared/'.length));
          return route.fulfill({ contentType: asset.endsWith('.css') ? 'text/css' : 'text/javascript', body: fs.readFileSync(asset) });
        }
        if (url.pathname === '/admin/analytics/ga4') { calls++; return route.fulfill({ json: { status: 'ready', updated_at: '2026-10-10 12:00', reports } }); }
        return route.fulfill(route.request().isNavigationRequest() ? { contentType: 'text/html', body: html } : { json: { version: 0 } });
      });
      await page.goto('http://traffic.test/admin/traffic');
      assert.equal(await page.locator('#trafficSources, #system-summary, .analytics-feature-table').count(), 0, 'Remove legacy duplicate statistics');
      assert.equal(await page.locator('a[href="/admin/analytics"]').count(), 0, 'Remove the duplicate page analytics entry');
      assert.equal(await page.locator('#usageRange').count(), 1, 'Keep one account-range control');
      await page.locator('#google-analytics').scrollIntoViewIfNeeded();
      await page.locator('#ga4Report').waitFor({ state: 'visible' });
      assert.ok(calls >= 1, 'Load visible GA4 reports without blocking the page');
      assert.match(await page.locator('#ga4Metrics').innerText(), /66 秒/);
      assert.match(await page.locator('#ga4-features').innerText(), /作業日曆/);
      assert.doesNotMatch(await page.locator('#ga4-features').innerText(), /自訂代辦/);
      assert.match(await page.locator('#ga4-login_funnel').innerText(), /54/);
      assert.match(await page.locator('#ga4-line_funnel').innerText(), /暫時無法提供/);
      await page.locator('#ga4Range').selectOption('7');
      await page.locator('#ga4Report').waitFor({ state: 'visible' });
      assert.ok(calls >= 2);
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'No viewport overflow');
      for (const theme of ['light', 'dark']) {
        await page.locator(`[data-admin-theme=${theme}]`).click();
        await page.locator('#google-analytics').screenshot({ path: path.join(root, `output/playwright/ga4-${width}-${theme}.png`) });
      }
      await page.close();
    }
    const page = await browser.newPage();
    const landing = fixture('--landing');
    const google = [];
    await page.route('**/*', route => {
      const url = new URL(route.request().url());
      if (url.hostname === 'www.googletagmanager.com') { google.push(url.href); return route.fulfill({ body: '', contentType: 'text/javascript' }); }
      if (url.origin !== 'http://traffic.test') return route.abort();
      const match = url.pathname.match(/^\/assets\/(shared|assignments)\/(.+)$/);
      if (match) return route.fulfill({ contentType: match[2].endsWith('.css') ? 'text/css' : 'text/javascript', body: fs.readFileSync(path.join(root, `frontend/${match[1]}/static`, match[2])) });
      if (route.request().isNavigationRequest()) return route.fulfill({ body: landing, contentType: 'text/html' });
      return route.fulfill({ json: { ok: true, version: 0 } });
    });
    page.on('pageerror', error => errors.push(error.message));
    await page.goto('http://traffic.test/login?username=112550103&session=secret');
    assert.equal(google.length, 0);
    await page.locator('[data-analytics-choice=denied]').click();
    await page.locator('#analyticsConsent').waitFor({ state: 'hidden' });
    assert.equal(google.length, 0);
    await page.locator('#analyticsPrivacy').click();
    await page.locator('[data-analytics-choice=granted]').click();
    await page.waitForFunction(() => window.dataLayer?.length > 0);
    assert.ok(!JSON.stringify(await page.evaluate(() => Array.from(window.dataLayer).map(args => Array.from(args)))).includes('112550103'));
    assert.deepEqual(errors, []);
    console.log('GA4 desktop/mobile themes, deferred reports, alpha failure and consent privacy passed.');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
