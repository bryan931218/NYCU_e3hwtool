// node backend/tests/traffic_sources_browser.cjs .venv/Scripts/python.exe
const { chromium } = require('playwright');
const { execFileSync } = require('node:child_process');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '../..');
const fixture = (...args) => execFileSync(process.argv[2] || 'python', [path.join(__dirname, 'traffic_sources_fixture.py'), ...args], {encoding:'utf8', env:{...process.env, PYTHONUTF8:'1'}});

(async () => {
    const browser = await chromium.launch({channel: process.env.PLAYWRIGHT_CHANNEL || 'msedge', headless:true});
    const errors = [];
    const html = fixture();
    fs.mkdirSync(path.join(root, 'output'), {recursive:true});
    try {
        for (const width of [1440, 390]) {
            const context = await browser.newContext({viewport:{width,height:1000}});
            const page = await context.newPage();
            page.on('pageerror', err => errors.push(err.message));
            await page.route('**/*', route => {
                const url = new URL(route.request().url());
                if (url.origin !== 'http://traffic.test') return route.abort();
                if (url.pathname.startsWith('/assets/shared/')) {
                    const asset = path.join(root, 'frontend/shared/static', url.pathname.slice('/assets/shared/'.length));
                    return route.fulfill({contentType:asset.endsWith('.css')?'text/css':'text/javascript',body:fs.readFileSync(asset)});
                }
                return route.fulfill(route.request().isNavigationRequest()
                    ? {contentType:'text/html', body:html}
                    : {contentType:'application/json', body:'{"version":0}'});
            });
            await page.goto('http://traffic.test/admin/traffic');
            assert.equal(await page.locator('.admin-brand h1').innerText(), '流量監控');
            assert.equal(await page.locator('#pageVisitTotal').innerText(), '9');
            assert.equal(await page.locator('.source-row').count(), 6);
            assert.match(await page.locator('.source-row[data-source=Dcard]').innerText(), /3 次\s+33.3%/);
            await page.locator('.source-history summary').click();
            assert.equal(await page.locator('#sourceVisits tr:visible').count(), 9);
            await page.locator('#sourceFilter').selectOption('Google');
            assert.equal(await page.locator('#sourceVisits tr:visible').count(), 2);
            await page.locator('#sourceFilter').selectOption('');
            assert.equal(await page.locator('#sourceVisits tr:visible').count(), 9);
            assert.equal((await page.locator('#trafficSources').innerText()).includes('never-store'), false);
            assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'Mobile page must not overflow');
            await page.locator('.source-history summary').click();
            for (const theme of ['light', 'dark']) {
                await page.locator(`[data-admin-theme=${theme}]`).click();
                await page.locator('#trafficSources').scrollIntoViewIfNeeded();
                assert.equal(await page.locator('html').getAttribute('data-theme'), theme);
                await page.locator('#trafficSources').screenshot({path:path.join(root, `output/traffic-current-${width}-${theme}.png`)});
            }
            await context.close();
        }
        assert.deepEqual(errors, []);
        console.log('Current traffic page: sources, filters, mobile, theme switching and privacy passed.');
    } finally { await browser.close(); }
})().catch(error => {console.error(error); process.exitCode=1;});
