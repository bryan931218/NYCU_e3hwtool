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
            await page.getByText('取得分享來源連結', {exact:true}).click();
            assert.match(await page.locator('#sourceShareLink').inputValue(), /utm_source=dcard/);
            await page.locator('#sourceShareChannel').selectOption('line');
            assert.match(await page.locator('#sourceShareLink').inputValue(), /utm_source=line/);
            await page.getByText('取得分享來源連結', {exact:true}).click();
            assert.equal(await page.locator('.admin-brand h1').innerText(), '流量監控');
            assert.equal(await page.locator('#pageVisitTotal').innerText(), '8');
            assert.equal(await page.locator('.source-row').count(), 5);
            assert.match(await page.locator('.source-row[data-source=Dcard]').innerText(), /3 次\s+37.5%/);
            assert.equal((await page.locator('#trafficSources').innerText()).includes('站內導覽'), false);
            await page.locator('#sourceVisitHistory summary').click();
            assert.equal(await page.locator('#sourceVisits tr:visible').count(), 8);
            await page.locator('#sourceFilter').selectOption('Google');
            assert.equal(await page.locator('#sourceVisits tr:visible').count(), 2);
            await page.locator('#sourceFilter').selectOption('');
            assert.equal(await page.locator('#sourceVisits tr:visible').count(), 8);
            assert.equal((await page.locator('#trafficSources').innerText()).includes('never-store'), false);
            assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'Mobile page must not overflow');
            await page.locator('#sourceVisitHistory summary').click();
            for (const theme of ['light', 'dark']) {
                await page.locator(`[data-admin-theme=${theme}]`).click();
                await page.locator('#trafficSources').scrollIntoViewIfNeeded();
                assert.equal(await page.locator('html').getAttribute('data-theme'), theme);
                await page.locator('#trafficSources').screenshot({path:path.join(root, `output/traffic-current-${width}-${theme}.png`)});
            }
            await context.close();
        }
        const reports = [];
        const landingHTML = fixture('--landing');
        const landing = await browser.newPage();
        landing.on('pageerror', err => errors.push(err.message));
        await landing.route('**/*', route => {
            const url = new URL(route.request().url());
            if (url.origin !== 'http://traffic.test') return route.abort();
            if (url.pathname === '/traffic/arrival') {
                reports.push(route.request().postDataJSON());
                return route.fulfill({contentType:'application/json',body:'{"ok":true}'});
            }
            const assetMatch = url.pathname.match(/^\/assets\/(shared|assignments)\/(.+)$/);
            if (assetMatch) {
                const asset = path.join(root, `frontend/${assetMatch[1]}/static`, assetMatch[2]);
                return route.fulfill({contentType:asset.endsWith('.css')?'text/css':'text/javascript',body:fs.readFileSync(asset)});
            }
            if (url.pathname === '/source') return route.fulfill({contentType:'text/html',body:'<a href="/login">Open</a>'});
            if (route.request().isNavigationRequest()) return route.fulfill({contentType:'text/html',body:landingHTML});
            return route.fulfill({contentType:'application/json',body:'{"version":0}'});
        });
        await landing.goto('http://traffic.test/source?private=never-send');
        const firstReport = landing.waitForResponse(response => response.url().endsWith('/traffic/arrival'));
        await landing.getByText('Open', {exact:true}).click();
        await firstReport;
        assert.equal(reports[0].referrer, 'http://traffic.test');
        assert.equal(reports[0].navigation, 'navigate');
        const reloadReport = landing.waitForResponse(response => response.url().endsWith('/traffic/arrival'));
        await landing.reload();
        await reloadReport;
        assert.equal(reports[1].navigation, 'reload');
        assert.equal(JSON.stringify(reports).includes('never-send'), false);
        assert.deepEqual(errors, []);
        console.log('Current traffic page: sources, filters, mobile, theme switching and privacy passed.');
    } finally { await browser.close(); }
})().catch(error => {console.error(error); process.exitCode=1;});
