// Synthetic monitoring data only; never reads a real account or browser session.
const {chromium} = require('playwright');
const {execFileSync} = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '../..');
const fixture = execFileSync(process.argv[2] || 'python', [path.join(__dirname, 'traffic_sources_fixture.py'), '--active-users'], {
  encoding:'utf8', env:{...process.env, PYTHONUTF8:'1'}, maxBuffer:8*1024*1024,
});
(async () => {
  const browser = await chromium.launch({channel:'msedge', headless:true});
  fs.mkdirSync(path.join(root, 'output/playwright'), {recursive:true});
  try {
    for (const width of [1440, 390, 320]) {
      const page = await browser.newPage({viewport:{width,height:1000}});
      const errors = [];
      page.on('pageerror', e => errors.push(e.message));
      await page.route('**/*', route => {
        const url = new URL(route.request().url());
        if (url.origin === 'https://cdn.jsdelivr.net') return route.continue();
        if (url.origin !== 'http://traffic.test') return route.abort();
        if (url.pathname.startsWith('/assets/shared/')) {
          const file = path.join(root, 'frontend/shared/static', url.pathname.slice('/assets/shared/'.length));
          return route.fulfill({body:fs.readFileSync(file), contentType:file.endsWith('.css')?'text/css':file.endsWith('.svg')?'image/svg+xml':'text/javascript'});
        }
        return route.fulfill(route.request().isNavigationRequest() ? {body:fixture,contentType:'text/html'} : {json:{version:0}});
      });
      await page.goto('http://traffic.test/admin/traffic');
      assert.equal(await page.locator('#visit-trend #trendUniqueUsers').innerText(), '3');
      assert.equal(await page.locator('#trendUniqueUsers').count(), 1, 'No separate duplicate statistics section');
      assert.ok(await page.locator('#visit-trend .trend-presets a[href*="range=30d"]').count());
      for (const theme of ['light','dark']) {
        await page.locator(`[data-admin-theme="${theme}"]`).click();
        await page.locator('#visit-trend').scrollIntoViewIfNeeded();
        assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'No horizontal overflow');
        const canvasPixels = await page.locator('#visitChart').evaluate(canvas => {
          if (canvas.hidden) return 0;
          return canvas.getContext('2d').getImageData(0,0,canvas.width,canvas.height).data.filter((v,i) => i%4===3 && v>0).length;
        });
        assert.ok(canvasPixels > 0, 'Real Chart.js canvas must render');
        await page.locator('#visit-trend').screenshot({path:path.join(root,`output/playwright/traffic-unique-${width}-${theme}.png`)});
      }
      assert.deepEqual(errors, []);
      console.log(JSON.stringify({width, unique:3, passed:true}));
      await page.close();
    }
  } finally { await browser.close(); }
})().catch(error => {console.error(error); process.exitCode=1;});
