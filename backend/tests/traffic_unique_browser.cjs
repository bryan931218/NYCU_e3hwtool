// Synthetic monitoring data only; never reads a real account or browser session.
const {chromium} = require('playwright');
const {execFileSync} = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '../..');
const fixtures = new Map();
function fixtureFor(url) {
  const flags = [url.searchParams.has('empty') ? '--empty' : '--active-users'];
  for (const key of ['range', 'trend', 'start', 'end']) {
    if (url.searchParams.has(key)) flags.push(`--${key}=${url.searchParams.get(key)}`);
  }
  const cacheKey = flags.join(' ');
  if (!fixtures.has(cacheKey)) fixtures.set(cacheKey, execFileSync(process.argv[2] || 'python', [
    path.join(__dirname, 'traffic_sources_fixture.py'), ...flags,
  ], {encoding:'utf8', env:{...process.env, PYTHONUTF8:'1'}, maxBuffer:8*1024*1024}));
  return fixtures.get(cacheKey);
}
async function routePage(page, {blockChart = false} = {}) {
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    if (url.origin === 'https://cdn.jsdelivr.net') return blockChart ? route.abort() : route.continue();
    if (url.origin !== 'http://traffic.test') return route.abort();
    const asset = url.pathname.match(/^\/assets\/(shared|assignments)\/(.+)$/);
    if (asset) {
      const file = path.join(root, `frontend/${asset[1]}/static`, asset[2]);
      return route.fulfill({body:fs.readFileSync(file), contentType:file.endsWith('.css')?'text/css':file.endsWith('.svg')?'image/svg+xml':'text/javascript'});
    }
    return route.fulfill(route.request().isNavigationRequest() ? {body:fixtureFor(url),contentType:'text/html'} : {json:{version:0}});
  });
}
(async () => {
  const browser = await chromium.launch({channel:'msedge', headless:true});
  fs.mkdirSync(path.join(root, 'output/playwright'), {recursive:true});
  try {
    for (const width of [1440, 768, 390, 320]) {
      const page = await browser.newPage({viewport:{width,height:1000}});
      const errors = [];
      page.on('pageerror', e => errors.push(e.message));
      await routePage(page);
      await page.goto('http://traffic.test/admin/traffic');
      assert.equal(await page.locator('#visit-trend #trendUniqueUsers').innerText(), '3');
      assert.equal(await page.locator('#trendUniqueUsers').count(), 1, 'No separate duplicate statistics section');
      assert.ok(await page.locator('#visit-trend .trend-presets a[href*="range=30d"]').count());
      assert.match(await page.locator('.trend-new-total').innerText(), /1\s*人/);
      const newUsers = page.getByRole('button', {name:'其中新用戶', exact:true});
      await newUsers.click();
      assert.equal(await newUsers.getAttribute('aria-pressed'), 'false');
      assert.equal(await page.locator('#visitChart').evaluate(canvas => Chart.getChart(canvas).isDatasetVisible(1)), false);
      assert.equal(await page.locator('[data-trend-series="0"]').isDisabled(), true, 'Keep at least one readable series');
      await newUsers.press('Space');
      assert.equal(await newUsers.getAttribute('aria-pressed'), 'true');
      const dateSummary = page.locator('#trendDatePicker summary');
      await dateSummary.click();
      const picker = await page.locator('.trend-dates').boundingBox();
      assert.ok(picker.x >= 0 && picker.x + picker.width <= width, 'Date picker stays on screen');
      await page.locator('.trend-dates input[name="start"]').focus();
      await page.keyboard.press('Escape');
      assert.equal(await page.locator('#trendDatePicker').getAttribute('open'), null);
      assert.equal(await dateSummary.evaluate(el => el === document.activeElement), true);
      await dateSummary.click();
      await page.locator('#trend-title').click();
      assert.equal(await page.locator('#trendDatePicker').getAttribute('open'), null, 'Click outside closes date picker');
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
      if (width === 1440) {
        const bar = await page.locator('#visitChart').evaluate(canvas => {
          const chart = Chart.getChart(canvas), point = chart.getDatasetMeta(0).data.at(-1), rect = canvas.getBoundingClientRect();
          return {x:rect.left+point.x, y:rect.top+(point.y+point.base)/2};
        });
        await page.mouse.move(bar.x, bar.y);
        assert.ok(await page.locator('#visitChart').evaluate(canvas => Chart.getChart(canvas).tooltip.opacity > 0), 'Hover reveals exact values');
        await dateSummary.click();
        const today = await page.locator('.trend-dates input[name="end"]').inputValue();
        const daysBefore = days => new Date(Date.parse(today+'T00:00:00Z')-days*86400000).toISOString().slice(0,10);
        await page.locator('.trend-dates input[name="start"]').fill(daysBefore(1));
        await page.locator('.trend-dates input[name="end"]').fill(daysBefore(2));
        await page.locator('.trend-dates input[name="end"]').dispatchEvent('change');
        assert.equal(await page.locator('.trend-dates input[name="end"]').evaluate(el => el.checkValidity()), false);
        await page.locator('.trend-dates input[name="end"]').fill(today);
        await page.locator('.trend-dates input[name="end"]').dispatchEvent('change');
        await page.getByRole('button', {name:'套用日期', exact:true}).click();
        await page.waitForURL('**/admin/traffic?range=custom*');
        assert.ok((await page.locator('.trend-header').innerText()).includes(`${daysBefore(1).replaceAll('-','/')} — ${today.replaceAll('-','/')}`));
        assert.equal(await page.locator('#trendUniqueUsers').innerText(), '3');
        await page.locator('#visit-trend .trend-presets').getByRole('link', {name:'今天', exact:true}).click();
        assert.equal(await page.locator('#trendUniqueUsers').innerText(), '2');
        await page.locator('.trend-toggle').getByRole('link', {name:'每小時', exact:true}).click();
        assert.match(await page.locator('.trend-chart-title').innerText(), /每小時活躍帳號/);
      }
      assert.deepEqual(errors, []);
      console.log(JSON.stringify({width, unique:3, passed:true}));
      await page.close();
    }
    for (const [name, url, options] of [
      ['empty','http://traffic.test/admin/traffic?empty=1',{}],
      ['unavailable','http://traffic.test/admin/traffic',{blockChart:true}],
    ]) {
      const page = await browser.newPage({viewport:{width:390,height:1000}});
      const errors = [];
      page.on('pageerror', error => errors.push(error.message));
      await routePage(page, options);
      await page.goto(url);
      assert.equal(await page.locator('#chartEmpty').isVisible(), true);
      assert.equal(await page.locator('[data-trend-series="0"]').isDisabled(), true);
      if (name === 'unavailable') assert.equal(await page.locator('.trend-values').getAttribute('open'), '');
      assert.deepEqual(errors, []);
      await page.locator('#visit-trend').screenshot({path:path.join(root,`output/playwright/traffic-trend-${name}.png`)});
      await page.close();
      console.log(JSON.stringify({state:name, passed:true}));
    }
  } finally { await browser.close(); }
})().catch(error => {console.error(error); process.exitCode=1;});
