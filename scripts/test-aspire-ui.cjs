// Usage: node scripts/test-aspire-ui.cjs <VerificationOutput.json> <artifact-directory>
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require(process.env.DISTILL_PLAYWRIGHT_MODULE || 'playwright');
(async () => {
  const endpoints = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
  const output = process.argv[3];
  fs.mkdirSync(output, { recursive: true, mode: 0o700 });
  const browser = await chromium.launch({ executablePath: process.env.DISTILL_CHROMIUM_PATH });
  const results = [];
  try {
    for (const name of ['first', 'second']) {
      const page = await browser.newPage();
      const errors = [];
      page.on('pageerror', error => errors.push(error.message));
      await page.goto(endpoints[name] + '/counter');
      const button = page.getByRole('button', { name: 'Click me' });
      const status = page.getByRole('status');
      // Retry only while the initial prerendered page has not become interactive.
      for (let attempt = 0; attempt < 20 && (await status.textContent()).includes('count: 0'); attempt++) {
        await button.click();
        await page.waitForTimeout(100);
      }
      await page.waitForFunction(() => document.querySelector('[role=status]').textContent.includes('count: 1'));
      for (let count = 2; count <= 5; count++) {
        await button.click();
        await page.waitForFunction(expected => document.querySelector('[role=status]').textContent.trim() === `Current count: ${expected}`, count);
      }
      await page.screenshot({ path: path.join(output, name + '-counter.png') });
      await page.goto(endpoints[name] + '/weather');
      await page.locator('tbody tr').first().waitFor();
      if (await page.locator('tbody tr').count() !== 5) throw new Error('Weather rows missing');
      if (errors.length) throw new Error(`Blazor browser errors: ${errors.length}`);
      await page.screenshot({ path: path.join(output, name + '-weather.png') });
      results.push({ application: name, counterClicks: 5, weatherRows: 5, browserErrors: errors.length });
      await page.close();
    }
    fs.writeFileSync(path.join(output, 'browser-evidence.json'), JSON.stringify(results, null, 2));
    console.log('Real browser counter clicks and weather rendering passed for both producer versions');
  } finally { await browser.close(); }
})().catch(error => { console.error(error.message); process.exitCode = 1; });
