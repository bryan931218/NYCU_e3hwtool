// Run with a Node runtime that provides sharp (no website runtime dependency).
const fs = require('node:fs');
const path = require('node:path');
const sharp = require('sharp');

const root = path.resolve(__dirname, '../..');
const items = [
  ['我的作業', 'frontend/shared/static/icons/check.svg', '#e4f4ee', '#23755c'],
  ['課程訊息', 'frontend/shared/static/icons/inbox.svg', '#edf3fb', '#476fa2'],
  ['提醒設定', 'frontend/shared/static/icons/settings-2.svg', '#f5efe3', '#8c6b31'],
  ['綁定教學', 'frontend/shared/static/icons/shield-check.svg', '#edf3f0', '#557369'],
  ['通知說明', 'frontend/shared/static/icons/folder-open.svg', '#f2eff8', '#796492'],
  ['回報問題', 'frontend/assignments/static/vendor/lucide-calendar/pencil.svg', '#faeeea', '#a26453'],
];
const cells = items.map(([label, source, tint, ink], index) => {
  const x = (index % 3) * 2500 / 3;
  const y = Math.floor(index / 3) * 843;
  const cx = x + 2500 / 6;
  const icon = fs.readFileSync(path.join(root, source), 'utf8')
    .replace(/<svg[\s\S]*?>/, '').replace(/<\/svg>\s*$/, '');
  return `<g>
    <rect x="${x}" y="${y}" width="${2500 / 3}" height="843" fill="${index === 0 ? '#f0f8f4' : '#fcfdfc'}"/>
    <rect x="${cx - 112}" y="${y + 184}" width="224" height="224" rx="48" fill="${tint}"/>
    <g transform="translate(${cx - 69},${y + 227}) scale(5.75)" fill="none" stroke="${ink}" stroke-width="1.65" stroke-linecap="round" stroke-linejoin="round">${icon}</g>
    <text x="${cx}" y="${y + 534}" text-anchor="middle" font-family="Microsoft JhengHei, sans-serif" font-size="76" font-weight="700" fill="#28352f">${label}</text>
    <rect x="${cx - 22}" y="${y + 595}" width="44" height="5" rx="2.5" fill="${ink}"/>
  </g>`;
}).join('');
const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="2500" height="1686" viewBox="0 0 2500 1686">
${cells}
<g stroke="#dfe6e1" stroke-width="2"><path d="M833.333 0v1686M1666.667 0v1686M0 843h2500"/></g>
</svg>`;
sharp(Buffer.from(svg)).png().toFile(path.join(__dirname, 'rich-menu.png'))
  .then(info => console.log(JSON.stringify({width:info.width, height:info.height, bytes:info.size})))
  .catch(error => { console.error(error); process.exitCode = 1; });
