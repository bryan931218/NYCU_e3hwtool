export function installBrowser(agent = '') {
  return /Edg\//.test(agent) ? 'edge' : 'chrome';
}

export function managerAddress(browser) {
  return browser === 'edge' ? 'edge://extensions' : 'chrome://extensions';
}

export async function copyManagerAddress(input, feedback, clipboard) {
  try {
    if (!clipboard?.writeText) throw new Error('clipboard unavailable');
    await clipboard.writeText(input.value);
    feedback.textContent = '已複製，請貼到瀏覽器網址列。';
    return true;
  } catch {
    input.focus();
    input.select();
    feedback.textContent = '請手動複製已選取的網址，再貼到瀏覽器網址列。';
    return false;
  }
}

export function initializeGuide(doc = document, win = window) {
  doc.getElementById('guideTheme')?.addEventListener('click', () => {
    const value = doc.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    doc.documentElement.dataset.theme = value;
    doc.documentElement.style.colorScheme = value;
    try { win.localStorage.setItem('e3_theme', value); } catch { /* The theme still works without storage. */ }
  });
  const input = doc.getElementById('extensionManagerUrl');
  const feedback = doc.getElementById('copyManagerFeedback');
  if (input && feedback) {
    const selectBrowser = browser => {
      input.value = managerAddress(browser);
      feedback.textContent = '';
      doc.querySelectorAll('[data-browser-name]').forEach(node => node.textContent = browser === 'edge' ? 'Edge' : 'Chrome');
      doc.querySelectorAll('[data-load-label]').forEach(node => node.textContent = browser === 'edge' ? '載入解壓縮' : '載入未封裝項目');
      doc.querySelectorAll('[data-store-browser]').forEach(node => node.hidden = node.dataset.storeBrowser !== browser);
      const stores = doc.querySelector('.store-install');
      if (stores) stores.hidden = !doc.querySelector(`[data-store-browser="${browser}"]`);
      const copyIcon = doc.querySelector('#copyManagerUrl .guide-icon');
      if (copyIcon) copyIcon.className = 'guide-icon icon-copy';
      doc.querySelector('.manager-preview-content')?.setAttribute('aria-label', browser === 'edge' ? '開啟開發人員模式，再選擇載入解壓縮' : '開啟開發人員模式，再選擇載入未封裝項目');
    };
    const browser = installBrowser(win.navigator.userAgent);
    const radio = doc.querySelector(`[name="install_browser"][value="${browser}"]`);
    if (radio) radio.checked = true;
    selectBrowser(browser);
    doc.querySelectorAll('[name="install_browser"]').forEach(radio => radio.addEventListener('change', () => {
      if (radio.checked) selectBrowser(radio.value);
    }));
    doc.getElementById('copyManagerUrl')?.addEventListener('click', async () => {
      const copied = await copyManagerAddress(input, feedback, win.navigator.clipboard);
      doc.querySelector('#copyManagerUrl .guide-icon').className = `guide-icon ${copied ? 'icon-check' : 'icon-copy'}`;
    });
    if (/Android|iPhone|iPad|iPod/.test(win.navigator.userAgent) || (win.navigator.platform === 'MacIntel' && win.navigator.maxTouchPoints > 1)) {
      const note = doc.getElementById('guideDeviceNote');
      if (note) {
        note.dataset.mobile = 'true';
        note.textContent = '請在電腦版 Chrome 或 Edge 安裝。手機仍可使用追蹤系統與原 E3 連結。';
      }
    }
  }
  const pause = doc.getElementById('flowPause');
  pause?.addEventListener('click', () => {
    const paused = pause.getAttribute('aria-pressed') !== 'true';
    pause.setAttribute('aria-pressed', String(paused));
    pause.setAttribute('aria-label', paused ? '播放流程動畫' : '暫停流程動畫');
    pause.title = paused ? '播放流程動畫' : '暫停流程動畫';
    pause.querySelector('.guide-icon').className = `guide-icon ${paused ? 'icon-play' : 'icon-pause'}`;
    doc.getElementById('navigationFlow').dataset.paused = String(paused);
  });
}

if (typeof document !== 'undefined') initializeGuide();
