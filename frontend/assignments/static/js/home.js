(() => {
  const button = document.getElementById('homeThemeToggle');
  if (!button) return;
  const root = document.documentElement;
  const updateLabel = () => {
    const dark = root.dataset.theme === 'dark';
    button.setAttribute('aria-label', dark ? '切換淺色模式' : '切換深色模式');
    button.title = dark ? '切換淺色模式' : '切換深色模式';
    button.setAttribute('aria-pressed', String(dark));
  };
  updateLabel();
  button.hidden = false;
  button.addEventListener('click', () => {
    const next = root.dataset.theme === 'dark' ? 'light' : 'dark';
    root.dataset.theme = next;
    root.style.colorScheme = next;
    try { localStorage.setItem('e3_theme', next); } catch (_) { /* Theme switching also works without storage. */ }
    updateLabel();
  });
})();
