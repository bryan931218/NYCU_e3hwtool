(() => {
  const buttons = document.querySelectorAll('[data-e3-theme]');
  if (!buttons.length) return;
  const root = document.documentElement;
  function update(theme) {
    root.dataset.theme = theme;
    root.style.colorScheme = theme;
    const label = theme === 'dark' ? '切換至淺色模式' : '切換至深色模式';
    buttons.forEach(button => {
      button.title = label;
      button.setAttribute('aria-label', label);
    });
  }
  update(root.dataset.theme === 'dark' ? 'dark' : 'light');
  buttons.forEach(button => button.addEventListener('click', () => {
    const theme = root.dataset.theme === 'dark' ? 'light' : 'dark';
    update(theme);
    try { localStorage.setItem('e3_theme', theme); } catch { /* The current page can still switch themes. */ }
  }));
  window.addEventListener('storage', event => {
    if (event.key === 'e3_theme') update(event.newValue === 'dark' ? 'dark' : 'light');
  });
})();
