document.querySelector('[data-course-theme]').addEventListener('click', () => {
  const root = document.documentElement;
  const theme = root.dataset.theme === 'dark' ? 'light' : 'dark';
  root.dataset.theme = theme;
  root.style.colorScheme = theme;
  try { localStorage.setItem('e3_theme', theme); } catch {}
});

document.querySelector('[data-course-semester]')?.addEventListener('change', event => {
  document.querySelectorAll('[data-course-semester-link]').forEach(link => {
    const url = new URL(link.href);
    url.searchParams.set('semester', event.target.value);
    link.href = url.href;
  });
});
