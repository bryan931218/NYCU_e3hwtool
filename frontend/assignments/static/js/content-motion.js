const running = new WeakMap();

export function animateContentChange(element) {
  if (!element) return;
  running.get(element)?.cancel();
  const preference = globalThis.matchMedia?.('(prefers-reduced-motion: reduce)');
  if (preference?.matches || !element.animate || element.hidden) return;
  const animation = element.animate([
    { opacity: .72, transform: 'translateY(4px)' },
    { opacity: 1, transform: 'none' },
  ], { duration: 170, easing: 'cubic-bezier(.2, .7, .2, 1)' });
  running.set(element, animation);
  const cancel = () => { if (preference.matches) animation.cancel(); };
  preference?.addEventListener('change', cancel);
  const cleanup = () => {
    if (running.get(element) === animation) running.delete(element);
    preference?.removeEventListener('change', cancel);
  };
  animation.finished.then(cleanup, cleanup);
}
