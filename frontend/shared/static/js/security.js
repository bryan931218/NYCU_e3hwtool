(() => {
  const csrfToken = () => document.querySelector('meta[name="csrf-token"]')?.content;
  const originalFetch = window.fetch.bind(window);
  window.fetch = (input, options = {}) => {
    const token = csrfToken();
    const url = new URL(input instanceof Request ? input.url : input, location.href);
    const method = String(options.method || (input instanceof Request ? input.method : 'GET')).toUpperCase();
    if (token && url.origin === location.origin && !['GET', 'HEAD', 'OPTIONS'].includes(method)) {
      const headers = new Headers(options.headers || (input instanceof Request ? input.headers : undefined));
      if (!headers.has('X-CSRFToken')) headers.set('X-CSRFToken', token);
      options = { ...options, headers };
    }
    return originalFetch(input, options);
  };

  const protectForm = (form) => {
    if (form.method.toLowerCase() !== 'post' || new URL(form.action, location.href).origin !== location.origin) return;
    let field = form.querySelector('input[name="csrf_token"]');
    if (!field) {
      field = document.createElement('input');
      field.type = 'hidden';
      field.name = 'csrf_token';
      form.append(field);
    }
    field.value = csrfToken() || '';
  };
  // Legacy feature code constructs forms and submits without a submit event.
  const originalSubmit = HTMLFormElement.prototype.submit;
  HTMLFormElement.prototype.submit = function () {
    protectForm(this);
    return originalSubmit.call(this);
  };
  document.addEventListener('submit', (event) => {
    if (!(event.target instanceof HTMLFormElement)) return;
    const message = event.submitter?.dataset.confirm || event.target.dataset.confirm;
    if (message && !window.confirm(message)) {
      event.preventDefault();
      event.stopImmediatePropagation();
      return;
    }
    protectForm(event.target);
  }, true);
  document.addEventListener('change', (event) => {
    if (event.target.matches('[data-auto-submit]')) event.target.form?.requestSubmit();
  });
  document.addEventListener('click', (event) => {
    if (event.target.closest('#announcementToggle')) window.__openAnnouncementModal?.();
  });
  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('form').forEach(protectForm);
    window.renderStudyMath?.();
    window.renderPublicStudyMath?.();
  });
  // Never retain E3 credentials in browser storage; migrate old remembered data.
  try {
    const old = JSON.parse(localStorage.getItem('e3_remember') || '{}');
    if (old.username) localStorage.setItem('e3_remember', JSON.stringify({ username: old.username }));
    else localStorage.removeItem('e3_remember');
    localStorage.removeItem('e3_remember_session');
  } catch (_) {
    try { localStorage.removeItem('e3_remember'); localStorage.removeItem('e3_remember_session'); } catch (_) {}
  }
})();
