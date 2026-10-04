(() => {
  function statusFromDashboard(html, finalUrl) {
    try {
      const url = new URL(finalUrl);
      if (url.origin !== "https://e3p.nycu.edu.tw") return "unknown";
      if (/^\/login\/(?:index\.php)?$/.test(url.pathname)) return "login-required";
      // /my/ requires a real Moodle login (including for sites allowing guests).
      // A redirect to MFA, password change or policy acceptance is not success.
      if (!/^\/my\/(?:index\.php)?$/.test(url.pathname)) return "unknown";
      const match = html.slice(0, 1024 * 1024).match(/\bM\.cfg\s*=\s*(\{[\s\S]*?\});/);
      const userId = match ? JSON.parse(match[1]).userId : null;
      return Number.isInteger(userId) && userId > 0 ? "authenticated" : "unknown";
    } catch {
      return "unknown";
    }
  }
  globalThis.E3AutoNavigationProbe = Object.freeze({ statusFromDashboard });
})();
