export function isBrowserAutofilled(input) {
  return [":autofill", ":-webkit-autofill"].some((selector) => {
    try {
      return input.matches(selector);
    } catch {
      return false;
    }
  });
}

export function register(ctx) {}

export function initialize(ctx) {
  const input = ctx.assignmentSearch;
  if (!input) return;
  let timer;
  const syncQuery = () => {
    clearTimeout(timer);
    // Only browser-filled values are discarded; typed and pasted text stays intact.
    if (isBrowserAutofilled(input)) input.value = "";
    const query = input.value.trim().toLocaleLowerCase("zh-Hant");
    if (ctx.currentAssignmentQuery === query) return;
    ctx.currentAssignmentQuery = query;
    ctx.applyFilters();
  };
  input.form?.addEventListener("submit", (event) => {
    event.preventDefault();
    syncQuery();
  });
  input.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(syncQuery, 120);
  });
  input.addEventListener("change", syncQuery);
  input.addEventListener("animationstart", (event) => {
    if (event.animationName === "assignment-search-autofill") syncQuery();
  });
  window.addEventListener("pageshow", syncQuery);
  syncQuery();
}
