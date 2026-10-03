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
  const syncQuery = () => {
    // Only browser-filled values are discarded; typed and pasted text stays intact.
    if (isBrowserAutofilled(input)) input.value = "";
    ctx.currentAssignmentQuery = input.value.trim().toLocaleLowerCase("zh-Hant");
    ctx.applyFilters();
  };
  input.form?.addEventListener("submit", (event) => {
    event.preventDefault();
    syncQuery();
  });
  input.addEventListener("input", syncQuery);
  input.addEventListener("change", syncQuery);
  input.addEventListener("animationstart", (event) => {
    if (event.animationName === "assignment-search-autofill") syncQuery();
  });
  window.addEventListener("pageshow", syncQuery);
  syncQuery();
}
