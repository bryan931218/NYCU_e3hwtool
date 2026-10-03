export function register(ctx) {}

export function initialize(ctx) {
  if (ctx.IS_GUEST || ctx.IS_READONLY_VIEW) return;
  const log = (feature) => ctx.logUiEvent?.(`usage_${feature}`);
  log({ calendar: "calendar", course: "course_view", due: "due_view" }[ctx.currentViewMode] || "due_view");
  document.addEventListener("click", (event) => {
    const target = event.target;
    const selectors = [
      ["#viewCalendarBtn, #assignmentCalendar [data-date], #calendarToday, #calendarNextDeadline", "calendar"],
      ["#viewDueBtn", "due_view"], ["#viewCourseBtn", "course_view"],
      ["[data-status-filter]", "filters"], ["[data-e3-assignment]", "open_e3"],
      ["[data-ignore-assignment], [data-restore-assignment], #restoreIgnoredAssignmentsAll", "ignore"],
      ["#manualRefreshBtn, #archiveRefreshBtn", "refresh"],
    ];
    for (const [selector, feature] of selectors) {
      if (target.closest?.(selector)) { log(feature); break; }
    }
  });
  document.addEventListener("change", (event) => {
    if (event.target.matches?.("#courseFilter, [data-semester-filter]")) log("filters");
  });
  let searchRecorded = false;
  document.addEventListener("input", (event) => {
    if (!event.target.matches?.("#assignmentSearch")) return;
    const hasQuery = Boolean(event.target.value.trim());
    if (hasQuery && !searchRecorded) log("search");
    searchRecorded = hasQuery;
  });
  document.addEventListener("submit", (event) => {
    if (event.target.id === "customAssignmentForm") log("custom_todo");
    if (event.target.id === "dueEditForm") log("deadline_edit");
  });
}
