export function register(ctx) {
  ctx.sortFlatTableAsc = function sortFlatTableAsc() {
    const tbody = document.querySelector("#flatTable tbody");
    if (!tbody) return;
    const rows = Array.from(tbody.querySelectorAll("tr[data-uid]"));
    rows.sort((a, b) => {
      const A = parseInt(a.dataset.dueTs || "9999999999", 10);
      const B = parseInt(b.dataset.dueTs || "9999999999", 10);
      return A - B;
    });
    rows.forEach((r) => tbody.appendChild(r));
  };

  ctx.getStatusPriority = function getStatusPriority(row) {
    const status = row.dataset.primaryStatus || "pending";
    if (status === "pending" || status === "overdue") return 0;
    if (status === "graded") return 1;
    if (status === "completed") return 2;
    return 3;
  };

  ctx.sortAssignmentTable = function sortAssignmentTable(tbody) {
    if (!tbody) return;
    const rows = Array.from(tbody.querySelectorAll("tr[data-uid]"));
    rows.sort((a, b) => {
      if (ctx.currentStatusFilters.length > 1) {
        const priorityA = ctx.getStatusPriority(a);
        const priorityB = ctx.getStatusPriority(b);
        if (priorityA !== priorityB) {
          return priorityA - priorityB;
        }
      }
      const dueA = parseInt(a.dataset.dueTs || "9999999999", 10);
      const dueB = parseInt(b.dataset.dueTs || "9999999999", 10);
      return dueA - dueB;
    });
    rows.forEach((row) => tbody.appendChild(row));
  };

  ctx.normalizeSemesterFilters = function normalizeSemesterFilters(value) {
    if (!Array.isArray(value)) return [];
    const next = [];
    value.forEach((item) => {
      const key = String(item || "")
        .trim()
        .toLowerCase();
      if (!/^\d{2,3}-(?:1|2|summer)$/.test(key) && key !== "other") return;
      if (!next.includes(key)) next.push(key);
    });
    return next.slice(0, 1);
  };

  ctx.readCheckedSemesterFilters = function readCheckedSemesterFilters() {
    return ctx.normalizeSemesterFilters(
      Array.from(
        document.querySelectorAll("[data-semester-filter]:checked"),
      ).map((input) => input.value),
    );
  };

  ctx.syncSemesterSelectorSummary = function syncSemesterSelectorSummary(
    input,
    { collapse = false } = {},
  ) {
    if (!input) return;
    const label = document.getElementById("semesterSelectedLabel");
    const count = document.getElementById("semesterSelectedCount");
    if (label) label.textContent = input.dataset.semesterLabel || input.value;
    if (count) count.textContent = `${input.dataset.courseCount || 0} 門課`;
    if (collapse) {
      const selector = document.getElementById("semesterSelector");
      if (selector) selector.open = false;
    }
  };

  ctx.normalizeStatusFilters = function normalizeStatusFilters(value) {
    if (value == null) {
      return ctx.DEFAULT_STATUS_FILTERS.slice();
    }
    let raw = value;
    if (typeof raw === "string") {
      try {
        const parsed = JSON.parse(raw);
        raw = Array.isArray(parsed) ? parsed : [raw];
      } catch (err) {
        raw = [raw];
      }
    }
    if (!Array.isArray(raw)) {
      return ctx.DEFAULT_STATUS_FILTERS.slice();
    }
    const next = [];
    raw.forEach((item) => {
      const key = String(item || "")
        .trim()
        .toLowerCase();
      if (key === "all") {
        ctx.STATUS_FILTER_KEYS.forEach((statusKey) => {
          if (!next.includes(statusKey)) next.push(statusKey);
        });
        return;
      }
      if (ctx.STATUS_FILTER_LABELS[key] && !next.includes(key)) {
        next.push(key);
      }
    });
    return next;
  };

  ctx.isAllStatusFiltersSelected = function isAllStatusFiltersSelected(
    filters = ctx.currentStatusFilters,
  ) {
    return ctx.STATUS_FILTER_KEYS.every((key) => filters.includes(key));
  };

  ctx.formatStatusFilterSummary = function formatStatusFilterSummary(
    filters = ctx.currentStatusFilters,
  ) {
    if (ctx.isAllStatusFiltersSelected(filters)) {
      return "全部";
    }
    return (
      filters
        .map((key) => ctx.STATUS_FILTER_LABELS[key])
        .filter(Boolean)
        .join("、") || "待處理"
    );
  };

  ctx.syncStatusFilterButtons = function syncStatusFilterButtons() {
    if (!ctx.statusFilterGroup) return;
    const selected = new Set(ctx.currentStatusFilters);
    ctx.statusFilterGroup
      .querySelectorAll("[data-status-filter]")
      .forEach((btn) => {
        const key = btn.getAttribute("data-status-filter") || "";
        btn.classList.toggle(
          "active",
          key === "all" ? ctx.isAllStatusFiltersSelected() : selected.has(key),
        );
      });
  };
}

export function initialize(ctx) {
  ctx.STATUS_FILTER_LABELS = {
    pending: "待處理",
    completed: "已完成",
    graded: "已評分",
    overdue: "逾期未交",
  };

  ctx.STATUS_FILTER_KEYS = ["pending", "completed", "graded", "overdue"];

  ctx.DEFAULT_STATUS_FILTERS = ["pending"];

  ctx.currentViewMode = ["due", "course", "calendar"].includes(
    ctx.USER_PREFERENCES?.view_mode,
  ) ? ctx.USER_PREFERENCES.view_mode : "due";

  ctx.currentStatusFilters = ctx.normalizeStatusFilters(
    ctx.USER_PREFERENCES && ctx.USER_PREFERENCES.status_filter,
  );

  ctx.USER_PREFERENCES.status_filter = ctx.currentStatusFilters.slice();

  ctx.currentSemesterFilters = ctx.readCheckedSemesterFilters();

  if (!ctx.currentSemesterFilters.length)
    ctx.currentSemesterFilters = ctx.normalizeSemesterFilters(
      ctx.USER_PREFERENCES && ctx.USER_PREFERENCES.semester_filter,
    );

  ctx.USER_PREFERENCES.semester_filter = ctx.currentSemesterFilters.slice();

  ctx.USER_PREFERENCES.include_ignored_overdue =
    !!ctx.USER_PREFERENCES.include_ignored_overdue;
}
