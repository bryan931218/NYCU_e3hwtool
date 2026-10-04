export function register(ctx) {
  ctx.getIgnoredAssignmentUidSet = function getIgnoredAssignmentUidSet() {
    return new Set(
      (ctx.USER_PREFERENCES.ignored_assignment_uids || [])
        .map((item) => String(item || "").trim())
        .filter(Boolean),
    );
  };

  ctx.setIgnoredAssignmentUids = function setIgnoredAssignmentUids(nextUids) {
    ctx.USER_PREFERENCES.ignored_assignment_uids = Array.from(
      new Set(
        (Array.isArray(nextUids) ? nextUids : [])
          .map((item) => String(item || "").trim())
          .filter(Boolean),
      ),
    );
  };

  ctx.renderIgnoredAssignmentsList = function renderIgnoredAssignmentsList() {
    if (!ctx.ignoredAssignmentsList) return;
    const ignoredSet = ctx.getIgnoredAssignmentUidSet();
    const rows = Array.from(
      document.querySelectorAll('tr[data-uid]'),
    );
    const items = [];
    const seen = new Set();
    rows.forEach((row) => {
      const uid = row.dataset.uid || "";
      if (!ignoredSet.has(uid) || seen.has(uid)) return;
      seen.add(uid);
      items.push({
        uid,
        label:
          `${row.dataset.course || ""}｜${row.dataset.title || ""}`.replace(
            /^｜/,
            "",
          ),
      });
    });
    ctx.ignoredAssignmentsList.innerHTML = "";
    if (ctx.restoreIgnoredAssignmentsAll) {
      ctx.restoreIgnoredAssignmentsAll.hidden = items.length === 0;
    }
    if (!items.length) {
      ctx.ignoredAssignmentsList.innerHTML =
        '<div class="muted" style="font-size:12px">目前沒有已忽略的作業。</div>';
      return;
    }
    items.forEach((item) => {
      const chip = document.createElement("div");
      chip.className = "ignored-chip";
      chip.innerHTML = `
            <span class="ignored-chip-label" title="${ctx.escapeHtml(item.label)}">${ctx.escapeHtml(item.label)}</span>
            <button type="button" class="ignored-chip-btn" data-restore-assignment="${ctx.escapeHtml(item.uid)}">復原</button>
        `;
      ctx.ignoredAssignmentsList.appendChild(chip);
    });
  };

  ctx.updateFilterSummary = function updateFilterSummary() {
    if (!ctx.filterActiveCount) return;
    ctx.filterActiveCount.textContent = String(
      ctx.currentStatusFilters.length + ctx.currentSemesterFilters.length,
    );
  };

  ctx.rowMatchesWorkspaceQuery = function rowMatchesWorkspaceQuery(row) {
    const course = String(row.dataset.course || "").trim();
    const courseKey =
      String(row.dataset.semester || "") === "custom" ? "custom" : course;
    if (ctx.currentCourseFilter && courseKey !== ctx.currentCourseFilter)
      return false;
    if (!ctx.currentAssignmentQuery) return true;
    const searchable = `${row.dataset.title || ""} ${course}`.toLocaleLowerCase(
      "zh-Hant",
    );
    return searchable.includes(ctx.currentAssignmentQuery);
  };

  ctx.updateDashboardOverview = function updateDashboardOverview() {
    const ignoredSet = ctx.getIgnoredAssignmentUidSet();
    const selectedSemesters = new Set(ctx.currentSemesterFilters);
    const allRows = Array.from(
      document.querySelectorAll("#flatTable tbody tr[data-uid]"),
    );
    let pending = 0;
    let overdue = 0;
    allRows.forEach((row) => {
      const semesterVisible =
        row.dataset.semester === "custom" ||
        selectedSemesters.has(row.dataset.semester || "other");
      if (!semesterVisible || ignoredSet.has(row.dataset.uid || "")) return;
      const status = row.dataset.primaryStatus || "pending";
      if (status === "overdue") overdue += 1;
      if (status === "pending" || status === "overdue") pending += 1;
    });
    if (ctx.pendingSummaryCount)
      ctx.pendingSummaryCount.textContent = String(pending);
    if (ctx.overdueSummaryCount)
      ctx.overdueSummaryCount.textContent = String(overdue);

    const visibleRows = allRows.filter(
      (row) => !row.classList.contains("hidden"),
    );
    document
      .getElementById("workspaceEmpty")
      ?.classList.toggle("hidden", visibleRows.length > 0 || ctx.currentViewMode === "calendar");
  };

  ctx.updateCounts = function updateCounts() {
    const currentView =
      ctx.currentViewMode === "course" ? ctx.viewCourse : ctx.viewDue;
    if (!currentView) {
      if (ctx.totalCountEl) ctx.totalCountEl.textContent = "0";
      if (ctx.ignoredCountEl)
        ctx.ignoredCountEl.textContent = String(
          ctx.getIgnoredAssignmentUidSet().size,
        );
      return;
    }

    const visibleRows = currentView.querySelectorAll(
      "tbody tr[data-uid]:not(.hidden)",
    );
    const totalVisible = visibleRows.length;
    if (ctx.totalCountEl) ctx.totalCountEl.textContent = totalVisible;
    if (ctx.ignoredCountEl)
      ctx.ignoredCountEl.textContent = String(
        ctx.getIgnoredAssignmentUidSet().size,
      );
    ctx.updateDashboardOverview();
  };

  ctx.applyFilters = function applyFilters() {
    ctx.syncCourseColors?.();
    ctx.syncCourseFilter?.();
    const ignoredSet = ctx.getIgnoredAssignmentUidSet();
    const selectedStatuses = new Set(ctx.currentStatusFilters);
    const selectedSemesters = new Set(ctx.currentSemesterFilters);

    document.querySelectorAll("#flatTable tbody tr[data-uid]").forEach((tr) => {
      const primaryStatus = tr.dataset.primaryStatus || "pending";
      const ignored = ignoredSet.has(tr.dataset.uid || "");
      if (tr.dataset.ignored !== (ignored ? "1" : "0")) tr.dataset.ignored = ignored ? "1" : "0";
      const semesterVisible =
        tr.dataset.semester === "custom" ||
        selectedSemesters.has(tr.dataset.semester || "other");
      let visible =
        semesterVisible &&
        selectedStatuses.has(primaryStatus) &&
        ctx.rowMatchesWorkspaceQuery(tr);
      if (ignored) visible = false;
      tr.classList.toggle("hidden", !visible);
    });

    document
      .querySelectorAll(".courseTable tbody tr[data-uid]")
      .forEach((tr) => {
        const primaryStatus = tr.dataset.primaryStatus || "pending";
        const ignored = ignoredSet.has(tr.dataset.uid || "");
        if (tr.dataset.ignored !== (ignored ? "1" : "0")) tr.dataset.ignored = ignored ? "1" : "0";
        const semesterVisible =
          tr.dataset.semester === "custom" ||
          selectedSemesters.has(tr.dataset.semester || "other");
        let visible =
          semesterVisible &&
          selectedStatuses.has(primaryStatus) &&
          ctx.rowMatchesWorkspaceQuery(tr);
        if (ignored) visible = false;
        tr.classList.toggle("hidden", !visible);
      });

    ctx.sortAssignmentTable(document.querySelector("#flatTable tbody"));
    document
      .querySelectorAll(".courseTable tbody")
      .forEach((tbody) => ctx.sortAssignmentTable(tbody));
    document.querySelectorAll("#viewCourse .course-card").forEach((card) => {
      const courseSemester = card.dataset.semester || "other";
      const semesterVisible =
        courseSemester === "custom" || selectedSemesters.has(courseSemester);
      const visibleRows = card.querySelectorAll(
        "tbody tr[data-uid]:not(.hidden)",
      );
      const allRows = card.querySelectorAll("tbody tr[data-uid]");
      const emptyState = card.querySelector("[data-course-empty]");
      const courseKey =
        courseSemester === "custom"
          ? "custom"
          : String(card.dataset.courseTitle || "");
      const courseVisible =
        !ctx.currentCourseFilter || ctx.currentCourseFilter === courseKey;
      card.classList.toggle("hidden", !semesterVisible || !courseVisible);
      if (emptyState) {
        emptyState.textContent = allRows.length
          ? "這門課目前沒有符合篩選條件的作業。"
          : "這門課目前沒有同步到作業。";
        emptyState.classList.toggle(
          "hidden",
          !semesterVisible || visibleRows.length > 0,
        );
      }
    });

    ctx.updateCounts();
    ctx.renderIgnoredAssignmentsList();
    ctx.syncDeadlineCalendar?.();
    ctx.refreshCourseMessageUnread?.();
  };

  ctx.persistPreferences = function persistPreferences(partial = {}) {
    if (ctx.IS_READONLY_VIEW) return;
    if (!ctx.PREFERENCES_ENDPOINT) return;
    const payload = {};
    if (Object.prototype.hasOwnProperty.call(partial, "viewMode")) {
      const mode = ["due", "course", "calendar"].includes(partial.viewMode)
        ? partial.viewMode : "due";
      ctx.USER_PREFERENCES.view_mode = mode;
      payload.viewMode = mode;
    }
    if (
      Object.prototype.hasOwnProperty.call(partial, "statusFilters") ||
      Object.prototype.hasOwnProperty.call(partial, "statusFilter")
    ) {
      const nextFilters = ctx.normalizeStatusFilters(
        Object.prototype.hasOwnProperty.call(partial, "statusFilters")
          ? partial.statusFilters
          : partial.statusFilter,
      );
      ctx.USER_PREFERENCES.status_filter = nextFilters.slice();
      ctx.currentStatusFilters = nextFilters.slice();
      payload.statusFilters = nextFilters.slice();
    }
    if (
      Object.prototype.hasOwnProperty.call(partial, "semesterFilters") ||
      Object.prototype.hasOwnProperty.call(partial, "semesterFilter")
    ) {
      const nextSemesters = ctx.normalizeSemesterFilters(
        Object.prototype.hasOwnProperty.call(partial, "semesterFilters")
          ? partial.semesterFilters
          : partial.semesterFilter,
      );
      if (nextSemesters.length) {
        ctx.USER_PREFERENCES.semester_filter = nextSemesters.slice();
        ctx.currentSemesterFilters = nextSemesters.slice();
        payload.semesterFilters = nextSemesters.slice();
      }
    }
    if (Object.prototype.hasOwnProperty.call(partial, "ignoredAssignmentUids")) {
      ctx.setIgnoredAssignmentUids(partial.ignoredAssignmentUids);
      payload.ignoredAssignmentUids =
        ctx.USER_PREFERENCES.ignored_assignment_uids.slice();
    }
    if (!Object.keys(payload).length) return;
    fetch(ctx.PREFERENCES_ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }).catch(() => {});
  };

  ctx.applyServerPreferenceState = function applyServerPreferenceState(
    nextPrefs,
  ) {
    if (!nextPrefs || typeof nextPrefs !== "object") return;
    const incoming = {
      view_mode: ["due", "course", "calendar"].includes(nextPrefs.view_mode)
        ? nextPrefs.view_mode : "due",
      status_filter: ctx.normalizeStatusFilters(
        nextPrefs.status_filter ?? nextPrefs.statusFilters,
      ),
      semester_filter: ctx.normalizeSemesterFilters(
        nextPrefs.semester_filter ?? nextPrefs.semesterFilters,
      ),
      include_ignored_overdue: !!nextPrefs.include_ignored_overdue,
      ignored_assignment_uids: nextPrefs.ignored_assignment_uids
        ?? nextPrefs.ignored_overdue_uids ?? [],
    };
    incoming.ignored_assignment_uids = [...new Set((Array.isArray(incoming.ignored_assignment_uids)
      ? incoming.ignored_assignment_uids : []).map(value => String(value || "").trim()).filter(Boolean))];
    const same = (a, b) => a.length === b.length && a.every((value, index) => value === b[index]);
    const viewChanged = incoming.view_mode !== ctx.currentViewMode;
    if (!viewChanged && same(incoming.status_filter, ctx.currentStatusFilters)
      && (!incoming.semester_filter.length || same(incoming.semester_filter, ctx.currentSemesterFilters))
      && same(incoming.ignored_assignment_uids, ctx.USER_PREFERENCES.ignored_assignment_uids || [])) return false;
    ctx.USER_PREFERENCES.view_mode = incoming.view_mode;
    ctx.USER_PREFERENCES.status_filter = incoming.status_filter.slice();
    ctx.currentStatusFilters = incoming.status_filter.slice();
    if (incoming.semester_filter.length) {
      ctx.USER_PREFERENCES.semester_filter = incoming.semester_filter.slice();
      ctx.currentSemesterFilters = incoming.semester_filter.slice();
      document.querySelectorAll("[data-semester-filter]").forEach((input) => {
        input.checked = ctx.currentSemesterFilters.includes(input.value);
      });
      ctx.syncSemesterSelectorSummary(
        document.querySelector("[data-semester-filter]:checked"),
      );
    }
    ctx.setIgnoredAssignmentUids(incoming.ignored_assignment_uids);
    ctx.syncStatusFilterButtons();
    ctx.updateFilterSummary();
    if (viewChanged) ctx.setView(incoming.view_mode, { skipPersist: true, skipApply: true });
    ctx.applyFilters();
    return true;
  };

  ctx.ignoreAssignment = function ignoreAssignment(uid) {
    if (!uid || ctx.IS_READONLY_VIEW) return;
    const next = ctx.USER_PREFERENCES.ignored_assignment_uids.slice();
    next.push(uid);
    ctx.persistPreferences({ ignoredAssignmentUids: next });
    ctx.applyFilters();
  };

  ctx.restoreIgnoredAssignment =
    function restoreIgnoredAssignment(uid) {
      if (!uid || ctx.IS_READONLY_VIEW) return;
      const next = ctx.USER_PREFERENCES.ignored_assignment_uids.filter(
        (item) => item !== uid,
      );
      ctx.persistPreferences({ ignoredAssignmentUids: next });
      ctx.applyFilters();
    };

  ctx.restoreAllIgnoredAssignments =
    function restoreAllIgnoredAssignments() {
      if (ctx.IS_READONLY_VIEW) return;
      ctx.persistPreferences({ ignoredAssignmentUids: [] });
      ctx.applyFilters();
    };
}

export function initialize(ctx) {}
