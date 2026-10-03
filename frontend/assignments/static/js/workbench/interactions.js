export function register(ctx) {
  ctx.setView = function setView(mode, options = {}) {
    const nextMode = ["due", "course", "calendar"].includes(mode) ? mode : "due";
    ctx.currentViewMode = nextMode;
    const views = {
      due: [ctx.viewDue, ctx.viewDueBtn],
      course: [ctx.viewCourse, ctx.viewCourseBtn],
      calendar: [document.getElementById("viewCalendar"), document.getElementById("viewCalendarBtn")],
    };
    Object.entries(views).forEach(([key, [view, button]]) => {
      view?.classList.toggle("hidden", key !== nextMode);
      button?.classList.toggle("active", key === nextMode);
      button?.setAttribute("aria-pressed", String(key === nextMode));
    });
    if (!options.skipPersist) {
      ctx.persistPreferences({ viewMode: nextMode });
    }
    ctx.applyFilters();
  };
}

export function initialize(ctx) {
  ctx.hydrateLocalAssignments();

  ctx.sortFlatTableAsc();

  ctx.setView(ctx.currentViewMode, { skipPersist: true });

  if (ctx.statusFilterGroup) {
    ctx.statusFilterGroup
      .querySelectorAll("[data-status-filter]")
      .forEach((btn) => {
        btn.addEventListener("click", () => {
          const nextFilter =
            btn.getAttribute("data-status-filter") || "pending";
          let nextFilters = ctx.currentStatusFilters.slice();
          if (nextFilter === "all") {
            nextFilters = ctx.isAllStatusFiltersSelected()
              ? []
              : ctx.STATUS_FILTER_KEYS.slice();
          } else if (ctx.STATUS_FILTER_LABELS[nextFilter]) {
            if (nextFilters.includes(nextFilter)) {
              nextFilters = nextFilters.filter((item) => item !== nextFilter);
            } else {
              nextFilters.push(nextFilter);
            }
          }
          ctx.currentStatusFilters = ctx.normalizeStatusFilters(nextFilters);
          ctx.USER_PREFERENCES.status_filter = ctx.currentStatusFilters.slice();
          ctx.syncStatusFilterButtons();
          ctx.updateFilterSummary();
          ctx.applyFilters();
          ctx.persistPreferences({ statusFilters: ctx.currentStatusFilters });
        });
      });
  }

  if (ctx.courseFilter) {
    ctx.courseFilter.addEventListener("change", () => {
      ctx.currentCourseFilter = ctx.courseFilter.value;
      ctx.applyFilters();
    });
  }

  document.addEventListener("change", (evt) => {
    const semesterInput = evt.target.closest("[data-semester-filter]");
    if (!semesterInput || ctx.IS_READONLY_VIEW) return;
    const nextSemesters = ctx.readCheckedSemesterFilters();
    if (!nextSemesters.length) {
      semesterInput.checked = true;
      ctx.showToast("請至少保留一個學期。", "warning");
      return;
    }
    ctx.currentSemesterFilters = nextSemesters;
    ctx.USER_PREFERENCES.semester_filter = nextSemesters.slice();
    ctx.syncSemesterSelectorSummary(semesterInput, { collapse: true });
    ctx.applyFilters();
    ctx.updateFilterSummary();
    ctx.persistPreferences({ semesterFilters: nextSemesters });
  });

  if (ctx.viewCourseBtn) {
    ctx.viewCourseBtn.addEventListener("click", () => ctx.setView("course"));
  }

  if (ctx.viewDueBtn) {
    ctx.viewDueBtn.addEventListener("click", () => ctx.setView("due"));
  }

  document.getElementById("viewCalendarBtn")?.addEventListener("click", () =>
    ctx.setView("calendar"),
  );

  ctx.applyFilters();

  ctx.updateFilterSummary();

  document.addEventListener("click", (evt) => {
    const semesterSelector = document.getElementById("semesterSelector");
    if (semesterSelector?.open && !semesterSelector.contains(evt.target)) {
      semesterSelector.open = false;
    }
    const editDueBtn = evt.target.closest("[data-edit-due]");
    if (editDueBtn) {
      evt.preventDefault();
      ctx.openDueEditModal(editDueBtn.getAttribute("data-edit-due"));
      return;
    }
    const deleteCustomBtn = evt.target.closest("[data-delete-custom]");
    if (deleteCustomBtn) {
      evt.preventDefault();
      ctx.deleteCustomAssignment(
        deleteCustomBtn.getAttribute("data-delete-custom"),
      );
      ctx.showToast("已刪除自訂代辦。", "success");
      return;
    }
    const ignoreBtn = evt.target.closest("[data-ignore-assignment]");
    if (ignoreBtn) {
      evt.preventDefault();
      ctx.ignoreAssignment(
        ignoreBtn.getAttribute("data-ignore-assignment"),
      );
      return;
    }
    const restoreBtn = evt.target.closest("[data-restore-assignment]");
    if (restoreBtn) {
      evt.preventDefault();
      ctx.restoreIgnoredAssignment(
        restoreBtn.getAttribute("data-restore-assignment"),
      );
    }
  });

  if (ctx.restoreIgnoredAssignmentsAll) {
    ctx.restoreIgnoredAssignmentsAll.addEventListener("click", () => {
      ctx.restoreAllIgnoredAssignments();
    });
  }

  if (ctx.dueEditForm) {
    ctx.dueEditForm.addEventListener("submit", (evt) => {
      evt.preventDefault();
      const uid = ctx.dueEditUid ? ctx.dueEditUid.value : "";
      const dueTs = ctx.parseDatetimeLocalToTs(
        ctx.dueEditInput ? ctx.dueEditInput.value : "",
      );
      if (!uid || !dueTs) {
        ctx.showToast("請輸入有效的繳交期限。", "warning");
        return;
      }
      ctx.applyDueOverride(uid, dueTs);
      const customItem = ctx.customAssignments.find((item) => item.uid === uid);
      if (customItem) {
        customItem.due_ts = dueTs;
        ctx.saveCustomAssignments();
      }
      ctx.closeModalRoot(ctx.dueEditModal);
      ctx.showToast("繳交期限已更新。", "success");
    });
  }

  if (ctx.dueResetBtn) {
    ctx.dueResetBtn.addEventListener("click", () => {
      const uid = ctx.dueEditUid ? ctx.dueEditUid.value : "";
      if (!uid) return;
      if (uid.startsWith("custom|")) {
        ctx.showToast("自訂代辦沒有 E3 原始期限可還原。", "info");
        return;
      }
      ctx.resetDueOverride(uid);
      ctx.closeModalRoot(ctx.dueEditModal);
      ctx.showToast("已還原原本的繳交期限。", "success");
    });
  }

  document.querySelectorAll("[data-close-due-edit]").forEach((button) => {
    button.addEventListener("click", () =>
      ctx.closeModalRoot(ctx.dueEditModal),
    );
  });

  if (ctx.dueEditModal) {
    ctx.dueEditModal.addEventListener("click", (evt) => {
      if (evt.target.classList.contains("modal-backdrop")) {
        ctx.closeModalRoot(ctx.dueEditModal);
      }
    });
  }

  if (ctx.addCustomAssignmentBtn) {
    ctx.addCustomAssignmentBtn.addEventListener("click", () => {
      if (ctx.customAssignmentForm) ctx.customAssignmentForm.reset();
      if (ctx.customCourseInput) ctx.customCourseInput.value = "自訂代辦";
      ctx.openModalRoot(ctx.customAssignmentModal);
    });
  }

  if (ctx.customAssignmentForm) {
    ctx.customAssignmentForm.addEventListener("submit", (evt) => {
      evt.preventDefault();
      const dueTs = ctx.parseDatetimeLocalToTs(
        ctx.customDueInput ? ctx.customDueInput.value : "",
      );
      const title = ctx.customTitleInput
        ? ctx.customTitleInput.value.trim()
        : "";
      if (!title || !dueTs) {
        ctx.showToast("請輸入作業名稱與有效期限。", "warning");
        return;
      }
      ctx.addCustomAssignment(
        ctx.customCourseInput ? ctx.customCourseInput.value : "",
        title,
        dueTs,
      );
      ctx.closeModalRoot(ctx.customAssignmentModal);
      ctx.showToast("已新增自訂代辦。", "success");
    });
  }

  document
    .querySelectorAll("[data-close-custom-assignment]")
    .forEach((button) => {
      button.addEventListener("click", () =>
        ctx.closeModalRoot(ctx.customAssignmentModal),
      );
    });

  if (ctx.customAssignmentModal) {
    ctx.customAssignmentModal.addEventListener("click", (evt) => {
      if (evt.target.classList.contains("modal-backdrop")) {
        ctx.closeModalRoot(ctx.customAssignmentModal);
      }
    });
  }

  document.addEventListener("keydown", (evt) => {
    if (evt.key !== "Escape") return;
    const semesterSelector = document.getElementById("semesterSelector");
    if (semesterSelector?.open) {
      semesterSelector.open = false;
      semesterSelector.querySelector("summary").focus();
    }
    if (ctx.dueEditModal && !ctx.dueEditModal.classList.contains("hidden")) {
      ctx.closeModalRoot(ctx.dueEditModal);
    }
    if (
      ctx.customAssignmentModal &&
      !ctx.customAssignmentModal.classList.contains("hidden")
    ) {
      ctx.closeModalRoot(ctx.customAssignmentModal);
    }
  });

  setInterval(ctx.refreshRemainingTimes, 60000);
}
