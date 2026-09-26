export function register(ctx) {
  ctx.loadJsonMap = function loadJsonMap(key) {
    try {
      const parsed = JSON.parse(localStorage.getItem(key) || "{}");
      return parsed && typeof parsed === "object" && !Array.isArray(parsed)
        ? parsed
        : {};
    } catch (err) {
      return {};
    }
  };

  ctx.loadJsonArray = function loadJsonArray(key) {
    try {
      const parsed = JSON.parse(localStorage.getItem(key) || "[]");
      return Array.isArray(parsed) ? parsed : [];
    } catch (err) {
      return [];
    }
  };

  ctx.saveDueOverrides = function saveDueOverrides() {
    try {
      localStorage.setItem(
        ctx.DUE_OVERRIDES_KEY,
        JSON.stringify(ctx.dueOverrides),
      );
    } catch (err) {}
  };

  ctx.saveCustomAssignments = function saveCustomAssignments() {
    try {
      localStorage.setItem(
        ctx.CUSTOM_ASSIGNMENTS_KEY,
        JSON.stringify(ctx.customAssignments),
      );
    } catch (err) {}
  };

  ctx.escapeHtml = function escapeHtml(value) {
    return String(value ?? "").replace(
      /[&<>"']/g,
      (char) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[char],
    );
  };

  ctx.cssEscapeValue = function cssEscapeValue(value) {
    if (window.CSS && typeof window.CSS.escape === "function") {
      return window.CSS.escape(String(value));
    }
    return String(value).replace(/["\\]/g, "\\$&");
  };

  ctx.formatDueDisplayFromTs = function formatDueDisplayFromTs(ts) {
    if (!ts) return "";
    const date = new Date(Number(ts) * 1000);
    if (Number.isNaN(date.getTime())) return "";
    const pad = (num) => String(num).padStart(2, "0");
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
  };

  ctx.formatDatetimeLocalFromTs = function formatDatetimeLocalFromTs(ts) {
    if (!ts) return "";
    return ctx.formatDueDisplayFromTs(ts).replace(" ", "T");
  };

  ctx.parseDatetimeLocalToTs = function parseDatetimeLocalToTs(value) {
    if (!value) return null;
    const parsed = new Date(value);
    const ms = parsed.getTime();
    if (Number.isNaN(ms)) return null;
    return Math.floor(ms / 1000);
  };

  ctx.formatDuration = function formatDuration(seconds) {
    const total = Math.max(0, Math.floor(Number(seconds) || 0));
    const days = Math.floor(total / 86400);
    const hours = Math.floor((total % 86400) / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    if (days > 0) return `${days}天 ${hours}小時`;
    if (hours > 0) return `${hours}小時 ${minutes}分鐘`;
    return `${Math.max(1, minutes)}分鐘`;
  };

  ctx.computePrimaryStatus = function computePrimaryStatus(row, dueTs) {
    if (row.dataset.graded === "1") return "graded";
    if (row.dataset.completed === "1") return "completed";
    if (dueTs && dueTs < Math.floor(Date.now() / 1000)) return "overdue";
    return "pending";
  };

  ctx.updateRemainingForRow = function updateRemainingForRow(row) {
    if (!row) return;
    const dueTs = Number(row.dataset.dueTs || "0");
    const hasDue =
      row.dataset.hasDue === "1" &&
      Number.isFinite(dueTs) &&
      dueTs < 9999999999;
    const status =
      row.dataset.primaryStatus ||
      ctx.computePrimaryStatus(row, hasDue ? dueTs : null);
    const remainingEls = row.querySelectorAll(".remaining-text");
    if (!remainingEls.length) return;
    let cls = "neutral";
    let label = "無截止資訊";
    if (status === "graded") {
      cls = "done";
      label = "已評分";
    } else if (status === "completed") {
      cls = "done";
      label = "已完成";
    } else if (hasDue) {
      const diff = dueTs - Math.floor(Date.now() / 1000);
      if (diff < 0) {
        cls = "overdue";
        label = `已逾期 ${ctx.formatDuration(-diff)}`;
      } else if (diff <= 86400) {
        cls = "warning";
        label = `剩下 ${ctx.formatDuration(diff)}`;
      } else {
        cls = "normal";
        label = `剩下 ${ctx.formatDuration(diff)}`;
      }
    }
    remainingEls.forEach((el) => {
      el.className = `remaining-text ${cls}`;
      el.textContent = label;
    });
  };

  ctx.renderStatusBadges = function renderStatusBadges(row) {
    const wrap = row.querySelector(".row-status-badges");
    if (!wrap) return;
    const status = row.dataset.primaryStatus || "pending";
    wrap.innerHTML = "";
    if (status === "completed") {
      wrap.innerHTML = '<span class="badge done">已完成</span>';
    } else if (status === "graded") {
      wrap.innerHTML = '<span class="badge graded">已評分</span>';
    } else if (status === "overdue") {
      wrap.innerHTML = '<span class="badge overdue">逾期</span>';
    } else {
      wrap.innerHTML = '<span class="badge nodue">未完成</span>';
    }
    const stack = row.querySelector(".row-status-stack");
    const existingIgnore = row.querySelector("[data-ignore-overdue]");
    if (existingIgnore) existingIgnore.remove();
    if (
      stack &&
      status === "overdue" &&
      !ctx.IS_READONLY_VIEW &&
      !(row.dataset.uid || "").startsWith("custom|")
    ) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "row-ignore-btn";
      button.dataset.ignoreOverdue = row.dataset.uid || "";
      button.textContent = "忽略";
      stack.appendChild(button);
    }
  };

  ctx.setRowDue = function setRowDue(
    row,
    dueTs,
    dueText,
    sourceLabel = "自訂期限",
  ) {
    if (!row) return;
    const normalizedTs = Number(dueTs);
    const hasDue = Number.isFinite(normalizedTs) && normalizedTs > 0;
    row.dataset.dueTs = hasDue ? String(normalizedTs) : "9999999999";
    row.dataset.hasDue = hasDue ? "1" : "0";
    row.dataset.due = hasDue ? dueText : "無截止";
    row.dataset.overdue =
      hasDue && normalizedTs < Math.floor(Date.now() / 1000) ? "1" : "0";
    row.dataset.primaryStatus = ctx.computePrimaryStatus(
      row,
      hasDue ? normalizedTs : null,
    );
    const dueBox = row.querySelector(".assignment-due");
    if (dueBox) {
      dueBox.classList.toggle("empty", !hasDue);
      const main = dueBox.querySelector(".assignment-due-main");
      const sub = dueBox.querySelector(".assignment-due-sub");
      if (main) main.textContent = hasDue ? dueText : "無截止";
      if (sub) sub.textContent = hasDue ? sourceLabel : "截止資訊同步中";
    }
    ctx.updateRemainingForRow(row);
    ctx.renderStatusBadges(row);
  };

  ctx.applyDueOverrides = function applyDueOverrides() {
    document.querySelectorAll("tr[data-uid]").forEach((row) => {
      const override = ctx.dueOverrides[row.dataset.uid || ""];
      if (override && override.due_ts) {
        ctx.setRowDue(
          row,
          override.due_ts,
          override.due_at || ctx.formatDueDisplayFromTs(override.due_ts),
          "已手動修改期限",
        );
      } else if (!(row.dataset.uid || "").startsWith("custom|")) {
        const originalTs = row.dataset.originalDueTs
          ? Number(row.dataset.originalDueTs)
          : null;
        ctx.setRowDue(
          row,
          originalTs,
          row.dataset.originalDue || "",
          row.dataset.originalDue ? "E3 同步期限" : "截止資訊同步中",
        );
      }
    });
  };

  ctx.refreshRemainingTimes = function refreshRemainingTimes() {
    document.querySelectorAll("tr[data-uid]").forEach((row) => {
      const dueTs = Number(row.dataset.dueTs || "0");
      if (row.dataset.completed !== "1" && row.dataset.graded !== "1") {
        row.dataset.overdue =
          row.dataset.hasDue === "1" && dueTs < Math.floor(Date.now() / 1000)
            ? "1"
            : "0";
        row.dataset.primaryStatus = ctx.computePrimaryStatus(
          row,
          row.dataset.hasDue === "1" ? dueTs : null,
        );
        ctx.renderStatusBadges(row);
      }
      ctx.updateRemainingForRow(row);
    });
    ctx.applyFilters();
  };

  ctx.openModalRoot = function openModalRoot(modal) {
    if (!modal) return;
    modal.classList.remove("hidden");
    modal.style.display = "block";
    const backdrop = modal.querySelector(".modal-backdrop");
    const panel = modal.querySelector(".modal-panel");
    backdrop && backdrop.classList.add("show");
    panel && panel.classList.add("show");
  };

  ctx.closeModalRoot = function closeModalRoot(modal) {
    if (!modal) return;
    const backdrop = modal.querySelector(".modal-backdrop");
    const panel = modal.querySelector(".modal-panel");
    backdrop && backdrop.classList.remove("show");
    panel && panel.classList.remove("show");
    setTimeout(() => {
      modal.classList.add("hidden");
      modal.style.display = "none";
    }, 150);
  };

  ctx.openDueEditModal = function openDueEditModal(uid) {
    if (ctx.IS_READONLY_VIEW || !uid || !ctx.dueEditModal || !ctx.dueEditForm)
      return;
    const row = document.querySelector(
      `tr[data-uid="${ctx.cssEscapeValue(uid)}"]`,
    );
    if (!row) return;
    const override = ctx.dueOverrides[uid];
    const dueTs =
      override && override.due_ts
        ? Number(override.due_ts)
        : Number(row.dataset.dueTs || row.dataset.originalDueTs || "0");
    ctx.dueEditUid.value = uid;
    ctx.dueEditInput.value =
      dueTs && dueTs < 9999999999 ? ctx.formatDatetimeLocalFromTs(dueTs) : "";
    if (ctx.dueEditTitle)
      ctx.dueEditTitle.textContent =
        `${row.dataset.course || ""}｜${row.dataset.title || ""}`.replace(
          /^｜/,
          "",
        );
    ctx.openModalRoot(ctx.dueEditModal);
  };

  ctx.applyDueOverride = function applyDueOverride(uid, dueTs) {
    const dueText = ctx.formatDueDisplayFromTs(dueTs);
    ctx.dueOverrides[uid] = { due_ts: dueTs, due_at: dueText };
    ctx.saveDueOverrides();
    document
      .querySelectorAll(`tr[data-uid="${ctx.cssEscapeValue(uid)}"]`)
      .forEach((row) => {
        ctx.setRowDue(row, dueTs, dueText, "已手動修改期限");
      });
    ctx.applyFilters();
  };

  ctx.resetDueOverride = function resetDueOverride(uid) {
    delete ctx.dueOverrides[uid];
    ctx.saveDueOverrides();
    document
      .querySelectorAll(`tr[data-uid="${ctx.cssEscapeValue(uid)}"]`)
      .forEach((row) => {
        const originalTs = row.dataset.originalDueTs
          ? Number(row.dataset.originalDueTs)
          : null;
        ctx.setRowDue(
          row,
          originalTs,
          row.dataset.originalDue || "",
          row.dataset.originalDue ? "E3 同步期限" : "截止資訊同步中",
        );
      });
    ctx.applyFilters();
  };

  ctx.customAssignmentRowHtml = function customAssignmentRowHtml(
    item,
    flat = false,
  ) {
    const dueText = ctx.formatDueDisplayFromTs(item.due_ts);
    const status =
      item.due_ts < Math.floor(Date.now() / 1000) ? "overdue" : "pending";
    const commonAttrs = `
        data-due-ts="${item.due_ts}"
        data-overdue="${status === "overdue" ? 1 : 0}"
        data-completed="0"
        data-graded="0"
        data-primary-status="${status}"
        data-ignored="0"
        data-uid="${ctx.escapeHtml(item.uid)}"
        data-course="${ctx.escapeHtml(item.course)}"
        data-semester="custom"
        data-title="${ctx.escapeHtml(item.title)}"
        data-due="${ctx.escapeHtml(dueText)}"
        data-has-due="1"
        data-original-due-ts="${item.due_ts}"
        data-original-due="${ctx.escapeHtml(dueText)}"
    `;
    const titleCell = flat
      ? `
        <td class="assignment-cell-main">
            <span class="assignment-eyebrow">${ctx.escapeHtml(item.course)}</span>
            <div class="assignment-title-row"><span class="assignment-title">${ctx.escapeHtml(item.title)}</span></div>
            <div class="assignment-meta"><span class="assignment-grade-inline empty"><span class="assignment-grade-inline-label">自訂</span><span class="assignment-grade-inline-text">代辦</span></span></div>
        </td>
    `
      : `
        <td class="assignment-cell-main">
            <div class="assignment-title-row"><span class="assignment-title">${ctx.escapeHtml(item.title)}</span></div>
            <div class="assignment-meta"><span class="remaining-text normal"></span><span class="assignment-grade-inline empty"><span class="assignment-grade-inline-label">自訂</span><span class="assignment-grade-inline-text">代辦</span></span></div>
        </td>
    `;
    const signalCell = flat
      ? '<td class="assignment-center"><span class="assignment-field-label">剩餘時間</span><div class="assignment-signal"><span class="remaining-text normal"></span></div></td>'
      : "";
    return `
        <tr ${commonAttrs}>
            ${signalCell}
            ${titleCell}
            <td>
                <span class="assignment-field-label">截止時間</span>
                <div class="assignment-due">
                    <div class="assignment-due-header">
                        <span class="assignment-due-main">${ctx.escapeHtml(dueText)}</span>
                        <button type="button" class="due-edit-btn" data-edit-due="${ctx.escapeHtml(item.uid)}" aria-label="修改繳交期限" title="修改繳交期限">
                            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"></path><path d="M16.5 3.5a2.12 2.12 0 0 1 3 3L7 19l-4 1 1-4Z"></path></svg>
                        </button>
                    </div>
                    <span class="assignment-due-sub">自訂期限</span>
                </div>
            </td>
            <td class="assignment-center"><span class="submission-pill">-</span></td>
            <td class="assignment-center"><span class="assignment-field-label">狀態</span><div class="row-status-stack"><div class="row-status-badges"></div></div></td>
            <td class="assignment-center assignment-link"><button type="button" class="btn custom-delete-btn" data-delete-custom="${ctx.escapeHtml(item.uid)}">刪除</button></td>
        </tr>
    `;
  };

  ctx.ensureCustomCourseSection = function ensureCustomCourseSection(
    courseName,
  ) {
    if (!ctx.viewCourse) return null;
    const existing = Array.from(
      ctx.viewCourse.querySelectorAll('.course-card[data-semester="custom"]'),
    ).find((card) => {
      return card.dataset.courseTitle === courseName;
    });
    if (existing) return existing.querySelector("tbody");
    const section = document.createElement("section");
    section.className = "card course-card";
    section.dataset.semester = "custom";
    section.dataset.courseTitle = courseName;
    section.innerHTML = `
        <div class="panel" style="display:flex;justify-content:space-between;align-items:center;gap:12px">
            <div><h2>${ctx.escapeHtml(courseName)} <span style="color:var(--muted);font-size:14px">#自訂</span></h2></div>
        </div>
        <div class="panel">
            <div class="table-shell">
                <table class="courseTable assignment-table">
                    <colgroup>
                        <col class="course-col-title"><col class="course-col-due"><col class="course-col-stats"><col class="course-col-status"><col class="course-col-link">
                    </colgroup>
                    <thead><tr><th>作業名稱</th><th>截止時間</th><th class="assignment-center">繳交統計</th><th class="assignment-center">狀態</th><th class="assignment-center">連結</th></tr></thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
    `;
    ctx.viewCourse.appendChild(section);
    return section.querySelector("tbody");
  };

  ctx.renderCustomAssignments = function renderCustomAssignments() {
    document
      .querySelectorAll('tr[data-uid^="custom|"]')
      .forEach((row) => row.remove());
    ctx.customAssignments = ctx.customAssignments.filter(
      (item) => item && item.uid && item.title && item.due_ts,
    );
    ctx.customAssignments.forEach((item) => {
      const flatTbody = document.querySelector("#flatTable tbody");
      if (flatTbody) {
        flatTbody.insertAdjacentHTML(
          "beforeend",
          ctx.customAssignmentRowHtml(item, true),
        );
      }
      const courseTbody = ctx.ensureCustomCourseSection(
        item.course || "自訂代辦",
      );
      if (courseTbody) {
        courseTbody.insertAdjacentHTML(
          "beforeend",
          ctx.customAssignmentRowHtml(item, false),
        );
      }
    });
    document.querySelectorAll('tr[data-uid^="custom|"]').forEach((row) => {
      ctx.updateRemainingForRow(row);
      ctx.renderStatusBadges(row);
    });
  };

  ctx.addCustomAssignment = function addCustomAssignment(course, title, dueTs) {
    const item = {
      uid: `custom|${Date.now()}|${Math.random().toString(36).slice(2, 8)}`,
      course: String(course || "").trim() || "自訂代辦",
      title: String(title || "").trim(),
      due_ts: dueTs,
    };
    if (!item.title || !item.due_ts) return false;
    ctx.customAssignments.push(item);
    ctx.saveCustomAssignments();
    ctx.renderCustomAssignments();
    ctx.applyFilters();
    return true;
  };

  ctx.deleteCustomAssignment = function deleteCustomAssignment(uid) {
    ctx.customAssignments = ctx.customAssignments.filter(
      (item) => item.uid !== uid,
    );
    delete ctx.dueOverrides[uid];
    ctx.saveCustomAssignments();
    ctx.saveDueOverrides();
    document
      .querySelectorAll(`tr[data-uid="${ctx.cssEscapeValue(uid)}"]`)
      .forEach((row) => row.remove());
    ctx.applyFilters();
  };

  ctx.hydrateLocalAssignments = function hydrateLocalAssignments() {
    ctx.renderCustomAssignments();
    ctx.applyDueOverrides();
    ctx.refreshRemainingTimes();
  };
}

export function initialize(ctx) {
  if (!Array.isArray(ctx.USER_PREFERENCES.ignored_overdue_uids)) {
    ctx.USER_PREFERENCES.ignored_overdue_uids = [];
  }

  ctx.STORAGE_USER_KEY =
    (document.body.dataset.viewUser || "guest").replace(/[^\w.-]/g, "_") ||
    "guest";

  ctx.DUE_OVERRIDES_KEY = `e3_due_overrides_${ctx.STORAGE_USER_KEY}`;

  ctx.CUSTOM_ASSIGNMENTS_KEY = `e3_custom_assignments_${ctx.STORAGE_USER_KEY}`;

  ctx.dueOverrides = ctx.loadJsonMap(ctx.DUE_OVERRIDES_KEY);

  ctx.customAssignments = ctx.loadJsonArray(ctx.CUSTOM_ASSIGNMENTS_KEY);
}
