export function register(ctx) {
  ctx.applyTheme = function applyTheme(theme) {
    const target = theme === "dark" ? "dark" : "light";
    document.documentElement.setAttribute("data-theme", target);
    document.documentElement.style.colorScheme = target;
    try {
      localStorage.setItem("e3_theme", target);
    } catch (err) {}
    if (ctx.themeToggle) {
      ctx.themeToggle.setAttribute(
        "title",
        target === "dark" ? "切換至淺色模式" : "切換至深色模式",
      );
    }
  };

  ctx.logUiEvent = function logUiEvent(action, status = "success", meta) {
    if (!action || !ctx.UI_EVENT_ENDPOINT) return;
    const payload = JSON.stringify({ action, status, meta });
      fetch(ctx.UI_EVENT_ENDPOINT, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: payload,
        keepalive: true,
      }).catch(() => {});
  };

  ctx.showLoader = function showLoader(message = "載入中...") {
    if (!ctx.loader) return;
    if (ctx.loaderMessageEl) {
      ctx.loaderMessageEl.textContent = message;
    }
    ctx.loader.classList.add("active");
    document.body.classList.add("ui-locked");
  };

  ctx.hideLoader = function hideLoader() {
    if (!ctx.loader) return;
    ctx.loader.classList.remove("active");
    document.body.classList.remove("ui-locked");
    if (ctx.loaderMessageEl) {
      ctx.loaderMessageEl.textContent = "載入中...";
    }
  };

  ctx.showToast = function showToast(message, type = "info", duration = 5000) {
    const toast = document.createElement("div");
    toast.className = `toast ${type}`;

    toast.innerHTML = `
    <div class="toast-icon">${ctx.TOAST_ICONS[type] || ctx.TOAST_ICONS.info}</div>
    <div class="toast-content"></div>
    <button class="toast-close" aria-label="關閉">×</button>
    <div class="toast-progress"></div>
    `;

    toast.querySelector(".toast-content").textContent = message;
    ctx.toastContainer.appendChild(toast);

    const timeout = setTimeout(() => {
      ctx.closeToast(toast);
    }, duration);

    const closeBtn = toast.querySelector(".toast-close");
    closeBtn.addEventListener("click", () => {
      clearTimeout(timeout);
      ctx.closeToast(toast);
    });

    if (ctx.toastContainer.children.length > 3) {
      ctx.closeToast(ctx.toastContainer.children[0]);
    }

    return toast;
  };

  ctx.closeToast = function closeToast(toast) {
    if (!toast || toast.classList.contains("closing")) return;

    toast.classList.add("closing");
    toast.addEventListener("animationend", () => {
      toast.remove();
    });
  };

  ctx.processFlashMessages = function processFlashMessages() {
    const flashMessages = document.getElementById("flashMessages");
    if (flashMessages) {
      const messages = flashMessages.querySelectorAll("div");
      messages.forEach((msg) => {
        const category = msg.dataset.category || "info";
        const message = msg.dataset.message;
        if (message) {
          ctx.showToast(message, category);
        }
      });
    }
  };
}

export function initialize(ctx) {
  ctx.USER_PREFERENCES = ctx.config.preferences || {};

  ctx.PREFERENCES_ENDPOINT = ctx.config.preferencesUrl;

  ctx.CACHE_SYNC_ENDPOINT = ctx.config.cacheUrl;

  ctx.CACHE_SYNC_INTERVAL = 120000;

  ctx.currentCacheTs = Number(document.body.dataset.cacheTs || "0");

  if (Number.isNaN(ctx.currentCacheTs)) {
    ctx.currentCacheTs = 0;
  }

  ctx.refreshInFlight = false;

  ctx.IS_GUEST = ctx.config.guest;

  ctx.IS_READONLY_VIEW = document.body.dataset.readonlyView === "1";

  ctx.loader = document.getElementById("loader");

  ctx.loaderMessageEl = ctx.loader
    ? ctx.loader.querySelector(".loader-message")
    : null;

  ctx.bgRefreshIndicator = document.getElementById("bgRefreshIndicator");

  ctx.statusFilterGroup = document.getElementById("statusFilterGroup");

  ctx.ignoredOverdueList = document.getElementById("ignoredOverdueList");

  ctx.restoreIgnoredOverdueAll = document.getElementById(
    "restoreIgnoredOverdueAll",
  );

  ctx.filterMenu = document.getElementById("filterMenu");

  ctx.filterActiveCount = document.getElementById("filterActiveCount");

  ctx.viewDue = document.getElementById("viewDue");

  ctx.viewCourse = document.getElementById("viewCourse");

  ctx.viewCourseBtn = document.getElementById("viewCourseBtn");

  ctx.viewDueBtn = document.getElementById("viewDueBtn");

  ctx.hasCache = document.body.dataset.hasCache === "1";

  ctx.toastContainer = document.getElementById("toastContainer");

  ctx.UI_EVENT_ENDPOINT = ctx.config.eventUrl;

  ctx.themeToggle = document.getElementById("themeToggle");

  ctx.totalCountEl = document.getElementById("totalCount");

  ctx.ignoredCountEl = document.getElementById("ignoredCount");

  ctx.assignmentSearch = document.getElementById("assignmentSearch");

  ctx.courseFilter = document.getElementById("courseFilter");

  ctx.pendingSummaryCount = document.getElementById("pendingSummaryCount");

  ctx.overdueSummaryCount = document.getElementById("overdueSummaryCount");

  ctx.addCustomAssignmentBtn = document.getElementById(
    "addCustomAssignmentBtn",
  );

  ctx.dueEditModal = document.getElementById("dueEditModal");

  ctx.dueEditForm = document.getElementById("dueEditForm");

  ctx.dueEditUid = document.getElementById("dueEditUid");

  ctx.dueEditInput = document.getElementById("dueEditInput");

  ctx.dueEditTitle = document.getElementById("dueEditTitle");

  ctx.dueResetBtn = document.getElementById("dueResetBtn");

  ctx.customAssignmentModal = document.getElementById("customAssignmentModal");

  ctx.customAssignmentForm = document.getElementById("customAssignmentForm");

  ctx.customCourseInput = document.getElementById("customCourseInput");

  ctx.customTitleInput = document.getElementById("customTitleInput");

  ctx.customDueInput = document.getElementById("customDueInput");

  ctx.currentAssignmentQuery = "";

  ctx.currentCourseFilter = "";

  ctx.TOAST_ICONS = {
    success:
      '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg>',
    error:
      '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="15" y1="9" x2="9" y2="15"></line><line x1="9" y1="9" x2="15" y2="15"></line></svg>',
    warning:
      '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>',
    info: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line><line x1="12" y1="8" x2="12.01" y2="8"></line></svg>',
  };

  ctx.googleSyncBtn = document.getElementById("googleSyncBtn");

  ctx.googleModal = document.getElementById("googleModal");

  ctx.googleModalList = document.getElementById("googleModalList");

  ctx.googleModalSelectAll = document.getElementById("googleModalSelectAll");

  ctx.googleModalSubmit = document.getElementById("googleModalSubmit");

  ctx.googleModalCancel = document.getElementById("googleModalCancel");

  ctx.googleModalClose = document.getElementById("googleModalClose");

  ctx.googleSyncInput = document.getElementById("googleSyncInput");

  ctx.googleSyncForm = document.getElementById("googleSyncForm");

  if (ctx.googleModalSubmit && !ctx.googleModalSubmit.dataset.defaultText) {
    ctx.googleModalSubmit.dataset.defaultText =
      ctx.googleModalSubmit.textContent.trim();
  }

  if (ctx.themeToggle) {
    ctx.themeToggle.addEventListener("click", () => {
      const current =
        document.documentElement.getAttribute("data-theme") === "dark"
          ? "dark"
          : "light";
      ctx.applyTheme(current === "dark" ? "light" : "dark");
    });
  }

  ctx.startupTheme =
    typeof window !== "undefined" && window.__initialTheme
      ? window.__initialTheme
      : document.documentElement.getAttribute("data-theme") || "light";

  ctx.applyTheme(ctx.startupTheme);

  document.addEventListener("DOMContentLoaded", ctx.processFlashMessages);
}
