export function register(ctx) {
  ctx.collectVisibleAssignments = function collectVisibleAssignments() {
    const mode = ctx.currentViewMode === "course" ? "course" : "due";
    const selector =
      mode === "due" ? "#flatTable tbody tr" : ".courseTable tbody tr";
    const rows = document.querySelectorAll(selector);
    return Array.from(rows)
      .filter(
        (row) =>
          row.dataset.uid &&
          row.dataset.hasDue === "1" &&
          !row.classList.contains("hidden") &&
          (ctx.currentViewMode === "calendar" || row.offsetParent !== null),
      )
      .map((row) => ({
        uid: row.dataset.uid,
        course: row.dataset.course || "",
        title: row.dataset.title || "",
        due: row.dataset.due || "",
        status:
          ctx.STATUS_FILTER_LABELS[row.dataset.primaryStatus || "pending"] ||
          "待處理",
      }));
  };

  ctx.openGoogleModal = function openGoogleModal() {
    if (!ctx.googleModal || !ctx.googleModalList) return;
    const items = ctx.collectVisibleAssignments();
    ctx.googleModalList.innerHTML = "";
    if (ctx.googleModalSelectAll) {
      ctx.googleModalSelectAll.disabled = items.length === 0;
      ctx.googleModalSelectAll.checked = items.length > 0;
    }
    if (ctx.googleModalSubmit) {
      ctx.googleModalSubmit.disabled = items.length === 0;
      ctx.googleModalSubmit.textContent =
        ctx.googleModalSubmit.dataset.defaultText || "導入";
    }
    ctx.hideLoader();
    if (!items.length) {
      ctx.googleModalList.innerHTML =
        '<div class="modal-empty">目前視圖沒有可導入的作業，請調整篩選後再試。</div>';
    } else {
      items.forEach((item) => {
        const wrapper = document.createElement("label");
        wrapper.className = "modal-item";
        wrapper.innerHTML = `
                <input type="checkbox" value="${ctx.escapeHtml(item.uid)}" checked>
                <div>
                    <div style="font-weight:600">${ctx.escapeHtml(item.course)}</div>
                    <div>${ctx.escapeHtml(item.title)}</div>
                    <div class="muted" style="font-size:12px;margin-top:4px">${ctx.escapeHtml(item.due)} ｜ ${ctx.escapeHtml(item.status)}</div>
                </div>
            `;
        ctx.googleModalList.appendChild(wrapper);
      });
    }
    ctx.googleModal.style.display = "block";
    ctx.googleModal.classList.remove("hidden");
    const backdrop = ctx.googleModal.querySelector(".modal-backdrop");
    const panel = ctx.googleModal.querySelector(".modal-panel");
    backdrop && backdrop.classList.add("show");
    panel && panel.classList.add("show");
  };

  ctx.closeGoogleModal = function closeGoogleModal() {
    if (!ctx.googleModal) return;
    const backdrop = ctx.googleModal.querySelector(".modal-backdrop");
    const panel = ctx.googleModal.querySelector(".modal-panel");
    backdrop && backdrop.classList.remove("show");
    panel && panel.classList.remove("show");
    setTimeout(() => {
      ctx.googleModal.classList.add("hidden");
      ctx.googleModal.style.display = "none";
    }, 150);
  };
}

export function initialize(ctx) {}
