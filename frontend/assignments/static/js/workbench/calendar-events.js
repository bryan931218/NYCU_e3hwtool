export function register(ctx) {}

export function initialize(ctx) {
  if (ctx.googleSyncBtn && ctx.googleModal) {
    ctx.googleSyncBtn.addEventListener("click", ctx.openGoogleModal);
    if (ctx.googleModalCancel) {
      ctx.googleModalCancel.addEventListener("click", ctx.closeGoogleModal);
    }
    if (ctx.googleModalClose) {
      ctx.googleModalClose.addEventListener("click", ctx.closeGoogleModal);
    }
    ctx.googleModal.addEventListener("click", (evt) => {
      if (evt.target.classList.contains("modal-backdrop")) {
        ctx.closeGoogleModal();
      }
    });
    document.addEventListener("keydown", (evt) => {
      if (
        evt.key === "Escape" &&
        !ctx.googleModal.classList.contains("hidden")
      ) {
        ctx.closeGoogleModal();
      }
    });
    if (ctx.googleModalSelectAll && ctx.googleModalList) {
      ctx.googleModalSelectAll.addEventListener("change", (evt) => {
        const checked = evt.target.checked;
        ctx.googleModalList
          .querySelectorAll('input[type="checkbox"]')
          .forEach((input) => {
            input.checked = checked;
          });
      });
    }
    if (ctx.googleModalSubmit && ctx.googleModalList) {
      ctx.googleModalSubmit.addEventListener("click", () => {
        if (!ctx.googleSyncForm || !ctx.googleSyncInput) {
          ctx.showToast("系統設定錯誤，請重新整理頁面。", "error");
          return;
        }
        if (ctx.googleSyncForm.dataset.submitting === "1") {
          return;
        }
        const selected = Array.from(
          ctx.googleModalList.querySelectorAll(
            'input[type="checkbox"]:checked',
          ),
        ).map((el) => el.value);
        if (!selected.length) {
          ctx.showToast("請至少選擇一個作業再導入日曆。", "warning");
          return;
        }
        ctx.googleSyncForm.dataset.submitting = "1";
        if (ctx.googleModalSubmit) {
          ctx.googleModalSubmit.disabled = true;
          ctx.googleModalSubmit.innerHTML =
            '<span class="inline-spinner"></span><span style="margin-left:8px">導入中...</span>';
        }
        ctx.showLoader("正在匯入 Google 日曆，請稍候...");
        ctx.closeGoogleModal();
        ctx.googleSyncInput.value = JSON.stringify(selected);
        ctx.logUiEvent("google_sync_submit", "info", {
          count: selected.length,
        });
        ctx.googleSyncForm.submit();
      });
    }
  }
}
