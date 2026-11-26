const STORAGE_PREFIX = "e3_workbench_tour_v1:";

export function tourSeen(preferences, storage, key) {
  if (preferences?.onboarding_completed === true) return true;
  try { return !!key && storage?.getItem(STORAGE_PREFIX + key) === "done"; }
  catch { return false; }
}

export function rememberTour(ctx, storage) {
  ctx.USER_PREFERENCES.onboarding_completed = true;
  try { if (ctx.config.onboardingKey) storage?.setItem(STORAGE_PREFIX + ctx.config.onboardingKey, "done"); } catch {}
  if (!ctx.IS_GUEST) ctx.persistPreferences({ onboardingCompleted: true });
}

export function tourSteps(doc) {
  return [
    { selector: "#guestFile", title: "先匯入作業資料", text: "使用匯出工具產生 JSON，再從這裡匯入。訪客模式不會自動連線 E3。" },
    { selector: ".workspace-actions", title: "掌握本學期的作業", text: doc.querySelector("#semesterSelector") ? "這裡可以切換學期，也會統計待處理與逾期作業。" : "待處理與逾期作業的數量會顯示在這裡。課程同步後，也能切換學期。" },
    { selector: ".toolbar-stack", title: "快速找到作業", text: "輸入關鍵字、選擇課程，再用「篩選」切換作業狀態。已忽略的作業也能在篩選中復原。" },
    { selector: ".seg", title: "切換列表或日曆", text: "按到期日安排先後、依課程分類，或用日曆查看每天的截止作業。" },
    { selector: ".workspace-messages", title: "查看課程訊息", text: "課程公告與信件集中在這裡，分頁查看。有新訊息時，入口會亮起紅點。" },
    { selector: ".workspace-files", menu: "#moreOperations", title: "附件與匯出工具", text: "從這裡找課程附件。這個選單也有 Excel 匯出、Google 日曆，以及可重看的「使用教學」。" },
    { selector: "#moreOperations > summary", title: "更多工具", text: "匯出與整合工具都在這裡。之後想重看導覽，點選單內的「使用教學」。", fallback: ".workspace-files" },
    { selector: "#addCustomAssignmentBtn", title: "新增個人代辦", text: "考試、報告或個人任務也能加入，並設定期限。代辦會保存在這台瀏覽器。" },
    { selector: 'a[aria-label="通知設定"]', title: "設定作業提醒", text: "開啟瀏覽器或 LINE 通知，選擇新作業、到期前提醒或成績通知。" },
  ].filter(step => doc.querySelector(step.selector) && !(step.fallback && doc.querySelector(step.fallback)));
}

export function placeTourCard(rect, viewport, card) {
  const margin = 16, gap = 18;
  const width = Math.min(card.width, viewport.width - margin * 2);
  const height = Math.min(card.height, viewport.height - margin * 2);
  const clamp = (value, max) => Math.max(margin, Math.min(value, Math.max(margin, max)));
  let left = clamp(rect.left + rect.width / 2 - width / 2, viewport.width - width - margin);
  let top;
  if (viewport.height - rect.bottom >= height + gap + margin) top = rect.bottom + gap;
  else if (rect.top >= height + gap + margin) top = rect.top - height - gap;
  else if (viewport.width - rect.right >= width + gap + margin) {
    left = rect.right + gap;
    top = clamp(rect.top, viewport.height - height - margin);
  } else if (rect.left >= width + gap + margin) {
    left = rect.left - width - gap;
    top = clamp(rect.top, viewport.height - height - margin);
  } else top = viewport.height - height - margin;
  return { left: Math.round(left), top: Math.round(clamp(top, viewport.height - height - margin)) };
}

export function initialize(ctx) {
  const dialog = document.getElementById("workbenchTour");
  const replay = document.getElementById("onboardingReplay");
  if (!dialog || !replay || ctx.IS_READONLY_VIEW || typeof dialog.showModal !== "function") return;
  const card = document.getElementById("tourCard");
  const title = document.getElementById("tourTitle");
  const description = document.getElementById("tourDescription");
  const count = document.getElementById("tourCount");
  const progress = document.getElementById("tourProgress");
  const fill = document.getElementById("tourProgressFill");
  const mask = document.getElementById("tourMask");
  const ring = document.getElementById("tourRing");
  const back = document.getElementById("tourBack");
  const next = document.getElementById("tourNext");
  let storage;
  try { storage = window.localStorage; } catch {}
  let pending = !tourSeen(ctx.USER_PREFERENCES, storage, ctx.config.onboardingKey);
  let steps = [], index = 0, frame = 0, timer = 0, activeMenu = null;
  let menuWasOpen = false, menuScroll = 0, originalFocus = null, originalScroll = 0;
  const blockers = [...document.querySelectorAll(".modal-root, #loader, #serverSsoPanel")];

  function isBusy() {
    return document.hidden || blockers.some(node => node.getClientRects().length && (
      node.id === "loader" ? node.classList.contains("active") : !node.hidden && !node.classList.contains("hidden")
    )) || [...document.querySelectorAll("dialog[open]")].some(node => node !== dialog);
  }

  function restoreMenu() {
    if (!activeMenu) return;
    activeMenu.open = menuWasOpen;
    const panel = activeMenu.querySelector(".more-panel");
    if (panel) panel.scrollTop = menuScroll;
    activeMenu = null;
  }

  function paint() {
    frame = 0;
    if (!dialog.open) return;
    const target = document.querySelector(steps[index]?.selector);
    if (!target || !target.getClientRects().length) return;
    const width = window.innerWidth, height = window.innerHeight;
    const raw = target.getBoundingClientRect();
    const left = Math.max(6, raw.left - 6), top = Math.max(6, raw.top - 6);
    const right = Math.min(width - 6, raw.right + 6), bottom = Math.min(height - 6, raw.bottom + 6);
    const radius = Math.max(0, Math.min(12, (right - left) / 2, (bottom - top) / 2));
    mask.parentElement.setAttribute("viewBox", `0 0 ${width} ${height}`);
    mask.setAttribute("d", `M0 0H${width}V${height}H0Z M${left + radius} ${top}H${right - radius}Q${right} ${top} ${right} ${top + radius}V${bottom - radius}Q${right} ${bottom} ${right - radius} ${bottom}H${left + radius}Q${left} ${bottom} ${left} ${bottom - radius}V${top + radius}Q${left} ${top} ${left + radius} ${top}Z`);
    Object.assign(ring.style, { left: `${left}px`, top: `${top}px`, width: `${Math.max(0, right - left)}px`, height: `${Math.max(0, bottom - top)}px` });
    card.style.maxHeight = `${height - 32}px`;
    const point = placeTourCard({ left, top, right, bottom, width: right - left }, { width, height }, { width: card.offsetWidth, height: card.offsetHeight });
    Object.assign(card.style, { left: `${point.left}px`, top: `${point.top}px` });
  }

  function schedulePaint() {
    if (!frame && dialog.open) frame = window.requestAnimationFrame(paint);
  }
  const resize = typeof ResizeObserver === "function" ? new ResizeObserver(schedulePaint) : null;

  function render() {
    restoreMenu();
    const step = steps[index];
    if (step.menu) {
      activeMenu = document.querySelector(step.menu);
      if (activeMenu) {
        menuWasOpen = activeMenu.open;
        menuScroll = activeMenu.querySelector(".more-panel")?.scrollTop || 0;
        activeMenu.open = true;
      }
    }
    const target = document.querySelector(step.selector);
    title.textContent = step.title;
    description.textContent = step.text;
    count.textContent = `${index + 1} / ${steps.length}`;
    progress.setAttribute("aria-valuenow", String(index + 1));
    progress.setAttribute("aria-valuemax", String(steps.length));
    progress.setAttribute("aria-valuetext", `第 ${index + 1} 步，共 ${steps.length} 步：${step.title}`);
    fill.style.width = `${(index + 1) / steps.length * 100}%`;
    back.disabled = index === 0;
    next.firstChild.textContent = index === steps.length - 1 ? "開始使用" : "下一步";
    target?.scrollIntoView({ block: "center", inline: "nearest", behavior: "instant" });
    resize?.disconnect();
    if (target) resize?.observe(target);
    resize?.observe(card);
    paint();
    title.focus({ preventScroll: true });
  }

  function close(remember = true) {
    pending = !remember;
    if (remember) rememberTour(ctx, storage);
    window.cancelAnimationFrame(frame);
    window.clearTimeout(timer);
    frame = 0;
    resize?.disconnect();
    restoreMenu();
    dialog.close();
    document.documentElement.classList.remove("tour-open");
    window.scrollTo({ top: originalScroll, behavior: "instant" });
    if (remember && originalFocus?.isConnected) originalFocus.focus({ preventScroll: true });
    if (!remember) document.querySelector('#serverSsoPanel:not([hidden]) input')?.focus({ preventScroll: true });
  }

  function start(reset = false) {
    if (dialog.open || isBusy()) return;
    steps = tourSteps(document);
    if (!steps.length) return;
    if (reset || index >= steps.length) index = 0;
    originalFocus = document.activeElement;
    originalScroll = window.scrollY;
    pending = false;
    document.documentElement.classList.add("tour-open");
    dialog.showModal();
    render();
  }

  function attemptStart() {
    window.clearTimeout(timer);
    if (pending) timer = window.setTimeout(() => start(), 400);
  }
  replay.addEventListener("click", () => {
    document.getElementById("moreOperations").open = false;
    pending = true;
    index = 0;
    start(true);
  });
  dialog.addEventListener("click", event => event.stopPropagation());
  document.getElementById("tourSkip").addEventListener("click", () => close());
  back.addEventListener("click", () => { if (index > 0) { index--; render(); } });
  next.addEventListener("click", () => { if (index < steps.length - 1) { index++; render(); } else close(); });
  dialog.addEventListener("cancel", event => { event.preventDefault(); close(); });
  dialog.addEventListener("keydown", event => {
    if (!["ArrowRight", "ArrowLeft", "Escape"].includes(event.key)) return;
    event.preventDefault();
    event.stopPropagation();
    if (event.key === "Escape") close();
    else if (event.key === "ArrowRight") next.click();
    else if (!back.disabled) back.click();
  });
  window.addEventListener("resize", schedulePaint);
  window.addEventListener("scroll", schedulePaint, { capture: true, passive: true });
  document.addEventListener("visibilitychange", () => { if (!document.hidden) { attemptStart(); schedulePaint(); } });
  if (typeof MutationObserver === "function") {
    const observer = new MutationObserver(() => {
      if (dialog.open && isBusy()) close(false);
      attemptStart();
    });
    blockers.forEach(node => observer.observe(node, { attributes: true, attributeFilter: ["class", "style", "hidden"] }));
  }
  attemptStart();
}
