function node(tag, text, className) {
  const element = document.createElement(tag);
  element.textContent = text;
  if (className) element.className = className;
  return element;
}

function dateText(value) {
  const day = value.slice(5, 10).replace("-", "/");
  return value.length === 10 ? day : day + " " + value.slice(11, 16);
}

async function courseRequest(url, data, csrf) {
  const options = data === undefined ? {} : {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-CSRFToken": csrf },
    body: JSON.stringify(data),
  };
  const response = await fetch(url, { ...options, cache: "no-store", credentials: "same-origin" });
  if (!response.headers.get("content-type")?.includes("application/json")) {
    throw new Error("請重新登入後再試。");
  }
  const body = await response.json();
  if (!response.ok) throw new Error(body.error || "暫時無法讀取課程內容。");
  return body;
}

async function loadInsights(config, semester, include, manual = false) {
  const url = new URL(config.dataUrl, location.href);
  if (semester) url.searchParams.set("semester", semester);
  if (include) url.searchParams.set("include", include);
  if (include === "overview" && config.overviewVersion) url.searchParams.set("overview_version", config.overviewVersion);
  const result = await courseRequest(url);
  const queue = include === "overview" && (manual || (result.needs_scan && !result.queued_here));
  const due = !result.running && result.retry_at <= Date.now() / 1000;
  if (result.configured && result.count && (queue || due)) {
    const job = await courseRequest(config.refreshUrl, { semester: result.semester, manual, queue }, config.csrf);
    if (job.status === "running") {
      result.running = true;
      result.running_here = true;
      result.error = "";
      result.error_code = "";
    }
    if (queue && ["running", "queued"].includes(job.status)) result.queued_here = true;
    if (job.error) result.error = job.error;
    if (job.status === "session_expired") result.error_code = "session_expired";
  }
  return result;
}

export function initHomeInsights(context) {
  const section = document.getElementById("courseInsightHome");
  const entry = document.getElementById("courseOverviewLink");
  let generation = 0;
  let timer;
  const semester = () => document.querySelector("[data-semester-filter]:checked")?.value || "";
  const link = (course = "", eventId = "") => {
    const target = new URL(section.dataset.overviewUrl, location.href);
    if (semester()) target.searchParams.set("semester", semester());
    if (course) target.searchParams.set("course", course);
    if (course && eventId) target.hash = "insight-event-" + course + "-" + eventId;
    return target.href;
  };
  entry?.addEventListener("click", () => {
    entry.href = link();
  });
  async function update() {
    clearTimeout(timer);
    const token = ++generation;
    if (document.hidden) return;
    try {
      const result = await loadInsights(section.dataset, semester());
      if (token !== generation) return;
      const events = result.events || [];
      section.dataset.events = JSON.stringify(events);
      section.dataset.semester = result.semester || "";
      section.querySelector("[data-overview-link]").href = link();
      const today = new Intl.DateTimeFormat("sv-SE", { timeZone: "Asia/Taipei" }).format(new Date());
      const limit = new Intl.DateTimeFormat("sv-SE", { timeZone: "Asia/Taipei" }).format(new Date(Date.now() + 14 * 86400000));
      const upcoming = events.filter(event => event.date.slice(0, 10) >= today && event.date.slice(0, 10) <= limit);
      section.querySelector("ul").replaceChildren(...upcoming.slice(0, 3).map(event => {
        const li = node("li", "");
        const date = node("time", dateText(event.date));
        date.dateTime = event.date;
        const text = node("div", "");
        const title = node("a", event.title, "insight-home-event");
        title.href = link(event.course_id, event.id);
        text.append(title, node("span", event.course, "insight-course-label"));
        const deadline = event.kind === "deadline";
        li.append(date, text, node("span", deadline ? "期限" : "考試", deadline ? "insight-tag insight-tag-due" : "insight-tag"));
        return li;
      }));
      section.hidden = !upcoming.length;
      if (context.deadlineCalendar) context.syncDeadlineCalendar?.();
      if (result.configured) timer = setTimeout(update, result.running ? 11000 : 95000);
    } catch {
      if (token === generation) timer = setTimeout(update, 120000);
    }
  }
  document.addEventListener("visibilitychange", () => { if (!document.hidden) update(); });
  document.addEventListener("change", event => {
    if (!event.target.matches("[data-semester-filter]")) return;
    section.hidden = true;
    section.dataset.events = "[]";
    update();
  });
  update();
}

function activeDay() {
  return new Intl.DateTimeFormat("sv-SE", { timeZone: "Asia/Taipei" }).format(new Date());
}

export function showScheduleEntry({ range, day, cancelled, today, course, selectedCourse }) {
  if (selectedCourse && course !== selectedCourse) return false;
  return range === "all" || (!cancelled && day >= today);
}

function setupNavigation(page, cards) {
  if (!cards) return null;
  const courseSelect = page.querySelector("#insightCourseSelect");
  const title = page.querySelector("#insightScopeTitle");
  const links = new Map([...page.querySelectorAll("#insightCourseNav a")].map(link => [link.dataset.courseOption, link]));
  let range = page.dataset.historical === "1" ? "all" : "upcoming";
  const target = document.getElementById(location.hash.slice(1));
  if (target?.dataset.scheduleDate && cards.contains(target) && !showScheduleEntry({ range, day: target.dataset.scheduleDate,
    cancelled: target.dataset.scheduleCancelled === "1", today: activeDay() })) {
    range = "all";
  }
  function render() {
    const selected = page.dataset.course;
    if (courseSelect) courseSelect.value = selected;
    if (title) title.textContent = links.get(selected)?.dataset.courseName || "全部課程";
    for (const [id, link] of links) {
      if (id === selected) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    }
    page.querySelectorAll("[data-current-course]").forEach(input => { input.value = selected; });
    cards.querySelectorAll("[data-course-info]").forEach(panel => {
      panel.hidden = panel.dataset.courseInfo !== selected;
    });
    const schedule = cards.querySelector(".insight-schedule");
    if (!schedule) return;
    const rows = [...schedule.querySelectorAll("[data-schedule-date]")];
    const inScope = rows.some(row => !selected || row.dataset.eventCourse === selected);
    const controls = schedule.querySelector(".insight-range");
    if (controls) controls.hidden = !inScope;
    controls?.querySelectorAll("button").forEach(button => {
      button.setAttribute("aria-pressed", String(button.dataset.scheduleRange === range));
    });
    const today = activeDay();
    for (const row of rows) {
      row.hidden = !showScheduleEntry({ range, day: row.dataset.scheduleDate,
        cancelled: row.dataset.scheduleCancelled === "1", today,
        course: row.dataset.eventCourse, selectedCourse: selected });
      row.querySelector("[data-event-scope-link]").hidden = Boolean(selected);
    }
    const empty = schedule.querySelector("[data-empty-schedule]");
    if (empty) {
      empty.hidden = rows.some(row => !row.hidden);
      empty.textContent = inScope && range === "upcoming"
        ? "近期沒有已確認的考試或期限。" : empty.dataset.emptyLabel;
    }
  }
  function select(course) {
    if (!links.has(course) || course === page.dataset.course) return;
    page.dataset.course = course;
    const url = new URL(location.href);
    // Keep course links bookmarkable without leaving a stale setup or event anchor.
    url.pathname = new URL(links.get(course).href).pathname;
    url.searchParams.delete("setup");
    url.searchParams.delete("q");
    url.hash = "";
    if (course) url.searchParams.set("course", course);
    else url.searchParams.delete("course");
    history.replaceState(history.state, "", url);
    render();
  }
  page.addEventListener("click", event => {
    const link = event.target.closest("a[data-course-option]");
    if (link && !event.defaultPrevented && event.button === 0 && !event.ctrlKey && !event.metaKey && !event.shiftKey && !event.altKey) {
      event.preventDefault();
      select(link.dataset.courseOption);
      return;
    }
    const limit = event.target.closest("button[data-schedule-range]");
    if (limit) {
      range = limit.dataset.scheduleRange;
      render();
    }
  });
  courseSelect?.addEventListener("change", () => select(courseSelect.value));
  render();
  return render;
}

function initOverview(page) {
  const semesterForm = page.querySelector("#insightSemesterForm");
  semesterForm?.querySelector("select").addEventListener("change", () => semesterForm.requestSubmit());
  const semesterRefresh = page.querySelector("#insightSemesterRefresh");
  const semesterButton = semesterRefresh?.querySelector("button");
  semesterRefresh?.addEventListener("submit", () => {
    semesterButton.disabled = true;
    semesterButton.textContent = "更新學期中…";
    semesterRefresh.setAttribute("aria-busy", "true");
  });
  window.addEventListener("pageshow", event => {
    if (!event.persisted || !semesterButton) return;
    semesterButton.disabled = false;
    semesterButton.textContent = "更新學期";
    semesterRefresh.removeAttribute("aria-busy");
  });
  const cards = page.querySelector("#overviewCards");
  const renderNavigation = setupNavigation(page, cards);
  if (page.dataset.configured !== "1" || !page.dataset.semester || !cards) return;
  const status = page.querySelector("#overviewStatus");
  const action = page.querySelector("#overviewSyncAction");
  const syncButton = page.querySelector("#insightRefresh");
  const setupState = page.querySelector(".insight-setup-state");
  let timer;
  let isChecking = false;
  let renderedOverview;

  function renderProgress(result) {
    const partial = Object.values(result.course_states || {}).filter(state => state === "partial").length;
    const progress = result.count ? "已整理 " + result.ready + "／" + result.count + " 門課" : "尚無課程";
    const detail = partial ? progress + " · " + partial + " 門來源未齊" : progress;
    status.textContent = result.error ? detail + " · " + result.error : result.running_here ? "正在整理 · " + detail
      : result.queued_here ? "等候接續整理 · " + detail : detail;
    syncButton.disabled = Boolean(result.running_here || result.queued_here);
    syncButton.textContent = result.running_here ? "整理中" : result.queued_here ? "等候整理" : "整理本學期";
    const expired = result.error_code === "session_expired";
    action.hidden = !expired && result.error_code !== "key";
    action.textContent = expired ? "重新登入" : "更新金鑰";
    action.href = expired ? page.dataset.loginUrl : "#courseSetup";
    if (setupState) setupState.textContent = result.error_code === "key" ? "金鑰需要更新" : "已啟用";
    const stateLabels = {ready: "已整理", partial: "部分來源尚未讀取", pending: "等候整理"};
    for (const dot of page.querySelectorAll("[data-course-state]")) {
      const state = result.course_states?.[dot.dataset.courseState] || "pending";
      const label = stateLabels[state];
      dot.dataset.state = state;
      dot.title = label;
      dot.setAttribute("aria-label", label);
    }
  }

  function renderCards(markup) {
    if (typeof markup !== "string" || markup === renderedOverview) {
      renderNavigation();
      return;
    }
    const fragment = document.createElement("template");
    fragment.innerHTML = markup;
    const existing = new Map([...cards.children].map(card => [card.dataset.viewKey, card]));
    for (const card of [...fragment.content.children]) {
      const previous = existing.get(card.dataset.viewKey);
      if (!previous) cards.append(card);
      else if (previous.innerHTML !== card.innerHTML) {
        const expanded = new Set([...previous.querySelectorAll("details[open]")].map(detail => detail.textContent));
        for (const detail of card.querySelectorAll("details")) {
          if (expanded.has(detail.textContent)) detail.open = true;
        }
        previous.replaceWith(card);
      }
      existing.delete(card.dataset.viewKey);
    }
    for (const stale of existing.values()) stale.remove();
    renderedOverview = markup;
    renderNavigation();
  }

  async function refresh(manual = false) {
    if (document.hidden || isChecking) return;
    clearTimeout(timer);
    isChecking = true;
    if (manual) {
      syncButton.disabled = true;
      status.textContent = "正在檢查更新…";
    }
    try {
      const result = await loadInsights(page.dataset, page.dataset.semester, "overview", manual);
      if (!result.configured) {
        status.textContent = "課程 AI 已解除綁定，請重新整理頁面。";
        syncButton.disabled = true;
        return;
      }
      renderProgress(result);
      renderCards(result.overview);
      page.dataset.overviewVersion = result.overview_version || "";
      timer = setTimeout(refresh, result.running_here || result.queued_here ? 9000 : 90000);
    } catch (error) {
      status.textContent = error.message;
      syncButton.disabled = false;
      timer = setTimeout(refresh, 120000);
    } finally {
      isChecking = false;
    }
  }
  action.addEventListener("click", () => {
    if (action.getAttribute("href") === "#courseSetup") page.querySelector("#courseSetup").open = true;
  });
  syncButton.addEventListener("click", () => refresh(true));
  document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });
  refresh();
}

const overviewPage = globalThis.document?.getElementById("courseOverviewPage");
if (overviewPage) initOverview(overviewPage);
