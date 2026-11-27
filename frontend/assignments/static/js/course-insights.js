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

function sourceLink(url, label) {
  const target = new URL(url, window.location.href);
  if (!["https:", "http:"].includes(target.protocol)) return node("span", label);
  const anchor = node("a", label);
  anchor.href = target.href;
  anchor.target = "_blank";
  anchor.rel = "noopener noreferrer";
  return anchor;
}

async function courseRequest(url, data, csrf, signal) {
  const options = data === undefined ? {} : {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-CSRFToken": csrf },
    body: JSON.stringify(data),
  };
  const response = await fetch(url, { ...options, cache: "no-store", credentials: "same-origin", signal });
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
  if (result.configured && result.count && !result.running && (manual || result.retry_at <= Date.now() / 1000)) {
    const job = await courseRequest(config.refreshUrl, { semester: result.semester, manual }, config.csrf);
    if (job.status === "running") result.running = true;
    if (job.error) result.error = job.error;
    if (job.status === "session_expired") result.error_code = "session_expired";
    if (manual) result.manualOutcome = job.status;
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
    try {
      sessionStorage.setItem("e3.courseOverviewQuestion", document.getElementById("assignmentSearch").value.trim());
    } catch { /* Search still opens if browser storage is unavailable. */ }
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

export function showScheduleEntry({ range, day, cancelled, today }) {
  return range === "all" || (!cancelled && day >= today);
}

function setupNavigation(page, cards, onSelectionChanged) {
  if (!cards) return null;
  const courseSelect = page.querySelector("#insightCourseSelect");
  const label = page.querySelector("#insightSearchScope");
  const links = new Map([...page.querySelectorAll("#insightCourseNav a")].map(link => [link.dataset.courseOption, link]));
  let range = "upcoming";
  function render() {
    const selected = page.dataset.course;
    if (courseSelect) courseSelect.value = selected;
    if (label) label.textContent = links.get(selected)?.dataset.courseName || "全部課程";
    for (const [id, link] of links) {
      if (id === selected) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    }
    page.querySelectorAll("[data-current-course]").forEach(input => { input.value = selected; });
    cards.querySelectorAll("[data-course-panel]").forEach(panel => {
      panel.hidden = panel.dataset.coursePanel !== selected;
    });
    const schedule = cards.querySelector(".insight-schedule");
    if (!schedule) return;
    const rows = [...schedule.querySelectorAll("[data-schedule-date]")];
    const controls = schedule.querySelector(".insight-range");
    if (controls) controls.hidden = false;
    controls?.querySelectorAll("button").forEach(button => {
      button.setAttribute("aria-pressed", String(button.dataset.scheduleRange === range));
    });
    const today = activeDay();
    for (const row of rows) {
      row.hidden = !showScheduleEntry({ range, day: row.dataset.scheduleDate,
        cancelled: row.dataset.scheduleCancelled === "1", today });
    }
    const empty = schedule.querySelector("[data-empty-schedule]");
    if (empty) empty.hidden = rows.some(row => !row.hidden);
  }
  function select(course) {
    if (!links.has(course) || course === page.dataset.course) return;
    page.dataset.course = course;
    const url = new URL(location.href);
    // Preserve a shareable course selection, but leave setup and search inputs local.
    url.pathname = new URL(links.get(course).href).pathname;
    url.searchParams.delete("setup");
    url.searchParams.delete("q");
    url.hash = "";
    if (course) url.searchParams.set("course", course);
    else url.searchParams.delete("course");
    history.replaceState(history.state, "", url);
    render();
    onSelectionChanged();
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
  return { render, select };
}

function setupSearch(page, navigation) {
  const answer = page.querySelector("#overviewAnswer");
  const query = page.querySelector("#overviewQuery");
  const form = page.querySelector("#overviewSearch");
  if (!form || !answer || !query) return () => {};
  const button = form.querySelector("button");
  const suggestions = page.querySelector("#insightSuggestions");
  let pending;
  try {
    const carried = sessionStorage.getItem("e3.courseOverviewQuestion");
    sessionStorage.removeItem("e3.courseOverviewQuestion");
    if (carried && !query.value) query.value = carried.slice(0, 300);
  } catch { /* Browser storage is optional. */ }
  function clearAnswer(focus = false) {
    pending?.abort();
    pending = undefined;
    button.disabled = false;
    answer.hidden = true;
    answer.replaceChildren();
    if (focus) query.focus();
  }
  async function search() {
    const question = query.value.trim();
    if (question.length < 2) return;
    pending?.abort();
    const request = new AbortController();
    pending = request;
    button.disabled = true;
    answer.hidden = false;
    answer.textContent = "正在查找原文…";
    try {
      const result = await courseRequest(page.dataset.searchUrl, {
        query: question, course_id: Number(page.dataset.course) || null, semester: page.dataset.semester,
      }, page.dataset.csrf, request.signal);
      if (pending !== request) return;
      const heading = node("div", "", "insight-answer-heading");
      const dismiss = node("button", "收合結果", "insight-answer-dismiss");
      dismiss.type = "button";
      dismiss.addEventListener("click", () => clearAnswer(true));
      heading.append(node("h3", question), dismiss);
      answer.replaceChildren(heading);
      if (result.choices?.length) {
        const options = node("div", "", "insight-answer-options");
        answer.append(node("p", "要查哪一門課？"), options);
        for (const choice of result.choices) {
          const option = node("button", choice.title, "btn");
          option.type = "button";
          option.addEventListener("click", () => { navigation.select(String(choice.id)); search(); });
          options.append(option);
        }
      } else if (result.points?.length) {
        for (const point of result.points) {
          const item = node("article", "", "insight-answer-item");
          const detail = node("details", "", "insight-source");
          detail.append(node("summary", ["查看依據", point.source.title, point.source.location].filter(Boolean).join(" · ")),
            node("blockquote", point.quote), sourceLink(point.source.url, "在 E3 查看來源 ↗"));
          item.append(node("span", point.course, "insight-tag"), node("p", point.text), detail);
          answer.append(item);
        }
      } else {
        answer.append(node("p", "目前已同步資料找不到明確說明。"));
      }
      if (result.partial) answer.append(node("p", "部分課程資料仍未讀取。", "insight-muted"));
    } catch (error) {
      if (pending === request) answer.textContent = error.message;
    } finally {
      if (pending === request) {
        pending = undefined;
        button.disabled = false;
      }
    }
  }
  form.addEventListener("submit", event => { event.preventDefault(); if (!button.disabled) search(); });
  suggestions?.addEventListener("click", event => {
    const suggestion = event.target.closest("[data-question]");
    if (!suggestion) return;
    query.value = suggestion.dataset.question;
    search();
  });
  return clearAnswer;
}

function initOverview(page) {
  const semesterForm = page.querySelector("#insightSemesterForm");
  semesterForm?.querySelector("select").addEventListener("change", () => semesterForm.requestSubmit());
  const cards = page.querySelector("#overviewCards");
  let clearAnswer = () => {};
  const navigation = setupNavigation(page, cards, () => clearAnswer());
  if (page.dataset.configured !== "1" || !page.dataset.semester || !cards) return;
  clearAnswer = setupSearch(page, navigation);
  const status = page.querySelector("#overviewStatus");
  const action = page.querySelector("#overviewSyncAction");
  const syncButton = page.querySelector("#insightRefresh");
  const setupState = page.querySelector(".insight-setup-state");
  let timer;
  let isChecking = false;
  let renderedOverview;

  function renderProgress(result) {
    const progress = result.count ? "已整理 " + result.ready + "／" + result.count + " 門課" : "尚無課程";
    status.textContent = result.error || (result.running ? "正在整理・" + progress
      : result.manualOutcome === "waiting" ? progress + "。剛檢查過，稍後可再試。" : progress);
    syncButton.disabled = Boolean(result.running);
    syncButton.textContent = result.running ? "整理中" : "檢查更新";
    const expired = result.error_code === "session_expired";
    action.hidden = !expired && result.error_code !== "key";
    action.textContent = expired ? "重新登入" : "更新金鑰";
    action.href = expired ? page.dataset.loginUrl : "#courseSetup";
    if (setupState) setupState.textContent = result.error_code === "key" ? "金鑰需要更新" : "已啟用";
    for (const dot of page.querySelectorAll("[data-course-state]")) {
      const ready = result.courses_ready?.includes(dot.dataset.courseState);
      dot.dataset.ready = ready ? "1" : "0";
      dot.setAttribute("aria-label", ready ? "已整理" : "待整理");
    }
  }

  function renderCards(markup) {
    if (typeof markup !== "string" || markup === renderedOverview) {
      navigation.render();
      return;
    }
    const fragment = document.createElement("template");
    fragment.innerHTML = markup;
    const existing = new Map([...cards.children].map(card => [card.dataset.coursePanel, card]));
    for (const card of [...fragment.content.children]) {
      const previous = existing.get(card.dataset.coursePanel);
      if (!previous) cards.append(card);
      else if (previous.innerHTML !== card.innerHTML) {
        const expanded = new Set([...previous.querySelectorAll("details[open]")].map(detail => detail.textContent));
        for (const detail of card.querySelectorAll("details")) {
          if (expanded.has(detail.textContent)) detail.open = true;
        }
        previous.replaceWith(card);
      }
      existing.delete(card.dataset.coursePanel);
    }
    for (const stale of existing.values()) stale.remove();
    renderedOverview = markup;
    navigation.render();
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
      timer = setTimeout(refresh, result.running ? 9000 : 90000);
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
