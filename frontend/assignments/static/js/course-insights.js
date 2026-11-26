function node(tag, text, className) {
  const element = document.createElement(tag);
  element.textContent = text;
  if (className) element.className = className;
  return element;
}

function dateText(value) {
  return value.length === 10 ? value : value.slice(0, 16).replace("T", " ");
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

async function loadInsights(config, semester, include) {
  const url = new URL(config.dataUrl, location.href);
  if (semester) url.searchParams.set("semester", semester);
  if (include) url.searchParams.set("include", include);
  const result = await courseRequest(url);
  if (result.configured && result.count && !result.running && result.retry_at <= Date.now() / 1000) {
    const job = await courseRequest(config.refreshUrl, { semester: result.semester }, config.csrf);
    if (job.status === "running") result.running = true;
    if (job.error) result.error = job.error;
  }
  return result;
}

export function initHomeInsights(context) {
  const section = document.getElementById("courseInsightHome");
  const entry = document.getElementById("courseOverviewLink");
  let generation = 0;
  let timer;
  const semester = () => document.querySelector("[data-semester-filter]:checked")?.value || "";
  const link = () => {
    const target = new URL(section.dataset.overviewUrl, location.href);
    if (semester()) target.searchParams.set("semester", semester());
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
        const title = event.kind === "deadline" ? event.title + " · 課程期限" : event.title;
        li.append(date, node("strong", title), node("span", event.course, "insight-course-label"),
          sourceLink(event.origin.url, "來源"));
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

function initOverview(page) {
  if (page.dataset.configured !== "1" || !page.dataset.semester) return;
  const status = document.getElementById("overviewStatus");
  const cards = document.getElementById("overviewCards");
  const answer = document.getElementById("overviewAnswer");
  const query = document.getElementById("overviewQuery");
  const course = document.getElementById("overviewCourse");
  const form = document.getElementById("overviewSearch");
  const button = form?.querySelector("button");
  let timer;
  let renderedOverview;
  try {
    const pending = sessionStorage.getItem("e3.courseOverviewQuestion");
    sessionStorage.removeItem("e3.courseOverviewQuestion");
    if (pending && query && !query.value) query.value = pending.slice(0, 300);
  } catch { /* Browser storage is optional. */ }
  async function refresh() {
    clearTimeout(timer);
    if (document.hidden) return;
    try {
      const result = await loadInsights(page.dataset, page.dataset.semester, "overview");
      status.textContent = result.error || (result.running ? "正在整理課程…" : "") +
        (result.count ? " 已整理 " + result.ready + "／" + result.count + " 門課" : "尚無課程");
      if (typeof result.overview === "string" && result.overview !== renderedOverview) {
        const next = document.createElement("template");
        next.innerHTML = result.overview;
        const existing = new Map([...cards.children].map(card => [card.dataset.insightCourse, card]));
        for (const card of next.content.children) {
          const previous = existing.get(card.dataset.insightCourse);
          if (!previous) {
            cards.append(card.cloneNode(true));
          } else if (previous.innerHTML !== card.innerHTML) {
            card.open = previous.open || (card.open && !renderedOverview);
            previous.replaceWith(card.cloneNode(true));
          }
        }
        renderedOverview = result.overview;
      }
      timer = setTimeout(refresh, result.running ? 9000 : 90000);
    } catch (error) {
      status.textContent = error.message;
      timer = setTimeout(refresh, 120000);
    }
  }
  async function search() {
    button.disabled = true;
    answer.textContent = "正在查找原文…";
    try {
      const result = await courseRequest(page.dataset.searchUrl, {
        query: query.value.trim(), course_id: course.value ? Number(course.value) : null, semester: page.dataset.semester,
      }, page.dataset.csrf);
      answer.replaceChildren();
      if (result.choices?.length) {
        const options = node("div", "", "insight-bar");
        answer.append(node("p", "要查哪一門課？"), options);
        for (const choice of result.choices) {
          const option = node("button", choice.title, "btn");
          option.type = "button";
          option.addEventListener("click", () => { course.value = String(choice.id); search(); });
          options.append(option);
        }
      } else if (result.points?.length) {
        for (const point of result.points) {
          const detail = node("details", "", "insight-source");
          detail.append(node("summary", [point.course, point.source.title, point.source.location].filter(Boolean).join(" · ")),
            node("blockquote", point.quote), sourceLink(point.source.url, "在 E3 查看來源 ↗"));
          answer.append(node("p", point.text), detail);
        }
      } else {
        answer.append(node("p", "目前已同步資料找不到明確說明。"));
      }
      if (result.partial) answer.append(node("p", "部分課程資料仍未讀取。", "insight-muted"));
    } catch (error) {
      answer.textContent = error.message;
    } finally {
      button.disabled = false;
    }
  }
  form?.addEventListener("submit", event => { event.preventDefault(); if (!button.disabled) search(); });
  document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(); });
  refresh();
}

const overviewPage = document.getElementById("courseOverviewPage");
if (overviewPage) initOverview(overviewPage);
