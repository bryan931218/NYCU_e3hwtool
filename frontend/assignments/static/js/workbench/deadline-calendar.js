import { courseColorKey } from "./course-colors.js";

const TAIPEI = "Asia/Taipei";
const partsFormatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: TAIPEI, year: "numeric", month: "2-digit", day: "2-digit",
  hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23",
});

export function taipeiDeadline(ts) {
  const seconds = Number(ts);
  if (!Number.isFinite(seconds) || seconds <= 0 || seconds >= 9999999999) return null;
  const date = new Date(seconds * 1000);
  if (Number.isNaN(date.getTime())) return null;
  const parts = Object.fromEntries(partsFormatter.formatToParts(date).map((part) => [part.type, part.value]));
  const day = `${parts.year}-${parts.month}-${parts.day}`;
  const time = `${parts.hour}:${parts.minute}`;
  return { day, time, iso: `${day}T${time}:${parts.second}+08:00` };
}

export function calendarAssignments(rows) {
  const seen = new Set();
  return Array.from(rows).filter((row) => {
    const uid = row.dataset.uid;
    if (!uid || seen.has(uid) || row.classList.contains("hidden")) return false;
    seen.add(uid);
    return true;
  }).map((row) => {
    const deadline = row.dataset.hasDue === "1" ? taipeiDeadline(row.dataset.dueTs) : null;
    return {
      uid: row.dataset.uid,
      title: row.dataset.title || "未命名作業",
      course: row.dataset.course || "個人代辦",
      courseKey: courseColorKey(row.dataset),
      status: row.dataset.primaryStatus || "pending",
      deadline,
      dueTs: deadline ? Number(row.dataset.dueTs) : Infinity,
      url: row.querySelector("[data-e3-assignment]")?.getAttribute("href") || "",
      row,
    };
  }).sort((a, b) => a.dueTs - b.dueTs || a.title.localeCompare(b.title, "zh-Hant"));
}

export function safeCalendarUrl(value, base) {
  if (!value || value === "#") return "";
  try {
    const url = new URL(value, base);
    return ["https:", "http:"].includes(url.protocol) ? url.href : "";
  } catch { return ""; }
}

export function nextActionableAssignment(items, now = Date.now() / 1000) {
  return items.filter((item) => item.deadline && item.status === "pending" && item.dueTs >= now)
    .sort((a, b) => a.dueTs - b.dueTs || a.title.localeCompare(b.title, "zh-Hant"))[0] || null;
}

export function adjacentCalendarDay(day, offset) {
  const date = new Date(`${day}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + offset);
  return date.toISOString().slice(0, 10);
}

function element(tag, className, text) {
  const node = document.createElement(tag);
  node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function icon(name) {
  const node = element("span", "calendar-icon");
  node.dataset.icon = name;
  node.setAttribute("aria-hidden", "true");
  return node;
}

function action(name, label, attribute, uid) {
  const button = element("button", "calendar-icon-button");
  button.type = "button";
  button.title = label;
  button.setAttribute("aria-label", label);
  button.setAttribute(attribute, uid);
  button.append(icon(name));
  return button;
}

export function register(ctx) {
  ctx.calendarSelectedDay = taipeiDeadline(Date.now() / 1000).day;
  ctx.deadlineAssignments = [];
  ctx.calendarShowUndated = false;
  ctx.calendarHeight = () => window.innerWidth <= 600 ? 350 : Math.min(660, Math.max(520, (document.getElementById("assignmentCalendar")?.clientWidth || 800) * .72));

  ctx.paintCalendarDays = function paintCalendarDays() {
    const counts = new Map();
    const coursesByDay = new Map();
    ctx.deadlineAssignments.forEach((item) => {
      if (!item.deadline) return;
      const day = item.deadline.day;
      counts.set(day, (counts.get(day) || 0) + 1);
      if (!coursesByDay.has(day)) coursesByDay.set(day, new Map());
      coursesByDay.get(day).set(item.courseKey, item.course);
    });
    document.querySelectorAll("#assignmentCalendar [data-date]").forEach((cell) => {
      const day = cell.dataset.date;
      const count = counts.get(day) || 0;
      cell.classList.toggle("calendar-selected-day", !ctx.calendarShowUndated && day === ctx.calendarSelectedDay);
      const number = cell.querySelector(".fc-daygrid-day-number");
      number?.setAttribute("aria-pressed", String(!ctx.calendarShowUndated && day === ctx.calendarSelectedDay));
      number?.setAttribute("aria-label", `${day}，${count} 件作業`);
      cell.querySelector(".calendar-mobile-load")?.remove();
      if (count) {
        const marker = element("span", "calendar-mobile-load", String(count));
        marker.setAttribute("aria-hidden", "true");
        const courses = element("span", "calendar-mobile-courses");
        [...coursesByDay.get(day)].slice(0, 4).forEach(([key, title]) => {
          const dot = element("span", "calendar-course-dot");
          dot.title = title;
          dot.style.setProperty("--course-accent", ctx.courseAccent(key));
          courses.append(dot);
        });
        marker.prepend(courses);
        cell.querySelector(".fc-daygrid-day-frame")?.append(marker);
      }
    });
  };

  ctx.selectCalendarDay = function selectCalendarDay(day, uid = "") {
    ctx.calendarSelectedDay = day;
    ctx.calendarShowUndated = false;
    ctx.calendarSelectedUid = uid;
    if (ctx.deadlineCalendar && !day.startsWith(ctx.deadlineCalendar.getDate().toISOString().slice(0, 7))) {
      ctx.deadlineCalendar.gotoDate(day);
    }
    ctx.paintCalendarDays();
    ctx.renderCalendarAgenda();
  };

  ctx.renderCalendarAgenda = function renderCalendarAgenda() {
    const list = document.getElementById("calendarDayList");
    if (!list) return;
    const day = ctx.calendarSelectedDay;
    const undated = ctx.calendarShowUndated;
    const items = ctx.deadlineAssignments.filter((item) => undated ? !item.deadline : item.deadline?.day === day);
    const date = new Date(`${day}T12:00:00+08:00`);
    document.getElementById("calendarDayLabel").textContent = undated ? "無截止日期" : new Intl.DateTimeFormat("zh-TW", { timeZone: TAIPEI, month: "long", day: "numeric" }).format(date);
    document.getElementById("calendarDayWeekday").textContent = undated ? "作業清單" : new Intl.DateTimeFormat("zh-TW", { timeZone: TAIPEI, weekday: "long" }).format(date);
    document.getElementById("calendarDaySummary").textContent = `${items.length} 件作業`;
    document.getElementById("calendarUndated")?.setAttribute("aria-pressed", String(undated));
    const addButton = document.getElementById("calendarAddTask");
    if (addButton) addButton.hidden = undated;
    list.replaceChildren();
    if (!items.length) {
      const empty = element("div", "calendar-agenda-empty");
      empty.append(icon("calendar-days"), element("p", "", "沒有符合篩選的作業"));
      list.append(empty);
      return;
    }
    items.forEach((item) => {
      const article = element("article", "calendar-task");
      article.dataset.calendarUid = item.uid;
      article.dataset.status = item.status;
      article.style.setProperty("--course-accent", ctx.courseAccent(item.courseKey));
      article.classList.toggle("calendar-task-selected", ctx.calendarSelectedUid === item.uid);
      const top = element("div", "calendar-task-top");
      const course = element("span", "calendar-task-course", item.course);
      course.title = item.course;
      top.append(course, element("span", `calendar-task-status calendar-status-${item.status}`, ctx.STATUS_FILTER_LABELS[item.status] || "待處理"));
      const title = element("h4", "calendar-task-title", item.title);
      const bottom = element("div", "calendar-task-bottom");
      const time = element("time", "calendar-task-time", item.deadline?.time || "無截止日期");
      if (item.deadline) time.dateTime = item.deadline.iso;
      const actions = element("div", "calendar-task-actions");
      if (!ctx.IS_READONLY_VIEW) {
        actions.append(action("pencil", "修改繳交期限", "data-edit-due", item.uid));
        if (item.uid.startsWith("custom|")) {
          actions.append(action("trash-2", "刪除代辦", "data-delete-custom", item.uid));
        } else if (["pending", "overdue"].includes(item.status)) {
          const ignore = element("button", "calendar-ignore", "忽略");
          ignore.type = "button";
          ignore.dataset.ignoreAssignment = item.uid;
          actions.append(ignore);
        }
      }
      const url = safeCalendarUrl(item.url, window.location.href);
      if (url) {
        const link = element("a", "calendar-task-open", "前往 E3");
        link.href = url;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.setAttribute("data-e3-assignment", "");
        link.append(icon("arrow-up-right"));
        actions.append(link);
      }
      bottom.append(time, actions);
      article.append(top, title, bottom);
      list.append(article);
    });
    if (ctx.calendarSelectedUid) {
      Array.from(list.children).find((item) => item.dataset.calendarUid === ctx.calendarSelectedUid)?.scrollIntoView({ block: "nearest", behavior: "instant" });
    }
  };

  ctx.renderCalendarMonthSummary = function renderCalendarMonthSummary() {
    const calendar = ctx.deadlineCalendar;
    if (!calendar) return;
    const month = calendar.getDate().toISOString().slice(0, 7);
    const count = ctx.deadlineAssignments.filter((item) => item.deadline?.day.startsWith(month)).length;
    document.getElementById("calendarMonthLabel").textContent = `${Number(month.slice(0, 4))} 年 ${Number(month.slice(5))} 月`;
    document.getElementById("calendarMonthCount").textContent = `${count} 件到期`;
    document.getElementById("calendarMonthPicker").value = month;
    const legend = document.getElementById("calendarCourseLegend");
    const courses = new Map();
    ctx.deadlineAssignments.filter((item) => item.deadline?.day.startsWith(month)).forEach((item) => {
      if (!courses.has(item.courseKey)) courses.set(item.courseKey, { title: item.course, count: 0 });
      courses.get(item.courseKey).count += 1;
    });
    legend.replaceChildren(...[...courses].map(([key, course]) => {
      const label = element("span", "calendar-legend-course");
      label.style.setProperty("--course-accent", ctx.courseAccent(key));
      label.title = `${course.title}｜${course.count} 件到期`;
      label.append(element("span", "calendar-course-dot"), element("span", "calendar-legend-title", course.title));
      return label;
    }));
    legend.hidden = courses.size === 0;
    const next = nextActionableAssignment(ctx.deadlineAssignments);
    const nextButton = document.getElementById("calendarNextDeadline");
    nextButton.hidden = !next;
    if (next) {
      nextButton.title = `${next.title}｜${next.course}`;
      const time = document.getElementById("calendarNextDeadlineDate");
      time.textContent = `${Number(next.deadline.day.slice(5, 7))}/${Number(next.deadline.day.slice(8))} ${next.deadline.time}`;
      time.dateTime = next.deadline.iso;
    }
  };

  ctx.syncDeadlineCalendar = function syncDeadlineCalendar() {
    if (ctx.currentViewMode !== "calendar") return;
    const root = document.getElementById("assignmentCalendar");
    if (!root) return;
    ctx.deadlineAssignments = calendarAssignments(document.querySelectorAll("#flatTable tbody tr[data-uid]"));
    if (!ctx.deadlineCalendar) {
      if (!window.FullCalendar) {
        document.getElementById("calendarUnavailable").hidden = false;
        return;
      }
      ctx.deadlineCalendar = new window.FullCalendar.Calendar(root, {
        initialView: "dayGridMonth",
        initialDate: ctx.calendarSelectedDay,
        now: () => taipeiDeadline(Date.now() / 1000).iso,
        timeZone: TAIPEI,
        locale: "zh-tw",
        firstDay: 1,
        headerToolbar: false,
        height: ctx.calendarHeight(),
        fixedWeekCount: false,
        expandRows: true,
        dayMaxEvents: 2,
        eventOrder: "deadlineTs,title",
        eventTimeFormat: { hour: "2-digit", minute: "2-digit", hour12: false },
        dayHeaderContent: (arg) => ["日", "一", "二", "三", "四", "五", "六"][arg.date.getUTCDay()],
        dayCellContent: (arg) => String(arg.date.getUTCDate()),
        moreLinkContent: (arg) => `+${arg.num} 件`,
        moreLinkClick: (arg) => {
          ctx.selectCalendarDay(arg.date.toISOString().slice(0, 10));
          // Keep the complete list in the agenda instead of a second popover.
          return true;
        },
        dateClick: (arg) => ctx.selectCalendarDay(arg.dateStr.slice(0, 10)),
        eventClick: (arg) => ctx.selectCalendarDay(arg.event.extendedProps.day, arg.event.id),
        eventContent: (arg) => {
          const wrapper = element("span", "calendar-event-content");
          wrapper.append(element("span", "calendar-event-time", arg.event.extendedProps.time));
          wrapper.append(element("span", "calendar-event-title", arg.event.title));
          if (["overdue", "completed", "graded"].includes(arg.event.extendedProps.status)) {
            const status = element("span", `calendar-event-status calendar-status-${arg.event.extendedProps.status}`);
            status.setAttribute("aria-hidden", "true");
            wrapper.append(status);
          }
          return { domNodes: [wrapper] };
        },
        eventDidMount: (arg) => {
          const { course, courseKey, time, status } = arg.event.extendedProps;
          arg.el.title = `${time} ${arg.event.title}｜${course}｜${ctx.STATUS_FILTER_LABELS[status] || "待處理"}`;
          arg.el.style.setProperty("--course-accent", ctx.courseAccent(courseKey));
          arg.el.setAttribute("role", "button");
          arg.el.setAttribute("tabindex", "0");
          arg.el.setAttribute("aria-label", arg.el.title);
          arg.el.addEventListener("keydown", (event) => {
            if (event.key === "Enter" || event.key === " ") {
              event.preventDefault();
              ctx.selectCalendarDay(arg.event.extendedProps.day, arg.event.id);
            }
          });
        },
        dayCellDidMount: (arg) => {
          const number = arg.el.querySelector(".fc-daygrid-day-number");
          number?.setAttribute("role", "button");
          number?.setAttribute("tabindex", "0");
          number?.addEventListener("keydown", (event) => {
            if (event.key === "Enter" || event.key === " ") {
              event.preventDefault();
              ctx.selectCalendarDay(arg.el.dataset.date);
            }
            const offset = { ArrowLeft: -1, ArrowRight: 1, ArrowUp: -7, ArrowDown: 7 }[event.key];
            if (offset) {
              event.preventDefault();
              const day = adjacentCalendarDay(arg.el.dataset.date, offset);
              ctx.selectCalendarDay(day);
              root.querySelector(`[data-date="${day}"] .fc-daygrid-day-number`)?.focus({ preventScroll: true });
            }
          });
        },
        datesSet: () => {
          const month = ctx.deadlineCalendar?.getDate().toISOString().slice(0, 7);
          if (month && !ctx.calendarSelectedDay.startsWith(month)) {
            ctx.calendarSelectedDay = ctx.deadlineAssignments.find((item) => item.deadline?.day.startsWith(month))?.deadline.day || `${month}-01`;
            ctx.calendarSelectedUid = "";
            ctx.calendarShowUndated = false;
            ctx.renderCalendarAgenda();
          }
          ctx.renderCalendarMonthSummary();
          ctx.paintCalendarDays();
        },
      });
      ctx.deadlineCalendar.render();
    }
    const events = ctx.deadlineAssignments.filter((item) => item.deadline).map((item) => ({
      // A deadline is a point, not an hour-long event spilling into tomorrow.
      id: item.uid, title: item.title, start: item.deadline.day, allDay: true,
      classNames: [`calendar-event-${item.status}`],
      extendedProps: { course: item.course, courseKey: item.courseKey, day: item.deadline.day, time: item.deadline.time, status: item.status, deadlineTs: item.dueTs },
    }));
    ctx.deadlineCalendar.batchRendering(() => {
      ctx.deadlineCalendar.getEventSources().forEach((source) => source.remove());
      ctx.deadlineCalendar.addEventSource(events);
    });
    ctx.deadlineCalendar.setOption("height", ctx.calendarHeight());
    ctx.deadlineCalendar.updateSize();
    const undatedCount = ctx.deadlineAssignments.length - events.length;
    document.getElementById("calendarUndated").hidden = undatedCount === 0;
    document.getElementById("calendarUndatedCount").textContent = String(undatedCount);
    ctx.renderCalendarMonthSummary();
    ctx.paintCalendarDays();
    ctx.renderCalendarAgenda();
  };
}

export function initialize(ctx) {
  document.getElementById("calendarMonthPicker")?.addEventListener("change", (event) => {
    const month = event.target.value;
    if (!/^(19|20)\d{2}-(0[1-9]|1[0-2])$|^2100-(0[1-9]|1[0-2])$/.test(month)) return;
    ctx.selectCalendarDay(`${month}-01`);
  });
  document.getElementById("calendarNextDeadline")?.addEventListener("click", () => {
    const next = nextActionableAssignment(ctx.deadlineAssignments);
    if (next) ctx.selectCalendarDay(next.deadline.day, next.uid);
  });
  document.getElementById("calendarPrevious")?.addEventListener("click", () => ctx.deadlineCalendar?.prev());
  document.getElementById("calendarNext")?.addEventListener("click", () => ctx.deadlineCalendar?.next());
  document.getElementById("calendarToday")?.addEventListener("click", () => {
    const today = taipeiDeadline(Date.now() / 1000).day;
    ctx.deadlineCalendar?.gotoDate(today);
    ctx.selectCalendarDay(today);
  });
  document.getElementById("calendarUndated")?.addEventListener("click", () => {
    ctx.calendarShowUndated = !ctx.calendarShowUndated;
    ctx.calendarSelectedUid = "";
    ctx.paintCalendarDays();
    ctx.renderCalendarAgenda();
  });
  document.getElementById("calendarAddTask")?.addEventListener("click", () => {
    if (ctx.IS_READONLY_VIEW || !ctx.customAssignmentForm) return;
    ctx.customAssignmentForm.reset();
    ctx.customCourseInput.value = "自訂代辦";
    ctx.customDueInput.value = ctx.formatDatetimeLocalFromTs(Date.parse(`${ctx.calendarSelectedDay}T23:59:00+08:00`) / 1000);
    ctx.openModalRoot(ctx.customAssignmentModal);
    ctx.customTitleInput?.focus();
  });
  window.addEventListener("resize", () => {
    if (ctx.currentViewMode === "calendar") {
      ctx.deadlineCalendar?.setOption("height", ctx.calendarHeight());
      ctx.deadlineCalendar?.updateSize();
    }
  });
}
