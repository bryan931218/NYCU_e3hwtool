import { courseColorKey } from "./course-colors.js";

export function semesterCourseTitles(courses, semesters) {
  const selected = new Set(semesters);
  return [
    ...new Set(
      courses
        .filter((course) => selected.has(course.semester || "other"))
        .map((course) => String(course.courseTitle || "").trim())
        .filter(Boolean),
    ),
  ];
}

export function register(ctx) {
  ctx.syncCourseFilter = function syncCourseFilter() {
    if (!ctx.courseFilter) return;
    // Course cards include courses without assignments and survive status filtering.
    const courses = Array.from(
      document.querySelectorAll("#viewCourse .course-card"),
    )
      .filter((card) => card.dataset.semester !== "custom")
      .map((card) => card.dataset);
    const titles = semesterCourseTitles(courses, ctx.currentSemesterFilters);
    const options = [
      ["", "全部課程"],
      ...titles.map((title) => [title, title]),
      ["custom", "個人代辦"],
    ];
    const value = options.some(([key]) => key === ctx.currentCourseFilter)
      ? ctx.currentCourseFilter
      : "";
    const current = Array.from(ctx.courseFilter.options);
    const unchanged =
      current.length === options.length &&
      current.every(
        (option, index) =>
          option.value === options[index][0] &&
          option.textContent === options[index][1],
      );
    if (!unchanged) {
      ctx.courseFilter.replaceChildren(
        ...options.map(([key, label]) => {
          const option = document.createElement("option");
          option.value = key;
          option.textContent = label;
          return option;
        }),
      );
    }
    ctx.currentCourseFilter = value;
    ctx.courseFilter.value = value;
    ctx.syncCoursePicker?.();
  };
}

export function initialize(ctx) {
  const select = ctx.courseFilter;
  const toggle = document.getElementById("courseFilterToggle");
  const list = document.getElementById("courseFilterOptions");
  const label = document.getElementById("courseFilterLabel");
  if (!select || !toggle || !list || typeof list.showPopover !== "function") return;
  const isOpen = () => list.matches(":popover-open");
  const options = () => Array.from(list.querySelectorAll('[role="option"]'));
  const position = () => {
    const rect = toggle.getBoundingClientRect();
    const width = Math.min(520, window.innerWidth - 32);
    const below = window.innerHeight - rect.bottom - 16;
    const above = rect.top - 16;
    const upwards = below < 180 && above > below;
    Object.assign(list.style, {
      width: `${width}px`, left: `${Math.max(16, Math.min(rect.left, window.innerWidth - width - 16))}px`,
      maxHeight: `${Math.max(80, Math.min(340, upwards ? above : below))}px`,
      top: upwards ? "auto" : `${rect.bottom + 6}px`,
      bottom: upwards ? `${window.innerHeight - rect.top + 6}px` : "auto",
    });
  };
  let signature = "";
  ctx.syncCoursePicker = () => {
    const entries = Array.from(select.options);
    const next = JSON.stringify(entries.map(option => [option.value, option.textContent]));
    if (next !== signature) {
      const focusedValue = list.contains(document.activeElement) ? document.activeElement.dataset.value : undefined;
      const cards = ctx.coursePickerCourses?.() || Array.from(document.querySelectorAll("#viewCourse .course-card"));
      const nodes = entries.map(option => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "course-option";
        button.dataset.value = option.value;
        button.setAttribute("role", "option");
        button.tabIndex = -1;
        const swatch = document.createElement("span");
        swatch.className = "course-option-swatch";
        swatch.setAttribute("aria-hidden", "true");
        const card = cards.find(card => option.value === "custom" ? card.dataset.semester === "custom" : card.dataset.courseTitle === option.value);
        if (card && ctx.courseAccent) swatch.style.backgroundColor = ctx.courseAccent(courseColorKey(card.dataset));
        const text = document.createElement("span");
        text.textContent = option.textContent;
        button.append(swatch, text);
        return button;
      });
      list.replaceChildren(...nodes);
      signature = next;
      if (isOpen() && focusedValue !== undefined) {
        (nodes.find(node => node.dataset.value === focusedValue) || nodes[0])?.focus({preventScroll: true});
      }
    }
    const current = entries.find(option => option.value === select.value);
    label.textContent = current?.textContent || "全部課程";
    toggle.title = label.textContent;
    options().forEach(option => {
      const selected = option.dataset.value === select.value;
      option.setAttribute("aria-selected", String(selected));
      option.tabIndex = selected ? 0 : -1;
    });
  };
  const open = (last = false) => {
    ctx.syncCoursePicker();
    position();
    if (!isOpen()) list.showPopover();
    const items = options();
    (items.find(option => option.dataset.value === select.value) || items[last ? items.length - 1 : 0])?.focus({preventScroll: true});
  };
  toggle.addEventListener("click", () => { if (isOpen()) list.hidePopover(); else open(); });
  toggle.addEventListener("keydown", event => {
    if (!["ArrowDown", "ArrowUp"].includes(event.key)) return;
    event.preventDefault();
    open(event.key === "ArrowUp");
  });
  list.addEventListener("toggle", () => toggle.setAttribute("aria-expanded", String(isOpen())));
  list.addEventListener("click", event => {
    const option = event.target.closest('[role="option"]');
    if (!option) return;
    select.value = option.dataset.value;
    list.hidePopover();
    toggle.focus({preventScroll: true});
    select.dispatchEvent(new Event("change", {bubbles: true}));
    ctx.syncCoursePicker();
  });
  let prefix = "";
  let lastKey = 0;
  list.addEventListener("keydown", event => {
    const items = options();
    const index = Math.max(0, items.indexOf(document.activeElement));
    let target;
    if (event.key === "Escape") {
      event.preventDefault();
      list.hidePopover();
      toggle.focus({preventScroll: true});
      return;
    }
    if (event.key === "ArrowDown") target = items[(index + 1) % items.length];
    if (event.key === "ArrowUp") target = items[(index - 1 + items.length) % items.length];
    if (event.key === "Home") target = items[0];
    if (event.key === "End") target = items.at(-1);
    if (event.key.length === 1 && event.key !== " " && !event.ctrlKey && !event.metaKey && !event.altKey) {
      prefix = (Date.now() - lastKey < 700 ? prefix : "") + event.key.toLocaleLowerCase();
      lastKey = Date.now();
      target = items.find(item => item.textContent.toLocaleLowerCase().startsWith(prefix));
    }
    if (target) { event.preventDefault(); target.focus(); }
  });
  document.addEventListener("focusin", event => {
    if (isOpen() && event.target !== toggle && !list.contains(event.target)) list.hidePopover();
  });
  window.addEventListener("resize", () => { if (isOpen()) position(); });
  window.addEventListener("scroll", () => { if (isOpen()) list.hidePopover(); }, {passive: true});
  toggle.hidden = false;
  list.hidden = false;
  select.hidden = true;
  select.setAttribute("aria-hidden", "true");
  ctx.syncCoursePicker();
}
