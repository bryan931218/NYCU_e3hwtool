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
  };
}

export function initialize(ctx) {}
