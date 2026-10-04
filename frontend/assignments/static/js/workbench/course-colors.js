const PALETTE = [
  "#3678d2", "#26946b", "#ca5872", "#8d64cb", "#c48827", "#26949d",
  "#b66c32", "#b94c9a", "#657bb5", "#799235", "#c36347", "#637b85",
];

export function courseColorKey(data) {
  const title = String(data.course || data.courseTitle || "").trim();
  if (data.semester === "custom" || String(data.uid || "").startsWith("custom|")) {
    return `custom:${title || "個人代辦"}`;
  }
  const id = String(data.courseId || String(data.uid || "").split("|")[0]).trim();
  return id ? `e3:${id}` : `course:${data.semester || "other"}|${title}`;
}

export function createCourseColorRegistry() {
  const colors = new Map();
  const used = new Set();
  let overflow = 0;
  const colorFor = (key) => {
    if (colors.has(key)) return colors.get(key);
    let hash = 0;
    for (const char of String(key)) hash = (Math.imul(hash, 31) + char.charCodeAt(0)) >>> 0;
    let color;
    for (let offset = 0; offset < PALETTE.length; offset += 1) {
      const candidate = PALETTE[(hash + offset) % PALETTE.length];
      if (!used.has(candidate)) { color = candidate; break; }
    }
    // Extend the palette rather than reusing a color in larger course catalogs.
    if (!color) {
      const hue = ((overflow++ * 137.508 + 18) % 360).toFixed(3);
      color = `hsl(${hue} 58% 47%)`;
    }
    colors.set(key, color);
    used.add(color);
    return color;
  };
  return {
    colorFor,
    ensure(keys) {
      [...new Set(keys)].sort().forEach(colorFor);
    },
  };
}

export function register(ctx) {
  const registry = createCourseColorRegistry();
  const applied = new WeakMap();
  ctx.courseAccent = (key) => registry.colorFor(key);
  ctx.syncCourseColors = () => {
    const elements = [...document.querySelectorAll("[data-course-id], tr[data-uid]")];
    // Seed from the complete catalog so searching or changing views cannot reassign colors.
    registry.ensure(elements.map((element) => courseColorKey(element.dataset)));
    elements.forEach((element) => {
      const color = registry.colorFor(courseColorKey(element.dataset));
      if (applied.get(element) !== color) {
        element.style.setProperty("--course-accent", color);
        applied.set(element, color);
      }
    });
  };
}

export function initialize(ctx) {}
