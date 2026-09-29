import * as feedback from "./feedback.js";
import * as cache from "./cache.js";
import * as filterState from "./filter-state.js";
import * as courseFilter from "./course-filter.js";
import * as localAssignments from "./local-assignments.js";
import * as filters from "./filters.js";
import * as calendar from "./calendar.js";
import * as interactions from "./interactions.js";
import * as cacheEvents from "./cache-events.js";
import * as calendarEvents from "./calendar-events.js";
import * as startup from "./startup.js";
import * as announcements from "./announcements.js";
import * as traffic from "./traffic.js";
import * as profile from "./profile.js";
import * as e3Navigation from "./e3-navigation.js";

const config = JSON.parse(
  document.getElementById("workbench-config").textContent,
);
const context = { config };
const features = [
  feedback,
  cache,
  filterState,
  courseFilter,
  localAssignments,
  filters,
  calendar,
  interactions,
  cacheEvents,
  calendarEvents,
  startup,
  announcements,
  traffic,
  profile,
  e3Navigation,
];

// Register callbacks before initializing state and wiring DOM listeners.
features.forEach((feature) => feature.register(context));
features.forEach((feature) => feature.initialize(context));
