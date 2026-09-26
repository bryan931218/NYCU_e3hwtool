export function register(ctx) {
  ctx.fetchAndSwapContent = async function fetchAndSwapContent() {
    const contentResponse = await fetch(window.location.href);
    if (!contentResponse.ok) {
      throw new Error("無法獲取更新後的內容");
    }

    const html = await contentResponse.text();
    const parser = new DOMParser();
    const doc = parser.parseFromString(html, "text/html");

    const docBody = doc.querySelector("body");
    if (docBody) {
      const nextTs = Number(docBody.dataset.cacheTs || "0");
      if (!Number.isNaN(nextTs)) {
        ctx.currentCacheTs = nextTs;
      }
    }

    const newViewDue = doc.getElementById("viewDue");
    if (newViewDue && ctx.viewDue) {
      ctx.viewDue.innerHTML = newViewDue.innerHTML;
    }

    const newViewCourse = doc.getElementById("viewCourse");
    if (newViewCourse && ctx.viewCourse) {
      ctx.viewCourse.innerHTML = newViewCourse.innerHTML;
    }
    if (!doc.getElementById("workspaceEmpty")) {
      document.getElementById("workspaceEmpty")?.remove();
    }

    const nextCourseFilter = doc.getElementById("courseFilter");
    if (ctx.courseFilter && nextCourseFilter) {
      ctx.courseFilter.innerHTML = nextCourseFilter.innerHTML;
      ctx.courseFilter.value = ctx.currentCourseFilter;
      if (ctx.courseFilter.selectedIndex < 0) {
        ctx.currentCourseFilter = "";
        ctx.courseFilter.value = "";
      }
    }

    const currentSemesterSelector = document.getElementById("semesterSelector");
    const newSemesterSelector = doc.getElementById("semesterSelector");
    if (currentSemesterSelector && newSemesterSelector) {
      currentSemesterSelector.replaceWith(newSemesterSelector.cloneNode(true));
    } else if (!currentSemesterSelector && newSemesterSelector) {
      const actions = document.querySelector(".workspace-actions");
      if (actions) actions.prepend(newSemesterSelector.cloneNode(true));
    } else if (currentSemesterSelector && !newSemesterSelector) {
      currentSemesterSelector.remove();
    }
    const refreshedSemesterFilters = ctx.readCheckedSemesterFilters();
    if (refreshedSemesterFilters.length) {
      ctx.currentSemesterFilters = refreshedSemesterFilters;
      ctx.USER_PREFERENCES.semester_filter = refreshedSemesterFilters.slice();
    }

    const newTotalCount = doc.getElementById("totalCount");
    const newIgnoredCount = doc.getElementById("ignoredCount");

    ["siteOnlineCount", "siteTotalCount", "siteLastUpdated"].forEach((id) => {
      const current = document.getElementById(id);
      const next = doc.getElementById(id);
      if (current && next) current.textContent = next.textContent;
    });

    if (newTotalCount && ctx.totalCountEl)
      ctx.totalCountEl.textContent = newTotalCount.textContent;
    if (newIgnoredCount && ctx.ignoredCountEl)
      ctx.ignoredCountEl.textContent = newIgnoredCount.textContent;

    const excelLink = document.querySelector(".tc-center a[download]");
    const newExcelLink = doc.querySelector(".tc-center a[download]");
    if (excelLink && newExcelLink) {
      excelLink.href = newExcelLink.href;
    } else if (newExcelLink) {
      const tcCenter = document.querySelector(".tc-center");
      if (tcCenter) {
        tcCenter.innerHTML = doc.querySelector(".tc-center").innerHTML;
      }
    }

    ctx.hydrateLocalAssignments();
    ctx.sortFlatTableAsc();
    ctx.applyFilters();
    void ctx.refreshUserAvatar?.(true);
  };

  ctx.fetchCacheStatus = async function fetchCacheStatus() {
    const resp = await fetch(ctx.CACHE_SYNC_ENDPOINT || ctx.config.cacheUrl, {
      cache: "no-store",
      credentials: "same-origin",
    });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    const data = await resp.json();
    if (!data.ok) throw new Error(data.error || "cache error");
    return data;
  };

  ctx.waitForCacheUpdate = async function waitForCacheUpdate(
    prevTs,
    timeoutMs = 300000,
    intervalMs = 2500,
  ) {
    const start = Date.now();
    let lastStatus = null;
    while (Date.now() - start < timeoutMs) {
      try {
        const status = await ctx.fetchCacheStatus();
        lastStatus = status;
        if (status.refresh_status === "error") {
          throw new Error(status.refresh_error || "背景更新失敗");
        }
        const ts = Number(status.ts || 0);
        if (!Number.isNaN(ts) && ts > prevTs) {
          return { ts, pending: false };
        }
        if (
          status.refresh_status === "success" &&
          !status.refresh_in_progress &&
          !Number.isNaN(ts) &&
          ts >= prevTs
        ) {
          return { ts, pending: false };
        }
      } catch (err) {
        if (lastStatus && lastStatus.refresh_status === "error") {
          throw err;
        }
        console.error("poll cache failed:", err.message);
      }
      await new Promise((res) => setTimeout(res, intervalMs));
    }
    if (lastStatus && lastStatus.refresh_in_progress) {
      return { ts: Number(lastStatus.ts || 0), pending: true };
    }
    throw new Error("等待更新逾時");
  };

  ctx.refreshAssignments = async function refreshAssignments(
    background = true,
    semesterFilters = ctx.currentSemesterFilters,
    includeArchived = false,
  ) {
    if (ctx.refreshInFlight) {
      return;
    }
    ctx.refreshInFlight = true;
    let retryAfterCurrentRefresh = false;
    const normalizedSemesters = ctx.normalizeSemesterFilters(semesterFilters);
    document
      .querySelectorAll("#manualRefreshBtn, #archiveRefreshBtn")
      .forEach((button) => {
        button.disabled = true;
      });
    try {
      if (background) {
        ctx.bgRefreshIndicator &&
          ctx.bgRefreshIndicator.classList.add("active");
      } else {
        ctx.showLoader("資料更新中，請稍候...");
      }

      const requestPayload = normalizedSemesters.length
        ? { semesterFilters: normalizedSemesters }
        : {};
      if (includeArchived) requestPayload.includeArchived = true;
      const r = await fetch(ctx.config.assignmentsUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(requestPayload),
      });

      if (!r.ok) {
        throw new Error("HTTP " + r.status);
      }

      const response = await r.json();
      if (response.ok) {
        retryAfterCurrentRefresh =
          !!response.in_progress && normalizedSemesters.length > 0;
        const prevTs = Number(ctx.currentCacheTs || 0);
        if (response.background) {
          const updateResult = await ctx.waitForCacheUpdate(prevTs);
          if (updateResult.pending) {
            ctx.showToast("背景更新仍在進行，完成後會自動同步畫面。", "info");
            return;
          }
          ctx.currentCacheTs = updateResult.ts;
        }
        await ctx.fetchAndSwapContent();
        if (!retryAfterCurrentRefresh) {
          ctx.showToast(
            includeArchived ? "舊學期已載入" : "資料已成功更新",
            "success",
          );
        }
      } else {
        throw new Error("更新失敗");
      }
    } catch (e) {
      if (background) {
        ctx.bgRefreshIndicator &&
          ctx.bgRefreshIndicator.classList.remove("active");
        console.error("背景更新失敗：", e.message);
        ctx.showToast("背景更新失敗：" + e.message, "error");
      } else {
        ctx.hideLoader();
        ctx.showToast("更新失敗：" + e.message, "error");
      }
    } finally {
      ctx.refreshInFlight = false;
      document
        .querySelectorAll("#manualRefreshBtn, #archiveRefreshBtn")
        .forEach((button) => {
          button.disabled = false;
        });
      if (background) {
        ctx.bgRefreshIndicator &&
          ctx.bgRefreshIndicator.classList.remove("active");
      } else {
        ctx.hideLoader();
      }
      if (retryAfterCurrentRefresh) {
        setTimeout(
          () =>
            ctx.refreshAssignments(
              background,
              normalizedSemesters,
              includeArchived,
            ),
          100,
        );
      }
    }
  };
}

export function initialize(ctx) {}
