const ENDPOINT = "/api/custom-todos";
const REFRESH_INTERVAL_MS = 120000;

async function api(method = "GET", body) {
  const response = await fetch(ENDPOINT, {
    method,
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  if (response.redirected) throw new Error("登入已失效，請重新登入");
  const data = await response.json();
  if (!response.ok || !data.ok) {
    throw new Error(data.message || "自訂待辦同步失敗");
  }
  return data;
}

export function register(ctx) {
  const localSaveCustomAssignments = ctx.saveCustomAssignments;
  ctx.saveCustomAssignments = function saveCustomAssignments() {
    if (ctx.IS_GUEST) localSaveCustomAssignments();
  };

  const applyDueOverrides = ctx.applyDueOverrides;
  ctx.applyDueOverrides = function applyServerSafeDueOverrides() {
    if (!ctx.IS_GUEST && ctx.dueOverrides) {
      let changed = false;
      Object.keys(ctx.dueOverrides).forEach((uid) => {
        if (uid.startsWith("custom|")) {
          delete ctx.dueOverrides[uid];
          changed = true;
        }
      });
      if (changed) ctx.saveDueOverrides();
    }
    return applyDueOverrides();
  };

  ctx.persistCustomTodo = async function persistCustomTodo(item) {
    if (ctx.IS_GUEST || ctx.IS_READONLY_VIEW || !item?.uid) return null;
    const data = await api("POST", {
      uid: item.uid,
      course: item.course || "自訂代辦",
      title: item.title || "",
      due_ts: Number(item.due_ts || 0),
    });
    return data.item || null;
  };

  ctx.removeRemoteCustomTodo = async function removeRemoteCustomTodo(uid) {
    if (ctx.IS_GUEST || ctx.IS_READONLY_VIEW || !uid) return;
    await api("DELETE", { uid });
  };

  ctx.loadRemoteCustomTodos = async function loadRemoteCustomTodos() {
    if (ctx.IS_GUEST || ctx.IS_READONLY_VIEW) return;
    const data = await api("GET");
    const items = Array.isArray(data.items) ? data.items : [];
    if (JSON.stringify(items) === JSON.stringify(ctx.customAssignments)) return;
    ctx.customAssignments = items;
    ctx.renderCustomAssignments();
    ctx.applyFilters?.();
  };

  const addCustomAssignment = ctx.addCustomAssignment;
  ctx.addCustomAssignment = function addSyncedCustomAssignment(...args) {
    const result = addCustomAssignment(...args);
    if (!result || ctx.IS_GUEST || ctx.IS_READONLY_VIEW) return result;
    const item = ctx.customAssignments?.[ctx.customAssignments.length - 1];
    if (item) {
      void ctx.persistCustomTodo(item).catch((error) => {
        ctx.showToast?.(`跨裝置同步失敗：${error.message}`, "error");
        void ctx.loadRemoteCustomTodos().catch(() => {});
      });
    }
    return result;
  };

  const deleteCustomAssignment = ctx.deleteCustomAssignment;
  ctx.deleteCustomAssignment = function deleteSyncedCustomAssignment(uid) {
    const result = deleteCustomAssignment(uid);
    if (!ctx.IS_GUEST && !ctx.IS_READONLY_VIEW) {
      void ctx.removeRemoteCustomTodo(uid).catch((error) => {
        ctx.showToast?.(`刪除同步失敗：${error.message}`, "error");
        void ctx.loadRemoteCustomTodos().catch(() => {});
      });
    }
    return result;
  };

  const applyDueOverride = ctx.applyDueOverride;
  ctx.applyDueOverride = function applySyncedDueOverride(uid, dueTs) {
    if (ctx.IS_GUEST || !String(uid || "").startsWith("custom|")) {
      return applyDueOverride(uid, dueTs);
    }

    const dueText = ctx.formatDueDisplayFromTs(dueTs);
    document
      .querySelectorAll(`tr[data-uid="${ctx.cssEscapeValue(uid)}"]`)
      .forEach((row) => ctx.setRowDue(row, dueTs, dueText, "自訂期限"));
    ctx.applyFilters();

    setTimeout(() => {
      const item = (ctx.customAssignments || []).find(
        (candidate) => candidate.uid === uid,
      );
      if (!item) return;
      void ctx.persistCustomTodo(item).catch((error) => {
        ctx.showToast?.(`期限同步失敗：${error.message}`, "error");
        void ctx.loadRemoteCustomTodos().catch(() => {});
      });
    }, 0);
    return true;
  };
}

export function initialize(ctx) {
  if (ctx.IS_GUEST || ctx.IS_READONLY_VIEW) return;

  const legacyItems = Array.isArray(ctx.customAssignments)
    ? ctx.customAssignments.filter(
        (item) => item?.uid && item?.title && Number(item?.due_ts) > 0,
      )
    : [];

  // Logged-in accounts use the server as the source of truth. Clear the temporary
  // browser copy immediately so another device's deleted items cannot reappear.
  ctx.customAssignments = [];
  ctx.renderCustomAssignments();

  const migrationKey = `e3_custom_assignments_server_migrated_${ctx.STORAGE_USER_KEY}`;

  const bootstrap = async () => {
    let migrated = false;
    try {
      migrated = localStorage.getItem(migrationKey) === "1";
    } catch (err) {}

    if (!migrated) {
      try {
        for (const item of legacyItems) {
          await ctx.persistCustomTodo(item);
        }
        try {
          localStorage.setItem(migrationKey, "1");
          localStorage.removeItem(ctx.CUSTOM_ASSIGNMENTS_KEY);
        } catch (err) {}
      } catch (error) {
        ctx.showToast?.(`舊待辦搬移失敗：${error.message}`, "error");
      }
    }

    try {
      await ctx.loadRemoteCustomTodos();
    } catch (error) {
      ctx.showToast?.(`無法載入跨裝置待辦：${error.message}`, "error");
    }
  };

  void bootstrap();

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      void ctx.loadRemoteCustomTodos().catch(() => {});
    }
  });

  setInterval(() => {
    if (!document.hidden) {
      void ctx.loadRemoteCustomTodos().catch(() => {});
    }
  }, REFRESH_INTERVAL_MS);
}
