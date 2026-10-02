const ENDPOINT = "/api/notifications/custom-todo";

export function register(ctx) {
  ctx.syncCustomTodoNotification = async function syncCustomTodoNotification(item) {
    if (ctx.IS_GUEST || ctx.IS_READONLY_VIEW || !item?.uid) return;
    try {
      const response = await fetch(ENDPOINT, {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          uid: item.uid,
          course: item.course || "自訂代辦",
          title: item.title || "",
          due_ts: Number(item.due_ts || 0),
        }),
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const data = await response.json();
      if (!data.ok) throw new Error(data.message || "sync failed");
    } catch (err) {
      console.warn("Custom todo notification sync failed:", err?.message || err);
    }
  };

  ctx.cancelCustomTodoNotification = async function cancelCustomTodoNotification(uid) {
    if (ctx.IS_GUEST || ctx.IS_READONLY_VIEW || !uid) return;
    try {
      const response = await fetch(ENDPOINT, {
        method: "DELETE",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ uid }),
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
    } catch (err) {
      console.warn("Custom todo notification cancel failed:", err?.message || err);
    }
  };

  ctx.syncAllCustomTodoNotifications = function syncAllCustomTodoNotifications() {
    if (ctx.IS_GUEST || ctx.IS_READONLY_VIEW) return;
    (ctx.customAssignments || []).forEach((item) => {
      void ctx.syncCustomTodoNotification(item);
    });
  };

  const addCustomAssignment = ctx.addCustomAssignment;
  ctx.addCustomAssignment = function addCustomAssignmentWithNotification(...args) {
    const result = addCustomAssignment(...args);
    if (result) {
      const item = ctx.customAssignments?.[ctx.customAssignments.length - 1];
      if (item) void ctx.syncCustomTodoNotification(item);
    }
    return result;
  };

  const deleteCustomAssignment = ctx.deleteCustomAssignment;
  ctx.deleteCustomAssignment = function deleteCustomAssignmentWithNotification(uid) {
    const result = deleteCustomAssignment(uid);
    void ctx.cancelCustomTodoNotification(uid);
    return result;
  };

  const applyDueOverride = ctx.applyDueOverride;
  ctx.applyDueOverride = function applyDueOverrideWithNotification(uid, dueTs) {
    const result = applyDueOverride(uid, dueTs);
    if (String(uid || "").startsWith("custom|")) {
      // The existing submit handler writes due_ts back to customAssignments just after
      // applyDueOverride returns. Queue this to run after that synchronous update.
      setTimeout(() => {
        const item = (ctx.customAssignments || []).find(
          (candidate) => candidate.uid === uid,
        );
        if (item) void ctx.syncCustomTodoNotification(item);
      }, 0);
    }
    return result;
  };
}

export function initialize(ctx) {
  if (ctx.IS_GUEST || ctx.IS_READONLY_VIEW) return;
  // Reconcile browser-local todos whenever the workbench opens. This also picks up
  // changed LINE/Web Push settings without moving the todo data out of localStorage.
  setTimeout(() => ctx.syncAllCustomTodoNotifications(), 0);
}
