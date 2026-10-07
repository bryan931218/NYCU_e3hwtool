"""Fixed, non-sensitive assignment feature names for aggregate telemetry."""

from e3_tracker.platform.services.account_identity import student_identity

FEATURE_LABELS = {
    "due_view": "到期日列表", "course_view": "依課程列表", "calendar": "作業日曆",
    "search": "作業搜尋", "filters": "課程／狀態／學期篩選",
    "deadline_edit": "修改繳交期限", "ignore": "忽略／恢復作業",
    "custom_todo": "自訂代辦", "open_e3": "前往 E3",
    "refresh": "手動更新作業", "excel": "匯出 Excel",
    "ics": "匯出日曆", "google_sync": "Google 日曆同步",
    "notification_settings": "通知設定",
}
ACTION_FEATURES = {
    **{f"usage_{key}": key for key in FEATURE_LABELS},
    "download_excel": "excel", "export_calendar": "ics", "google_sync": "google_sync",
}


def feature_for_event(action, status, meta):
    if status not in {"success", "info"} or meta.get("is_guest"):
        return None
    if meta.get("site") not in {None, "", "assignments"}:
        return None
    return ACTION_FEATURES.get(action)
