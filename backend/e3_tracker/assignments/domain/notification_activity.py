"""Server-confirmed notification actions and non-sensitive setting changes."""

NOTIFICATION_ACTION_LABELS = {
    "notification_settings_updated": "更新通知設定",
    "notification_browser_enabled": "啟用瀏覽器通知裝置",
    "notification_browser_disabled": "移除瀏覽器通知裝置",
    "notification_line_link_started": "產生 LINE 綁定碼",
    "notification_line_linked": "LINE 綁定成功",
    "notification_line_unlinked": "解除 LINE 綁定",
    "notification_test_sent": "送出測試通知",
    "notification_test_failed": "測試通知傳送失敗",
}


def notification_setting_changes(before, after):
    labels = {
        "browser_enabled": "瀏覽器通知",
        "line_enabled": "LINE 通知",
        "new_assignment": "新作業通知",
        "new_announcement": "課程公告通知",
        "new_mail": "課程信件通知",
        "deadline_changes": "期限異動提醒",
        "due_reminder": "到期提醒",
    }
    changes = [
        f"{label}：{'啟用' if after.get(key, False) else '關閉'}"
        for key, label in labels.items() if before.get(key, False) != after.get(key, False)
    ]
    if before.get("days_before") != after["days_before"]:
        days = "、".join(str(day) for day in after["days_before"])
        changes.append(f"到期前 {days} 天提醒")
    return "；".join(changes)
