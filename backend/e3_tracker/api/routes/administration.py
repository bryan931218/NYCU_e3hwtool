"""Administration routes and their feature helpers."""

from ...services.traffic import PASSIVE_TRAFFIC_ACTIONS
import json
import time
from collections import Counter
from datetime import datetime
from typing import Dict, List, Optional, Set
from flask import flash, redirect, render_template_string, request, url_for
from ...services.google_calendar import GOOGLE_CALENDAR_SCOPE
from ...shared.constants import TAIPEI_TZ


def register_administration_routes(*,
    ADMIN_FEEDBACK_TEMPLATE,
    ANNOUNCEMENTS_TEMPLATE,
    FEEDBACK_TEMPLATE,
    HOME_TEMPLATE,
    TRAFFIC_TEMPLATE,
    WEB_TEMPLATE,
    _build_dashboard_context,
    add_announcement,
    add_feedback_entry,
    app,
    app_home_url,
    current_stats_version,
    current_user,
    delete_announcement_entry,
    list_admin_view_options,
    list_feedback_entries,
    load_announcements,
    login_required,
    record_ui_event,
    storage,
    support_email,
    traffic_tracker,
    update_feedback_status_entry,
    usage_stats,
):
    @app.route("/admin/traffic", methods=["GET"])
    @login_required
    def admin_traffic():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員瀏覽流量資訊。", "error")
            return redirect(url_for("index"))
        admin_view_options = list_admin_view_options()
        selected_view_username = user["username"]
        requested_view_username = (request.args.get("view_user") or "").strip()
        if requested_view_username:
            valid_usernames = {item["username"] for item in admin_view_options}
            if requested_view_username in valid_usernames:
                selected_view_username = requested_view_username

        def _fmt_ts(ts: Optional[float]) -> str:
            if not ts:
                return "-"
            try:
                dt = datetime.fromtimestamp(ts, tz=TAIPEI_TZ)
            except Exception:
                return "-"
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        ACTION_LABELS = {
            "login_success": "登入成功",
            "logout": "登出",
            "guest_login": "訪客登入",
            "guest_import": "匯入訪客資料",
            "refresh_assignments": "更新作業資料",
            "ui-event": "操作事件",
        }

        def _action_description(action: str) -> str:
            action = action or "-"
            return ACTION_LABELS.get(action, action.replace("_", " "))

        user_breakdown = traffic_tracker.user_breakdown()
        ip_overview = traffic_tracker.ip_summary()
        guest_overview = traffic_tracker.guest_summary()
        formatted_users = [
            {
                "username": entry["username"],
                "count": entry["count"],
                "online": entry["online"],
                "last_seen": _fmt_ts(entry.get("last_seen")),
            }
            for entry in user_breakdown
        ]
        raw_events = traffic_tracker.recent_events(500)
        filtered_events = [
            ev
            for ev in raw_events
            if (ev.get("action") or "").lower() not in PASSIVE_TRAFFIC_ACTIONS
        ]
        formatted_events = []
        for ev in reversed(filtered_events[-200:]):
            meta = ev.get("meta") or {}
            role = "訪客" if meta.get("is_guest") else ("管理員" if meta.get("is_admin") else "一般使用者")
            detail_parts: List[str] = []
            for key in ("info", "course", "message", "target", "action_detail"):
                val = meta.get(key)
                if val:
                    detail_parts.append(f"{key}: {val}")
            extra = {
                key: value
                for key, value in meta.items()
                if key
                not in {"username", "is_guest", "is_admin", "info", "course", "message", "target", "action_detail"}
            }
            if extra:
                try:
                    detail_parts.append(json.dumps(extra, ensure_ascii=False))
                except Exception:
                    detail_parts.append(str(extra))
            formatted_events.append(
                {
                    "ts": _fmt_ts(ev.get("ts")),
                    "ip": ev.get("ip") or "-",
                    "action": ev.get("action") or "-",
                    "status": ev.get("status") or "info",
                    "username": meta.get("username") or "-",
                    "description": _action_description(ev.get("action") or "-"),
                    "details": "；".join(detail_parts),
                }
            )
        trend_window = request.args.get("trend", "hour")
        if trend_window not in {"hour", "day"}:
            trend_window = "hour"
        hourly_series = traffic_tracker.hourly_series()
        if trend_window == "day":
            daily_map: Dict[int, Set[str]] = {}
            for ts, members in traffic_tracker.hourly_buckets().items():
                day_dt = datetime.fromtimestamp(ts, tz=TAIPEI_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
                day_ts = int(day_dt.timestamp())
                day_set = daily_map.setdefault(day_ts, set())
                day_set.update(members)

            if daily_map:
                sorted_days_keys = sorted(daily_map.keys())
                min_day_ts = sorted_days_keys[0]
                max_day_ts = sorted_days_keys[-1]

                full_daily_series = []
                current_ts = min_day_ts
                while current_ts <= max_day_ts:
                    full_daily_series.append({
                        "ts": current_ts,
                        "count": len(daily_map.get(current_ts, []))
                    })
                    current_ts += 86400

                chart_labels = [datetime.fromtimestamp(item["ts"], tz=TAIPEI_TZ).strftime("%Y-%m-%d") for item in full_daily_series]
                chart_values = [item["count"] for item in full_daily_series]
            else:
                chart_labels = []
                chart_values = []
        else:
            if hourly_series:
                full_series = []
                series_dict = {item["ts"]: item["count"] for item in hourly_series}
                min_ts = hourly_series[0]["ts"]
                max_ts = hourly_series[-1]["ts"]

                current_ts = min_ts
                while current_ts <= max_ts:
                    full_series.append({
                        "ts": current_ts,
                        "count": series_dict.get(current_ts, 0)
                    })
                    current_ts += 3600

                hourly_series = full_series

            chart_labels = [datetime.fromtimestamp(item["ts"], tz=TAIPEI_TZ).strftime("%m-%d %H:00") for item in hourly_series]
            chart_values = [item["count"] for item in hourly_series]
        if not chart_labels or not chart_values:
            # fallback to on-the-fly aggregation of filtered events to avoid空白圖
            buckets: Dict[datetime, Set[str]] = {}
            for ev in filtered_events:
                ts = ev.get("ts")
                if not ts:
                    continue
                meta = ev.get("meta") or {}
                username = meta.get("username")
                if not username or meta.get("is_guest"):
                    continue
                try:
                    dt = datetime.fromtimestamp(float(ts), tz=TAIPEI_TZ)
                except Exception:
                    continue
                action = (ev.get("action") or "").lower()
                if action in PASSIVE_TRAFFIC_ACTIONS:
                    continue
                if trend_window == "day":
                    bucket = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    bucket = dt.replace(minute=0, second=0, microsecond=0)
                bucket_set = buckets.setdefault(bucket, set())
                bucket_set.add(str(username))
            sorted_keys = sorted(buckets.keys())
            chart_labels = [
                key.strftime("%Y-%m-%d") if trend_window == "day" else key.strftime("%m-%d %H:00")
                for key in sorted_keys
            ]
            chart_values = [len(buckets[key]) for key in sorted_keys]
        action_counter: Counter = Counter()
        for ev in filtered_events:
            action_counter[ev.get("action") or "-"] += 1
        top_actions = [{"action": action, "count": count} for action, count in action_counter.most_common(5)]
        recent_unique_keys = set()
        for ev in raw_events:
            meta = ev.get("meta") or {}
            username = meta.get("username")
            if username and not meta.get("is_guest"):
                recent_unique_keys.add(username)
            elif not username and ev.get("ip"):
                recent_unique_keys.add(ev.get("ip"))
        summary = {
            "unique_users": len(formatted_users),
            "online_users": sum(1 for entry in formatted_users if entry["online"]),
            "recent_unique_users": len(recent_unique_keys),
            "last_event": _fmt_ts(filtered_events[-1].get("ts")) if filtered_events else "-",
            "last_action": (filtered_events[-1].get("action") or "-") if filtered_events else "-",
            "event_samples": len(filtered_events),
        }
        if formatted_users:
            summary["top_user"] = formatted_users[0]["username"]
            summary["top_user_count"] = formatted_users[0]["count"]
        else:
            summary["top_user"] = "-"
            summary["top_user_count"] = 0
        summary["ip_total_hits"] = ip_overview["total"]
        summary["unique_ips"] = ip_overview["unique"]
        summary["online_ips"] = ip_overview["online"]
        summary["guest_total"] = guest_overview.get("total", 0)
        summary["guest_online"] = guest_overview.get("online", 0)
        return render_template_string(
            TRAFFIC_TEMPLATE,
            stats=usage_stats(),
            stats_version=current_stats_version(),
            user_rows=formatted_users,
            events=formatted_events,
            generated_at=_fmt_ts(time.time()),
            admin_user=user,
            chart_labels=chart_labels,
            chart_values=chart_values,
            top_actions=top_actions,
            top_users=formatted_users[:5],
            summary=summary,
            trend_window=trend_window,
            trend_label="每小時" if trend_window == "hour" else "每天",
            ip_summary=ip_overview,
            admin_view_options=admin_view_options,
            selected_view_username=selected_view_username,
        )

    @app.route("/admin/traffic/reset", methods=["POST"])
    @login_required
    def admin_traffic_reset():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        traffic_tracker.reset()
        flash("已清除所有流量統計與累積訪問次數。", "success")
        record_ui_event("reset_traffic", "success")
        return redirect(url_for("admin_traffic"))

    @app.post("/admin/traffic/reset-user")
    @login_required
    def admin_traffic_reset_user():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        target = (request.form.get("username") or "").strip()
        if not target:
            flash("請提供要清除統計的帳號名稱。", "warning")
            return redirect(url_for("admin_traffic"))
        removed = traffic_tracker.remove_user_stats(target)
        deleted_events = storage.delete_traffic_events_for_user(target)
        if removed or deleted_events:
            flash(f"已清除 {target} 的統計與事件紀錄（移除 {deleted_events} 筆事件）。", "success")
            record_ui_event("reset_traffic_user", meta={"target": target, "events_removed": deleted_events})
        else:
            flash("找不到對應的統計資料，未執行變更。", "info")
        return redirect(url_for("admin_traffic"))

    @app.route("/admin/feedback", methods=["GET", "POST"])
    @login_required
    def admin_feedback():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        if request.method == "POST":
            feedback_id = request.form.get("id")
            status = request.form.get("status", "open")
            if update_feedback_status_entry(feedback_id, status):
                flash("狀態已更新。", "success")
            else:
                flash("更新失敗，請稍後再試。", "error")
            return redirect(url_for("admin_feedback"))
        feedback_items = list_feedback_entries()
        open_count = sum(1 for item in feedback_items if (item.get("status") or "open") == "open")
        return render_template_string(
            ADMIN_FEEDBACK_TEMPLATE,
            admin_user=user,
            feedback_entries=feedback_items,
            open_count=open_count,
        )

    @app.route("/admin/announcements", methods=["GET", "POST"])
    @login_required
    def admin_announcements():
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        if request.method == "POST":
            title = request.form.get("title", "").strip()
            content = request.form.get("content", "").strip()
            if not title or not content:
                flash("請輸入公告標題與內容。", "error")
            else:
                add_announcement(title, content, user["username"])
                flash("公告已發布。", "success")
                return redirect(url_for("admin_announcements"))
        return render_template_string(
            ANNOUNCEMENTS_TEMPLATE,
            admin_user=user,
            announcements=load_announcements(),
        )

    @app.post("/admin/announcements/<announcement_id>/delete")
    @login_required
    def delete_announcement(announcement_id: str):
        user = current_user()
        if not user or not user.get("is_admin"):
            flash("僅限管理員操作。", "error")
            return redirect(url_for("index"))
        if delete_announcement_entry(announcement_id):
            flash("公告已刪除。", "info")
        else:
            flash("找不到指定的公告。", "error")
        return redirect(url_for("admin_announcements"))

    @app.route("/feedback", methods=["GET", "POST"])
    def feedback():
        user = current_user()
        if request.method == "POST":
            message = request.form.get("message", "")
            email = request.form.get("email", "")
            name = request.form.get("name", "")
            username = user["username"] if user else name
            feedback_id = add_feedback_entry(message, email, username)
            if feedback_id:
                flash("已收到回報，感謝你的意見！", "success")
                record_ui_event("feedback_submitted", "success", {"feedback_id": feedback_id})
                return redirect(url_for("feedback"))
            flash("請輸入回報內容。", "error")
        return render_template_string(
            FEEDBACK_TEMPLATE,
            admin_user=user,
            support_email=support_email,
            stats=usage_stats(),
            stats_version=current_stats_version(),
        )

    @app.route("/", methods=["GET"])
    def index():
        user = current_user()
        if not user:
            return render_template_string(
                HOME_TEMPLATE,
                stats=usage_stats(),
                stats_version=current_stats_version(),
                google_scope=GOOGLE_CALENDAR_SCOPE,
                app_home_url=app_home_url,
                support_email=support_email,
            )
        context = _build_dashboard_context(user)
        return render_template_string(
            WEB_TEMPLATE,
            **context,
            app_home_url=app_home_url,
            support_email=support_email,
        )

    return (
        admin_traffic,
        admin_traffic_reset,
        admin_traffic_reset_user,
        admin_feedback,
        admin_announcements,
        delete_announcement,
        feedback,
        index,
    )
