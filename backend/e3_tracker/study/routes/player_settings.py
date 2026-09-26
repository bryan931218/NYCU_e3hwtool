"""Player and Discord HTTP endpoints; settings persistence stays in storage."""
import os
from pathlib import Path
from typing import Any
from flask import Response, flash, redirect, render_template_string, request, send_file, session, url_for
from e3_tracker.study.persistence.deployment import StudyDeploymentStorage
from e3_tracker.study.services.discord_presence import DISCORD_APPLICATION_ID_PATTERN, issue_discord_presence_token, verify_discord_presence_signed_token, build_discord_presence_payload
from e3_tracker.platform.auth import admin_access as _admin_access, admin_username


def register_player_settings_routes(
    app: Any,
    storage: StudyDeploymentStorage,
    template: str,
    agent_path: Path,
) -> None:
    if "admin_study_player_settings" in app.view_functions:
        return

    def require_admin():
        authenticated, is_admin = _admin_access(storage)
        if not authenticated:
            return redirect(url_for("login"))
        if not is_admin:
            return Response("Forbidden", status=403, mimetype="text/plain")
        return None

    def admin_study_player_settings():
        denied = require_admin()
        if denied is not None:
            return denied
        base_url = str(os.getenv("E3_APP_HOME_URL") or request.url_root).rstrip("/")

        def render_settings(*, revealed_token: str = ""):
            return render_template_string(
                template,
                settings=storage.load_study_player_settings(),
                discord=storage.load_discord_presence_settings(),
                discord_token_once=revealed_token,
                discord_api_url=f"{base_url}/api/discord-presence",
                discord_agent_url=f"{base_url}/downloads/e3-discord-presence.py",
                username=admin_username(storage) or "管理員",
            )

        if request.method == "POST":
            action = str(request.form.get("action") or "save_player").strip()
            if action == "discord_generate":
                application_id = str(request.form.get("discord_application_id") or "").strip()
                if not DISCORD_APPLICATION_ID_PATTERN.fullmatch(application_id):
                    flash("Discord Application ID 格式不正確。", "error")
                    return redirect(url_for("admin_study_player_settings", _anchor="discord-presence"))
                token = issue_discord_presence_token(application_id, app.secret_key)
                storage.save_discord_presence_settings(
                    application_id=application_id,
                    token=token,
                    enabled=True,
                )
                flash("Discord 連線權杖已更新，請下載新的 Windows 安裝檔。", "success")
                return render_settings(revealed_token=token)
            if action == "discord_revoke":
                storage.revoke_discord_presence_token()
                flash("Discord 學習狀態連線已停用。", "success")
                return redirect(url_for("admin_study_player_settings", _anchor="discord-presence"))
            saved = storage.save_study_player_settings(
                {
                    "default_playback_rate": request.form.get("default_playback_rate"),
                    "hold_space_rate": request.form.get("hold_space_rate"),
                    "hold_delay_ms": request.form.get("hold_delay_ms"),
                    "seek_back_seconds": request.form.get("seek_back_seconds"),
                    "seek_forward_seconds": request.form.get("seek_forward_seconds"),
                    "seek_repeat_ms": request.form.get("seek_repeat_ms"),
                    "playback_rate_step": request.form.get("playback_rate_step"),
                    "volume_step": request.form.get("volume_step"),
                    "controls_hide_ms": request.form.get("controls_hide_ms"),
                    "center_click_toggle": request.form.get("center_click_toggle") == "1",
                    "pause_on_marker": request.form.get("pause_on_marker") == "1",
                    "show_speed_presets": request.form.get("show_speed_presets") == "1",
                    "show_shortcut_hint": request.form.get("show_shortcut_hint") == "1",
                    "hint_duration_ms": request.form.get("hint_duration_ms"),
                }
            )
            flash(
                "播放器設定已儲存："
                f"預設 {saved['default_playback_rate']:g}×，"
                f"後退 {saved['seek_back_seconds']} 秒、"
                f"快進 {saved['seek_forward_seconds']} 秒。",
                "success",
            )
            return redirect(url_for("admin_study_player_settings"))
        return render_settings()

    def admin_study_player_settings_json():
        denied = require_admin()
        if denied is not None:
            return denied
        return {
            "ok": True,
            "settings": storage.load_study_player_settings(),
        }

    def discord_presence_api():
        authorization = str(request.headers.get("Authorization") or "")
        scheme, _separator, token = authorization.partition(" ")
        authorized = bool(
            scheme.lower() == "bearer"
            and storage.verify_discord_presence_token(token)
        )
        if not authorized and scheme.lower() == "bearer":
            recovered_application_id = verify_discord_presence_signed_token(
                token,
                app.secret_key,
            )
            current_settings = storage.load_discord_presence_settings()
            can_recover = bool(
                recovered_application_id
                and not current_settings.get("application_id")
                and not current_settings.get("has_token")
            )
            if can_recover:
                storage.save_discord_presence_settings(
                    application_id=recovered_application_id,
                    token=token,
                    enabled=True,
                )
                authorized = True
        if not authorized:
            return (
                {"ok": False, "error": "invalid_presence_token"},
                401,
                {
                    "Cache-Control": "no-store, max-age=0",
                    "WWW-Authenticate": 'Bearer realm="E3 Discord Presence"',
                },
            )
        base_url = str(os.getenv("E3_APP_HOME_URL") or request.url_root).rstrip("/")
        return (
            build_discord_presence_payload(
                storage,
                public_url=f"{base_url}/study-progress",
                cover_image_url=f"{base_url}/assets/study/discord/e3-study-cover.png",
            ),
            200,
            {"Cache-Control": "no-store, max-age=0"},
        )

    def discord_presence_agent_download():
        if not agent_path.is_file():
            return Response("Not Found", status=404, mimetype="text/plain")
        return send_file(
            agent_path,
            mimetype="text/x-python",
            as_attachment=True,
            download_name="e3_discord_presence.py",
            conditional=True,
            max_age=3600,
        )

    app.add_url_rule(
        "/admin/study-player-settings",
        endpoint="admin_study_player_settings",
        view_func=admin_study_player_settings,
        methods=["GET", "POST"],
    )
    app.add_url_rule(
        "/admin/study-player-settings.json",
        endpoint="admin_study_player_settings_json",
        view_func=admin_study_player_settings_json,
        methods=["GET"],
    )
    app.add_url_rule(
        "/api/discord-presence",
        endpoint="discord_presence_api",
        view_func=discord_presence_api,
        methods=["GET"],
    )
    app.add_url_rule(
        "/downloads/e3-discord-presence.py",
        endpoint="discord_presence_agent_download",
        view_func=discord_presence_agent_download,
        methods=["GET"],
    )
