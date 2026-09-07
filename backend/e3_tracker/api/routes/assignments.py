"""Assignments routes and their feature helpers."""

import json
import secrets
import threading
import hashlib
from typing import List, Optional
from urllib.parse import urlsplit, urlunsplit
import requests
from flask import Response, flash, redirect, render_template_string, request, send_file, session, url_for
from werkzeug.http import http_date
from ...services.collector import normalize_semester_keys
from ...services.google_calendar import GOOGLE_CALENDAR_SCOPE, GoogleUnauthorizedError, build_google_authorize_url, compute_expiry, exchange_code_for_google_token, sync_assignments_to_google_calendar
from ...services.http import login_with_password


def register_assignments_routes(*,
    LOGIN_TEMPLATE,
    PRIVACY_TEMPLATE,
    ROOT_DIR,
    TERMS_TEMPLATE,
    _build_calendar,
    _build_google_state,
    _ensure_google_access_token,
    _generate_excel_data,
    _google_ready,
    _google_redirect_uri,
    _mark_refresh_job_done,
    _mark_refresh_job_started,
    _refresh_job_state,
    _select_assignments_from_result,
    _start_web_session,
    _verify_google_state,
    admin_user_id,
    app,
    app_home_url,
    base_url,
    canonical_host,
    clear_google_tokens,
    current_stats_version,
    current_user,
    default_timeout,
    fetch_assignments_for,
    get_assign_cache,
    get_user_preferences,
    get_viewed_username,
    google_calendar_id,
    google_client_id,
    google_client_secret,
    is_admin_viewing_other_user,
    legal_effective_date,
    legal_entity_name,
    load_announcements,
    load_cache_from_disk,
    load_google_tokens,
    login_required,
    record_ui_event,
    save_google_tokens,
    set_announcement_vote,
    set_assign_cache,
    set_assign_cache_for_user,
    storage,
    support_email,
    update_user_preferences,
    usage_stats,
):
    @app.before_request
    def enforce_canonical_host():
        if not canonical_host:
            return
        forwarded_host = request.headers.get("X-Forwarded-Host")
        host = (forwarded_host or request.host).split(",", 1)[0].strip()
        normalized_host = host.lower().rstrip(".")
        desired_host = canonical_host.lower().strip().rstrip("./")
        if "://" in desired_host:
            desired_host = (urlsplit(desired_host).netloc or desired_host).rstrip(".")
        proto = request.headers.get("X-Forwarded-Proto", request.scheme)
        needs_host_redirect = normalized_host != desired_host
        needs_proto_redirect = proto != "https"
        if needs_host_redirect or needs_proto_redirect:
            parts = urlsplit(request.url)
            new_url = urlunsplit(
                (
                    "https",
                    desired_host,
                    parts.path,
                    parts.query,
                    parts.fragment,
                )
            )
            return redirect(new_url, code=301)

    @app.after_request
    def add_no_store_headers(resp):
        cache_control = resp.headers.get("Cache-Control", "")
        content_disposition = resp.headers.get("Content-Disposition", "")
        if "attachment" in content_disposition.lower() or cache_control.startswith("public"):
            return resp
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if current_user():
            return redirect(url_for("index"))
        if request.method == "POST":
            login_type = request.form.get("login_type", "password")
            if login_type == "session":
                raw_session = request.form.get("moodle_session", "").strip()
                if not raw_session:
                    flash("請貼上有效的 MoodleSession 值。", "error")
                else:
                    digest = hashlib.sha1(raw_session.encode("utf-8")).hexdigest()[:10]
                    session_label = f"Session-{digest}"
                    existing_cache = load_cache_from_disk(session_label)
                    try:
                        result = None
                        excel_data = None
                        if not existing_cache:
                            result, excel_data = fetch_assignments_for(
                                {"username": session_label, "moodle_session": raw_session}
                            )
                        _start_web_session(
                            session_label,
                            moodle_session=raw_session,
                            is_guest=False,
                            is_admin=bool(admin_user_id and session_label == admin_user_id),
                            permanent=True,
                        )
                        record_ui_event("login_success", meta={"username": session_label})
                        if existing_cache:
                            flash("已載入先前的課程資料，系統將在背景自動更新最新內容。", "info")
                        else:
                            try:
                                set_assign_cache(result, excel_data)
                                flash("已成功透過 E3 Session 取得最新資訊。", "success")
                            except Exception:
                                flash("Session 登入成功，但暫存資料寫入失敗。", "warning")
                        response = redirect(url_for("index"))
                        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
                        response.headers["Pragma"] = "no-cache"
                        response.headers["Expires"] = "0"
                        return response
                    except Exception as exc:
                        flash(f"Session 驗證失敗：{exc}，請確認 MoodleSession 是否正確。", "error")
            else:
                raw_username = request.form.get("username", "").strip()
                raw_password = request.form.get("password", "")
                if not raw_username or not raw_password:
                    flash("請輸入帳號與密碼。", "error")
                else:
                    try:
                        sess = requests.Session()
                        login_with_password(sess, base_url, raw_username, raw_password, timeout=default_timeout)
                        cookie_val = sess.cookies.get("MoodleSession")
                        if not cookie_val:
                            raise RuntimeError("登入成功但未取得 MoodleSession。")
                        _start_web_session(
                            raw_username,
                            moodle_session=cookie_val,
                            is_guest=False,
                            is_admin=bool(admin_user_id and raw_username == admin_user_id),
                            permanent=True,
                        )
                        record_ui_event("login_success", meta={"username": raw_username})
                        existing_cache = load_cache_from_disk(raw_username)
                        if existing_cache:
                            flash("已載入先前的課程資料，系統將在背景自動更新最新內容。", "info")
                        else:
                            try:
                                result, excel_data = fetch_assignments_for(
                                    {"username": raw_username, "moodle_session": cookie_val}
                                )
                                set_assign_cache(result, excel_data)
                                flash("已成功獲取最新資訊。", "success")
                            except Exception as exc:
                                flash(f"登入成功但獲取資料失敗：{exc}，程式將在背景重試。", "warning")
                        response = redirect(url_for("index"))
                        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
                        response.headers["Pragma"] = "no-cache"
                        response.headers["Expires"] = "0"
                        return response
                    except Exception as exc:
                        flash(f"{exc}", "error")
        announcements_list = load_announcements()
        return render_template_string(
            LOGIN_TEMPLATE,
            stats=usage_stats(),
            stats_version=current_stats_version(),
            announcements=announcements_list,
            announcement_version=announcements_list[0]["id"] if announcements_list else None,
            support_email=support_email,
            app_home_url=app_home_url,
        )

    @app.route("/healthz", methods=["GET"])
    def health_check():
        return {"status": "ok"}, 200

    @app.route("/traffic/stats", methods=["GET"])
    def traffic_stats():
        stats = usage_stats()
        payload = {
            "version": current_stats_version(),
            "online": stats["online"],
            "total": stats["total"],
        }
        return payload, 200, {"Cache-Control": "no-store, max-age=0"}

    @app.route("/privacy", methods=["GET"])
    def privacy_policy():
        return render_template_string(
            PRIVACY_TEMPLATE,
            app_home_url=app_home_url,
            support_email=support_email,
            google_scope=GOOGLE_CALENDAR_SCOPE,
            legal_entity_name=legal_entity_name,
            effective_date=legal_effective_date,
        )

    @app.route("/terms", methods=["GET"])
    def terms_of_service():
        return render_template_string(
            TERMS_TEMPLATE,
            app_home_url=app_home_url,
            support_email=support_email,
            google_scope=GOOGLE_CALENDAR_SCOPE,
            legal_entity_name=legal_entity_name,
        )

    @app.post("/ui-event")
    @login_required
    def ui_event():
        payload = request.get_json(silent=True) or {}
        action = str(payload.get("action") or "").strip()
        status = str(payload.get("status") or "info").strip() or "info"
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            meta = None
        if not action:
            return {"ok": False, "error": "action required"}, 400
        record_ui_event(action, status, meta)
        return {"ok": True}

    @app.get("/session/status")
    @login_required
    def session_status():
        user = current_user()
        return {"ok": True, "username": user["username"] if user else None}

    @app.get("/api/cache")
    @login_required
    def api_cache():
        user = current_user()
        viewed_username = get_viewed_username(actor=user)
        cache = get_assign_cache(viewed_username) or {}
        preferences = get_user_preferences(viewed_username)
        include_cache = str(request.args.get("include_cache") or "").lower() in {"1", "true", "yes"}
        refresh_state = _refresh_job_state(viewed_username)
        payload = {
            "ok": True,
            "ts": cache.get("ts"),
            "has_result": bool(cache.get("result")) if cache else False,
            "preferences": preferences,
            "viewed_username": viewed_username,
            "readonly_view": is_admin_viewing_other_user(actor=user, viewed_username=viewed_username),
            "refresh_status": refresh_state.get("status") if refresh_state else None,
            "refresh_error": refresh_state.get("error") if refresh_state else None,
            "refresh_in_progress": bool(refresh_state and refresh_state.get("status") == "running"),
            "refresh_started_at": refresh_state.get("started_at") if refresh_state else None,
            "refresh_finished_at": refresh_state.get("finished_at") if refresh_state else None,
        }
        if include_cache:
            payload["cache"] = cache
        return payload

    @app.post("/preferences")
    @login_required
    def save_preferences():
        user = current_user()
        if is_admin_viewing_other_user(actor=user):
            return {"ok": False, "error": "readonly_view"}, 403
        payload = request.get_json(silent=True) or {}
        updated = update_user_preferences(payload)
        return {"ok": True, "preferences": updated}

    @app.route("/guest-login", methods=["POST"])
    def guest_login():
        guest_name = f"訪客_{secrets.token_hex(3)}"
        _start_web_session(
            guest_name,
            moodle_session=None,
            is_guest=True,
            is_admin=False,
            permanent=False,
        )
        flash("已進入訪客模式：請使用匯出工具生成 JSON 後上傳即可瀏覽作業。", "info")
        record_ui_event("guest_login", meta={"username": guest_name})
        return redirect(url_for("index"))

    @app.route("/guest-tool", methods=["GET"])
    def guest_tool():
        tool_path = ROOT_DIR / "backend" / "tools" / "guest_export.exe"
        if not tool_path.exists():
            flash("找不到匯出工具。", "error")
            return redirect(url_for("login"))
        payload = send_file(
            tool_path,
            as_attachment=True,
            download_name="guest_export.exe",
            conditional=True,
            max_age=604800,
            etag=True,
        )
        stat = tool_path.stat()
        payload.headers["Cache-Control"] = "public, max-age=604800, immutable"
        payload.headers["Last-Modified"] = http_date(stat.st_mtime)
        payload.headers["Content-Length"] = str(stat.st_size)
        return payload

    @app.route("/guest-tool.py", methods=["GET"])
    def guest_tool_source():
        source_path = ROOT_DIR / "backend" / "tools" / "guest_export.py"
        if not source_path.exists():
            flash("找不到匯出工具原始碼。", "error")
            return redirect(url_for("login"))
        payload = send_file(
            source_path,
            as_attachment=True,
            download_name="guest_export.py",
            mimetype="text/x-python",
        )
        stat = source_path.stat()
        payload.headers["Cache-Control"] = "public, max-age=604800, immutable"
        payload.headers["Last-Modified"] = http_date(stat.st_mtime)
        payload.headers["Content-Length"] = str(stat.st_size)
        return payload

    @app.route("/guest/import", methods=["POST"])
    @login_required
    def guest_import():
        user = current_user()
        if not user or not user.get("is_guest"):
            flash("訪客匯入僅限訪客模式使用。", "error")
            record_ui_event("guest_import", "error", {"reason": "not_guest"})
            return redirect(url_for("index"))
        uploaded = request.files.get("guest_file")
        if not uploaded or not uploaded.filename:
            flash("請上傳由 guest_export 匯出工具產生的 JSON 檔。", "warning")
            record_ui_event("guest_import", "error", {"reason": "missing_file"})
            return redirect(url_for("index"))
        try:
            payload = json.load(uploaded.stream)
        except Exception as exc:
            flash(f"解析上傳檔案失敗：{exc}", "error")
            record_ui_event("guest_import", "error", {"reason": "parse_failed"})
            return redirect(url_for("index"))
        if payload.get("mode") != "guest_export_v1":
            flash("檔案格式不支援，請使用 guest_export 匯出工具產生的 JSON。", "warning")
            record_ui_event("guest_import", "error", {"reason": "unsupported_mode"})
            return redirect(url_for("index"))
        result = payload.get("result")
        excel_data = payload.get("excel_data")
        if not result:
            flash("檔案內容缺少作業資料。", "error")
            record_ui_event("guest_import", "error", {"reason": "missing_result"})
            return redirect(url_for("index"))
        if not excel_data:
            excel_data = _generate_excel_data((result or {}).get("all_assignments"))
        set_assign_cache(result, excel_data)
        flash("已匯入訪客資料（檔案）。", "success")
        record_ui_event(
            "guest_import",
            "success",
            {"items": len(result.get("all_assignments", [])), "has_excel": bool(excel_data)},
        )
        return redirect(url_for("index"))

    @app.post("/announcements/<announcement_id>/vote")
    @login_required
    def announcement_vote(announcement_id: str):
        user = current_user()
        if not user:
            return {"ok": False, "error": "not_logged_in"}, 401
        payload = request.get_json(silent=True) or {}
        requested_vote = str(payload.get("vote") or "").strip().lower()
        if requested_vote not in {"up", "down", "clear"}:
            return {"ok": False, "error": "invalid_vote"}, 400
        resolved_vote = None if requested_vote == "clear" else requested_vote
        updated = set_announcement_vote(announcement_id, user["username"], resolved_vote)
        if not updated:
            return {"ok": False, "error": "announcement_not_found"}, 404
        record_ui_event(
            "announcement_vote",
            "success",
            {"announcement_id": announcement_id, "vote": resolved_vote or "clear"},
        )
        return {"ok": True, "announcement": updated}
    @app.route("/google/authorize")
    @login_required
    def google_authorize():
        if not _google_ready():
            flash("尚未設定 Google OAuth，請先在伺服器端提供 Client ID/Secret。", "warning")
            record_ui_event("google_link", "error", {"stage": "authorize", "reason": "not_ready"})
            return redirect(url_for("index"))
        state = _build_google_state()
        session["google_auth_state"] = state
        record_ui_event("google_link", "start", {"stage": "authorize"})
        return redirect(
            build_google_authorize_url(
                google_client_id,
                _google_redirect_uri(),
                scope=GOOGLE_CALENDAR_SCOPE,
                state=state,
            )
        )

    @app.route("/google/callback")
    def google_callback():
        user = current_user()
        if not user:
            flash("請先登入 E3，再進行 Google 授權。", "warning")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "not_logged_in"})
            return redirect(url_for("login"))
        if not _google_ready():
            flash("尚未設定 Google OAuth。", "warning")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "not_ready"})
            return redirect(url_for("index"))
        error = request.args.get("error")
        if error:
            flash(f"Google 授權失敗：{error}", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": error})
            return redirect(url_for("index"))
        code = request.args.get("code")
        state = request.args.get("state")
        stored_state = session.get("google_auth_state")
        state_valid = bool(state) and _verify_google_state(state)
        if not code or not state_valid:
            flash("Google 授權資訊錯誤，請重新嘗試。", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "invalid_state"})
            return redirect(url_for("index"))
        if stored_state and state != stored_state:
            record_ui_event("google_link", "info", {"stage": "callback", "reason": "state_mismatch_but_signed"})
        session.pop("google_auth_state", None)
        try:
            token_resp = exchange_code_for_google_token(
                code,
                client_id=google_client_id,
                client_secret=google_client_secret,
                redirect_uri=_google_redirect_uri(),
            )
        except Exception as exc:  # pragma: no cover
            flash(f"換取 Google Token 失敗：{exc}", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "token_exchange"})
            return redirect(url_for("index"))
        existing = load_google_tokens(user["username"]) or {}
        refresh_token = token_resp.get("refresh_token") or existing.get("refresh_token")
        if not refresh_token:
            flash("Google 未提供 refresh token，請勾選同意並再次授權。", "error")
            record_ui_event("google_link", "error", {"stage": "callback", "reason": "missing_refresh_token"})
            return redirect(url_for("index"))
        tokens = {
            "access_token": token_resp.get("access_token"),
            "refresh_token": refresh_token,
            "scope": token_resp.get("scope"),
            "token_type": token_resp.get("token_type"),
            "expires_at": compute_expiry(token_resp.get("expires_in", 3600)),
        }
        save_google_tokens(user["username"], tokens)
        flash("已成功連結 Google 日曆，可同步作業。", "success")
        record_ui_event("google_link", "success", {"stage": "callback"})
        return redirect(url_for("index"))

    @app.post("/google/unlink")
    @login_required
    def google_unlink():
        user = current_user()
        if user:
            clear_google_tokens(user["username"])
        flash("已解除 Google 日曆連結。", "info")
        record_ui_event("google_unlink", "success")
        return redirect(url_for("index"))

    @app.post("/google/sync")
    @login_required
    def google_sync():
        if not _google_ready():
            flash("尚未設定 Google OAuth，無法同步日曆。", "warning")
            record_ui_event("google_sync", "error", {"reason": "not_ready"})
            return redirect(url_for("index"))
        user = current_user()
        if not user:
            flash("請先登入後再同步。", "warning")
            record_ui_event("google_sync", "error", {"reason": "not_logged_in"})
            return redirect(url_for("login"))
        raw_selected = request.form.get("selected_uids", "")
        selected_uids: List[str] = []
        if raw_selected:
            try:
                selected_uids = json.loads(raw_selected)
            except Exception:
                selected_uids = []
        if not selected_uids:
            flash("請先選擇要導入的作業。", "warning")
            record_ui_event("google_sync", "error", {"reason": "no_selection"})
            return redirect(url_for("index"))
        tokens = load_google_tokens(user["username"])
        if not tokens:
            flash("尚未連結 Google 日曆。", "warning")
            record_ui_event("google_sync", "error", {"reason": "not_linked"})
            return redirect(url_for("index"))
        try:
            tokens = _ensure_google_access_token(user["username"], tokens)
        except Exception as exc:
            flash(f"無法更新 Google Token：{exc}", "error")
            record_ui_event("google_sync", "error", {"reason": "token_refresh"})
            return redirect(url_for("index"))
        record_ui_event("google_sync", "start", {"count": len(selected_uids)})
        try:
            cache = get_assign_cache() or {}
            result = cache.get("result") or {}
            excel_data = cache.get("excel_data")
            assignments = _select_assignments_from_result(result, selected_uids)
            if user.get("is_guest"):
                if not assignments:
                    raise RuntimeError("找不到訪客匯入的作業資料，請重新匯入後再試。")
            elif not assignments:
                result, excel_data = fetch_assignments_for({"username": user["username"], "moodle_session": session.get("moodle_session")})
                set_assign_cache(result, excel_data)
                assignments = _select_assignments_from_result(result, selected_uids)
            if not assignments:
                flash("找不到選擇的作業，請重新整理後再試。", "warning")
                record_ui_event("google_sync", "error", {"reason": "not_found"})
                return redirect(url_for("index"))
            synced = sync_assignments_to_google_calendar(
                assignments,
                access_token=tokens["access_token"],
                calendar_id=google_calendar_id,
            )
            flash(f"已將 {synced} 筆作業同步到 Google 日曆。", "success")
            record_ui_event("google_sync", "success", {"synced": synced})
        except GoogleUnauthorizedError:
            clear_google_tokens(user["username"])
            flash("Google 授權已失效，請重新連結後再嘗試。", "error")
            record_ui_event("google_sync", "error", {"reason": "unauthorized"})
        except Exception as exc:
            flash(f"同步 Google 日曆失敗：{exc}", "error")
            record_ui_event("google_sync", "error", {"reason": "exception"})
        return redirect(url_for("index"))

    @app.route("/logout")
    def logout():
        old_user = session.get("username")
        session_token = session.get("session_token")
        was_guest = bool(session.get("is_guest"))
        if old_user:
            if was_guest:
                storage.delete_user_cache(old_user)
            clear_google_tokens(old_user)
        if session_token:
            storage.clear_web_session(session_token)
        if old_user:
            record_ui_event("logout", meta={"username": old_user})
        session.clear()
        session.permanent = False
        session.modified = True
        flash("已登出。", "success")
        resp = redirect(url_for("index"))
        session_cookie_name = app.config.get("SESSION_COOKIE_NAME", "session")
        cookie_path = app.config.get("SESSION_COOKIE_PATH", "/")
        cookie_domain = app.config.get("SESSION_COOKIE_DOMAIN")
        resp.delete_cookie(session_cookie_name, path=cookie_path, domain=cookie_domain)
        host_only_domain = (request.host.split(":", 1)[0] or "").strip() or None
        if host_only_domain and host_only_domain != cookie_domain:
            resp.delete_cookie(session_cookie_name, path=cookie_path, domain=host_only_domain)
            if not host_only_domain.startswith("."):
                resp.delete_cookie(session_cookie_name, path=cookie_path, domain=f".{host_only_domain}")
        return resp

    @app.post("/api/assignments")
    @login_required
    def api_assignments():
        user = current_user()
        if is_admin_viewing_other_user(actor=user):
            return {"ok": False, "error": "readonly_view"}, 403
        if user and user.get("is_guest"):
            return {"ok": False, "error": "訪客模式不支援自動更新"}, 400
        username = user["username"]
        moodle_session_val = session.get("moodle_session")
        payload = request.get_json(silent=True) or {}
        include_archived = bool(payload.get("includeArchived"))
        requested_semesters: Optional[List[str]] = None
        if "semesterFilters" in payload or "semester_filter" in payload:
            requested_semesters = normalize_semester_keys(
                payload.get("semesterFilters", payload.get("semester_filter"))
            )
            if not requested_semesters:
                return {"ok": False, "error": "請至少選擇一個學期"}, 400
            update_user_preferences({"semester_filter": requested_semesters})
        prev_cache = get_assign_cache() or {}
        prev_ts = prev_cache.get("ts") or 0
        if not _mark_refresh_job_started(username):
            return {
                "ok": True,
                "message": "背景更新已在進行中。",
                "background": True,
                "in_progress": True,
                "ts": prev_ts,
            }

        def _run_background():
            with app.app_context():
                try:
                    result, excel_data = fetch_assignments_for(
                        {"username": username, "moodle_session": moodle_session_val},
                        semester_keys=requested_semesters,
                        include_archived=include_archived,
                    )
                    set_assign_cache_for_user(username, result, excel_data)
                    record_ui_event(
                        "refresh_assignments",
                        "success",
                        {
                            "items": len(result.get("all_assignments", [])),
                            "mode": "background",
                            "include_archived": include_archived,
                        },
                    )
                except Exception as exc:  # pragma: no cover - background logging
                    record_ui_event("refresh_assignments", "error", {"reason": str(exc), "mode": "background"})
                    _mark_refresh_job_done(username, status="error", error=str(exc))
                else:
                    _mark_refresh_job_done(username, status="success")

        threading.Thread(target=_run_background, daemon=True).start()
        return {
            "ok": True,
            "message": "已啟動背景更新，稍後將自動刷新。",
            "background": True,
            "ts": prev_ts,
        }

    @app.route("/calendar.ics")
    @login_required
    def calendar_export():
        cache = get_assign_cache()
        assignments = cache.get("result", {}).get("all_assignments", []) if cache else []
        calendar = _build_calendar(assignments)
        if not calendar:
            flash("尚無可匯出的作業資料。", "info")
            record_ui_event("export_calendar", "error", {"reason": "no_assignments"})
            return redirect(url_for("index"))
        record_ui_event("export_calendar", "success", {"items": len(assignments)})
        return Response(
            calendar,
            mimetype="text/calendar",
            headers={"Content-Disposition": "attachment; filename=pending_assignments.ics"},
        )

    return (
        enforce_canonical_host,
        add_no_store_headers,
        login,
        health_check,
        traffic_stats,
        privacy_policy,
        terms_of_service,
        ui_event,
        session_status,
        api_cache,
        save_preferences,
        guest_login,
        guest_tool,
        guest_tool_source,
        guest_import,
        announcement_vote,
        google_authorize,
        google_callback,
        google_unlink,
        google_sync,
        logout,
        api_assignments,
        calendar_export,
    )
