"""Bounded login diagnostics for administrator activity; never retain credentials."""

from e3_tracker.assignments.services.http import E3LoginError, login_failure_reason


FAILURE_MESSAGES = {
    "missing_fields": "未填寫必要的登入資料。",
    "login_rejected": "E3 未建立已驗證的登入狀態。",
    "session_cookie_missing": "E3 未提供 MoodleSession 登入憑證。",
    "upstream_tls": "無法驗證 E3 的 TLS 安全連線。",
    "upstream_timeout": "連線至 E3 逾時。",
    "upstream_connection": "無法連線至 E3。",
    "upstream_access_denied": "E3 拒絕連線請求（HTTP 403）。",
    "upstream_rate_limited": "E3 限制登入請求（HTTP 429）。",
    "upstream_http_error": "E3 回傳 HTTP 錯誤。",
    "internal_error": "登入處理發生程式錯誤。",
    "mobile_api_unavailable": "E3 官方 API 未能完成帳密驗證。",
    "mobile_session_unavailable": "E3 API 未能建立經本人驗證的網頁 Session。",
    "session_handoff_limited": "E3 暫時限制重複建立網頁 Session，請稍後再試。",
    "account_action_required": "E3 要求先完成帳號確認、密碼更新或額外驗證。",
    "identity_mismatch": "E3 回傳的帳號或網站與這次登入不一致，已停止登入。",
    "upstream_response_invalid": "E3 回傳的登入資料格式不符合預期。",
    "api_token_invalid": "E3 API 登入憑證已失效，請重新登入。",
    "csrf_validation_failed": "本站表單安全驗證失敗，請重新整理登入頁後再試。",
    "login_rate_limited": "本站暫時限制登入嘗試，請等待五分鐘後再試。",
    "too_many_requests": "本站暫時限制過多的連線請求。",
    "cross_origin_request": "本站拒絕來自其他來源的登入請求。",
    "cross_site_request": "本站拒絕跨網站的登入請求。",
}
NOTICE_MESSAGES = {
    "credentials": "E3 回報帳號或密碼不被接受。",
    "token": "E3 回報登入權杖無效。",
    "cookies": "E3 回報 Cookie 驗證失敗。",
    "verification": "E3 要求額外驗證。",
    "rate_limit": "E3 回報帳號鎖定或嘗試次數過多。",
    "challenge": "E3 要求安全驗證。",
    "none": "E3 頁面沒有提供錯誤提示。",
    "unknown": "未取得可判讀的 E3 錯誤提示。",
}
PAGE_LABELS = {"login": "登入頁", "dashboard": "首頁", "verification": "驗證頁", "policy": "使用條款頁", "other": "其他頁面", "unknown": "未知頁面"}
STAGE_LABELS = {"input": "表單檢查", "request_validation": "本站請求檢查", "e3_authentication": "E3 帳密驗證", "session_validation": "E3 Session 驗證", "web_session": "建立本站登入", "post_login": "登入後處理"}


def failure_metadata(exc, *, method, stage, username="", code=None):
    reason = code if code in FAILURE_MESSAGES else (login_failure_reason(exc) if exc is not None else "missing_fields")
    method = method if method in {"session", "password"} else "unknown"
    stage = stage if stage in STAGE_LABELS else "e3_authentication"
    details = ["登入方式：" + {"session": "Session", "password": "帳號／密碼", "unknown": "未判定"}[method], "階段：" + STAGE_LABELS[stage], "錯誤代碼：" + reason]
    meta = {
        "site": "assignments", "login_method": method, "login_stage": stage,
        "error_code": reason, "message": FAILURE_MESSAGES[reason],
    }
    if reason == "internal_error" and method == "session" and stage == "session_validation":
        meta["message"] = "未能完成 Session 驗證或課程資料載入，原因尚未確認。"
    # This is an unverified form identifier, never an authenticated identity.
    if method == "password" and username:
        meta["attempted_username"] = str(username)[:191]
    if isinstance(exc, E3LoginError):
        meta.update(response_page=exc.page, response_notice=exc.notice,
                    response_form=exc.form, response_menu=exc.menu)
        if exc.auth_step:
            meta["auth_step"] = exc.auth_step
            meta["api_code"] = exc.api_code
            details.append("API 步驟：" + {"token": "取得登入憑證", "identity": "核對本人帳號", "handoff": "建立 E3 Session", "web_session": "確認 E3 網頁登入", "courses": "讀取課程", "assignments": "讀取作業", "submission": "讀取繳交狀態"}[exc.auth_step])
            if exc.api_code:
                details.append("E3 API 錯誤代碼：" + exc.api_code)
        details.extend(["E3 回應：" + PAGE_LABELS[exc.page], NOTICE_MESSAGES[exc.notice],
                        "帳密表單：" + ("有" if exc.form else "無"),
                        "使用者選單：" + ("有" if exc.menu else "無")])
        if exc.trace:
            meta["response_trace"] = exc.trace
            pages = {**PAGE_LABELS, "home": "網站入口"}
            details.append("回應流程：" + " → ".join(
                f"{step['method']} {step['status']} {pages[step['page']]}"
                + ("（有回傳 Session Cookie）" if step["session_cookie"] else "")
                for step in exc.trace
            ))
    meta["action_detail"] = "；".join(details)
    return meta
