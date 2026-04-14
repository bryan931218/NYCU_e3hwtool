import os
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv
from flask import Flask, Response, render_template, request

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=ROOT / ".env", override=False)

BACKEND_BASE = os.getenv("BACKEND_URL", "http://127.0.0.1:8000").rstrip("/")
FRONTEND_HOST = os.getenv("FRONTEND_HOST", "0.0.0.0")
FRONTEND_PORT = int(os.getenv("FRONTEND_PORT", "3000"))
TEMPLATE_DIR = ROOT / "frontend" / "templates"
DEV_RELOAD_INTERVAL_MS = int(os.getenv("E3_DEV_RELOAD_INTERVAL_MS", "1200"))

SUPPORTED_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]

app = Flask(__name__, template_folder=str(TEMPLATE_DIR))
app.jinja_env.globals.setdefault("url_for", lambda *_, **__: "#")
app.jinja_env.globals.setdefault("get_flashed_messages", lambda **__: [])


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _dev_reload_enabled() -> bool:
    return _env_flag("E3_DEV_RELOAD", default=False)


def _compute_reload_token() -> str:
    latest_mtime = 0
    watch_paths = [TEMPLATE_DIR, ROOT / "frontend" / "server.py"]
    for path in watch_paths:
        if not path.exists():
            continue
        if path.is_file():
            latest_mtime = max(latest_mtime, path.stat().st_mtime_ns)
            continue
        for child in path.rglob("*"):
            if child.is_file():
                latest_mtime = max(latest_mtime, child.stat().st_mtime_ns)
    return str(latest_mtime)


def _inject_live_reload(html: str) -> str:
    if not _dev_reload_enabled():
        return html
    script = f"""
<script>
(function () {{
    const endpoint = "/_dev/reload-token";
    const intervalMs = {DEV_RELOAD_INTERVAL_MS};
    let currentToken = null;
    async function poll() {{
        try {{
            const resp = await fetch(endpoint, {{ cache: "no-store", credentials: "same-origin" }});
            if (!resp.ok) return;
            const payload = await resp.json();
            const nextToken = String(payload.token || "");
            if (!currentToken) {{
                currentToken = nextToken;
                return;
            }}
            if (nextToken && nextToken !== currentToken) {{
                window.location.reload();
            }}
        }} catch (err) {{}}
    }}
    poll();
    setInterval(poll, intervalMs);
}})();
</script>
""".strip()
    closing_body_index = html.lower().rfind("</body>")
    if closing_body_index == -1:
        return html + script
    return html[:closing_body_index] + script + html[closing_body_index:]


def _non_hop_headers():
    hop_headers = {"host", "content-length", "connection", "accept-encoding"}
    headers = {k: v for k, v in request.headers.items() if k.lower() not in hop_headers}
    if request.remote_addr:
        chain = headers.get("X-Forwarded-For")
        headers["X-Forwarded-For"] = f"{chain}, {request.remote_addr}" if chain else request.remote_addr
    headers["X-Forwarded-Host"] = request.host
    headers["X-Forwarded-Proto"] = request.scheme
    return headers


def _build_target(path: str) -> str:
    path = path.lstrip("/")
    return urljoin(f"{BACKEND_BASE}/", path)


@app.route("/healthz", methods=["GET"])
def health_check():
    return {"status": "ok", "backend": BACKEND_BASE}


@app.route("/_dev/reload-token", methods=["GET"])
def dev_reload_token():
    if not _dev_reload_enabled():
        return {"enabled": False, "token": ""}
    return {"enabled": True, "token": _compute_reload_token()}


@app.route("/", defaults={"path": ""}, methods=SUPPORTED_METHODS)
@app.route("/<path:path>", methods=SUPPORTED_METHODS)
def proxy(path: str):
    url = _build_target(path or "")
    headers = _non_hop_headers()
    data = request.get_data()
    files = None
    form_data = None
    if request.files:
        files = {
            key: (file.filename, file.stream, file.mimetype)
            for key, file in request.files.items()
        }
        form_data = request.form.to_dict(flat=False)
    stream = request.method == "GET"
    try:
        resp = requests.request(
            request.method,
            url,
            params=request.args,
            data=form_data if files is not None else data,
            files=files,
            headers=headers,
            cookies=request.cookies,
            allow_redirects=False,
            timeout=30,
            stream=stream,
        )
    except requests.RequestException as exc:
        if request.method == "GET" and _accepts_html():
            return _render_mock_page(request.path, exc)
        return Response(f"Backend unreachable: {exc}", status=502)

    excluded_headers = {"content-encoding", "transfer-encoding", "connection"}
    content_type = resp.headers.get("Content-Type", "")
    should_inject_live_reload = _dev_reload_enabled() and "text/html" in content_type.lower()
    if stream:
        if should_inject_live_reload:
            body_text = resp.text
            resp.close()
            proxy_response = Response(_inject_live_reload(body_text), status=resp.status_code)
        else:
            def generate():
                try:
                    for chunk in resp.iter_content(chunk_size=64 * 1024):
                        if chunk:
                            yield chunk
                finally:
                    resp.close()

            body = generate()
            proxy_response = Response(body, status=resp.status_code, direct_passthrough=True)
    else:
        proxy_response = Response(resp.content, status=resp.status_code)
    for header, value in resp.headers.items():
        if header.lower() in excluded_headers:
            continue
        if should_inject_live_reload and header.lower() == "content-length":
            continue
        proxy_response.headers[header] = value
    return proxy_response


def _accepts_html() -> bool:
    accept = request.headers.get("Accept", "")
    return "text/html" in accept or "*/*" in accept or not accept


def _mock_context() -> dict:
    now = datetime.now()
    upcoming = now + timedelta(days=3)
    overdue = now - timedelta(days=1)
    sample_courses = [
        {
            "id": 1,
            "title": "【112上】資料結構",
            "url": "#",
            "assignments": [
                {
                    "course_id": 1,
                    "course_title": "【112上】資料結構",
                    "title": "HW1 - Linked List",
                    "url": "#",
                    "due_at": upcoming.strftime("%Y-%m-%d %H:%M"),
                    "due_ts": int(upcoming.timestamp()),
                    "overdue": False,
                    "completed": False,
                    "raw_status_text": "繳交期限內",
                    "grade_text": "95 / 100",
                },
                {
                    "course_id": 1,
                    "course_title": "【112上】資料結構",
                    "title": "Lab Exercise",
                    "url": "#",
                    "due_at": overdue.strftime("%Y-%m-%d %H:%M"),
                    "due_ts": int(overdue.timestamp()),
                    "overdue": True,
                    "completed": False,
                    "raw_status_text": "已逾期",
                    "grade_text": None,
                },
            ],
            "detected_assign_links": 2,
        }
    ]
    return {
        "result": {
            "courses": sample_courses,
            "all_assignments": sample_courses[0]["assignments"],
            "errors": [],
        },
        "excel_data": None,
        "google_ready": False,
        "google_linked": False,
        "cache_ts": int(now.timestamp()),
        "now_ts": int(now.timestamp()),
    }


def _render_mock_page(path: str, exc: Exception) -> Response:
    template = "login.html" if path.strip("/").startswith("login") else "web.html"
    context = _mock_context()
    context["mock_message"] = str(exc)
    html = render_template(template, **context)
    return Response(html, status=200)


def main():
    reload_enabled = _dev_reload_enabled()
    extra_files = [str(path) for path in TEMPLATE_DIR.rglob("*.html")]
    app.run(
        host=FRONTEND_HOST,
        port=FRONTEND_PORT,
        debug=reload_enabled,
        use_reloader=reload_enabled,
        extra_files=extra_files if reload_enabled else None,
    )


if __name__ == "__main__":
    main()
