import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env", override=False)

from e3_tracker.bootstrap import create_app


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def main():
    from e3_tracker.platform.security import production_mode
    if production_mode():
        raise RuntimeError("Use gunicorn with backend/wsgi.py in production, not Flask's development server")
    app = create_app()
    host = os.getenv("HOST", "127.0.0.1")
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise RuntimeError("Development server must bind to loopback; use production WSGI for public access")
    port = int(os.getenv("PORT", "8000"))
    reload_enabled = _env_flag("E3_DEV_RELOAD", default=False)
    frontend_dir = BASE_DIR / "frontend"
    extra_files = [str(path) for owner in ("assignments", "study", "shared")
                   for path in (frontend_dir / owner).rglob("*")
                   if path.is_file() and path.suffix in {".html", ".jinja", ".css", ".js"}]
    app.run(
        host=host,
        port=port,
        debug=False,
        use_reloader=reload_enabled,
        extra_files=extra_files if reload_enabled else None,
    )


if __name__ == "__main__":
    main()
