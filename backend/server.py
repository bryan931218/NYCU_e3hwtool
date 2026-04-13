import os
from pathlib import Path

from dotenv import load_dotenv

from e3_tracker.api.web import create_app

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env", override=False)


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def main():
    app = create_app()
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    reload_enabled = _env_flag("E3_DEV_RELOAD", default=False)
    template_dir = BASE_DIR / "frontend" / "templates"
    extra_files = [str(path) for path in template_dir.rglob("*.html")]
    app.run(
        host=host,
        port=port,
        debug=reload_enabled,
        use_reloader=reload_enabled,
        extra_files=extra_files if reload_enabled else None,
    )


if __name__ == "__main__":
    main()
