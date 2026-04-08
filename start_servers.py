import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
LOCAL_DATA_DIR = ROOT / ".localdata"

load_dotenv(dotenv_path=ROOT / ".env", override=False)


def _start_process(label: str, script: Path, extra_env: Optional[Dict[str, str]] = None) -> Tuple[str, subprocess.Popen]:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    cmd = [sys.executable, str(script)]
    proc = subprocess.Popen(cmd, cwd=str(ROOT), env=env)
    print(f"[starter] launched {label} ({' '.join(cmd)}) pid={proc.pid}")
    return label, proc


def _public_host(host: str) -> str:
    normalized = (host or "").strip()
    if normalized in {"", "0.0.0.0", "::"}:
        return "127.0.0.1"
    return normalized


def _normalize_windows_path(value: Path | str) -> str:
    text = str(value)
    if os.name != "nt":
        return text
    if text.startswith("\\\\?\\UNC\\"):
        return "\\" + text[7:]
    if text.startswith("\\\\?\\"):
        return text[4:]
    return text


def build_local_env() -> Dict[str, str]:
    backend_host = os.getenv("HOST", "127.0.0.1")
    backend_port = os.getenv("PORT", "8000")
    frontend_host = os.getenv("FRONTEND_HOST", "127.0.0.1")
    frontend_port = os.getenv("FRONTEND_PORT", "3000")
    public_backend_host = _public_host(backend_host)
    public_frontend_host = _public_host(frontend_host)
    backend_url = f"http://{public_backend_host}:{backend_port}"
    frontend_url = f"http://{public_frontend_host}:{frontend_port}"
    local_db_path = (LOCAL_DATA_DIR / "e3_tracker.sqlite3").resolve()

    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env.update(
        {
            "HOST": backend_host,
            "PORT": backend_port,
            "FRONTEND_HOST": frontend_host,
            "FRONTEND_PORT": frontend_port,
            "BACKEND_URL": backend_url,
            "E3_SESSION_COOKIE_SECURE": "0",
            "E3_CANONICAL_HOST": "",
            "E3_APP_HOME_URL": f"{frontend_url}/",
            "E3_GOOGLE_REDIRECT_URI": f"{frontend_url}/google/callback",
            "E3_CACHE_DIR": _normalize_windows_path(LOCAL_DATA_DIR),
            "E3_DATABASE_URL": _normalize_windows_path(local_db_path),
        }
    )
    return env


def main():
    env = build_local_env()
    frontend_url = env["E3_APP_HOME_URL"].rstrip("/")
    backend_url = env["BACKEND_URL"]
    processes: list[tuple[str, subprocess.Popen]] = []
    try:
        print(f"[starter] local frontend: {frontend_url}")
        print(f"[starter] local backend: {backend_url}")
        print(f"[starter] local data dir: {LOCAL_DATA_DIR}")
        processes.append(_start_process("backend", ROOT / "backend" / "server.py", env))
        processes.append(_start_process("frontend", ROOT / "frontend" / "server.py", env))
        while True:
            time.sleep(1)
            for label, proc in processes:
                code = proc.poll()
                if code is not None:
                    raise RuntimeError(f"{label} server stopped with exit code {code}")
    except KeyboardInterrupt:
        print("[starter] ctrl-c received, stopping servers…")
    except RuntimeError as exc:
        print(f"[starter] {exc}")
    finally:
        for label, proc in processes:
            if proc.poll() is None:
                print(f"[starter] terminating {label} (pid={proc.pid})")
                proc.terminate()
        for _, proc in processes:
            if proc.poll() is None:
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()


if __name__ == "__main__":
    main()
