import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent

load_dotenv(dotenv_path=ROOT / ".env", override=False)


def _start_process(label: str, script: Path, extra_env: Optional[Dict[str, str]] = None) -> Tuple[str, subprocess.Popen]:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    cmd = [sys.executable, str(script)]
    proc = subprocess.Popen(cmd, cwd=str(ROOT), env=env)
    print(f"[starter] launched {label} ({' '.join(cmd)}) pid={proc.pid}")
    return label, proc


def main():
    processes: list[tuple[str, subprocess.Popen]] = []
    try:
        processes.append(_start_process("backend", ROOT / "backend" / "server.py"))
        processes.append(
            _start_process(
                "frontend",
                ROOT / "frontend" / "server.py",
                {"BACKEND_URL": os.getenv("BACKEND_URL", "http://127.0.0.1:8000")},
            )
        )
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
