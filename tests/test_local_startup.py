import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import requests

from start_servers import build_local_env


ROOT = Path(__file__).resolve().parents[1]


class LocalStartupSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.backend_port = "18000"
        cls.frontend_port = "13000"
        cls.backend_url = f"http://127.0.0.1:{cls.backend_port}"
        cls.frontend_url = f"http://127.0.0.1:{cls.frontend_port}"
        cls.env = build_local_env()
        cls.env.update(
            {
                "HOST": "127.0.0.1",
                "PORT": cls.backend_port,
                "FRONTEND_HOST": "127.0.0.1",
                "FRONTEND_PORT": cls.frontend_port,
                "BACKEND_URL": cls.backend_url,
                "E3_APP_HOME_URL": f"{cls.frontend_url}/",
                "E3_GOOGLE_REDIRECT_URI": f"{cls.frontend_url}/google/callback",
                "E3_CACHE_DIR": cls.temp_dir.name,
                "E3_DATABASE_URL": str((Path(cls.temp_dir.name) / "e3_tracker.sqlite3").resolve()),
            }
        )

        cls.backend_proc = subprocess.Popen(
            [sys.executable, str(ROOT / "backend" / "server.py")],
            cwd=str(ROOT),
            env=cls.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        cls._wait_for_url(f"{cls.backend_url}/healthz", "backend")

        cls.frontend_proc = subprocess.Popen(
            [sys.executable, str(ROOT / "frontend" / "server.py")],
            cwd=str(ROOT),
            env=cls.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        cls._wait_for_url(f"{cls.frontend_url}/healthz", "frontend")

    @classmethod
    def tearDownClass(cls):
        for proc in (getattr(cls, "frontend_proc", None), getattr(cls, "backend_proc", None)):
            if proc is None:
                continue
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        if hasattr(cls, "temp_dir"):
            cls.temp_dir.cleanup()

    @classmethod
    def _wait_for_url(cls, url: str, label: str, timeout: float = 20.0) -> None:
        deadline = time.time() + timeout
        last_error = "service did not respond"
        while time.time() < deadline:
            try:
                response = requests.get(url, timeout=2)
                if response.ok:
                    return
                last_error = f"unexpected status {response.status_code}"
            except requests.RequestException as exc:
                last_error = str(exc)
            time.sleep(0.5)
        raise RuntimeError(f"{label} failed to start at {url}: {last_error}")

    def test_backend_healthz(self):
        response = requests.get(f"{self.backend_url}/healthz", timeout=5)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")

    def test_frontend_healthz(self):
        response = requests.get(f"{self.frontend_url}/healthz", timeout=5)
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["backend"], self.backend_url)

    def test_frontend_homepage_renders(self):
        response = requests.get(f"{self.frontend_url}/", timeout=5)
        self.assertEqual(response.status_code, 200)
        self.assertIn("E3", response.text)

    def test_guest_login_works_over_local_http(self):
        session = requests.Session()
        response = session.post(f"{self.frontend_url}/guest-login", allow_redirects=False, timeout=5)
        self.assertEqual(response.status_code, 302)
        cookie_header = response.headers.get("Set-Cookie", "")
        self.assertIn("session=", cookie_header)
        self.assertNotIn("Secure", cookie_header)

        page = session.get(f"{self.frontend_url}/", timeout=5)
        self.assertEqual(page.status_code, 200)
        self.assertIn("訪客模式資料匯入", page.text)


if __name__ == "__main__":
    unittest.main()
