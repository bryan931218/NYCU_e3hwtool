#!/usr/bin/env python3
"""Sync E3 study time to the signed-in Discord desktop client on Windows."""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import shutil
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, Optional


APP_DIR = Path(os.getenv("LOCALAPPDATA") or Path.home()) / "E3DiscordPresence"
CONFIG_PATH = APP_DIR / "config.json"
LOG_PATH = APP_DIR / "presence.log"
AGENT_PATH = APP_DIR / "e3_discord_presence.py"
TASK_NAME = "E3 Discord Study Presence"
RUN_VALUE_NAME = "E3DiscordStudyPresence"
RUN_KEY_PATH = r"Software\Microsoft\Windows\CurrentVersion\Run"
POLL_SECONDS = 30
STALE_CLEAR_SECONDS = 120
ERROR_ALREADY_EXISTS = 183


def _log(message: str) -> None:
    APP_DIR.mkdir(parents=True, exist_ok=True)
    try:
        if LOG_PATH.exists() and LOG_PATH.stat().st_size > 512 * 1024:
            backup = LOG_PATH.with_suffix(".log.1")
            backup.unlink(missing_ok=True)
            LOG_PATH.replace(backup)
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with LOG_PATH.open("a", encoding="utf-8") as output:
            output.write(f"[{stamp}] {message}\n")
    except OSError:
        pass


def _read_config() -> Dict[str, Any]:
    try:
        payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        raise RuntimeError(f"Missing or invalid config: {CONFIG_PATH}")
    if not isinstance(payload, dict):
        raise RuntimeError(f"Invalid config: {CONFIG_PATH}")
    api_url = str(payload.get("api_url") or "").strip()
    client_id = str(payload.get("client_id") or "").strip()
    token = str(payload.get("token") or "").strip()
    if not api_url.startswith(("https://", "http://127.0.0.1", "http://localhost")):
        raise RuntimeError("The API URL must use HTTPS.")
    if not (client_id.isdigit() and 17 <= len(client_id) <= 20):
        raise RuntimeError("Invalid Discord Application ID.")
    if len(token) < 32:
        raise RuntimeError("Invalid E3 presence token.")
    return {"api_url": api_url, "client_id": client_id, "token": token}


def _write_config(api_url: str, client_id: str, token: str) -> None:
    APP_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "api_url": str(api_url).strip(),
        "client_id": str(client_id).strip(),
        "token": str(token).strip(),
    }
    CONFIG_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _read_config()


def fetch_presence(config: Dict[str, Any], timeout: int = 15) -> Dict[str, Any]:
    request = urllib.request.Request(
        str(config["api_url"]),
        headers={
            "Authorization": f"Bearer {config['token']}",
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "User-Agent": "E3DiscordPresence/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise RuntimeError("The E3 presence API returned an invalid response.")
    return payload


def build_activity(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not payload.get("active"):
        return None
    details = str(payload.get("details") or "正在讀書").strip()[:128]
    state = str(payload.get("state") or "").strip()[:128]
    activity: Dict[str, Any] = {
        "type": 0,
        "details": details,
        "state": state,
        "instance": False,
    }
    try:
        started_at = int(payload.get("session_started_at") or 0)
    except (TypeError, ValueError):
        started_at = 0
    if started_at > 0:
        activity["timestamps"] = {"start": started_at}
    public_url = str(payload.get("public_url") or "").strip()
    if public_url.startswith("https://"):
        activity["buttons"] = [{"label": "查看學習進度", "url": public_url}]
    return activity


class DiscordIpc:
    HANDSHAKE = 0
    FRAME = 1
    CLOSE = 2
    PING = 3
    PONG = 4

    def __init__(self, client_id: str) -> None:
        self.client_id = str(client_id)
        self.pipe: Any = None
        self.reader: Optional[threading.Thread] = None
        self.stop_reader = threading.Event()
        self.ready = threading.Event()
        self.write_lock = threading.Lock()

    @staticmethod
    def encode_frame(opcode: int, payload: Any) -> bytes:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return struct.pack("<II", int(opcode), len(body)) + body

    @staticmethod
    def decode_header(header: bytes) -> tuple[int, int]:
        if len(header) != 8:
            raise EOFError("Discord IPC header is incomplete")
        return struct.unpack("<II", header)

    def _write(self, opcode: int, payload: Any) -> None:
        if self.pipe is None:
            raise ConnectionError("Discord IPC is not connected")
        frame = self.encode_frame(opcode, payload)
        with self.write_lock:
            self.pipe.write(frame)
            self.pipe.flush()

    def _read_exact(self, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size and not self.stop_reader.is_set():
            chunk = self.pipe.read(size - len(chunks))
            if not chunk:
                raise EOFError("Discord IPC closed")
            chunks.extend(chunk)
        return bytes(chunks)

    def _drain(self) -> None:
        try:
            while not self.stop_reader.is_set() and self.pipe is not None:
                opcode, length = self.decode_header(self._read_exact(8))
                body = self._read_exact(length) if length else b""
                if opcode == self.PING:
                    payload = json.loads(body.decode("utf-8")) if body else None
                    self._write(self.PONG, payload)
                elif opcode == self.CLOSE:
                    break
                elif opcode == self.FRAME and body:
                    payload = json.loads(body.decode("utf-8"))
                    if payload.get("evt") == "READY":
                        self.ready.set()
        except (OSError, EOFError, ValueError, ConnectionError):
            pass
        finally:
            self.stop_reader.set()

    def connect(self) -> None:
        if self.pipe is not None and not self.stop_reader.is_set():
            return
        self.close()
        last_error: Optional[Exception] = None
        for index in range(10):
            try:
                pipe = open(rf"\\?\pipe\discord-ipc-{index}", "r+b", buffering=0)
                self.pipe = pipe
                self.stop_reader.clear()
                self.ready.clear()
                self._write(self.HANDSHAKE, {"v": 1, "client_id": self.client_id})
                self.reader = threading.Thread(target=self._drain, daemon=True)
                self.reader.start()
                if not self.ready.wait(timeout=3):
                    raise ConnectionError("Discord IPC handshake timed out")
                _log(f"Connected to Discord IPC pipe {index}.")
                return
            except (OSError, ConnectionError) as exc:
                last_error = exc
                self.close()
        raise ConnectionError("Discord desktop client is not available") from last_error

    def set_activity(self, activity: Optional[Dict[str, Any]]) -> None:
        self.connect()
        self._write(
            self.FRAME,
            {
                "cmd": "SET_ACTIVITY",
                "args": {"pid": os.getpid(), "activity": activity},
                "nonce": uuid.uuid4().hex,
            },
        )

    def close(self) -> None:
        self.stop_reader.set()
        self.ready.clear()
        pipe, self.pipe = self.pipe, None
        if pipe is not None:
            try:
                pipe.close()
            except OSError:
                pass


def _acquire_single_instance() -> Any:
    if os.name != "nt":
        return object()
    handle = ctypes.windll.kernel32.CreateMutexW(None, False, "Local\\E3DiscordStudyPresence")
    if not handle or ctypes.windll.kernel32.GetLastError() == ERROR_ALREADY_EXISTS:
        raise RuntimeError("E3 Discord Presence is already running.")
    return handle


def run_agent(*, once: bool = False) -> int:
    config = _read_config()
    mutex = _acquire_single_instance()
    discord = DiscordIpc(str(config["client_id"]))
    last_activity_json = ""
    last_good_at = 0.0
    cleared_after_error = False
    _log("Agent started.")
    try:
        while True:
            try:
                payload = fetch_presence(config)
                activity = build_activity(payload)
                serialized = json.dumps(activity, ensure_ascii=False, sort_keys=True)
                if serialized != last_activity_json or discord.stop_reader.is_set():
                    discord.set_activity(activity)
                    last_activity_json = serialized
                last_good_at = time.monotonic()
                cleared_after_error = False
            except urllib.error.HTTPError as exc:
                _log(f"E3 API HTTP error: {exc.code}")
                if exc.code == 401:
                    return 2
            except (urllib.error.URLError, TimeoutError, OSError, ValueError, RuntimeError, ConnectionError) as exc:
                _log(f"Sync retry: {exc}")
                if last_good_at and time.monotonic() - last_good_at >= STALE_CLEAR_SECONDS and not cleared_after_error:
                    try:
                        discord.set_activity(None)
                        last_activity_json = "null"
                        cleared_after_error = True
                    except (OSError, ConnectionError):
                        discord.close()
            if once:
                return 0
            time.sleep(POLL_SECONDS)
    finally:
        discord.close()
        if os.name == "nt" and mutex:
            ctypes.windll.kernel32.CloseHandle(mutex)
        _log("Agent stopped.")


def _pythonw_path() -> Path:
    executable = Path(sys.executable)
    candidate = executable.with_name("pythonw.exe")
    return candidate if candidate.is_file() else executable


def _startup_folder_launcher() -> Path:
    appdata = str(os.getenv("APPDATA") or "").strip()
    if not appdata:
        raise RuntimeError("Windows APPDATA is unavailable.")
    return (
        Path(appdata)
        / "Microsoft"
        / "Windows"
        / "Start Menu"
        / "Programs"
        / "Startup"
        / "E3 Discord Study Presence.vbs"
    )


def _set_registry_startup(command: str) -> None:
    import winreg

    with winreg.CreateKey(winreg.HKEY_CURRENT_USER, RUN_KEY_PATH) as key:
        winreg.SetValueEx(key, RUN_VALUE_NAME, 0, winreg.REG_SZ, command)


def _register_user_startup(command: str) -> str:
    try:
        _set_registry_startup(command)
        launcher = _startup_folder_launcher()
        launcher.unlink(missing_ok=True)
        return "current-user registry"
    except (ImportError, OSError):
        launcher = _startup_folder_launcher()
        launcher.parent.mkdir(parents=True, exist_ok=True)
        escaped_command = command.replace('"', '""')
        launcher.write_text(
            "Set runner = CreateObject(\"WScript.Shell\")\n"
            f"runner.Run \"{escaped_command}\", 0, False\n",
            encoding="utf-8-sig",
        )
        return "Startup folder"


def _remove_user_startup() -> None:
    try:
        import winreg

        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            RUN_KEY_PATH,
            0,
            winreg.KEY_SET_VALUE,
        ) as key:
            winreg.DeleteValue(key, RUN_VALUE_NAME)
    except (ImportError, FileNotFoundError, OSError):
        pass
    try:
        _startup_folder_launcher().unlink(missing_ok=True)
    except (OSError, RuntimeError):
        pass


def install_startup() -> None:
    if os.name != "nt":
        raise RuntimeError("Automatic startup is only supported on Windows.")
    _read_config()
    APP_DIR.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).resolve()
    if source != AGENT_PATH.resolve():
        shutil.copy2(source, AGENT_PATH)
    command = f'"{_pythonw_path()}" "{AGENT_PATH}" run'
    startup_method = _register_user_startup(command)
    _log(f"Installed automatic startup via {startup_method}.")
    creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    subprocess.Popen(
        [str(_pythonw_path()), str(AGENT_PATH), "run"],
        creationflags=creation_flags,
        close_fds=True,
    )


def uninstall_startup(remove_config: bool = False) -> None:
    _remove_user_startup()
    if os.name == "nt":
        subprocess.run(
            ["schtasks", "/Delete", "/TN", TASK_NAME, "/F"],
            check=False,
            capture_output=True,
        )
    if remove_config:
        CONFIG_PATH.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="E3 Discord study presence agent")
    subparsers = parser.add_subparsers(dest="command", required=True)
    configure = subparsers.add_parser("configure")
    configure.add_argument("--url", required=True)
    configure.add_argument("--client-id", required=True)
    configure.add_argument("--token", required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--once", action="store_true")
    subparsers.add_parser("install-startup")
    uninstall = subparsers.add_parser("uninstall-startup")
    uninstall.add_argument("--remove-config", action="store_true")
    subparsers.add_parser("status")
    args = parser.parse_args()

    if args.command == "configure":
        _write_config(args.url, args.client_id, args.token)
        print(f"Configured: {CONFIG_PATH}")
        return 0
    if args.command == "install-startup":
        install_startup()
        print("Installed and started E3 Discord Presence.")
        return 0
    if args.command == "uninstall-startup":
        uninstall_startup(args.remove_config)
        print("Removed the Windows startup task.")
        return 0
    if args.command == "status":
        print(json.dumps(fetch_presence(_read_config()), ensure_ascii=False, indent=2))
        return 0
    return run_agent(once=bool(args.once))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as error:
        _log(f"Fatal error: {error}")
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
