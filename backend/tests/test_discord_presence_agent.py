import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import e3_discord_presence as presence_agent
from tools.e3_discord_presence import DiscordIpc, build_activity


class DiscordPresenceAgentTests(unittest.TestCase):
    def test_ipc_write_sends_the_complete_frame_through_the_descriptor(self):
        class FakePipe:
            @staticmethod
            def fileno():
                return 42

            def write(self, _payload):
                raise AssertionError("FileIO.write would deadlock with the reader thread")

        ipc = DiscordIpc("123456789012345678")
        ipc.pipe = FakePipe()
        with patch(
            "tools.e3_discord_presence.os.write",
            side_effect=lambda _descriptor, payload: len(payload),
        ) as write:
            ipc._write(DiscordIpc.FRAME, {"state": "讀書"})

        self.assertEqual(write.call_args.args[0], 42)
        self.assertGreater(len(write.call_args.args[1]), 8)

    def test_inactive_payload_clears_presence(self):
        self.assertIsNone(build_activity({"active": False}))

    def test_activity_contains_today_time_and_public_progress_button(self):
        activity = build_activity(
            {
                "active": True,
                "details": "正在讀資料結構",
                "state": "今日實際學習 2 小時 15 分",
                "session_started_at": 1787788800,
                "public_url": "https://www.e3hwtool.space/study-progress",
            }
        )

        self.assertEqual(activity["details"], "正在讀資料結構")
        self.assertEqual(activity["state"], "今日實際學習 2 小時 15 分")
        self.assertEqual(activity["timestamps"], {"start": 1787788800})
        self.assertEqual(activity["buttons"][0]["label"], "查看學習進度")

    def test_discord_ipc_frame_uses_little_endian_header_and_utf8_json(self):
        frame = DiscordIpc.encode_frame(DiscordIpc.FRAME, {"state": "正在讀書"})
        opcode, length = struct.unpack("<II", frame[:8])
        payload = json.loads(frame[8:].decode("utf-8"))

        self.assertEqual(opcode, DiscordIpc.FRAME)
        self.assertEqual(length, len(frame) - 8)
        self.assertEqual(payload["state"], "正在讀書")

    def test_startup_falls_back_to_the_current_users_startup_folder(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            appdata = Path(temp_dir)
            with patch.dict(presence_agent.os.environ, {"APPDATA": str(appdata)}), patch(
                "tools.e3_discord_presence._set_registry_startup",
                side_effect=PermissionError("blocked"),
            ):
                method = presence_agent._register_user_startup(
                    '"C:\\Python\\pythonw.exe" "C:\\E3\\agent.py" run'
                )
                launcher = presence_agent._startup_folder_launcher()
                source = launcher.read_text(encoding="utf-8-sig")
            self.assertEqual(method, "Startup folder")
            self.assertIn("WScript.Shell", source)
            self.assertIn('""C:\\Python\\pythonw.exe""', source)


if __name__ == "__main__":
    unittest.main()
