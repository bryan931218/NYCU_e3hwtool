import json
import struct
import unittest

from tools.e3_discord_presence import DiscordIpc, build_activity


class DiscordPresenceAgentTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
