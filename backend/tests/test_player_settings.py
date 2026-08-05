import tempfile
import unittest
from pathlib import Path

from e3_tracker.shared.deployment_runtime import (
    DeploymentSafeStorage,
    PLAYER_SETTINGS_DEFAULTS,
    normalize_player_settings,
)
from e3_tracker.shared.player_control_runtime import install_player_control_dock


class PlayerSettingsTests(unittest.TestCase):
    def test_defaults_are_available_before_first_save(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = DeploymentSafeStorage(str(Path(temp_dir) / "settings.sqlite3"))
            self.assertEqual(
                storage.load_study_player_settings(),
                PLAYER_SETTINGS_DEFAULTS,
            )
            storage._engine.dispose()

    def test_settings_survive_storage_restart(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = str(Path(temp_dir) / "settings.sqlite3")
            storage = DeploymentSafeStorage(database_path)
            saved = storage.save_study_player_settings(
                {
                    "hold_space_rate": "1.75",
                    "hold_delay_ms": "450",
                    "seek_seconds": "15",
                    "center_click_toggle": True,
                    "show_shortcut_hint": False,
                    "hint_duration_ms": "2200",
                }
            )
            storage._engine.dispose()

            restarted = DeploymentSafeStorage(database_path)
            self.assertEqual(restarted.load_study_player_settings(), saved)
            self.assertEqual(saved["hold_space_rate"], 1.75)
            self.assertEqual(saved["hold_delay_ms"], 450)
            self.assertEqual(saved["seek_seconds"], 15)
            self.assertTrue(saved["center_click_toggle"])
            self.assertFalse(saved["show_shortcut_hint"])
            self.assertEqual(saved["hint_duration_ms"], 2200)
            restarted._engine.dispose()

    def test_invalid_values_are_normalized_to_safe_limits(self):
        settings = normalize_player_settings(
            {
                "hold_space_rate": 2.42,
                "hold_delay_ms": 10,
                "seek_seconds": 999,
                "center_click_toggle": "0",
                "show_shortcut_hint": "yes",
                "hint_duration_ms": 9000,
            }
        )
        self.assertEqual(settings["hold_space_rate"], 2.0)
        self.assertEqual(settings["hold_delay_ms"], 150)
        self.assertEqual(settings["seek_seconds"], 120)
        self.assertFalse(settings["center_click_toggle"])
        self.assertTrue(settings["show_shortcut_hint"])
        self.assertEqual(settings["hint_duration_ms"], 5000)

    def test_player_control_dock_is_appended_only_once(self):
        class FakeWebModule:
            STUDY_UPLOAD_TRACKER_TEMPLATE = "<div>tracker</div>"

        module = FakeWebModule()
        install_player_control_dock(module)
        installed_template = module.STUDY_UPLOAD_TRACKER_TEMPLATE
        install_player_control_dock(module)

        self.assertEqual(module.STUDY_UPLOAD_TRACKER_TEMPLATE, installed_template)
        self.assertIn("__e3PlayerControlDockInstalled", installed_template)
        self.assertIn("data-e3-volume", installed_template)
        self.assertIn("data-e3-rate", installed_template)


if __name__ == "__main__":
    unittest.main()
