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
                    "default_playback_rate": "1.35",
                    "hold_space_rate": "1.75",
                    "hold_delay_ms": "450",
                    "seek_back_seconds": "7",
                    "seek_forward_seconds": "18",
                    "seek_repeat_ms": "125",
                    "playback_rate_step": "0.1",
                    "volume_step": "3",
                    "controls_hide_ms": "4100",
                    "center_click_toggle": True,
                    "pause_on_marker": True,
                    "show_speed_presets": False,
                    "show_shortcut_hint": False,
                    "hint_duration_ms": "2200",
                }
            )
            storage._engine.dispose()

            restarted = DeploymentSafeStorage(database_path)
            self.assertEqual(restarted.load_study_player_settings(), saved)
            self.assertEqual(saved["hold_space_rate"], 1.75)
            self.assertEqual(saved["hold_delay_ms"], 450)
            self.assertEqual(saved["default_playback_rate"], 1.35)
            self.assertEqual(saved["seek_back_seconds"], 7)
            self.assertEqual(saved["seek_forward_seconds"], 18)
            self.assertEqual(saved["seek_repeat_ms"], 125)
            self.assertEqual(saved["playback_rate_step"], 0.1)
            self.assertEqual(saved["volume_step"], 3)
            self.assertEqual(saved["controls_hide_ms"], 4100)
            self.assertTrue(saved["center_click_toggle"])
            self.assertTrue(saved["pause_on_marker"])
            self.assertFalse(saved["show_speed_presets"])
            self.assertFalse(saved["show_shortcut_hint"])
            self.assertEqual(saved["hint_duration_ms"], 2200)
            restarted._engine.dispose()

    def test_invalid_values_are_normalized_to_safe_limits(self):
        settings = normalize_player_settings(
            {
                "hold_space_rate": 2.42,
                "hold_delay_ms": 10,
                "default_playback_rate": 9,
                "seek_back_seconds": 0,
                "seek_forward_seconds": 999,
                "seek_repeat_ms": 1,
                "playback_rate_step": 0.18,
                "volume_step": 50,
                "controls_hide_ms": 100,
                "center_click_toggle": "0",
                "show_shortcut_hint": "yes",
                "hint_duration_ms": 9000,
            }
        )
        self.assertEqual(settings["hold_space_rate"], 2.0)
        self.assertEqual(settings["hold_delay_ms"], 150)
        self.assertEqual(settings["default_playback_rate"], 2.0)
        self.assertEqual(settings["seek_back_seconds"], 1)
        self.assertEqual(settings["seek_forward_seconds"], 120)
        self.assertEqual(settings["seek_repeat_ms"], 75)
        self.assertEqual(settings["playback_rate_step"], 0.25)
        self.assertEqual(settings["volume_step"], 20)
        self.assertEqual(settings["controls_hide_ms"], 1200)
        self.assertFalse(settings["center_click_toggle"])
        self.assertTrue(settings["show_shortcut_hint"])
        self.assertEqual(settings["hint_duration_ms"], 5000)

    def test_hold_speed_uses_fine_grained_five_percent_steps(self):
        self.assertEqual(
            normalize_player_settings({"hold_space_rate": 1.37})["hold_space_rate"],
            1.35,
        )
        self.assertEqual(
            normalize_player_settings({"hold_space_rate": 1.39})["hold_space_rate"],
            1.4,
        )
        self.assertEqual(
            normalize_player_settings({"hold_space_rate": 0.5})["hold_space_rate"],
            1.05,
        )

    def test_player_control_dock_is_appended_only_once(self):
        class FakeWebModule:
            STUDY_UPLOAD_TRACKER_TEMPLATE = "<div>tracker</div>"

        module = FakeWebModule()
        install_player_control_dock(module)
        installed_template = module.STUDY_UPLOAD_TRACKER_TEMPLATE
        install_player_control_dock(module)

        self.assertIn("__e3PlayerControlDockInstalled", module.STUDY_UPLOAD_TRACKER_TEMPLATE)
        self.assertEqual(module.STUDY_UPLOAD_TRACKER_TEMPLATE, installed_template)
        self.assertIn("data-e3-volume", module.STUDY_UPLOAD_TRACKER_TEMPLATE)
        self.assertIn("data-e3-rate", module.STUDY_UPLOAD_TRACKER_TEMPLATE)
        self.assertIn('data-e3-rate type="range" min="0.25" max="2" step="0.05"', module.STUDY_UPLOAD_TRACKER_TEMPLATE)

    def test_fullscreen_mouse_movement_reveals_native_controls_without_hiding_dock(self):
        template_dir = Path(__file__).resolve().parents[2] / "frontend" / "templates"
        shortcut_source = (template_dir / "_player_shortcut_compat.html").read_text(
            encoding="utf-8"
        )
        dock_source = (template_dir / "_player_control_dock.html").read_text(
            encoding="utf-8"
        )
        tracker_source = (template_dir / "_study_upload_tracker.html").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "capture.addEventListener('pointermove', releaseForNativeControls",
            shortcut_source,
        )
        self.assertIn("nativeControlsActivityFrame", shortcut_source)
        self.assertIn("requestAnimationFrame(\n            flushNativeControlActivity", shortcut_source)
        self.assertNotIn(
            "capture.addEventListener('pointerenter', activateShortcutSurface)",
            shortcut_source,
        )
        self.assertIn("pointer-events: none !important", shortcut_source)
        self.assertIn(
            ".player-frame.is-focus-mode.e3-controls-idle:not(.e3-native-controls-active)",
            dock_source,
        )
        self.assertIn("opacity: 1;\n        pointer-events: none;", dock_source)
        self.assertIn("pointer-events: auto;\n        font: inherit;", dock_source)
        self.assertIn("controlsActivityFrame", dock_source)
        self.assertIn("requestAnimationFrame(flushControlActivity)", dock_source)
        self.assertIn("-webkit-backdrop-filter: none;", dock_source)
        self.assertIn(
            "if (!window.__e3PlayerShortcutCompatibilityInstalled) capture.blur()",
            tracker_source,
        )

    def test_quick_marker_uses_r_and_m_remains_mute(self):
        template_dir = Path(__file__).resolve().parents[2] / "frontend" / "templates"
        plan_source = (template_dir / "admin_study_plan.html").read_text(
            encoding="utf-8"
        )
        dock_source = (template_dir / "_player_control_dock.html").read_text(
            encoding="utf-8"
        )
        shortcut_source = (template_dir / "_player_shortcut_compat.html").read_text(
            encoding="utf-8"
        )

        self.assertIn("plainShortcut && pressedKey === 'r'", plan_source)
        self.assertNotIn("plainShortcut && pressedKey === 'm'", plan_source)
        self.assertIn('<span class="marker-shortcut">R</span>', plan_source)
        self.assertIn("event.code === 'KeyM'", dock_source)
        self.assertIn("R 快速紀錄", plan_source)
        self.assertIn("R 快速紀錄　M 靜音", shortcut_source)

    def test_seek_shortcuts_repeat_until_the_key_is_released(self):
        shortcut_source = (
            Path(__file__).resolve().parents[2]
            / "frontend"
            / "templates"
            / "_player_shortcut_compat.html"
        ).read_text(encoding="utf-8")

        self.assertIn("Number(settings.seek_repeat_ms) || 150", shortcut_source)
        self.assertIn("if (!event.repeat || heldSeekCode !== event.code)", shortcut_source)
        self.assertIn("seekOnKeyDown(event, direction)", shortcut_source)
        self.assertIn("stopContinuousSeek(event.code)", shortcut_source)
        self.assertNotIn("if (!event.repeat) seekBy", shortcut_source)

    def test_player_settings_expose_fine_grained_controls(self):
        template_dir = Path(__file__).resolve().parents[2] / "frontend" / "templates"
        settings_source = (template_dir / "admin_study_player_settings.html").read_text(
            encoding="utf-8"
        )
        shortcut_source = (template_dir / "_player_shortcut_compat.html").read_text(
            encoding="utf-8"
        )
        dock_source = (template_dir / "_player_control_dock.html").read_text(
            encoding="utf-8"
        )

        for field_name in (
            "default_playback_rate",
            "seek_back_seconds",
            "seek_forward_seconds",
            "seek_repeat_ms",
            "playback_rate_step",
            "volume_step",
            "controls_hide_ms",
            "pause_on_marker",
            "show_speed_presets",
        ):
            self.assertIn(f'name="{field_name}"', settings_source)
        self.assertIn("settings.seek_back_seconds", shortcut_source)
        self.assertIn("settings.seek_forward_seconds", shortcut_source)
        self.assertIn("settings.pause_on_marker", shortcut_source)
        self.assertIn("playerSettings.volume_step", dock_source)
        self.assertIn("playerSettings.controls_hide_ms", dock_source)

    def test_video_study_timer_starts_a_new_session_after_five_idle_minutes(self):
        template_path = (
            Path(__file__).resolve().parents[2]
            / "frontend"
            / "templates"
            / "admin_study_plan.html"
        )
        source = template_path.read_text(encoding="utf-8")
        tracker_source = (
            template_path.parent / "_study_upload_tracker.html"
        ).read_text(encoding="utf-8")

        self.assertIn("VIDEO_STUDY_IDLE_TIMEOUT_MS = 5 * 60 * 1000", source)
        self.assertIn("scheduleVideoStudyIdleCutoff", source)
        self.assertNotIn("pauseSession(videoStudySession);\n                if (!videoStudySession.pausedAt)", source)
        self.assertIn("筆記寬限中・5 分鐘內仍持續計時", tracker_source)
        self.assertIn("finalizeVideoStudySession('idle', false)", source)
        self.assertIn("runningSince: null, pausedAt: null", source)
        self.assertIn("VIDEO_STUDY_IDLE_TIMEOUT_MS = 5 * 60 * 1000", tracker_source)
        self.assertIn("scheduleIdleCutoff", tracker_source)
        self.assertIn("startForPlayback", tracker_source)
        self.assertIn("pauseForIdle", tracker_source)
        self.assertIn("event.source !== iframe.contentWindow", tracker_source)


if __name__ == "__main__":
    unittest.main()
