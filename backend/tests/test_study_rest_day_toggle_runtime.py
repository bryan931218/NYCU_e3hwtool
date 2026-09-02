import unittest
from types import SimpleNamespace

from e3_tracker.shared.study_rest_day_toggle_runtime import (
    decorate_rest_day_toggle_markup,
    install_rest_day_toggle,
)


RESTORE_BUTTON = (
    '<button class="day-rest-button restore" type="submit" '
    'aria-label="恢復 {{ day.date }} 的原定安排" '
    'title="恢復原定安排">復</button>'
)


class StudyRestDayToggleRuntimeTests(unittest.TestCase):
    def test_active_rest_day_keeps_same_rest_button_label(self):
        rendered = decorate_rest_day_toggle_markup(RESTORE_BUTTON)

        self.assertIn('class="day-rest-button restore is-active"', rendered)
        self.assertIn('aria-label="取消 {{ day.date }} 的休息日"', rendered)
        self.assertIn('title="取消休息日"', rendered)
        self.assertIn('aria-pressed="true">休</button>', rendered)
        self.assertNotIn('>復</button>', rendered)

    def test_installer_decorates_study_template_before_render(self):
        captured = {}

        def render(template, **context):
            captured["template"] = template
            return template

        web = SimpleNamespace(render_template_string=render)
        install_rest_day_toggle(web)
        web.render_template_string(RESTORE_BUTTON)

        self.assertIn('aria-pressed="true">休</button>', captured["template"])


if __name__ == "__main__":
    unittest.main()
