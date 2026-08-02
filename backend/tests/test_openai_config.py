import os
import unittest
from unittest.mock import patch

from e3_tracker.shared.config import (
    DEFAULT_OPENAI_MODEL,
    load_env_defaults,
    normalize_openai_reasoning_effort,
)


class OpenAIConfigTests(unittest.TestCase):
    def test_gpt5_mini_is_the_default_model(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(DEFAULT_OPENAI_MODEL, "gpt-5-mini")
            self.assertEqual(load_env_defaults()["openai_model"], "gpt-5-mini")
            self.assertEqual(
                load_env_defaults()["app_home_url"],
                "https://www.e3hwtool.space/",
            )

    def test_current_low_cost_models_use_low_when_effort_is_omitted(self):
        self.assertEqual(
            normalize_openai_reasoning_effort("gpt-5.6-terra", None),
            "low",
        )
        self.assertEqual(
            normalize_openai_reasoning_effort("gpt-5.4-mini", None),
            "low",
        )

    def test_current_low_cost_models_map_legacy_minimal_to_none(self):
        self.assertEqual(
            normalize_openai_reasoning_effort("gpt-5.6-terra", "minimal"),
            "none",
        )
        self.assertEqual(
            normalize_openai_reasoning_effort("gpt-5.4-mini", "minimal"),
            "none",
        )

    def test_existing_explicit_effort_is_preserved(self):
        self.assertEqual(
            normalize_openai_reasoning_effort("gpt-5.6-terra", "medium"),
            "medium",
        )
        self.assertEqual(
            normalize_openai_reasoning_effort("gpt-5-mini", "minimal"),
            "minimal",
        )


if __name__ == "__main__":
    unittest.main()
