import unittest
from types import SimpleNamespace

from e3_tracker.shared.study_recall_library_runtime import (
    install_study_recall_library_runtime,
)


class StudyRecallLibraryRuntimeTests(unittest.TestCase):
    def test_subject_library_partial_is_injected_only_once(self):
        module = SimpleNamespace(
            STUDY_RECALL_TEMPLATE="<html><body><main>recall</main></body></html>",
        )

        install_study_recall_library_runtime(module)
        first_template = module.STUDY_RECALL_TEMPLATE
        install_study_recall_library_runtime(module)

        self.assertEqual(module.STUDY_RECALL_TEMPLATE, first_template)
        self.assertEqual(
            module.STUDY_RECALL_TEMPLATE.count(
                "__e3StudyRecallSubjectLibraryInstalled"
            ),
            1,
        )
        self.assertIn("data-recall-subject-library", module.STUDY_RECALL_TEMPLATE)
        self.assertIn("科目筆記", module.STUDY_RECALL_TEMPLATE)
        self.assertIn("全部展開", module.STUDY_RECALL_TEMPLATE)


if __name__ == "__main__":
    unittest.main()
