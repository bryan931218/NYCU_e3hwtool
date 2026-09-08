import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STYLE_PATH = PROJECT_ROOT / "frontend" / "templates" / "pages" / "study_recall" / "style-05.css.jinja"


class StudyRecallLibraryScrollbarTests(unittest.TestCase):
    def test_subject_library_has_an_independent_desktop_scroll_region(self):
        stylesheet = STYLE_PATH.read_text(encoding="utf-8")

        self.assertIn(".workspace-ready .subject-library-groups {", stylesheet)
        self.assertIn("max-height:max(260px,min(520px,calc(100dvh - 350px)))", stylesheet)
        self.assertIn("overflow-y:scroll", stylesheet)
        self.assertIn("overscroll-behavior:contain", stylesheet)
        self.assertIn("scrollbar-gutter:stable", stylesheet)
        self.assertIn(".workspace-ready .subject-library-groups { max-height:300px; }", stylesheet)


if __name__ == "__main__":
    unittest.main()
