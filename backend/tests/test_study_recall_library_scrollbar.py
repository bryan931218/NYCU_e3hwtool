import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
STYLE_PATH = PROJECT_ROOT / "frontend" / "templates" / "pages" / "study_recall" / "style-05.css.jinja"
LIBRARY_PARTIAL_PATH = PROJECT_ROOT / "frontend" / "templates" / "_study_recall_subject_library.html"


class StudyRecallLibraryScrollbarTests(unittest.TestCase):
    def test_subject_library_has_an_independent_desktop_scroll_region(self):
        stylesheet = STYLE_PATH.read_text(encoding="utf-8")
        library_partial = LIBRARY_PARTIAL_PATH.read_text(encoding="utf-8")

        self.assertIn(".workspace-ready .subject-library-groups {", stylesheet)
        self.assertIn("display:block", stylesheet)
        self.assertIn("max-height:max(260px,min(520px,calc(100dvh - 350px)))", stylesheet)
        self.assertIn("overflow-y:scroll", stylesheet)
        self.assertIn("overscroll-behavior:contain", stylesheet)
        self.assertIn("scrollbar-gutter:stable", stylesheet)
        self.assertIn(".workspace-ready .subject-library-groups { max-height:300px; }", stylesheet)
        self.assertIn(".subject-library-groups { display:block; }", library_partial)
        self.assertNotIn(".subject-library-groups { display:grid;", library_partial)
        self.assertIn(".subject-library-group + .subject-library-group { margin-top:8px; }", library_partial)


if __name__ == "__main__":
    unittest.main()
