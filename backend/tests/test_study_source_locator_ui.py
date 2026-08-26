import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RECALL_TEMPLATE = PROJECT_ROOT / "frontend" / "templates" / "study_recall.html"


class StudySourceLocatorUiTests(unittest.TestCase):
    def test_highlight_spans_full_image_width_and_preserves_vertical_bbox(self):
        template = RECALL_TEMPLATE.read_text(encoding="utf-8")

        self.assertIn("highlight.style.left = '0';", template)
        self.assertIn("highlight.style.width = '100%';", template)
        self.assertIn("highlight.style.top = `${box.top / 10}%`;", template)
        self.assertIn("highlight.style.height = `${(box.bottom - box.top) / 10}%`;", template)
        self.assertIn("const centerX = canvas.offsetLeft + canvas.clientWidth / 2;", template)
        self.assertIn("box-sizing:border-box", template)


if __name__ == "__main__":
    unittest.main()
