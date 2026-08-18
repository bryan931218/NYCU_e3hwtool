import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RECALL_TEMPLATE = PROJECT_ROOT / "frontend" / "templates" / "study_recall.html"
HOME_TEMPLATE = PROJECT_ROOT / "frontend" / "templates" / "admin_study_home.html"
QUICK_TEMPLATE = PROJECT_ROOT / "frontend" / "templates" / "study_recall_quick.html"
WEB_MODULE = PROJECT_ROOT / "backend" / "e3_tracker" / "api" / "web.py"


class StudyCodeBlockTests(unittest.TestCase):
    def test_note_surfaces_render_fenced_code_without_katex_interference(self):
        for path in (RECALL_TEMPLATE, HOME_TEMPLATE, QUICK_TEMPLATE):
            with self.subTest(template=path.name):
                template = path.read_text(encoding="utf-8")
                self.assertIn("window.renderStudyCodeBlocks", template)
                self.assertIn("study-code-block", template)
                self.assertIn("data-study-code", template)
                self.assertIn("ignoredTags", template)
                self.assertIn("'pre'", template)
                self.assertIn("'code'", template)
                self.assertIn("overflow:auto", template)

    def test_ai_prompts_keep_program_code_out_of_latex(self):
        source = WEB_MODULE.read_text(encoding="utf-8")

        self.assertIn("程式碼不是數學公式，禁止轉成 LaTeX", source)
        self.assertIn("程式碼一律不得轉成 LaTeX", source)
        self.assertIn("Markdown fenced code block", source)
        self.assertIn("保留來源縮排、大小寫、括號、分號、陣列索引", source)
        self.assertIn("math_validation_text, _protected_code = protect_markdown_code(text)", source)


if __name__ == "__main__":
    unittest.main()
