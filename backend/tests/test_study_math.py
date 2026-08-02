import unittest

from e3_tracker.shared.study_math import (
    is_pure_math_expression,
    repair_math_delimiters,
    wrap_bare_math_candidate,
)


class StudyMathTests(unittest.TestCase):
    def test_mixed_english_wraps_only_the_equation(self):
        self.assertEqual(
            wrap_bare_math_candidate("Change of basis: P = B'^{-1}B."),
            "Change of basis: \\(P = B'^{-1}B\\).",
        )

    def test_mixed_chinese_wraps_only_the_equation(self):
        self.assertEqual(
            wrap_bare_math_candidate("若 A 可逆，則 A^{-1}A=I。"),
            "若 A 可逆，則 \\(A^{-1}A=I\\)。",
        )

    def test_pure_equation_is_recognized(self):
        self.assertTrue(is_pure_math_expression("P = B'^{-1}B"))
        self.assertEqual(
            wrap_bare_math_candidate("P = B'^{-1}B"),
            "\\(P = B'^{-1}B\\)",
        )

    def test_function_formula_does_not_create_nested_delimiters(self):
        self.assertEqual(
            wrap_bare_math_candidate("時間複雜度 O(n^2)"),
            "時間複雜度 \\(O(n^2)\\)",
        )

    def test_repairs_missing_and_mismatched_closing_delimiters(self):
        self.assertEqual(
            repair_math_delimiters("結果為 \\(A^{-1}B。"),
            "結果為 \\(A^{-1}B。\\)",
        )
        self.assertEqual(
            repair_math_delimiters("\\[A^2 = I\\)"),
            "\\[A^2 = I\\]",
        )

    def test_wraps_bare_matrix_without_wrapping_surrounding_prose(self):
        matrix = r"\begin{bmatrix}1&0\\0&1\end{bmatrix}"
        self.assertEqual(
            wrap_bare_math_candidate(f"單位矩陣是 {matrix}。"),
            f"單位矩陣是 \\({matrix}\\)。",
        )


if __name__ == "__main__":
    unittest.main()
