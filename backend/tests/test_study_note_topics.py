import unittest

from e3_tracker.services.note_topics import coarse_study_topic


class StudyNoteTopicTests(unittest.TestCase):
    def test_heap_variants_share_one_parent_topic(self):
        topics = (
            "Heap 定義與性質",
            "Heap 操作",
            "Heap 刪除 (Delete Max)",
            "Heap 建構方法 - Top-Down",
            "Min-Max Heap 刪除示例",
        )

        self.assertEqual({coarse_study_topic(topic) for topic in topics}, {"Heap"})

    def test_related_structures_keep_their_own_parent_topics(self):
        self.assertEqual(coarse_study_topic("Deap 操作—刪除最小值"), "Deap")
        self.assertEqual(coarse_study_topic("SMMH 操作範例"), "SMMH")
        self.assertEqual(coarse_study_topic("AVL minimal nodes formula"), "AVL 樹")
        self.assertEqual(coarse_study_topic("WEPL 最小化—Huffman 演算法"), "Huffman 與 WEPL")

    def test_unknown_topics_drop_detail_suffix_without_becoming_empty(self):
        self.assertEqual(coarse_study_topic("關聯式資料庫正規化步驟"), "關聯式資料庫正規化")
        self.assertEqual(coarse_study_topic("記憶體配置", ""), "記憶體配置")


if __name__ == "__main__":
    unittest.main()
