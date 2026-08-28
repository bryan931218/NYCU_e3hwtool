import unittest
import tempfile
from pathlib import Path

from e3_tracker.api.web import (
    _load_study_note_batch_checkpoint,
    _offset_study_note_batch_analysis,
    _save_study_note_batch_checkpoint,
    _study_note_batch_signature,
    _study_upload_parallel_progress,
    _study_upload_time_weighted_progress,
)


class StudyUploadProgressTests(unittest.TestCase):
    def test_single_batch_uses_most_of_the_progress_bar(self):
        self.assertEqual(
            _study_upload_time_weighted_progress(20, batch_index=0, batch_count=1),
            10,
        )
        self.assertEqual(
            _study_upload_time_weighted_progress(100, batch_index=0, batch_count=1),
            94,
        )

    def test_completed_batches_advance_in_equal_time_weighted_segments(self):
        self.assertEqual(
            _study_upload_time_weighted_progress(100, batch_index=0, batch_count=3),
            38,
        )
        self.assertEqual(
            _study_upload_time_weighted_progress(100, batch_index=1, batch_count=3),
            66,
        )
        self.assertEqual(
            _study_upload_time_weighted_progress(100, batch_index=2, batch_count=3),
            94,
        )

    def test_progress_is_clamped_and_monotonic_within_a_batch(self):
        values = [
            _study_upload_time_weighted_progress(progress, batch_index=1, batch_count=3)
            for progress in (0, 20, 31, 43, 52, 70, 78, 82, 100, 120)
        ]
        self.assertEqual(values, sorted(values))
        self.assertGreaterEqual(values[0], 10)
        self.assertLessEqual(values[-1], 94)

    def test_parallel_progress_uses_the_average_of_all_batches(self):
        self.assertEqual(_study_upload_parallel_progress([20, 20]), 10)
        self.assertEqual(_study_upload_parallel_progress([100, 20]), 52)
        self.assertEqual(_study_upload_parallel_progress([100, 100]), 94)

    def test_parallel_progress_does_not_jump_when_one_batch_reports_early(self):
        values = [
            _study_upload_parallel_progress(progress)
            for progress in ([20, 20, 20], [80, 20, 20], [80, 60, 20], [100, 100, 100])
        ]
        self.assertEqual(values, sorted(values))
        self.assertLess(values[1], 40)

    def test_batch_offset_updates_source_reference_and_bbox_page(self):
        analysis = {
            "source_transcription": [{"image_index": 2}],
            "uncertain_fragments": [{"image_index": 2}],
            "correction_records": [{"image_index": 2}],
            "key_concepts": [
                {
                    "source_refs": [
                        {
                            "image_index": 2,
                            "bbox": {"source_image_index": 2},
                        }
                    ],
                    "coverage_ids": ["p2b3"],
                }
            ],
        }

        _offset_study_note_batch_analysis(analysis, 8)

        self.assertEqual(analysis["source_transcription"][0]["image_index"], 10)
        self.assertEqual(analysis["uncertain_fragments"][0]["image_index"], 10)
        self.assertEqual(analysis["correction_records"][0]["image_index"], 10)
        source_ref = analysis["key_concepts"][0]["source_refs"][0]
        self.assertEqual(source_ref["image_index"], 10)
        self.assertEqual(source_ref["bbox"]["source_image_index"], 10)
        self.assertEqual(analysis["key_concepts"][0]["coverage_ids"], ["p10b3"])

    def test_batch_checkpoint_reuses_only_the_exact_same_images(self):
        images = [("page.png", b"same-image-bytes", "image/png")]
        analysis = {
            "summary": "已完成",
            "key_concepts": [{"concept": "Queue"}],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            signature = _study_note_batch_signature(images, batch_start=4)
            _save_study_note_batch_checkpoint(
                directory,
                batch_index=1,
                signature=signature,
                analysis=analysis,
            )

            restored = _load_study_note_batch_checkpoint(
                directory,
                batch_index=1,
                signature=signature,
            )
            changed_signature = _study_note_batch_signature(
                [("page.png", b"changed-image-bytes", "image/png")],
                batch_start=4,
            )
            rejected = _load_study_note_batch_checkpoint(
                directory,
                batch_index=1,
                signature=changed_signature,
            )

        self.assertEqual(restored, analysis)
        self.assertIsNone(rejected)


if __name__ == "__main__":
    unittest.main()
