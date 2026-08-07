import unittest
from types import SimpleNamespace

from e3_tracker.shared.study_note_upload_runtime import (
    SAFE_STUDY_NOTE_AI_BATCH_SIZE,
    install_study_note_upload_runtime,
)


class StudyNoteUploadRuntimeTests(unittest.TestCase):
    def test_large_default_batch_is_reduced_for_dense_notes(self):
        module = SimpleNamespace(STUDY_NOTE_AI_BATCH_SIZE=8)

        install_study_note_upload_runtime(module)

        self.assertEqual(module.STUDY_NOTE_AI_BATCH_SIZE, SAFE_STUDY_NOTE_AI_BATCH_SIZE)
        self.assertEqual(module.STUDY_NOTE_AI_BATCH_SIZE, 4)

    def test_existing_smaller_batch_is_preserved(self):
        module = SimpleNamespace(STUDY_NOTE_AI_BATCH_SIZE=3)

        install_study_note_upload_runtime(module)

        self.assertEqual(module.STUDY_NOTE_AI_BATCH_SIZE, 3)

    def test_installation_is_idempotent(self):
        module = SimpleNamespace(STUDY_NOTE_AI_BATCH_SIZE=8)

        install_study_note_upload_runtime(module)
        install_study_note_upload_runtime(module)

        self.assertEqual(module.STUDY_NOTE_AI_BATCH_SIZE, 4)


if __name__ == "__main__":
    unittest.main()
