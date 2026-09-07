import unittest

from e3_tracker.services.youtube_matching import match_playlist_entries
from e3_tracker.shared.study_plan_data import STUDY_PLAN_VIDEO_INVENTORY


class YoutubeMatchingTests(unittest.TestCase):
    def setUp(self):
        self.videos = [
            {'subject': '資料結構', 'sequence': 1, 'title': '001_01-01_Stack(1時02分03秒)', 'duration_seconds': 3723},
            {'subject': '資料結構', 'sequence': 2, 'title': '002_01-02_Queue(1時02分03秒)', 'duration_seconds': 3723},
        ]

    def entry(self, video, identity='ABCDEFGHIJK', **changes):
        return {**video, 'youtube_video_id': identity, **changes}

    def test_order_does_not_control_matching(self):
        entries = [self.entry(self.videos[1], playlist_position=1), self.entry(self.videos[0], 'LMNOPQRSTUV', playlist_position=2)]
        matches, skipped = match_playlist_entries(entries, self.videos)
        self.assertEqual([item['sequence'] for item in matches], [2, 1])
        self.assertFalse(skipped)

    def test_youtube_joins_page_number_and_duration(self):
        video = {'subject': '線性代數', 'sequence': 2, 'title': '002_01-02_P.1-21(1時46分55秒)', 'duration_seconds': 6415.9}
        entry = self.entry(video, title='002 01 02 P 1 211時46分55秒')
        matched, skipped = match_playlist_entries([entry], [video])
        self.assertEqual(len(matched), 1)
        self.assertFalse(skipped)

    def test_missing_title_never_falls_back_to_position_or_old_id(self):
        matched, skipped = match_playlist_entries([self.entry(self.videos[0], title='', playlist_position=1)], self.videos)
        self.assertFalse(matched)
        self.assertEqual(len(skipped), 1)

    def test_conflicting_uploads_both_need_review(self):
        entries = [self.entry(self.videos[0]), self.entry(self.videos[0], 'LMNOPQRSTUV')]
        matched, skipped = match_playlist_entries(entries, self.videos)
        self.assertFalse(matched)
        self.assertEqual(len(skipped), 2)

    def test_duration_conflict_is_rejected(self):
        matched, skipped = match_playlist_entries([self.entry(self.videos[0], duration_seconds=60)], self.videos)
        self.assertFalse(matched)
        self.assertTrue(skipped)

    def test_different_subject_is_rejected(self):
        matched, _ = match_playlist_entries([self.entry(self.videos[0], subject='作業系統')], self.videos)
        self.assertFalse(matched)

    def test_repeated_playlist_entry_only_matches_once(self):
        entry = self.entry(self.videos[0])
        matched, skipped = match_playlist_entries([entry, entry], self.videos)
        self.assertEqual(len(matched), 1)
        self.assertFalse(skipped)

    def test_all_six_subjects_with_youtube_style_punctuation(self):
        entries = [self.entry(video, str(index), title=video['title'].replace('_', ' ').replace('-', ' '))
                   for index, video in enumerate(STUDY_PLAN_VIDEO_INVENTORY)]
        matched, skipped = match_playlist_entries(list(reversed(entries)), STUDY_PLAN_VIDEO_INVENTORY)
        self.assertFalse(skipped)
        self.assertEqual(len(matched), len(STUDY_PLAN_VIDEO_INVENTORY))

    def test_same_number_different_content_is_not_enough(self):
        matched, _ = match_playlist_entries([self.entry(self.videos[0], title='001 01-01 Completely unrelated')], self.videos)
        self.assertFalse(matched)
