"""Conservative, order-independent matching of course video titles."""

import math
import re
import unicodedata
from collections import Counter
from difflib import SequenceMatcher


def title_identity(value, duration_seconds=0):
    title = unicodedata.normalize('NFKC', str(value or '')).casefold().strip()
    title = re.sub(r'\.(mp4|mkv|webm|mov)$', '', title)
    # YouTube may remove brackets: P.1-21(1時...) becomes P 1 211時...
    # Anchor the hour to metadata when available, never swallow page digits.
    try:
        hours = str(int(float(duration_seconds) // 3600)) if float(duration_seconds or 0) > 0 else r'\d'
    except (TypeError, ValueError, OverflowError):
        hours = r'\d'
    duration = re.search(r'(' + hours + r')\s*(?:小時|時)\s*(\d+)\s*分\s*(\d+)\s*秒', title)
    seconds = (int(duration[1]) * 3600 + int(duration[2]) * 60 + int(duration[3])) if duration else 0
    if duration:
        title = title[:duration.start()] + title[duration.end():]
    sequence = re.match(r'^(\d{1,3})(?=[\s_.\-、])', title)
    number = int(sequence[1]) if sequence else None
    # Normalize punctuation, not digits: chapter/page numbers distinguish lessons.
    key = ''.join(char for char in title if char.isalnum())
    return key, number, seconds


def _duration(item, fallback):
    try:
        value = float(item.get('duration_seconds') or fallback or 0)
        return value if math.isfinite(value) and value > 0 else 0
    except (ValueError, TypeError):
        return 0


def match_playlist_entries(entries, videos):
    proposals = []
    skipped = []
    # Duplicate appearances of the same upload must not occupy multiple lessons.
    unique = {}
    for entry in entries:
        identity = (entry.get('subject'), entry.get('youtube_video_id'))
        unique.setdefault(identity, entry)
    for entry in unique.values():
        key, number, title_duration = title_identity(entry.get('title'), entry.get('duration_seconds'))
        candidates = []
        for video in videos:
            if video.get('subject') != entry.get('subject'):
                continue
            target, target_number, target_duration = title_identity(video.get('title'), video.get('duration_seconds'))
            if not key or not target:
                continue
            if number is not None and target_number is not None and number != target_number:
                continue
            actual_duration = _duration(entry, title_duration)
            expected_duration = _duration(video, target_duration)
            if actual_duration and expected_duration and abs(actual_duration - expected_duration) > max(90, expected_duration * .05):
                continue
            similarity = SequenceMatcher(None, key, target, autojunk=False).ratio()
            # Fuzzy matches require both numbering and duration corroboration.
            if key != target and not (
                similarity >= .93 and number is not None and number == target_number
                and actual_duration and expected_duration
            ):
                continue
            candidates.append((similarity, video))
        candidates.sort(key=lambda candidate: candidate[0], reverse=True)
        if not candidates or (len(candidates) > 1 and candidates[0][0] - candidates[1][0] < .08):
            skipped.append({**entry, 'reason': '標題或片長不足以確認，或存在多個相近候選'})
            continue
        score, video = candidates[0]
        proposals.append({**entry, 'sequence': video['sequence'], 'match_score': round(score, 3)})
    counts = Counter((item['subject'], item['sequence']) for item in proposals)
    matched = []
    for item in proposals:
        if counts[(item['subject'], item['sequence'])] != 1:
            skipped.append({**item, 'reason': '多支 YouTube 影片對應同一堂課，需手動確認'})
        else:
            matched.append(item)
    return matched, skipped
