"""Check media retrieval; --question additionally makes a paid AI request.

Uses an isolated temporary database, never edits saved study progress.
"""

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from e3_tracker.services.youtube_frames import fetch_youtube_audio_clip, fetch_youtube_cached_frame


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('youtube_id')
    parser.add_argument('--seconds', type=float, default=120)
    parser.add_argument('--question', help='Makes one paid request through the actual question route')
    args = parser.parse_args()
    for kind, fetch in (
        ('audio', lambda: fetch_youtube_audio_clip(args.youtube_id, args.seconds, radius_seconds=10)),
        ('frame', lambda: fetch_youtube_cached_frame(args.youtube_id, args.seconds, prefer_exact=True)),
    ):
        started = time.monotonic()
        try:
            result = fetch()
            print(json.dumps({'stage': kind, 'seconds': round(time.monotonic() - started, 2),
                              'bytes': len(result['bytes']), 'source': result.get('source')}, ensure_ascii=False))
        except Exception as exc:
            print(json.dumps({'stage': kind, 'error': str(exc), 'seconds': round(time.monotonic() - started, 2)}, ensure_ascii=False))
    if not args.question:
        return
    with tempfile.TemporaryDirectory() as directory:
        os.environ.update(E3_CACHE_DIR=directory, E3_DATABASE_URL='', E3_SESSION_COOKIE_SECURE='0', E3_CANONICAL_HOST='')
        from e3_tracker.bootstrap import create_app
        app = create_app()
        storage = app.extensions['e3_storage']
        try:
            video = next(item for item in storage.list_study_plan_videos_with_records()
                         if item['youtube_video_id'] == args.youtube_id)
            storage.save_web_session('media-diagnostic', 'test-admin')
            client = app.test_client()
            with client.session_transaction() as session:
                session.update(username='test-admin', session_token='media-diagnostic', is_admin=True)
            started = time.monotonic()
            response = client.post('/admin/study-plan/video-question', json={
                'video_id': video['id'], 'playback_seconds': args.seconds, 'question': args.question,
            })
            result = response.get_json()
            result.pop('frame_image', None)
            print(json.dumps({'stage': 'answer', 'status': response.status_code,
                              'seconds': round(time.monotonic() - started, 2), 'result': result}, ensure_ascii=False))
            if response.status_code != 200:
                raise SystemExit(1)
        finally:
            storage._engine.dispose()


if __name__ == '__main__':
    main()
