import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from e3_tracker.services.media_cache import MediaCache


class MediaCacheTests(unittest.TestCase):
    def test_concurrent_duplicate_only_runs_once(self):
        cache = MediaCache()
        calls = []
        def factory():
            calls.append(1)
            time.sleep(.03)
            return 'transcript'
        with ThreadPoolExecutor(max_workers=5) as executor:
            values = list(executor.map(lambda _: cache.get_or_create('key', factory), range(5)))
        self.assertEqual(values, ['transcript'] * 5)
        self.assertEqual(len(calls), 1)

    def test_failures_and_empty_transcripts_are_not_cached(self):
        cache = MediaCache()
        with self.assertRaises(ValueError):
            cache.get_or_create('key', lambda: int('invalid'))
        self.assertEqual(cache.get_or_create('key', lambda: ''), '')
        self.assertEqual(cache.get_or_create('key', lambda: 'retry worked'), 'retry worked')

    def test_limit_and_expiry(self):
        cache = MediaCache(limit=2, ttl=0)
        for key in range(3):
            cache.get_or_create(key, lambda: 'value')
        self.assertEqual(len(cache.values), 2)
        self.assertEqual(cache.get_or_create(2, lambda: 'new value'), 'new value')
