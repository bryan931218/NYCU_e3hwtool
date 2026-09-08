"""Bounded successful-result cache with per-key request coalescing."""

import threading
import time
from collections import OrderedDict


class MediaCache:
    def __init__(self, limit=32, ttl=600):
        self.limit = limit
        self.ttl = ttl
        self.values = OrderedDict()
        self.lock = threading.Lock()
        self.stripes = [threading.Lock() for _ in range(32)]

    def get_or_create(self, key, factory):
        with self.stripes[hash(key) % len(self.stripes)]:
            with self.lock:
                entry = self.values.get(key)
                if entry and time.monotonic() - entry[0] < self.ttl:
                    self.values.move_to_end(key)
                    return entry[1]
            value = factory()
            if value:
                with self.lock:
                    self.values[key] = (time.monotonic(), value)
                    self.values.move_to_end(key)
                    while len(self.values) > self.limit:
                        self.values.popitem(last=False)
            return value
