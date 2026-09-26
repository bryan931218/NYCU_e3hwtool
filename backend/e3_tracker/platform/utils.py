import glob
import os
from typing import Any, Dict, Iterable, Set


def cleanup_debug_glob(pattern: str) -> None:
    for fp in glob.glob(pattern):
        try:
            os.remove(fp)
        except Exception:
            pass


def cleanup_debug_files(paths: Set[str]) -> None:
    for path in list(paths):
        try:
            os.remove(path)
        except Exception:
            pass


def json_safe(obj: Any):
    """Convert sets to lists so the payload can be JSON serialized."""
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe(v) for v in obj]
    if isinstance(obj, set):
        return [json_safe(v) for v in obj]
    return obj
