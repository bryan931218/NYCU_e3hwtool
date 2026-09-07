"""Read template source including its static Jinja page fragments."""
import re
from pathlib import Path

TEMPLATE_ROOT = Path(__file__).resolve().parents[2] / 'frontend' / 'templates'


def read_source(path):
    source = Path(path).read_text(encoding='utf-8')
    return re.sub(
        r"\{% include '([^']+)' %\}",
        lambda match: read_source(TEMPLATE_ROOT / match.group(1)),
        source,
    )
