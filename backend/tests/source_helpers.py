"""Read template source including its static Jinja page fragments."""
import re
from pathlib import Path

FRONTEND_ROOT = Path(__file__).resolve().parents[2] / 'frontend'


def included_source(name):
    owner, relative = name.split('/', 1)
    return read_source(FRONTEND_ROOT / owner / 'templates' / relative)


def read_source(path):
    source = Path(path).read_text(encoding='utf-8')
    return re.sub(
        r"\{% include '([^']+)' %\}",
        lambda match: included_source(match.group(1)),
        source,
    )
