from __future__ import annotations

from pathlib import Path
from typing import Any


def install_study_recall_library_runtime(web_module: Any) -> None:
    """Upgrade the recall-note history into a subject-first library."""

    if getattr(web_module, "__e3_study_recall_library_runtime_installed", False):
        return
    web_module.__e3_study_recall_library_runtime_installed = True

    root_dir = Path(__file__).resolve().parents[3]
    partial_path = root_dir / "frontend" / "templates" / "_study_recall_subject_library.html"
    if not partial_path.exists():
        return

    partial = partial_path.read_text(encoding="utf-8")
    marker = "__e3StudyRecallSubjectLibraryInstalled"
    template = str(web_module.STUDY_RECALL_TEMPLATE)
    if marker in template:
        return
    if "</body>" in template:
        web_module.STUDY_RECALL_TEMPLATE = template.replace(
            "</body>",
            partial + "\n</body>",
            1,
        )
    else:
        web_module.STUDY_RECALL_TEMPLATE = template + "\n" + partial
