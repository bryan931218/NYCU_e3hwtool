"""Workspace paths shared by the single deployment's composition root."""
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[3]
FRONTEND_ROOT = ROOT_DIR / "frontend"
FRONTEND_OWNERS = ("assignments", "study", "shared")
