"""Inspect or apply versioned schema upgrades without starting the web app."""
import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

from dotenv import load_dotenv
from sqlalchemy import create_engine
from e3_tracker.shared.config import load_env_defaults
from e3_tracker.shared.storage import PersistentStorage
from e3_tracker.shared.persistence.migrations import migration_status, run_migrations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", help="Database URL or SQLite path; otherwise use configured environment")
    parser.add_argument("--status", action="store_true", help="Read migration status without applying upgrades")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env", override=False)
    defaults = load_env_defaults()
    database = args.database or defaults["database_url"]
    if not database:
        database = str(Path(defaults["cache_dir"] or ROOT / ".localdata") / "e3_tracker.sqlite3")
    if args.status and "://" not in database and not Path(database).is_file():
        parser.error("Database does not exist; --status will not create it")
    if args.status and database.startswith("sqlite:///") and not Path(database[len("sqlite:///"):]).is_file():
        parser.error("Database does not exist; --status will not create it")
    engine = create_engine(PersistentStorage._normalize_url(database), future=True, pool_pre_ping=True)
    try:
        if not args.status:
            applied = run_migrations(engine)
            print("Applied: " + (", ".join(applied) or "none"))
        for row in migration_status(engine):
            state = "applied" if row["applied"] else "pending"
            if not row["known"]:
                state += " (unknown; requires a newer application)"
            print(row["version"] + ": " + state)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
