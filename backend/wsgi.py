import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)
if os.getenv("E3_ENV", "production") != "production":
    raise RuntimeError("The public WSGI entry point requires E3_ENV=production")
os.environ["E3_ENV"] = "production"
from e3_tracker.bootstrap import create_app
app = create_app()
