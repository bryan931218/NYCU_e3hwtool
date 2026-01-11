import os
from pathlib import Path

from dotenv import load_dotenv

from e3_tracker.api.web import create_app

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env", override=False)


def main():
    app = create_app()
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    app.run(host=host, port=port)


if __name__ == "__main__":
    main()
