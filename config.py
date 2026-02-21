import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the FF project root
_env_path = Path(__file__).parent / ".env"
load_dotenv(_env_path)


def get_api_key() -> str:
    key = os.environ.get("CFBD_API_KEY", "")
    if not key:
        raise EnvironmentError(
            "CFBD_API_KEY not set. Add it to your .env file or environment."
        )
    return key


def get_db_path() -> str:
    path = os.environ.get("FF_DB_PATH", "ff.db")
    # If relative, resolve relative to the project root
    p = Path(path)
    if not p.is_absolute():
        p = Path(__file__).parent / p
    return str(p)
