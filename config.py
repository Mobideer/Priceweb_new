import os
from dotenv import load_dotenv

def load_config():
    """Loads environment variables from standard locations."""
    env_paths = [
        '/etc/priceweb_new.env',
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    ]

    loaded = False
    for path in env_paths:
        if os.path.exists(path):
            # override=True ensures .env file values take precedence
            load_dotenv(path, override=True)
            loaded = True
    return loaded

# Load immediately on import
load_config()

# Helper to get numeric port with fallback
def get_api_port():
    return int(os.environ.get('PORT', 5002))

def get_log_path():
    path = os.environ.get("PRICE_LOG_PATH")
    if not path:
        # Fallback based on DB path or current dir
        db_path = os.environ.get("PRICE_DB_PATH", "data/priceweb.db")
        db_dir = os.path.dirname(db_path) or "."
        path = os.path.join(db_dir, "cron_log.log")
    return path
