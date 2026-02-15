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
            load_dotenv(path)
            # We don't print here to avoid cluttering logs of every module
            loaded = True
            break
    return loaded

# Load immediately on import
load_config()

# Helper to get numeric port with fallback
def get_api_port():
    return int(os.environ.get('PORT', 5002))
