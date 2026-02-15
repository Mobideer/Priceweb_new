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
            # override=False by default means it won't overwrite existing env vars
            load_dotenv(path)
            loaded = True
    return loaded

# Load immediately on import
load_config()

# Helper to get numeric port with fallback
def get_api_port():
    return int(os.environ.get('PORT', 5002))
