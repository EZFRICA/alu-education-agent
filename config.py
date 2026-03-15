from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv(override=True)

# API Keys and URLs
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
LETTA_API_KEY = os.getenv("LETTA_API_KEY")
LETTA_BASE_URL = os.getenv("LETTA_BASE_URL", "https://api.letta.com")
WCD_CLUSTER_URL = os.getenv("WCD_CLUSTER_URL")
WCD_API_KEY = os.getenv("WCD_API_KEY")

# Meta-Configuration
USER_ID = os.getenv("USER_ID", "user_abc123")
MAX_DYNAMIC_BLOCKS = 8
FIXED_BLOCKS = ["traveler_profile", "traveler_preferences", "active_trip", "current_session"]

# Extra configuration params
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USER_DATA_DIR = os.path.join(BASE_DIR, "data", "users", USER_ID)

if not os.path.exists(USER_DATA_DIR):
    os.makedirs(USER_DATA_DIR, exist_ok=True)

METADATA_FILE = os.path.join(USER_DATA_DIR, "metadata_links.json")
