import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Registry Sync
MANIFEST_URL = "https://storage.googleapis.com/akili-registry/manifest.json"

# Local Storage
LANCE_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "storage", "akili_db")
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "storage", "cache")

# DLL Memory
METADATA_LINKS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "memory", "metadata_links.json")

# DLL Configuration
MAX_DYNAMIC_BLOCKS = 5
EDU_DEFAULT_CLASS  = "6eme"
EDU_DEFAULT_SUBJECT = "math"

# GCS Credentials for Sync
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
