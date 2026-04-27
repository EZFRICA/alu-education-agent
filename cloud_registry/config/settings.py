import os
from dotenv import load_dotenv

load_dotenv()

# Gemini Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
EMBEDDING_MODEL = "models/gemini-embedding-2"

# Weaviate Configuration
WCD_CLUSTER_URL = os.getenv("WCD_CLUSTER_URL")
WCD_API_KEY = os.getenv("WCD_API_KEY")
REGISTRY_COLLECTION = "RegistryIndex"

# Paths
COURSES_DIR = "courses"
OUTPUT_DIR = "../registry"

# GCS Configuration
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
