# config.py
# Local entry point: Uses environment variables for credentials
# Set these in your environment or .env file (DO NOT commit credentials!)

import os
from pipeline_project import run_once

# --- Load from environment variables (SECURE) ---
# Example setup:
#   export MONGO_URI="mongodb+srv://..."
#   export DRIVE_FOLDER_ID="your-folder-id"
#   export SA_JSON_PATH="/path/to/service-account.json"

MONGO_URI       = os.getenv("MONGO_URI", "")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "")

# Where the service account JSON lives on your machine.
SA_JSON_PATH    = os.getenv("SA_JSON_PATH", "drive-sa.json")

# Inline JSON is optional (useful for CI).
DRIVE_SA_JSON   = os.getenv("DRIVE_SA_JSON", "")

# Output file name and mode
OUTPUT_NAME     = os.getenv("OUTPUT_NAME", "NC-DA-Journal-Data.xlsx")
RUN_MODE        = os.getenv("RUN_MODE", "inc")   # "full" or "inc"

if __name__ == "__main__":
    if not MONGO_URI or not DRIVE_FOLDER_ID:
        print("ERROR: MONGO_URI and DRIVE_FOLDER_ID must be set in environment variables!")
        print("Example:")
        print('  export MONGO_URI="mongodb+srv://..."')
        print('  export DRIVE_FOLDER_ID="your-folder-id"')
        exit(1)
    
    cfg = {
        "MONGO_URI": MONGO_URI,
        "DRIVE_FOLDER_ID": DRIVE_FOLDER_ID,
        "OUTPUT_NAME": OUTPUT_NAME,
        "RUN_MODE": RUN_MODE,
        "SA_JSON_PATH": SA_JSON_PATH,
        "DRIVE_SA_JSON": DRIVE_SA_JSON,
    }
    run_once(cfg)
