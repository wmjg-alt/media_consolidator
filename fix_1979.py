import os
import shutil
import logging
import re
from datetime import datetime
from pathlib import Path
from src.utils import set_file_creation_time, load_config

# Setup
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("1979Fixer")

CONFIG = load_config("config/settings.yaml")
TARGET_ROOT = CONFIG['organization']['target_root']
DRY_RUN = False #-- Change to False to apply fixes

# Threshold: Jan 2, 1980 (Avoids timezone edge cases around the 1980 epoch)
MIN_VALID_TIMESTAMP = 315619200.0 

def fix_file(file_path):
    path = Path(file_path)
    
    # 1. Get current metadata
    try:
        stat = path.stat()
        mtime = stat.st_mtime
    except OSError:
        return

    # 2. Check if Modified Date is valid (newer than 1980)
    if mtime < MIN_VALID_TIMESTAMP:
        logger.warning(f"[SKIP] Modified date is also ancient/invalid: {path.name}")
        return

    # 3. Calculate New Path
    dt = datetime.fromtimestamp(mtime)
    year_folder = dt.strftime("%Y")
    month_folder = dt.strftime("%Y-%m")
    date_prefix = dt.strftime("%Y-%m-%d")
    
    # Update filename prefix (swap 1979-XX-XX with 2024-XX-XX)
    # Regex looks for start of string date
    new_name = re.sub(r'^\d{4}-\d{2}-\d{2}', date_prefix, path.name)
    
    relative_path = os.path.join(year_folder, month_folder, new_name)
    new_full_path = os.path.join(TARGET_ROOT, relative_path).replace('\\', '/')
    
    if str(path).replace('\\', '/') == new_full_path:
        return # No change needed

    logger.info(f"FIXING: {path.name}")
    logger.info(f"  Move: .../{path.parent.name}/{path.name} -> .../{month_folder}/{new_name}")
    
    if not DRY_RUN:
        try:
            # Create dirs
            os.makedirs(os.path.dirname(new_full_path), exist_ok=True)
            
            # Move
            shutil.move(str(path), new_full_path)
            
            # Fix Metadata (Set Creation to match Modified, since Modified is the only truth we have)
            set_file_creation_time(new_full_path, mtime)
            
            # Remove old empty dir if empty
            try:
                os.rmdir(path.parent) 
            except OSError:
                pass # Directory not empty
                
        except Exception as e:
            logger.error(f"Failed: {e}")

def main():
    if not os.path.exists(TARGET_ROOT):
        return

    logger.info(f"Scanning {TARGET_ROOT} for 1979/1980 artifacts...")
    
    for root, dirs, files in os.walk(TARGET_ROOT):
        # We only care about folders that look like dates "1979" or "1980"
        # Check current folder name
        current_folder = os.path.basename(root)
        
        # If we are inside a '1979' or '1980' folder, or the file starts with it
        for file in files:
            if file.startswith("1979-") or file.startswith("1980-01-01"):
                fix_file(os.path.join(root, file))
            elif "1979" in current_folder:
                fix_file(os.path.join(root, file))

if __name__ == "__main__":
    main()