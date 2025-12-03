import os
import re
import logging
from pathlib import Path
from src.utils import load_config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("Cleaner")

CONFIG_PATH = "config/settings.yaml"

# Regex to find the recursive artifact: "_from_" followed by "YYYY-MM"
# matches: _from_2024-01, _from_2022-12, etc.
RECURSIVE_PATTERN = re.compile(r'_from_\d{4}-\d{2}')

def main():
    if not os.path.exists(CONFIG_PATH):
        logger.error("Config not found.")
        return

    config = load_config(CONFIG_PATH)
    target_root = config['organization']['target_root']
    
    if not os.path.exists(target_root):
        logger.error(f"Target root does not exist: {target_root}")
        return

    logger.info(f"Scanning {target_root} for recursive filename artifacts...")
    
    renamed_count = 0
    errors = 0

    # Walk the directory tree
    for root, dirs, files in os.walk(target_root):
        for filename in files:
            # Check if file has the artifact
            if RECURSIVE_PATTERN.search(filename):
                
                old_path = os.path.join(root, filename)
                
                # Remove all instances of the pattern
                new_filename = RECURSIVE_PATTERN.sub('', filename)
                
                # Clean up potential double underscores left behind (e.g. Name__from_Source)
                new_filename = new_filename.replace('__', '_')
                
                new_path = os.path.join(root, new_filename)
                
                try:
                    # Windows Long Path handling (if needed)
                    # if len(old_path) > 260: old_path = "\\\\?\\" + os.path.abspath(old_path)
                    
                    os.rename(old_path, new_path)
                    logger.info(f"[FIXED] {filename} -> {new_filename}")
                    renamed_count += 1
                except OSError as e:
                    logger.error(f"Failed to rename {filename}: {e}")
                    errors += 1

    logger.info(f"Cleanup complete. Fixed {renamed_count} files. Errors: {errors}")

if __name__ == "__main__":
    main()