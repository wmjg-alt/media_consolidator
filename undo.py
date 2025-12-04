import os
import shutil
import logging
import sqlite3
import argparse
from pathlib import Path
from src.utils import load_config, setup_logger

# --- CONFIG ---
DB_PATH = "media_index.db"
CONFIG_PATH = "config/settings.yaml"
# --------------

def main():
    parser = argparse.ArgumentParser(description="Undo the last MediaConsolidator run.")
    parser.add_argument("--live", action="store_true", help="Actually move files back.")
    args = parser.parse_args()
    
    # Setup
    if not os.path.exists(CONFIG_PATH):
        print("Config not found.")
        return
        
    config = load_config(CONFIG_PATH)
    
    # We want a dedicated logger for this, outputs to console
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [UNDO] - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("UndoManager")

    if not os.path.exists(DB_PATH):
        logger.error(f"Database {DB_PATH} not found. Cannot undo.")
        logger.error("Did you run a new scan? That wipes the history needed for undoing.")
        return

    # Get Trash Root from config to find deleted files
    trash_root = config.get("organization", {}).get("trash_folder", "")
    if not trash_root:
        logger.error("Trash folder not defined in config.")
        return

    logger.info(f"Mode: {'LIVE (Restoring Files)' if args.live else 'DRY RUN (Simulation)'}")
    if not args.live:
        logger.info("Run with '--live' to actually perform operations.")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. UNDO KEEPERS (Organized -> Source)
    logger.info("--- Restoring KEEPERS ---")
    cursor.execute("SELECT file_path, target_path FROM media_files WHERE disposition='KEEP' AND target_path IS NOT NULL")
    keepers = cursor.fetchall()
    
    restored_keepers = 0
    for original_src, current_loc in keepers:
        # Check if the file is actually where we put it
        if not os.path.exists(current_loc):
            # It might be missing because we just moved it? No, loop hasn't run.
            # It might be missing because the user moved it manually.
            logger.warning(f"Missing expected file in Organized: {current_loc}")
            continue
            
        if perform_move(current_loc, original_src, logger, args.live):
            restored_keepers += 1

    # 2. UNDO DELETES (Trash -> Source)
    logger.info("--- Restoring DELETED ---")
    cursor.execute("SELECT id, file_path FROM media_files WHERE disposition='DELETE'")
    deletes = cursor.fetchall()
    
    restored_deletes = 0
    for row_id, original_src in deletes:
        filename = os.path.basename(original_src)
        # Reconstruct how Executioner named it: {ID}_{Filename}
        trash_filename = f"{row_id}_{filename}"
        current_loc = os.path.join(trash_root, trash_filename)
        
        if not os.path.exists(current_loc):
            logger.warning(f"Missing expected file in Trash: {current_loc}")
            continue
            
        if perform_move(current_loc, original_src, logger, args.live):
            restored_deletes += 1

    conn.close()
    
    logger.info("--- UNDO SUMMARY ---")
    logger.info(f"Keepers Restored: {restored_keepers}/{len(keepers)}")
    logger.info(f"Deletes Restored: {restored_deletes}/{len(deletes)}")
    
    if args.live:
        logger.info("The database is now out of sync with reality. You should delete 'media_index.db'.")

def perform_move(current_path, target_path, logger, is_live):
    """
    Moves file from current_path back to target_path (original location).
    """
    # Safety: Don't overwrite if something exists at origin
    if os.path.exists(target_path):
        logger.warning(f"Cannot restore: Target already exists: {target_path}")
        return False

    if not is_live:
        logger.info(f"[DRY] Restore: ...{os.path.basename(current_path)} -> {target_path}")
        return True

    try:
        # Ensure parent folder exists (in case user deleted empty source folders)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        
        shutil.move(current_path, target_path)
        logger.info(f"[RESTORED] -> {target_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to move {current_path}: {e}")
        return False

if __name__ == "__main__":
    main()