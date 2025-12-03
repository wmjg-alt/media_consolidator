import sqlite3
import os
import logging
from collections import defaultdict
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("TraceMaker")

DB_PATH = "media_index.db"
TRACE_FILENAME = "image_trace.txt"

def main():
    if not os.path.exists(DB_PATH):
        logger.error(f"Database {DB_PATH} not found! Did you run a new scan already?")
        return

    logger.info(f"Reading {DB_PATH} to generate trace receipts...")
    
    # Dictionary to hold the receipts
    # Key: Source Folder Path
    # Value: List of strings (lines to write)
    receipts = defaultdict(list)
    
    with sqlite3.connect(DB_PATH) as conn:
        # 1. Get KEEP files (Direct mapping)
        cursor = conn.execute("SELECT file_path, target_path FROM media_files WHERE disposition='KEEP'")
        keepers = cursor.fetchall()
        
        for src, dst in keepers:
            if not dst: continue # Should not happen if run completed
            
            src_path = Path(src)
            parent_folder = str(src_path.parent)
            
            line = f"[MOVED] '{src_path.name}' -> '{dst}'"
            receipts[parent_folder].append(line)
            
        # 2. Get DELETE files (Map to their 'Keeper' twin)
        # We join the table on itself matching Hash to find where the survivor went
        sql_dupes = """
        SELECT t1.file_path, t2.target_path 
        FROM media_files t1
        JOIN media_files t2 ON t1.hash_full = t2.hash_full
        WHERE t1.disposition = 'DELETE' 
          AND t2.disposition = 'KEEP'
        """
        cursor = conn.execute(sql_dupes)
        dupes = cursor.fetchall()
        
        for src, dst in dupes:
            if not dst: continue
            
            src_path = Path(src)
            parent_folder = str(src_path.parent)
            
            line = f"[DUPLICATE CONSOLIDATED] '{src_path.name}' -> '{dst}'"
            receipts[parent_folder].append(line)

    # 3. Write the receipts
    if not receipts:
        logger.info("No moves found in database.")
        return

    logger.info(f"Writing receipts to {len(receipts)} folders...")
    
    count = 0
    count_lines = 0
    for folder, lines in receipts.items():
        # Ensure the folder still exists (it should, we don't delete folders)
        if not os.path.exists(folder):
            logger.warning(f"Skipping missing folder: {folder}")
            continue
            
        trace_path = os.path.join(folder, TRACE_FILENAME)
        
        try:
            # Append mode ('a') in case we run this multiple times, 
            # or 'w' to overwrite? 'a' is safer for history.
            with open(trace_path, "w", encoding="utf-8") as f:
                f.write(f"--- Media Consolidator Run ({len(lines)} files) ---\n")
                for line in lines:
                    f.write(line + "\n")
                    count_lines += 1
            count += 1
        except Exception as e:
            logger.error(f"Failed to write trace to {folder}: {e}")

    logger.info(f"Success. Created {count} trace files. Total lines written: {count_lines}.")

if __name__ == "__main__":
    main()