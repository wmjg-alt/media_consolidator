import os
import shutil
import logging
import sqlite3
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from src.db import DatabaseManager

class Executioner:
    def __init__(self, db: DatabaseManager, config: dict, dry_run: bool = True):
        self.db = db
        self.logger = logging.getLogger("MediaConsolidator.Executioner")
        self.dry_run = dry_run
        
        org_cfg = config.get("organization", {})
        self.trash_root = org_cfg.get("trash_folder", "C:/MediaConsolidator_Trash")
        
        self.receipt_buffer = defaultdict(list)

    def execute(self):
        mode = "DRY RUN" if self.dry_run else "LIVE"
        self.logger.info(f"Starting Execution Phase. Mode: {mode}")
        
        self._process_keepers()
        self._process_deletes()
        self._write_trace_receipts()
        
        self.logger.info("Execution Phase Complete.")

    def _process_keepers(self):
        sql = "SELECT id, file_path, target_path FROM media_files WHERE disposition = 'KEEP' AND target_path IS NOT NULL"
        with self.db.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
            self.logger.info(f"Processing {len(rows)} Keepers...")
            for row_id, src, dst in rows:
                if self.dry_run:
                    self.logger.info(f"[DRY] Move: '{src}' -> '{dst}'")
                    continue
                
                # FIX: Only log receipt if _safe_move returns True (actually moved)
                if self._safe_move(src, dst):
                    self._log_receipt(src, f"[MOVED] '{os.path.basename(src)}' -> '{dst}'")

    def _process_deletes(self):
        sql = """
        SELECT t1.id, t1.file_path, t2.target_path 
        FROM media_files t1
        LEFT JOIN media_files t2 ON t1.hash_full = t2.hash_full AND t2.disposition = 'KEEP'
        WHERE t1.disposition = 'DELETE'
        """
        with self.db.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
            self.logger.info(f"Processing {len(rows)} files to Trash...")
            for row_id, src, winner_dst in rows:
                filename = os.path.basename(src)
                trash_path = os.path.join(self.trash_root, f"{row_id}_{filename}")
                if self.dry_run:
                    self.logger.info(f"[DRY] Trash: '{src}' -> '{trash_path}'")
                    continue
                
                if self._safe_move(src, trash_path):
                    msg = f"[DUPLICATE CONSOLIDATED] '{filename}' -> '{winner_dst}'" if winner_dst else f"[MOVED TO TRASH] '{filename}'"
                    self._log_receipt(src, msg)

    def _safe_move(self, src: str, dst: str) -> bool:
        """Returns True if file was physically moved, False if skipped."""
        try:
            abs_src = os.path.abspath(src)
            abs_dst = os.path.abspath(dst)

            if abs_src == abs_dst:
                # Log at debug level to keep console clean
                self.logger.debug(f"Skipping move (Already in place): {src}")
                return False # <--- CRITICAL FIX: Returns False so we don't write a receipt

            if not os.path.exists(src):
                self.logger.warning(f"Source file missing: {src}")
                return False

            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.move(src, dst)
            return True
            
        except Exception as e:
            self.logger.error(f"Move failed: {src} -> {dst} | Error: {e}")
            return False

    def _log_receipt(self, src_path: str, message: str):
        parent = os.path.dirname(src_path)
        self.receipt_buffer[parent].append(message)

    def _write_trace_receipts(self):
        if not self.receipt_buffer:
            self.logger.info("No files moved, so no receipts written.")
            return

        count = 0
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for folder, lines in self.receipt_buffer.items():
            try:
                trace_path = os.path.join(folder, "image_trace.txt")
                with open(trace_path, "a", encoding="utf-8") as f:
                    f.write(f"\n--- Media Consolidator Run: {timestamp} ---\n")
                    for line in lines:
                        f.write(line + "\n")
                count += 1
            except Exception as e:
                self.logger.warning(f"Failed to write trace to {folder}: {e}")

        self.logger.info(f"Trace Receipts written to {count} source folders.")