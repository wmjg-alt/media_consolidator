"""File movement execution with dry-run support and audit trails."""

import logging
import os
import shutil
import time
from collections import defaultdict
from datetime import datetime
from typing import Any

from src.db import DatabaseManager
from src.utils import set_file_creation_time, apply_jitter_if_midnight, resolve_best_timestamp


class Executioner:
    """Execute file organization plan by moving files to target locations.
    
    Moves files marked KEEP to organized locations and moves duplicates to
    trash. Supports dry-run mode for safe testing. Writes audit trails
    (trace receipts) to source folders documenting all file operations.
    """

    def __init__(
        self,
        db: DatabaseManager,
        config: dict[str, Any],
        dry_run: bool = True,
    ) -> None:
        """Initialize the executioner.
        
        Args:
            db: Database manager instance.
            config: Configuration dictionary containing organization.trash_folder.
            dry_run: If True, log operations without moving files. Defaults to True.
        """
        self.db = db
        self.logger = logging.getLogger("MediaConsolidator.Executioner")
        self.dry_run = dry_run
        
        org_cfg = config.get("organization", {})
        self.trash_root = org_cfg.get("trash_folder", "C:/MediaConsolidator_Trash")
        
        self.receipt_buffer: dict[str, list[str]] = defaultdict(list)

    def execute(self) -> None:
        """Execute the complete file organization plan.
        
        Processes files in two phases: moves KEEP files to organized locations,
        then moves DELETE files to trash. Writes audit trails to source folders.
        """
        mode = "DRY RUN" if self.dry_run else "LIVE"
        self.logger.info(f"Starting Execution Phase. Mode: {mode}")
        
        self._process_keepers()
        self._process_deletes()
        self._write_trace_receipts()
        
        self.logger.info("Execution Phase Complete.")

    def _process_keepers(self) -> None:
        """Move files marked KEEP to their organized target locations.
        
        Also updates the metadata of the moved file:
        1. Sets Creation Time to the oldest known date (jittered if midnight).
        2. Restores original Modified Time.
        """
        # UPDATED SQL: Fetch timestamps
        sql = """SELECT id, file_path, target_path, created_at, modified_at 
                 FROM media_files 
                 WHERE disposition = 'KEEP' AND target_path IS NOT NULL"""
        
        with self.db.get_connection() as conn:
            rows = conn.execute(sql).fetchall()
            self.logger.info(f"Processing {len(rows)} Keepers...")
            
            for row_id, src, dst, c_time, m_time in rows:
                if self.dry_run:
                    self.logger.info(f"[DRY] Move: '{src}' -> '{dst}'")
                    continue
                
                if self._safe_move(src, dst):
                    # --- METADATA UPDATE START ---
                    try:
                        # 1. Determine oldest known date
                        best_ts = resolve_best_timestamp(c_time, m_time)
                        
                        # 2. Apply Jitter if exact midnight
                        final_creation_ts = apply_jitter_if_midnight(best_ts)
                        
                        # 3. Set Creation Time (Win32)
                        set_file_creation_time(dst, final_creation_ts)
                        
                        # 4. Restore Modified Time (Legacy preservation)
                        # os.utime sets (atime, mtime). We use current for access.
                        os.utime(dst, (time.time(), m_time))
                        
                    except Exception as e:
                        self.logger.warning(f"Metadata fix failed for {dst}: {e}")
                    # --- METADATA UPDATE END ---

                    self._log_receipt(src, f"[MOVED] '{os.path.basename(src)}' -> '{dst}'")

    def _process_deletes(self) -> None:
        """Move duplicate files to trash.
        
        Identifies duplicates of KEEP files via full hash matching and moves
        them to the trash folder. Audit receipts distinguish between pure
        duplicates (consolidated) and orphaned files.
        """
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
                    msg = (
                        f"[DUPLICATE CONSOLIDATED] '{filename}' -> '{winner_dst}'"
                        if winner_dst
                        else f"[MOVED TO TRASH] '{filename}'"
                    )
                    self._log_receipt(src, msg)

    def _safe_move(self, src: str, dst: str) -> bool:
        """Safely move a file with validation and error handling.
        
        Verifies source exists, detects no-op moves (already in place),
        creates destination directories, and handles errors gracefully.
        Files already at their target location are not moved but return False
        to prevent writing redundant audit receipts.
        
        Args:
            src: Source file path.
            dst: Destination file path.
            
        Returns:
            True if file was physically moved, False if skipped or failed.
        """
        try:
            abs_src = os.path.abspath(src)
            abs_dst = os.path.abspath(dst)

            if abs_src == abs_dst:
                self.logger.debug(f"Skipping move (Already in place): {src}")
                return False

            if not os.path.exists(src):
                self.logger.warning(f"Source file missing: {src}")
                return False

            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.move(src, dst)
            return True
            
        except Exception as e:
            self.logger.error(f"Move failed: {src} -> {dst} | Error: {e}")
            return False

    def _log_receipt(self, src_path: str, message: str) -> None:
        """Buffer an audit trail entry for a file operation.
        
        Receipts are grouped by source folder and written at the end
        to minimize filesystem operations.
        
        Args:
            src_path: Original file path (used to determine source folder).
            message: Operation description to record in audit trail.
        """
        parent = os.path.dirname(src_path)
        self.receipt_buffer[parent].append(message)

    def _write_trace_receipts(self) -> None:
        """Write buffered audit trails to image_trace.txt files.
        
        Creates or appends to image_trace.txt in each source folder that had
        operations, documenting all file movements with timestamp.
        """
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