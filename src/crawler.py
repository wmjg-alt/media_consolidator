"""Recursive directory crawler for discovering media files."""

import logging
import os
from collections.abc import Generator
from pathlib import Path
from typing import Any

from src.db import DatabaseManager


class FileCrawler:
    """Recursively scan directories and index media files into the database.
    
    Filters files by allowed extensions and applies exclusion rules for system
    directories, trash folders, and user-specified blocklists. Uses batch
    insertion for efficient database writes.
    """

    def __init__(self, db: DatabaseManager, config: dict[str, Any]) -> None:
        """Initialize the crawler.
        
        Args:
            db: Database manager instance.
            config: Configuration dictionary containing:
                - extensions: Dict mapping file types to extension lists
                - organization.trash_folder: Path to exclude from scanning
                - organization.exclude_dirs: List of directory names to skip
        """
        self.db = db
        self.logger = logging.getLogger("MediaConsolidator.Crawler")
        
        ext_cfg = config.get("extensions", {})
        self.allowed_exts: set[str] = set()
        for cat in ext_cfg.values():
            for ext in cat:
                self.allowed_exts.add(ext.lower())
        
        self.batch_size = 1000
        org_config = config.get("organization", {})
        self.trash_path = org_config.get("trash_folder", "").replace('\\', '/')
        self.excluded_names = {x.lower() for x in org_config.get("exclude_dirs", [])}

    def scan_roots(self, root_paths: list[str]) -> int:
        """Scan one or more root directories for media files.
        
        Args:
            root_paths: List of root directory paths to scan.
            
        Returns:
            Total number of files indexed across all roots.
        """
        total_added = 0
        for root in root_paths:
            if not os.path.exists(root):
                self.logger.warning(f"Path not found: {root}")
                continue
            
            norm_root = root.replace('\\', '/')
            self.logger.info(f"Scanning: {norm_root}")
            total_added += self._process_directory(norm_root)
        
        return total_added

    def _process_directory(self, root_path: str) -> int:
        """Recursively process a directory and buffer files for insertion.
        
        Collects file entries into batches and inserts them into the database
        when the batch reaches the configured size. Skips excluded files and
        directories based on configured filters.
        
        Args:
            root_path: Directory path to process.
            
        Returns:
            Number of files added to the database.
        """
        buffer: list[tuple[str, int, str, float, float]] = []
        count = 0
        
        for entry in self._fast_scandir(root_path):
            full_path = entry.path.replace('\\', '/')
            
            if self.trash_path and full_path.startswith(self.trash_path):
                continue
            
            if self._is_media(entry.name):
                stat = entry.stat()
                c_time = getattr(stat, 'st_birthtime', stat.st_ctime)
                m_time = stat.st_mtime
                file_data = (full_path, stat.st_size, Path(full_path).suffix.lower(), c_time, m_time)
                buffer.append(file_data)

                if len(buffer) >= self.batch_size:
                    self._flush_buffer(buffer)
                    count += len(buffer)
                    buffer = []

        if buffer:
            self._flush_buffer(buffer)
            count += len(buffer)
            
        return count

    def _fast_scandir(self, path: str) -> Generator[os.DirEntry[str], None, None]:
        """Recursively scan directory tree, yielding file entries.
        
        Applies three exclusion rules to filter out unwanted paths:
        1. Trash folder: Skip configured trash_folder path entirely
        2. System/config dirs: Skip directories starting with $ or .
        3. Explicit blocklist: Skip directories matching excluded_names
        
        Args:
            path: Directory path to scan.
            
        Yields:
            File entry objects (directories are recursed into).
        """
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        name_lower = entry.name.lower()
                        
                        if name_lower.startswith('$') or name_lower.startswith('.'):
                            continue
                        
                        if name_lower in self.excluded_names:
                            continue
                            
                        yield from self._fast_scandir(entry.path)
                    elif entry.is_file(follow_symlinks=False):
                        yield entry
        except (PermissionError, OSError):
            pass

    def _is_media(self, filename: str) -> bool:
        """Check if a file has an allowed media extension.
        
        Args:
            filename: Name of the file to check.
            
        Returns:
            True if the file extension matches an allowed media type.
        """
        _, ext = os.path.splitext(filename)
        return ext.lower() in self.allowed_exts

    def _flush_buffer(self, data: list[tuple[str, int, str, float, float]]) -> None:
        """Insert buffered file records into the database.
        
        Uses INSERT OR IGNORE to prevent duplicate entries if the same file
        is encountered multiple times during scanning.
        
        Args:
            data: List of tuples (file_path, file_size, file_ext, created_at, modified_at).
        """
        sql = """INSERT OR IGNORE INTO media_files 
                 (file_path, file_size, file_ext, created_at, modified_at) 
                 VALUES (?, ?, ?, ?, ?)"""
        with self.db.get_connection() as conn:
            conn.executemany(sql, data)
            conn.commit()