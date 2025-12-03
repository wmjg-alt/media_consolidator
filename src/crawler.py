import os
import logging
from pathlib import Path
from typing import List, Set, Generator
from src.db import DatabaseManager

class FileCrawler:
    def __init__(self, db: DatabaseManager, config: dict):
        self.db = db
        self.logger = logging.getLogger("MediaConsolidator.Crawler")
        
        ext_cfg = config.get("extensions", {})
        self.allowed_exts: Set[str] = set()
        for cat in ext_cfg.values():
            for ext in cat:
                self.allowed_exts.add(ext.lower())
        
        self.batch_size = 1000
        org_config = config.get("organization", {})
        self.trash_path = org_config.get("trash_folder", "").replace('\\', '/')
        
        # Load Exclusions
        raw_excludes = org_config.get("exclude_dirs", [])
        self.excluded_names = {x.lower() for x in raw_excludes}

    def scan_roots(self, root_paths: List[str]) -> int:
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
        buffer = []
        count = 0
        
        for entry in self._fast_scandir(root_path):
            full_path = entry.path.replace('\\', '/')
            
            # EXCLUSION 1: Trash Path
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
            
        return count # <--- FIX: This is now un-indented correctly

    def _fast_scandir(self, path: str) -> Generator[os.DirEntry, None, None]:
        try:
            with os.scandir(path) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        name_lower = entry.name.lower()
                        
                        # EXCLUSION 2: System/Config folders
                        if name_lower.startswith('$') or name_lower.startswith('.'):
                            continue
                        
                        # EXCLUSION 3: Explicit blocklist
                        if name_lower in self.excluded_names:
                            continue
                            
                        yield from self._fast_scandir(entry.path)
                    elif entry.is_file(follow_symlinks=False):
                        yield entry
        except (PermissionError, OSError):
            pass 

    def _is_media(self, filename: str) -> bool:
        _, ext = os.path.splitext(filename)
        return ext.lower() in self.allowed_exts

    def _flush_buffer(self, data: List[tuple]):
        sql = "INSERT OR IGNORE INTO media_files (file_path, file_size, file_ext, created_at, modified_at) VALUES (?, ?, ?, ?, ?)"
        with self.db.get_connection() as conn:
            conn.executemany(sql, data)
            conn.commit()