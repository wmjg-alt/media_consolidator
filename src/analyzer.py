"""Media file analysis and duplicate resolution."""

import logging
import os
from typing import Any

from PIL import Image

from src.db import DatabaseManager
from src.utils import resolve_best_timestamp 

Image.MAX_IMAGE_PIXELS = None


class Analyzer:
    """Analyze media files and resolve duplicates based on metadata and timestamps.
    
    This class processes image metadata (EXIF data), identifies duplicate files,
    and assigns disposition (KEEP or DELETE) based on file quality and age.
    """

    def __init__(self, db: DatabaseManager, config: dict[str, Any]) -> None:
        """Initialize the analyzer.
        
        Args:
            db: Database manager instance.
            config: Configuration dictionary.
        """
        self.db = db
        self.logger = logging.getLogger("MediaConsolidator.Analyzer")
        self.ensure_schema()

    def ensure_schema(self) -> None:
        """Add disposition column to media_files table if it doesn't exist."""
        with self.db.get_connection() as conn:
            try:
                conn.execute("ALTER TABLE media_files ADD COLUMN disposition TEXT")
                conn.commit()
            except Exception:
                pass

    def process_metadata(self) -> None:
        """Extract and analyze EXIF metadata for image files.
        
        Scans image files (jpg, jpeg, png, heic, webp) and extracts EXIF data
        to assign metadata quality scores. Files with EXIF dates receive a score
        of 10, while files without receive 0.
        """
        sql = """
        SELECT id, file_path FROM media_files 
        WHERE analyzed = 0 AND file_ext IN ('.jpg', '.jpeg', '.png', '.heic', '.webp')
        """
        
        updates: list[tuple[int, int, int]] = []
        with self.db.get_connection() as conn:
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            
            if rows:
                self.logger.info(f"Analyzing metadata for {len(rows)} files...")
            
            for row_id, path in rows:
                has_exif, _ = self._extract_exif(path)
                score = 10 if has_exif else 0
                updates.append((has_exif, score, row_id))

            conn.executemany("""
                UPDATE media_files 
                SET has_exif_date = ?, metadata_score = ?, analyzed = 1 
                WHERE id = ?
            """, updates)
            conn.commit()

    def process_duplicates(self) -> None:
        """Identify and judge duplicate files.
        
        Groups files by full hash and applies disposition rules to each group:
        one file is marked KEEP (winner) and others are marked DELETE (losers).
        Files with unique hashes are automatically marked KEEP.
        """
        self.logger.info("Judging files...")
        
        with self.db.get_connection() as conn:
            sql_groups = """
            SELECT hash_full, COUNT(*) as cnt 
            FROM media_files 
            WHERE hash_full IS NOT NULL 
            GROUP BY hash_full 
            HAVING cnt > 1
            """
            groups = conn.execute(sql_groups).fetchall()
            self.logger.info(f"Found {len(groups)} sets of duplicates.")
            
            for hash_val, count in groups:
                self._judge_group(conn, hash_val)
            
            sql_uniques = "UPDATE media_files SET disposition = 'KEEP' WHERE disposition IS NULL"
            conn.execute(sql_uniques)
            conn.commit()

    def _judge_group(self, conn: Any, hash_val: str) -> None:
        """Assign KEEP/DELETE dispositions to a group of duplicate files.
        
        The winner (KEEP) is selected using a multi-criteria sort that prioritizes:
        1. Files with EXIF metadata (higher metadata_score)
        2. Files with earlier timestamps (oldest creation/modification date)
        3. Files with cleaner filenames (no "copy" or "(" characters)
        4. Files with shorter path lengths
        
        All other files in the group are marked for deletion.
        
        Args:
            conn: Database connection object.
            hash_val: Full hash value identifying the duplicate group.
        """
        rows = conn.execute(
            "SELECT id, file_path, metadata_score, created_at, modified_at FROM media_files WHERE hash_full = ?", 
            (hash_val,)
        ).fetchall()
        
        def sort_key(item: tuple[int, str, int, str, str]) -> tuple[int, str, int, int]:
            """Generate sort key for duplicate selection.
            
            Returns a tuple that orders files by: metadata score (descending),
            effective creation/modification date (ascending), filename cleanliness,
            and path length (ascending).
            """
            score = item[2]
            created = item[3]
            modified = item[4]
            path = item[1]
            
            effective_date = resolve_best_timestamp(created, modified)
            
            filename = os.path.basename(path)
            name_penalty = -100 if "copy" in filename.lower() or "(" in filename else 0
            
            return (-score, effective_date, -name_penalty, len(path))

        sorted_candidates = sorted(rows, key=sort_key)
        
        winner = sorted_candidates[0]
        losers = sorted_candidates[1:]
        
        conn.execute("UPDATE media_files SET disposition = 'KEEP' WHERE id = ?", (winner[0],))
        
        if losers:
            loser_ids = [str(x[0]) for x in losers]
            conn.execute(
                f"UPDATE media_files SET disposition = 'DELETE' WHERE id IN ({','.join(loser_ids)})"
            )

    def _extract_exif(self, path: str) -> tuple[int, str | None]:
        """Extract EXIF date from an image file.
        
        Attempts to read EXIF data from the image, prioritizing the original
        photo date (tag 36867) over general file date (tag 306).
        
        Args:
            path: File path to the image.
            
        Returns:
            A tuple of (has_exif: int, date_str: str | None) where has_exif
            is 1 if EXIF date was found, 0 otherwise.
        """
        try:
            with Image.open(path) as img:
                exif_data = img._getexif()
                if not exif_data:
                    return 0, None
                
                date_str = exif_data.get(36867)
                if date_str:
                    return 1, date_str
                date_str = exif_data.get(306)
                if date_str:
                    return 1, date_str
                return 0, None
        except Exception:
            return 0, None