"""File fingerprinting and duplicate detection using content hashing."""

import logging
import os
from typing import Any

import xxhash

    
from src.db import DatabaseManager
from src.cache import HashCache 


class Fingerprinter:
    """Compute file hashes to identify duplicate content.
    
    Implements a progressive four-stage funnel strategy to minimize disk I/O:
    
    1. Unique sizes: Files with unique file sizes cannot be duplicates
    2. Partial hashes: Compare start and end chunks of same-sized files
    3. Unique partials: Files with unique partial hashes cannot be duplicates
    4. Full hashes: Complete file hashing for remaining candidates
    
    This approach avoids expensive full-file reads until necessary.
    """

    def __init__(self, db: DatabaseManager, config: dict[str, Any]) -> None:
        """Initialize the fingerprinter.
        
        Args:
            db: Database manager instance.
            config: Configuration dictionary containing hashing.chunk_size.
        """
        self.db = db
        self.logger = logging.getLogger("MediaConsolidator.Hasher")
        self.chunk_size = config.get("hashing", {}).get("chunk_size", 4096)

        target_root = config.get("organization", {}).get("target_root", ".")
        self.cache = HashCache(target_root)

    def process_database(self) -> None:
        """Execute the complete four-stage fingerprinting pipeline.
        
        Processes all unfinished files through the funnel stages, moving
        qualified candidates forward and marking completed files.
        """
        self.logger.info("Starting Fingerprinting process...")
        
        self._mark_unique_sizes()
        self._process_partial_hashes()
        self._mark_unique_partials()
        self._process_full_hashes()
        
        self.logger.info("Fingerprinting complete.")

    def _mark_unique_sizes(self) -> None:
        """Identify files with unique sizes and mark as fully processed.
        
        Files with file sizes that appear only once in the database cannot
        be duplicates, so they are marked as hashed without computing any hashes.
        """
        sql = """
        UPDATE media_files
        SET hashed = 1
        WHERE id IN (
            SELECT id FROM media_files 
            WHERE file_size IN (
                SELECT file_size FROM media_files 
                GROUP BY file_size HAVING COUNT(*) = 1
            )
            AND hashed = 0
        )
        """
        with self.db.get_connection() as conn:
            cursor = conn.execute(sql)
            if cursor.rowcount > 0:
                self.logger.info(f"Skipped hashing for {cursor.rowcount} files with unique sizes.")
            conn.commit()

    def _process_partial_hashes(self) -> None:
        """Compute partial hashes for files sharing sizes.
        
        Reads start and end chunks of files that share the same file size,
        allowing efficient detection of non-duplicates before full reads.
        Small files (≤2 chunks) use only the start chunk.
        """
        sql_select = """
        SELECT id, file_path, file_size FROM media_files
        WHERE file_size IN (
            SELECT file_size FROM media_files 
            GROUP BY file_size HAVING COUNT(*) > 1
        )
        AND hash_partial IS NULL
        AND hashed = 0
        """
        
        updates: list[tuple[str, int]] = []
        with self.db.get_connection() as conn:
            cursor = conn.execute(sql_select)
            rows = cursor.fetchall()
            
            if rows:
                self.logger.info(f"Computing Partial Hashes for {len(rows)} candidates...")
            
            for row_id, path, size in rows:
                p_hash = self._compute_partial_hash(path, size)
                if p_hash:
                    updates.append((p_hash, row_id))

            if updates:
                conn.executemany("UPDATE media_files SET hash_partial = ? WHERE id = ?", updates)
                conn.commit()

    def _mark_unique_partials(self) -> None:
        """Identify files with unique partial hashes and mark as processed.
        
        Among files sharing the same size, if a partial hash is unique,
        the file cannot be a duplicate and is marked as complete.
        """
        sql = """
        UPDATE media_files
        SET hashed = 1
        WHERE id IN (
            SELECT id FROM media_files 
            WHERE hash_partial IS NOT NULL 
            AND hashed = 0
            GROUP BY file_size, hash_partial 
            HAVING COUNT(*) = 1
        )
        """
        with self.db.get_connection() as conn:
            cursor = conn.execute(sql)
            if cursor.rowcount > 0:
                self.logger.info(f"Marked {cursor.rowcount} unique partial hashes as processed.")
            conn.commit()

    def _process_full_hashes(self) -> None:
        """Compute full hashes for high-probability duplicates.
        
        Checks the persistent cache first to avoid re-reading files that have
        been processed in previous runs.
        """
        
        sql_select = """
        SELECT id, file_path, file_size, hash_partial FROM media_files
        WHERE (file_size, hash_partial) IN (
            SELECT file_size, hash_partial FROM media_files
            WHERE hash_partial IS NOT NULL
            GROUP BY file_size, hash_partial HAVING COUNT(*) > 1
        )
        AND hash_full IS NULL
        """
        
        updates: list[tuple[str, int, int]] = []
        with self.db.get_connection() as conn:
            cursor = conn.execute(sql_select)
            rows = cursor.fetchall()
            
            if rows:
                self.logger.info(f"Resolving Full Hashes for {len(rows)} high-probability duplicates...")
            
            cache_hits = 0
            
            for row_id, path, size, p_hash in rows:
                # 1. Check Cache
                cached_full = self.cache.get_full_hash(size, p_hash)
                
                if cached_full:
                    updates.append((cached_full, 1, row_id))
                    cache_hits += 1
                else:
                    # 2. Compute and Cache
                    f_hash = self._compute_full_hash(path)
                    if f_hash:
                        self.cache.put_full_hash(size, p_hash, f_hash)
                        updates.append((f_hash, 1, row_id))

            if cache_hits > 0:
                self.logger.info(f"Cache Hits: {cache_hits} files skipped full reading.")

            if updates:
                conn.executemany("UPDATE media_files SET hash_full = ?, hashed = ? WHERE id = ?", updates)
                conn.commit()

    def _compute_partial_hash(self, path: str, file_size: int) -> str | None:
        """Compute a partial hash from start and end chunks of a file.
        
        Reads the first chunk and (if file is large enough) the last chunk,
        then hashes them together. This provides fast duplicate detection
        without reading the entire file.
        
        Args:
            path: File path to hash.
            file_size: Size of the file in bytes.
            
        Returns:
            Hex digest string if successful, None if file could not be read.
        """
        try:
            with open(path, 'rb') as f:
                start_chunk = f.read(self.chunk_size)
                if file_size <= (self.chunk_size * 2):
                    end_chunk = b""
                else:
                    f.seek(-self.chunk_size, os.SEEK_END)
                    end_chunk = f.read(self.chunk_size)
                
                hasher = xxhash.xxh64()
                hasher.update(start_chunk)
                hasher.update(end_chunk)
                return hasher.hexdigest()
        except OSError:
            self.logger.error(f"Could not read file: {path}")
            return None

    def _compute_full_hash(self, path: str) -> str | None:
        """Compute a complete content hash of a file.
        
        Reads the entire file in chunks and produces a definitive hash
        for duplicate identification.
        
        Args:
            path: File path to hash.
            
        Returns:
            Hex digest string if successful, None if file could not be read.
        """
        try:
            hasher = xxhash.xxh64()
            with open(path, 'rb') as f:
                while chunk := f.read(65536):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except OSError:
            self.logger.error(f"Could not read file: {path}")
            return None