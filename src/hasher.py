import os
import logging
import xxhash
from typing import List, Tuple
from src.db import DatabaseManager

class Fingerprinter:
    """
    Computes file hashes to identify duplicates.
    Implements a 'Funnel' strategy to minimize Disk IO.
    """

    def __init__(self, db: DatabaseManager, config: dict):
        self.db = db
        self.logger = logging.getLogger("MediaConsolidator.Hasher")
        self.chunk_size = config.get("hashing", {}).get("chunk_size", 4096)

    def process_database(self):
        """
        Main execution pipeline.
        Runs the 4-stage funnel: Size -> Partial -> Unique Partials -> Full.
        """
        self.logger.info("Starting Fingerprinting process...")
        
        # Stage 1: Mark files with unique sizes as processed (no hashing needed)
        self._mark_unique_sizes()
        
        # Stage 2: Calculate Partial Hashes for size collisions
        self._process_partial_hashes()
        
        # Stage 3: If partial hash is unique, mark as processed (FIX for Limbo files)
        self._mark_unique_partials()
        
        # Stage 4: Calculate Full Hashes for partial collisions
        self._process_full_hashes()
        
        self.logger.info("Fingerprinting complete.")

    def _mark_unique_sizes(self):
        """
        Optimization: If a file size appears only once in the DB, 
        it cannot be a duplicate. Mark it as hashed.
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

    def _process_partial_hashes(self):
        """
        Finds unhashed files that share a file_size, computes partial hash.
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
        
        updates = []
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

    def _mark_unique_partials(self):
        """
        Optimization: If a partial hash is unique (among sharing sizes), 
        it cannot be a duplicate. Mark as hashed.
        """
        # We look for files that have a Partial Hash, are NOT yet done, 
        # and that partial hash appears only once for that specific file size.
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

    def _process_full_hashes(self):
        """
        Finds files where (Size + Partial) match, computes full hash.
        """
        sql_select = """
        SELECT id, file_path FROM media_files
        WHERE (file_size, hash_partial) IN (
            SELECT file_size, hash_partial FROM media_files
            WHERE hash_partial IS NOT NULL
            GROUP BY file_size, hash_partial HAVING COUNT(*) > 1
        )
        AND hash_full IS NULL
        """
        
        updates = []
        with self.db.get_connection() as conn:
            cursor = conn.execute(sql_select)
            rows = cursor.fetchall()
            
            if rows:
                self.logger.info(f"Computing Full Hashes for {len(rows)} high-probability duplicates...")
            
            for row_id, path in rows:
                f_hash = self._compute_full_hash(path)
                if f_hash:
                    updates.append((f_hash, 1, row_id))

            if updates:
                conn.executemany("UPDATE media_files SET hash_full = ?, hashed = ? WHERE id = ?", updates)
                conn.commit()

    def _compute_partial_hash(self, path: str, file_size: int) -> str:
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

    def _compute_full_hash(self, path: str) -> str:
        try:
            hasher = xxhash.xxh64()
            with open(path, 'rb') as f:
                while chunk := f.read(65536):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except OSError:
            self.logger.error(f"Could not read file: {path}")
            return None