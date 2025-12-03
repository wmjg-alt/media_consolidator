"""Persistent hash caching mechanism."""

import sqlite3
import os
import logging
from contextlib import contextmanager

class HashCache:
    """
    Maintains a persistent cache of full file hashes.
    Key: (File Size, Partial Hash)
    Value: Full Hash
    
    Located in the Target Root so it travels with the library.
    """
    
    def __init__(self, target_root: str):
        self.logger = logging.getLogger("MediaConsolidator.HashCache")
        
        # Ensure target root exists, otherwise put in CWD
        if not os.path.exists(target_root):
            try:
                os.makedirs(target_root)
            except OSError:
                target_root = "."
                
        self.db_path = os.path.join(target_root, ".media_hash_cache.db")
        self.initialize_schema()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def initialize_schema(self):
        """Creates the cache table if it doesn't exist."""
        schema = """
        CREATE TABLE IF NOT EXISTS hash_cache (
            file_size INTEGER,
            hash_partial TEXT,
            hash_full TEXT,
            last_seen INTEGER,
            PRIMARY KEY (file_size, hash_partial)
        );
        """
        with self.get_connection() as conn:
            conn.executescript(schema)
            conn.commit()

    def get_full_hash(self, file_size: int, partial_hash: str) -> str:
        """Retrieves full hash if we've processed this exact file signature before."""
        query = "SELECT hash_full FROM hash_cache WHERE file_size = ? AND hash_partial = ?"
        with self.get_connection() as conn:
            row = conn.execute(query, (file_size, partial_hash)).fetchone()
            if row:
                return row[0]
        return None

    def put_full_hash(self, file_size: int, partial_hash: str, full_hash: str):
        """Saves a computed hash for future runs."""
        # INSERT OR REPLACE updates the entry if it exists
        query = """
        INSERT OR REPLACE INTO hash_cache (file_size, hash_partial, hash_full, last_seen)
        VALUES (?, ?, ?, strftime('%s', 'now'))
        """
        try:
            with self.get_connection() as conn:
                conn.execute(query, (file_size, partial_hash, full_hash))
                conn.commit()
        except sqlite3.Error as e:
            self.logger.warning(f"Failed to cache hash: {e}")