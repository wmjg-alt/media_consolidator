"""SQLite database management for media file metadata."""

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager


class DatabaseManager:
    """Manage SQLite database connections and schema for media file metadata.
    
    Provides context-managed database connections and schema initialization
    for storing and querying media file information including hashes,
    timestamps, and organization metadata.
    """

    def __init__(self, db_path: str) -> None:
        """Initialize the database manager.
        
        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path

    @contextmanager
    def get_connection(self) -> Iterator[sqlite3.Connection]:
        """Get a database connection with automatic cleanup.
        
        Yields:
            A SQLite connection object that is automatically closed when
            the context manager exits.
        """
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def initialize_schema(self) -> None:
        """Create the media_files table and indexes if they don't exist.
        
        Creates a single media_files table with columns for file metadata,
        content hashes, EXIF data, processing state, and disposition.
        Both created_at and modified_at timestamps are stored to determine
        the true oldest timestamp of a file across system updates.
        """
        schema = """
        CREATE TABLE IF NOT EXISTS media_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT UNIQUE NOT NULL,
            file_size INTEGER,
            file_ext TEXT,
            created_at REAL,
            modified_at REAL,
            hash_partial TEXT,
            hash_full TEXT,
            phash TEXT,
            has_exif_date INTEGER DEFAULT 0,
            has_gps INTEGER DEFAULT 0,
            metadata_score INTEGER DEFAULT 0,
            scanned INTEGER DEFAULT 1,
            hashed INTEGER DEFAULT 0,
            analyzed INTEGER DEFAULT 0,
            disposition TEXT,
            target_path TEXT
        );
        
        CREATE INDEX IF NOT EXISTS idx_file_size ON media_files(file_size);
        CREATE INDEX IF NOT EXISTS idx_hash_full ON media_files(hash_full);
        """
        
        with self.get_connection() as conn:
            conn.executescript(schema)
            conn.commit()

    def wipe_db(self) -> None:
        """Drop the media_files table and all its data.
        
        This is a destructive operation that removes all indexed media files
        and their metadata from the database.
        """
        with self.get_connection() as conn:
            conn.execute("DROP TABLE IF EXISTS media_files")
            conn.commit()