import sqlite3
from contextlib import contextmanager

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def initialize_schema(self):
        schema = """
        CREATE TABLE IF NOT EXISTS media_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT UNIQUE NOT NULL,
            file_size INTEGER,
            file_ext TEXT,
            
            -- We keep BOTH to find the true oldest
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

    def wipe_db(self):
        with self.get_connection() as conn:
            conn.execute("DROP TABLE IF EXISTS media_files")
            conn.commit()