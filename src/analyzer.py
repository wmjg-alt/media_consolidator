import os
import logging
from PIL import Image, UnidentifiedImageError
from src.db import DatabaseManager

# FIX: Disable "Decompression Bomb" warning for large photos
Image.MAX_IMAGE_PIXELS = None 

class Analyzer:
    def __init__(self, db: DatabaseManager, config: dict):
        self.db = db
        self.logger = logging.getLogger("MediaConsolidator.Analyzer")
        self.ensure_schema()

    def ensure_schema(self):
        with self.db.get_connection() as conn:
            try:
                conn.execute("ALTER TABLE media_files ADD COLUMN disposition TEXT")
                conn.commit()
            except Exception:
                pass

    def process_metadata(self):
        sql = """
        SELECT id, file_path FROM media_files 
        WHERE analyzed = 0 AND file_ext IN ('.jpg', '.jpeg', '.png', '.heic', '.webp')
        """
        
        updates = []
        with self.db.get_connection() as conn:
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            
            if rows:
                self.logger.info(f"Analyzing metadata for {len(rows)} files...")
            
            for row_id, path in rows:
                has_exif, date_str = self._extract_exif(path)
                score = 10 if has_exif else 0
                updates.append((has_exif, score, row_id))

            conn.executemany("""
                UPDATE media_files 
                SET has_exif_date = ?, metadata_score = ?, analyzed = 1 
                WHERE id = ?
            """, updates)
            conn.commit()

    def process_duplicates(self):
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
            
            # Handle Uniques
            sql_uniques = "UPDATE media_files SET disposition = 'KEEP' WHERE disposition IS NULL"
            conn.execute(sql_uniques)
            conn.commit()

    def _judge_group(self, conn, hash_val):
        # Fetch both timestamps
        rows = conn.execute(
            "SELECT id, file_path, metadata_score, created_at, modified_at FROM media_files WHERE hash_full = ?", 
            (hash_val,)
        ).fetchall()
        
        def sort_key(item):
            score = item[2]
            created = item[3]
            modified = item[4]
            path = item[1]
            filename = os.path.basename(path)
            
            # THE LOGIC: The True Date is the absolute oldest timestamp the OS knows about
            effective_date = min(created, modified)
            
            name_penalty = -100 if "copy" in filename.lower() or "(" in filename else 0
            
            # Sort Order:
            # 1. Highest Meta Score (-score)
            # 2. Oldest Effective Date (smaller is older)
            # 3. Cleanest Filename
            # 4. Shortest Path
            return (-score, effective_date, -name_penalty, len(path))

        sorted_candidates = sorted(rows, key=sort_key)
        
        winner = sorted_candidates[0]
        losers = sorted_candidates[1:]
        
        conn.execute("UPDATE media_files SET disposition = 'KEEP' WHERE id = ?", (winner[0],))
        
        if losers:
            loser_ids = [str(x[0]) for x in losers]
            conn.execute(f"UPDATE media_files SET disposition = 'DELETE' WHERE id IN ({','.join(loser_ids)})")

    def _extract_exif(self, path: str):
        try:
            with Image.open(path) as img:
                exif_data = img._getexif()
                if not exif_data: return 0, None
                
                date_str = exif_data.get(36867) 
                if date_str: return 1, date_str
                date_str = exif_data.get(306)
                if date_str: return 1, date_str
                return 0, None
        except Exception:
            return 0, None