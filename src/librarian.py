import os
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict
from src.db import DatabaseManager

class Librarian:
    def __init__(self, db: DatabaseManager, config: dict):
        self.db = db
        self.logger = logging.getLogger("MediaConsolidator.Librarian")
        
        org_config = config.get("organization", {})
        target_raw = org_config.get("target_root", "C:/Photos")
        self.target_root = target_raw.replace('\\', '/')
        
        self.filename_template = org_config.get("filename_template", "{date}_{name}")
        
        self.ensure_schema()
        
        # Regex to strip date prefix (YYYY-MM-DD_)
        self.date_prefix_pattern = re.compile(r'^\d{4}[-_]\d{2}[-_]\d{2}[-_ ]?')
        # Regex to clean folder names
        self.folder_clean_pattern = re.compile(r'[^\w\-]')

    def ensure_schema(self):
        with self.db.get_connection() as conn:
            try:
                conn.execute("ALTER TABLE media_files ADD COLUMN target_path TEXT")
                conn.commit()
            except Exception:
                pass 

    def generate_organization_plan(self):
        self.logger.info("Generating organization plan...")
        
        sql = """
        SELECT id, file_path, created_at, modified_at, file_ext 
        FROM media_files 
        WHERE disposition = 'KEEP'
        """
        
        updates = []
        path_registry: Dict[str, int] = {}

        with self.db.get_connection() as conn:
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            
            self.logger.info(f"Planning moves for {len(rows)} files...")

            for row_id, src_path, c_time, m_time, ext in rows:
                
                # 1. Best Date
                best_ts = min(c_time, m_time)
                dt = datetime.fromtimestamp(best_ts)
                
                year_folder = dt.strftime("%Y")
                month_folder = dt.strftime("%Y-%m")
                date_prefix = dt.strftime("%Y-%m-%d")
                
                src_p = Path(src_path)
                
                # 2. Logic Split: Is this file ALREADY organized?
                # We normalize to ensure string matching works
                norm_src = src_path.replace('\\', '/')
                is_already_organized = norm_src.startswith(self.target_root)
                
                original_stem = src_p.stem
                clean_stem = self.date_prefix_pattern.sub('', original_stem)
                if not clean_stem: clean_stem = original_stem

                if is_already_organized:
                    # CASE A: Already organized. 
                    # DO NOT append folder name (it's just a date like '2024-01').
                    # DO NOT re-apply template. 
                    # Just ensure the date prefix matches the actual file date.
                    new_filename_stem = f"{date_prefix}_{clean_stem}"
                else:
                    # CASE B: New file from outside. 
                    # Apply full template with source folder tracking.
                    raw_folder = src_p.parent.name
                    clean_folder = self.folder_clean_pattern.sub('_', raw_folder)
                    
                    name_map = {
                        "date": date_prefix,
                        "name": clean_stem,
                        "folder": clean_folder
                    }
                    try:
                        new_filename_stem = self.filename_template.format(**name_map)
                    except KeyError:
                        new_filename_stem = f"{date_prefix}_{clean_stem}"
                
                safe_name = f"{new_filename_stem}{ext}"
                
                # 3. Path Calculation
                relative_path = os.path.join(year_folder, month_folder, safe_name)
                full_target = os.path.join(self.target_root, relative_path).replace('\\', '/')
                
                final_target = self._resolve_collision(full_target, path_registry)
                updates.append((final_target, row_id))

            if updates:
                conn.executemany("UPDATE media_files SET target_path = ? WHERE id = ?", updates)
                conn.commit()

    def _resolve_collision(self, target_path: str, registry: Dict[str, int]) -> str:
        lower_path = target_path.lower()
        if lower_path not in registry:
            registry[lower_path] = 1
            return target_path
        
        registry[lower_path] += 1
        count = registry[lower_path]
        
        p = Path(target_path)
        new_name = f"{p.stem}_{count}{p.suffix}"
        new_path = str(p.parent / new_name).replace('\\', '/')
        
        return self._resolve_collision(new_path, registry)