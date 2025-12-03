"""Media file organization planning with collision detection."""

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from src.db import DatabaseManager


class Librarian:
    """Generate file organization plans with path collision resolution.
    
    Determines target paths for KEEP files based on metadata timestamps,
    applies configurable naming templates, and handles path collisions
    by appending numeric suffixes.
    """

    def __init__(self, db: DatabaseManager, config: dict[str, Any]) -> None:
        """Initialize the librarian.
        
        Args:
            db: Database manager instance.
            config: Configuration dictionary containing:
                - organization.target_root: Root directory for organized files
                - organization.filename_template: Template for filenames
                  (supports {date}, {name}, {folder} placeholders)
        """
        self.db = db
        self.logger = logging.getLogger("MediaConsolidator.Librarian")
        
        org_config = config.get("organization", {})
        target_raw = org_config.get("target_root", "C:/Photos")
        self.target_root = target_raw.replace('\\', '/')
        
        self.filename_template = org_config.get("filename_template", "{date}_{name}")
        
        self.ensure_schema()
        
        self.date_prefix_pattern = re.compile(r'^\d{4}[-_]\d{2}[-_]\d{2}[-_ ]?')
        self.folder_clean_pattern = re.compile(r'[^\w\-]')

    def ensure_schema(self) -> None:
        """Add target_path column to media_files table if it doesn't exist."""
        with self.db.get_connection() as conn:
            try:
                conn.execute("ALTER TABLE media_files ADD COLUMN target_path TEXT")
                conn.commit()
            except Exception:
                pass

    def generate_organization_plan(self) -> None:
        """Generate target paths for all KEEP files.
        
        For each file marked KEEP, determines an appropriate target path by:
        1. Extracting the true file date (earliest of created/modified time)
        2. Determining if file is already in organized target or coming from
           an external source
        3. Applying the appropriate naming scheme and template
        4. Resolving path collisions by appending numeric suffixes
        
        Results are stored in the media_files.target_path column.
        """
        self.logger.info("Generating organization plan...")
        
        sql = """
        SELECT id, file_path, created_at, modified_at, file_ext 
        FROM media_files 
        WHERE disposition = 'KEEP'
        """
        
        updates: list[tuple[str, int]] = []
        path_registry: dict[str, int] = {}

        with self.db.get_connection() as conn:
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            
            self.logger.info(f"Planning moves for {len(rows)} files...")

            for row_id, src_path, c_time, m_time, ext in rows:
                # Step 1: Determine best date for organization
                best_ts = min(c_time, m_time)
                dt = datetime.fromtimestamp(best_ts)
                
                year_folder = dt.strftime("%Y")
                month_folder = dt.strftime("%Y-%m")
                date_prefix = dt.strftime("%Y-%m-%d")

                # Step 2: Determine naming strategy
                src_p = Path(src_path)
                norm_src = src_path.replace('\\', '/')
                is_already_organized = norm_src.startswith(self.target_root)
                
                original_stem = src_p.stem
                clean_stem = self.date_prefix_pattern.sub('', original_stem)
                if not clean_stem:
                    clean_stem = original_stem

                if is_already_organized:
                    # A: File is already in the target structure; retain existing name
                    new_filename_stem = f"{date_prefix}_{clean_stem}"
                else:
                    # B: File is from external source; apply naming template
                    raw_folder = src_p.parent.name
                    clean_folder = self.folder_clean_pattern.sub('_', raw_folder)
                    
                    # Schema from configuration template, modifiable here:
                    name_map = {
                        "date": date_prefix,
                        "name": clean_stem,
                        "folder": clean_folder
                    }
                    try:
                        new_filename_stem = self.filename_template.format(**name_map)
                    except KeyError:
                        new_filename_stem = f"{date_prefix}_{clean_stem}"
                
                # Step 3: Construct target path and resolve collisions
                safe_name = f"{new_filename_stem}{ext}"
                relative_path = os.path.join(year_folder, month_folder, safe_name)
                full_target = os.path.join(self.target_root, relative_path).replace('\\', '/')
                
                final_target = self._resolve_collision(full_target, path_registry)
                updates.append((final_target, row_id))

            if updates:
                conn.executemany("UPDATE media_files SET target_path = ? WHERE id = ?", updates)
                conn.commit()

    def _resolve_collision(self, target_path: str, registry: dict[str, int]) -> str:
        """Resolve path collisions by appending numeric suffixes.
        
        If a target path has already been assigned, appends a counter to the
        filename and recursively checks the new path. Uses case-insensitive
        matching to handle filesystem case sensitivity differences.
        
        Args:
            target_path: The desired target path.
            registry: Dict mapping lowercase paths to occurrence counts.
            
        Returns:
            A unique path with numeric suffix appended if needed.
        """
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