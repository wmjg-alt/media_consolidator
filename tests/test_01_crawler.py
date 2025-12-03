import os
import time
from src.crawler import FileCrawler

def test_crawler_respects_exclusions(db_manager, mock_config, temp_roots):
    """
    Ensure the crawler ignores system folders and the trash folder.
    """
    source = temp_roots["source"]
    
    # 1. Valid Media
    (source / "photo.jpg").touch()
    
    # 2. Excluded Folder (Configured in mock_config)
    win_dir = source / "Windows"
    win_dir.mkdir()
    (win_dir / "system_file.jpg").touch() # Should be ignored
    
    # 3. Trash Folder (Should be ignored by logic)
    # The crawler logic explicitly checks if path starts with trash_folder path.
    # Note: Trash is usually inside Target, but let's put a fake trash in source to test exclusion
    mock_config["organization"]["trash_folder"] = str(source / "_TRASH")
    trash_dir = source / "_TRASH"
    trash_dir.mkdir()
    (trash_dir / "deleted.jpg").touch()

    # Run
    crawler = FileCrawler(db_manager, mock_config)
    count = crawler.scan_roots([str(source)])
    
    # Assert
    assert count == 1 # Only root photo.jpg
    
    with db_manager.get_connection() as conn:
        rows = conn.execute("SELECT file_path FROM media_files").fetchall()
        paths = [r[0] for r in rows]
        
        # Check using 'in' to handle path separators safely
        assert any("photo.jpg" in p for p in paths)
        assert not any("system_file.jpg" in p for p in paths)
        assert not any("deleted.jpg" in p for p in paths)

def test_crawler_captures_dates(db_manager, mock_config, temp_roots):
    """
    Ensure we capture both Created (Birthtime) and Modified dates.
    """
    f = temp_roots["source"] / "timed.jpg"
    f.touch()
    
    crawler = FileCrawler(db_manager, mock_config)
    crawler.scan_roots([str(temp_roots["source"])])
    
    with db_manager.get_connection() as conn:
        row = conn.execute("SELECT created_at, modified_at FROM media_files").fetchone()
        assert row[0] is not None
        assert row[1] is not None