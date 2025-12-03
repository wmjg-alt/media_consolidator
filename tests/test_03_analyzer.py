from src.analyzer import Analyzer

def test_analyzer_oldest_date_wins(db_manager, mock_config):
    """
    If two files match hash:
    File A: Created 2024
    File B: Created 2025
    File A should be KEEP, File B should be DELETE.
    """
    # Timestamps
    ts_2024 = 1704067200.0 # 2024-01-01
    ts_2025 = 1735689600.0 # 2025-01-01
    
    hash_val = "abc123hash"
    
    with db_manager.get_connection() as conn:
        # File A (Oldest)
        conn.execute("""
            INSERT INTO media_files (file_path, hash_full, created_at, modified_at, disposition)
            VALUES (?, ?, ?, ?, NULL)
        """, ("C:/Old.jpg", hash_val, ts_2024, ts_2024))
        
        # File B (Newer)
        conn.execute("""
            INSERT INTO media_files (file_path, hash_full, created_at, modified_at, disposition)
            VALUES (?, ?, ?, ?, NULL)
        """, ("C:/New.jpg", hash_val, ts_2025, ts_2025))
        conn.commit()
        
    analyzer = Analyzer(db_manager, mock_config)
    analyzer.process_duplicates()
    
    with db_manager.get_connection() as conn:
        res_old = conn.execute("SELECT disposition FROM media_files WHERE file_path='C:/Old.jpg'").fetchone()
        res_new = conn.execute("SELECT disposition FROM media_files WHERE file_path='C:/New.jpg'").fetchone()
        
        assert res_old[0] == 'KEEP'
        assert res_new[0] == 'DELETE'

def test_analyzer_keeps_uniques(db_manager, mock_config):
    """Uniques should be marked KEEP, not ignored."""
    with db_manager.get_connection() as conn:
        conn.execute("""
            INSERT INTO media_files (file_path, hash_full) VALUES ('C:/Unique.jpg', 'uniquehash')
        """)
        conn.commit()

    analyzer = Analyzer(db_manager, mock_config)
    analyzer.process_duplicates()
    
    with db_manager.get_connection() as conn:
        res = conn.execute("SELECT disposition FROM media_files").fetchone()
        assert res[0] == 'KEEP'

def test_filename_penalty_tiebreaker(db_manager, mock_config):
    """
    If Date and Score are identical, the filename without 'copy' or '(1)' must win.
    """
    hash_val = "tiebreaker_hash"
    ts = 1000.0
    
    # Candidate A: Clean Name
    path_a = "C:/Photos/Vacation.jpg"
    
    # Candidate B: Dirty Name (Copy)
    path_b = "C:/Photos/Copy of Vacation.jpg"
    
    # Candidate C: Dirty Name (Numbered)
    path_c = "C:/Photos/Vacation (1).jpg"
    
    with db_manager.get_connection() as conn:
        for p in [path_a, path_b, path_c]:
            conn.execute("""
                INSERT INTO media_files (file_path, hash_full, created_at, modified_at, metadata_score)
                VALUES (?, ?, ?, ?, 10)
            """, (p, hash_val, ts, ts))
        conn.commit()
        
    analyzer = Analyzer(db_manager, mock_config)
    analyzer.process_duplicates()
    
    with db_manager.get_connection() as conn:
        # Check A (Should KEEP)
        res_a = conn.execute("SELECT disposition FROM media_files WHERE file_path=?", (path_a,)).fetchone()
        assert res_a[0] == 'KEEP'
        
        # Check B and C (Should DELETE)
        res_b = conn.execute("SELECT disposition FROM media_files WHERE file_path=?", (path_b,)).fetchone()
        assert res_b[0] == 'DELETE'
        
        res_c = conn.execute("SELECT disposition FROM media_files WHERE file_path=?", (path_c,)).fetchone()
        assert res_c[0] == 'DELETE'

