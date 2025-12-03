from src.hasher import Fingerprinter

def test_hasher_funnel_logic(db_manager, mock_config, temp_roots):
    """
    Verify:
    1. Unique Size -> Skipped (hashed=1, hash_full=NULL)
    2. Unique Partial -> Skipped Full Hash (hashed=1, hash_full=NULL)
    3. True Duplicate -> Full Hash (hash_full != NULL)
    """
    # Setup
    src = temp_roots["source"]
    
    # A. Unique Size
    (src / "unique_size.jpg").write_bytes(b"A"*100)
    
    # B. Unique Partial (Same size, different header)
    (src / "partial_1.jpg").write_bytes(b"B"*200 + b"1")
    (src / "partial_2.jpg").write_bytes(b"C"*200 + b"2") # Different start byte
    
    # C. True Duplicate
    (src / "dupe_1.jpg").write_bytes(b"D"*300)
    (src / "dupe_2.jpg").write_bytes(b"D"*300)
    
    # Insert manually to simulate crawler
    with db_manager.get_connection() as conn:
        for f in src.iterdir():
            conn.execute("INSERT INTO media_files (file_path, file_size) VALUES (?, ?)", 
                         (str(f), f.stat().st_size))
        conn.commit()
        
    # Run
    hasher = Fingerprinter(db_manager, mock_config)
    hasher.process_database()
    
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        # 1. Unique Size
        # Should be hashed=1, but NULL hash (Optimization)
        res = cursor.execute("SELECT hashed, hash_full FROM media_files WHERE file_size=100").fetchone()
        assert res[0] == 1
        assert res[1] is None 
        
        # 2. Unique Partial (The 'Limbo' Fix check)
        # Should be hashed=1, hash_partial populated, hash_full NULL
        res = cursor.execute("SELECT hashed, hash_partial, hash_full FROM media_files WHERE file_size=201").fetchall()
        for r in res:
            assert r[0] == 1
            assert r[1] is not None
            assert r[2] is None # Did not waste time on full hash
            
        # 3. True Duplicate
        # Should have hash_full
        res = cursor.execute("SELECT hash_full FROM media_files WHERE file_size=300").fetchall()
        assert res[0][0] is not None
        assert res[0][0] == res[1][0]

def test_zero_byte_file_handling(db_manager, mock_config, temp_roots):
    """
    Edge Case: 0-byte files should be handled gracefully.
    We create TWO to force the hasher to actually check content (skip size optimization).
    """
    src = temp_roots["source"]
    (src / "empty1.jpg").touch()
    (src / "empty2.jpg").touch()
    
    # Manually insert
    with db_manager.get_connection() as conn:
        conn.execute("INSERT INTO media_files (file_path, file_size) VALUES (?, ?)", 
                     (str(src / "empty1.jpg"), 0))
        conn.execute("INSERT INTO media_files (file_path, file_size) VALUES (?, ?)", 
                     (str(src / "empty2.jpg"), 0))
        conn.commit()
        
    hasher = Fingerprinter(db_manager, mock_config)
    hasher.process_database()
    
    with db_manager.get_connection() as conn:
        # Now they should have full hashes because they collided on size=0
        res = conn.execute("SELECT hash_full FROM media_files WHERE file_size=0").fetchall()
        assert len(res) == 2
        assert res[0][0] is not None
        # xxhash of empty string is a constant, so they must match
        assert res[0][0] == res[1][0]

        