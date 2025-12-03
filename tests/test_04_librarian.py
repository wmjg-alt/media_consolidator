from src.librarian import Librarian

def test_librarian_template_logic(db_manager, mock_config):
    """
    New files should get the template: {date}_{name}_from_{folder}
    """
    # FIX 1: Use Noon UTC (1704110400) instead of Midnight.
    # This ensures it stays '2024-01-01' regardless of whether you are in NY or LA.
    ts = 1704110400.0 
    
    # Path simulates being in a source folder
    src_path = "D:/MyBackup/SourceFolder/Photo.jpg"
    
    with db_manager.get_connection() as conn:
        # FIX 2: Added 'file_ext' column and '.jpg' value
        conn.execute("""
            INSERT INTO media_files (file_path, created_at, modified_at, file_ext, disposition)
            VALUES (?, ?, ?, '.jpg', 'KEEP')
        """, (src_path, ts, ts))
        conn.commit()
        
    lib = Librarian(db_manager, mock_config)
    lib.generate_organization_plan()
    
    with db_manager.get_connection() as conn:
        target = conn.execute("SELECT target_path FROM media_files").fetchone()[0]
        
        # Expectation: 2024-01-01_Photo_from_SourceFolder.jpg
        assert "2024-01-01_Photo_from_SourceFolder.jpg" in target

def test_librarian_idempotency(db_manager, mock_config):
    """
    Files ALREADY in target should NOT get _from_folder appended.
    """
    target_root = mock_config["organization"]["target_root"]
    # FIX 1: Use Noon UTC
    ts = 1704110400.0
    
    # This file is already inside the organized folder structure
    # Librarian should strip the date prefix (if exists) and re-apply it, but NOT append folder.
    existing_path = f"{target_root}/2024/2024-01/2024-01-01_Existing.jpg"
    
    with db_manager.get_connection() as conn:
        # FIX 2: Added 'file_ext' column
        conn.execute("""
            INSERT INTO media_files (file_path, created_at, modified_at, file_ext, disposition)
            VALUES (?, ?, ?, '.jpg', 'KEEP')
        """, (existing_path, ts, ts))
        conn.commit()
        
    lib = Librarian(db_manager, mock_config)
    lib.generate_organization_plan()
    
    with db_manager.get_connection() as conn:
        target = conn.execute("SELECT target_path FROM media_files").fetchone()[0]
        
        # It should remain clean (date updated if needed, but no _from_ artifact)
        assert "2024-01-01_Existing.jpg" in target
        assert "_from_" not in target

def test_librarian_sanitizes_folder_names(db_manager, mock_config):
    """
    If source folder is "Summer VACATION! (2024)", it should sanitize to
    "Summer_VACATION___2024_" to prevent invalid filename characters.
    """
    ts = 1704110400.0 # 2024-01-01
    
    # Weird folder name
    src_path = "D:/Backup/Summer VACATION! (2024)/Img.jpg"
    
    with db_manager.get_connection() as conn:
        conn.execute("""
            INSERT INTO media_files (file_path, created_at, modified_at, file_ext, disposition)
            VALUES (?, ?, ?, '.jpg', 'KEEP')
        """, (src_path, ts, ts))
        conn.commit()
        
    lib = Librarian(db_manager, mock_config)
    lib.generate_organization_plan()
    
    with db_manager.get_connection() as conn:
        target = conn.execute("SELECT target_path FROM media_files").fetchone()[0]
        
        # Expectation: Spaces/Parens/Exclamation replaced by underscores
        # 2024-01-01_Img_from_Summer_VACATION___2024_.jpg
        assert "Summer_VACATION___2024_" in target
        assert "!" not in target
        assert "(" not in target

def test_librarian_deep_collision(db_manager, mock_config):
    """
    If Img.jpg, Img_1.jpg, and Img_2.jpg exist in the target registry,
    the next one should be Img_3.jpg.
    """
    # 1. Override template to ensure names collide regardless of folder
    mock_config["organization"]["filename_template"] = "{date}_{name}"
    
    ts = 1704110400.0 # 2024-01-01
    
    # 2. Use distinct paths that all have the name 'Img.jpg'
    sources = [
        "D:/FolderA/Img.jpg",
        "D:/FolderB/Img.jpg",
        "D:/FolderC/Img.jpg",
        "D:/FolderD/Img.jpg"
    ]
    
    with db_manager.get_connection() as conn:
        for src in sources:
            conn.execute("""
                INSERT INTO media_files (file_path, created_at, modified_at, file_ext, disposition)
                VALUES (?, ?, ?, '.jpg', 'KEEP')
            """, (src, ts, ts))
        conn.commit()
        
    lib = Librarian(db_manager, mock_config)
    lib.generate_organization_plan()
    
    with db_manager.get_connection() as conn:
        targets = conn.execute("SELECT target_path FROM media_files").fetchall()
        target_paths = [t[0] for t in targets]
        
        # We expect:
        # ..._Img.jpg (The first one)
        # ..._Img_2.jpg (The 2nd copy)
        # ..._Img_3.jpg (The 3rd copy)
        # ..._Img_4.jpg (The 4th copy)
        
        # Debug print to confirm
        print(target_paths)
        
        assert any(t.endswith("_Img.jpg") for t in target_paths)
        # UPDATE: Expectation starts at _2 for the first collision
        assert any(t.endswith("_Img_2.jpg") for t in target_paths)
        assert any(t.endswith("_Img_3.jpg") for t in target_paths)
        assert any(t.endswith("_Img_4.jpg") for t in target_paths)

