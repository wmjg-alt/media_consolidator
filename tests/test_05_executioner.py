import os
from src.executioner import Executioner

def test_executioner_receipts(db_manager, mock_config, temp_roots):
    """
    Verify image_trace.txt is written to source folder after move.
    """
    src_file = temp_roots["source"] / "test_img.jpg"
    src_file.write_text("content")
    
    dest_path = str(temp_roots["target"] / "2024" / "test_img.jpg")
    
    with db_manager.get_connection() as conn:
        conn.execute("""
            INSERT INTO media_files (file_path, target_path, disposition)
            VALUES (?, ?, 'KEEP')
        """, (str(src_file), dest_path))
        conn.commit()
        
    # Run Live
    exec = Executioner(db_manager, mock_config, dry_run=False)
    exec.execute()
    
    # Check receipt
    trace_file = temp_roots["source"] / "image_trace.txt"
    assert trace_file.exists()
    content = trace_file.read_text()
    assert "[MOVED]" in content
    assert "test_img.jpg" in content

def test_executioner_skips_in_place(db_manager, mock_config, temp_roots):
    """
    If source == target, do nothing and DO NOT write receipt.
    """
    # Create file IN target
    target_file = temp_roots["target"] / "already_here.jpg"
    target_file.write_text("content")
    
    # DB says move it to itself
    with db_manager.get_connection() as conn:
        conn.execute("""
            INSERT INTO media_files (file_path, target_path, disposition)
            VALUES (?, ?, 'KEEP')
        """, (str(target_file), str(target_file)))
        conn.commit()
        
    exec = Executioner(db_manager, mock_config, dry_run=False)
    exec.execute()
    
    # Ensure NO trace file created in target root
    trace_file = temp_roots["target"] / "image_trace.txt"
    assert not trace_file.exists()