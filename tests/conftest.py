import pytest
import os
import yaml
from pathlib import Path
from src.db import DatabaseManager
from src.utils import normalize_path

@pytest.fixture
def temp_roots(tmp_path):
    """
    Creates a standard folder structure for testing:
    /source
    /target
    /trash
    """
    source = tmp_path / "source"
    target = tmp_path / "target"
    trash = target / "_TRASH"
    
    source.mkdir()
    target.mkdir()
    trash.mkdir(parents=True)
    
    return {
        "source": source,
        "target": target,
        "trash": trash,
        "root": tmp_path
    }

@pytest.fixture
def mock_config(temp_roots):
    """
    Returns a valid configuration dict pointing to temp folders.
    Includes standard exclusions and template settings.
    """
    return {
        "app": {
            "name": "TestMediaConsolidator",
            "db_name": str(temp_roots["root"] / "test_db.sqlite"),
            "log_level": "DEBUG",
            "log_dir": str(temp_roots["root"] / "logs")
        },
        "extensions": {
            "images": [".jpg", ".png"],
            "videos": [".mp4"]
        },
        "hashing": {
            "chunk_size": 1024 # Small chunk for fast tests
        },
        "organization": {
            "target_root": str(temp_roots["target"]),
            "trash_folder": str(temp_roots["trash"]),
            "filename_template": "{date}_{name}_from_{folder}",
            "source_dirs": [str(temp_roots["source"])],
            "exclude_dirs": ["Windows", "System32", "$RECYCLE.BIN"]
        }
    }

@pytest.fixture
def db_manager(mock_config):
    """
    Provides a fresh, initialized database for each test.
    """
    db_path = mock_config["app"]["db_name"]
    if os.path.exists(db_path):
        os.remove(db_path)
        
    mgr = DatabaseManager(db_path)
    mgr.initialize_schema()
    yield mgr
    
    # Cleanup
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except PermissionError:
            pass