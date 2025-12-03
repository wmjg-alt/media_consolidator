"""Configuration, logging, and filesystem utilities."""

import logging
import os
import sys
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from ctypes import windll, wintypes, byref

import yaml

MIN_VALID_TIMESTAMP = 315619200.0
_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def normalize_path(path_str: str) -> str:
    """Normalize a file path to use forward slashes and remove quotes."""
    if not path_str:
        return ""
    clean = path_str.strip('\'"')
    return clean.replace('\\', '/')


def load_config(config_path: str = "config/settings.yaml") -> dict[str, Any]:
    """Load and normalize configuration from a YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {path.absolute()}")
    
    with open(path, 'r', encoding='utf-8') as f:
        raw_content = f.read()
    
    sanitized_content = raw_content.replace('\\', '/')
    
    try:
        config = yaml.safe_load(sanitized_content)
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML: {e}") from e
    
    org = config.get('organization', {})
    if 'target_root' in org:
        org['target_root'] = normalize_path(org['target_root'])
    if 'trash_folder' in org:
        org['trash_folder'] = normalize_path(org['trash_folder'])
    if 'source_dirs' in org:
        org['source_dirs'] = [normalize_path(p) for p in org['source_dirs']]
    
    return config


def setup_logger(config: dict[str, Any]) -> logging.Logger:
    """Configure the root logger with console and file handlers."""
    app_cfg = config.get('app', {})
    level_name = app_cfg.get('log_level', 'INFO')
    log_dir = app_cfg.get('log_dir', 'logs')
    
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logger = logging.getLogger("MediaConsolidator")
    logger.setLevel(getattr(logging, level_name.upper(), logging.INFO))
    logger.handlers = []
    
    formatter = logging.Formatter(_LOG_FORMAT)
    
    c_handler = logging.StreamHandler(sys.stdout)
    c_handler.setFormatter(formatter)
    logger.addHandler(c_handler)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"{date_str}_session.log")
    f_handler = logging.FileHandler(log_file, encoding='utf-8')
    f_handler.setFormatter(formatter)
    logger.addHandler(f_handler)
    
    return logger


def apply_jitter_if_midnight(timestamp: float) -> float:
    """Apply random jitter if the timestamp represents exactly midnight.
    
    If the time is 00:00:00, changes it to Noon +/- 4 hours to make it look
    more natural. Otherwise returns the timestamp unchanged.
    
    Args:
        timestamp: Unix timestamp.
        
    Returns:
        Original timestamp, or jittered timestamp if input was midnight.
    """
    dt = datetime.fromtimestamp(timestamp)
    
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        # Reset to Noon on the same day
        noon_dt = dt.replace(hour=12, minute=0, second=0)
        # Add Jitter (+/- 4 hours = +/- 14400 seconds)
        jitter_seconds = random.randint(-14400, 14400)
        final_dt = noon_dt + timedelta(seconds=jitter_seconds)
        return final_dt.timestamp()
        
    return timestamp

def resolve_best_timestamp(created_ts: float, modified_ts: float) -> float:
    """Determine the most accurate creation date, filtering out epoch errors.
    
    Standard logic is min(created, modified). However, if one timestamp is
    older than 1980 (likely a DOS/Unix epoch default error), it is ignored
    in favor of the other.
    
    Args:
        created_ts: Creation timestamp.
        modified_ts: Modification timestamp.
    Returns:
        The most plausible oldest timestamp.
    """
    c_valid = created_ts > MIN_VALID_TIMESTAMP
    m_valid = modified_ts > MIN_VALID_TIMESTAMP
    
    if c_valid and m_valid:
        return min(created_ts, modified_ts)
    elif c_valid:
        return created_ts
    elif m_valid:
        return modified_ts
    else:
        # Both are garbage (ancient). Return the larger one (closer to today)
        # or just return 0. Returning max implies "at least it's not 1970".
        return max(created_ts, modified_ts)


def set_file_creation_time(path: str, timestamp: float) -> bool:
    """Set the Windows Creation Time (Birthtime) to a specific timestamp.
    
    Uses Win32 API via ctypes. Safe to call on non-Windows systems (returns False).
    
    Args:
        path: Path to the file.
        timestamp: Unix timestamp to apply.
        
    Returns:
        True on success, False on failure or non-Windows OS.
    """
    if os.name != 'nt':
        return False

    try:
        # Convert Unix timestamp to Windows FileTime (100ns intervals since Jan 1, 1601)
        wintime = int((timestamp * 10000000) + 116444736000000000)
        
        ft = wintypes.FILETIME()
        ft.dwLowDateTime = wintime & 0xFFFFFFFF
        ft.dwHighDateTime = wintime >> 32
        
        GENERIC_WRITE = 0x40000000
        OPEN_EXISTING = 3
        FILE_ATTRIBUTE_NORMAL = 0x80
        
        # CreateFileW is the Unicode version
        handle = windll.kernel32.CreateFileW(
            path, GENERIC_WRITE, 0, None, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, None
        )
        
        if handle == -1:
            return False
            
        # SetFileTime(handle, Creation, Access, Modification) -> We only set Creation here
        result = windll.kernel32.SetFileTime(handle, byref(ft), None, None)
        windll.kernel32.CloseHandle(handle)
        
        return result != 0
    except Exception:
        return False