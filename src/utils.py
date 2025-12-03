"""Configuration and logging utilities."""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def normalize_path(path_str: str) -> str:
    """Normalize a file path to use forward slashes and remove quotes.
    
    Strips surrounding quotes and converts backslashes to forward slashes
    for consistent cross-platform path handling.
    
    Args:
        path_str: Path string to normalize.
        
    Returns:
        Normalized path with forward slashes, or empty string if input was empty.
    """
    if not path_str:
        return ""
    clean = path_str.strip('\'"')
    return clean.replace('\\', '/')


def load_config(config_path: str = "config/settings.yaml") -> dict[str, Any]:
    """Load and normalize configuration from a YAML file.
    
    Reads YAML configuration and applies path normalization to all path
    fields (target_root, trash_folder, source_dirs) to ensure consistent
    forward-slash formatting across platforms.
    
    Args:
        config_path: Path to the configuration YAML file.
        
    Returns:
        Configuration dictionary with normalized paths.
        
    Raises:
        FileNotFoundError: If the configuration file does not exist.
        yaml.YAMLError: If the YAML file is malformed.
    """
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
    """Configure the root logger with console and file handlers.
    
    Sets up both console (stdout) and daily timestamped file logging.
    Clears any existing handlers and reconfigures the logger with settings
    from the configuration dictionary.
    
    Args:
        config: Configuration dictionary containing:
            - app.log_level: Logging level (INFO, DEBUG, etc.) defaults to INFO
            - app.log_dir: Directory for log files, defaults to 'logs'
            
    Returns:
        Configured logger instance.
    """
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