import yaml
import logging
import os
import sys
from pathlib import Path
from datetime import datetime

def normalize_path(path_str: str) -> str:
    if not path_str: return ""
    clean = path_str.strip('\'"')
    return clean.replace('\\', '/')

def load_config(config_path: str = "config/settings.yaml") -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {path.absolute()}")
    
    with open(path, 'r', encoding='utf-8') as f:
        raw_content = f.read()
        
    sanitized_content = raw_content.replace('\\', '/')
    
    try:
        config = yaml.safe_load(sanitized_content)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML: {e}")
        raise
        
    # Apply Path Normalization
    org = config.get('organization', {})
    if 'target_root' in org: org['target_root'] = normalize_path(org['target_root'])
    if 'trash_folder' in org: org['trash_folder'] = normalize_path(org['trash_folder'])
    if 'source_dirs' in org: org['source_dirs'] = [normalize_path(p) for p in org['source_dirs']]
        
    return config

def setup_logger(config: dict) -> logging.Logger:
    app_cfg = config.get('app', {})
    level_name = app_cfg.get('log_level', 'INFO')
    log_dir = app_cfg.get('log_dir', 'logs')
    
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    logger = logging.getLogger("MediaConsolidator")
    logger.setLevel(getattr(logging, level_name.upper(), logging.INFO))
    logger.handlers = [] # Clear existing

    # Console
    c_handler = logging.StreamHandler(sys.stdout)
    c_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(c_handler)
    
    # File
    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"{date_str}_session.log")
    f_handler = logging.FileHandler(log_file, encoding='utf-8')
    f_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(f_handler)
    
    return logger