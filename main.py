import argparse
import logging
import os
import sys
from pathlib import Path

from src.utils import load_config, setup_logger
from src.db import DatabaseManager
from src.crawler import FileCrawler
from src.hasher import Fingerprinter
from src.analyzer import Analyzer
from src.librarian import Librarian
from src.executioner import Executioner

CONFIG_PATH = "config/settings.yaml"

def main():
    parser = argparse.ArgumentParser(description="Media Consolidator: Organize and Deduplicate.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    subparsers.add_parser("scan")
    subparsers.add_parser("hash")
    subparsers.add_parser("analyze")
    subparsers.add_parser("plan")
    
    cmd_exec = subparsers.add_parser("execute")
    cmd_exec.add_argument("--live", action="store_true", help="DISABLE Dry Run (Actually move files)")
    
    cmd_all = subparsers.add_parser("all")
    cmd_all.add_argument("--live", action="store_true", help="DISABLE Dry Run")
    cmd_all.add_argument("roots", nargs="*", help="Optional: Override config folders")
    
    args = parser.parse_args()
    
    # 1. Setup & Path Normalization
    if not os.path.exists(CONFIG_PATH):
        print("ERROR: config/settings.yaml not found.")
        return

    config = load_config(CONFIG_PATH) 
    logger = setup_logger(config)
    
    db_path = config['app']['db_name']

    # === STATELESS MODE ===
    # We delete the DB at start to ensure we don't have stale data 
    # from files that were moved in a previous run.
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            logger.info("Previous database cleared for fresh run.")
        except OSError as e:
            logger.error(f"Could not clear database: {e}")
            return

    db = DatabaseManager(db_path)
    db.initialize_schema()
    
    # 2. Resolve Folders
    scan_roots = []
    
    # A. Get Target Dir (Always include)
    target_root = config['organization']['target_root']
    if not os.path.exists(target_root):
        try:
            os.makedirs(target_root)
            logger.info(f"Created Target Directory: {target_root}")
        except OSError:
            pass
    scan_roots.append(target_root)
    
    # B. Get Source Dirs
    if hasattr(args, 'roots') and args.roots:
        # User provided CLI overrides, normalize them too
        from src.utils import normalize_path
        scan_roots.extend([normalize_path(p) for p in args.roots])
    else:
        cfg_sources = config['organization'].get('source_dirs', [])
        scan_roots.extend(cfg_sources)
        
    scan_roots = list(set(scan_roots))

    logger.info("=== Media Consolidator Started ===")
    logger.info(f"Target Root: {target_root}")
    logger.info(f"Scan Scope: {scan_roots}")

    # 3. Execution Flow
    if args.command == "all":
        run_scan(db, config, scan_roots)
        run_hash(db, config)
        run_analyze(db, config)
        run_plan(db, config)
        
        if perform_audit(db, logger):
            run_exec(db, config, not args.live)
        else:
            logger.error("AUDIT FAILED. Aborting.")
            
    # (Keep individual commands if you want, but 'all' is the focus)
    elif args.command == "scan":
        run_scan(db, config, scan_roots)
    elif args.command == "hash":
        run_hash(db, config)
    elif args.command == "analyze":
        run_analyze(db, config)
    elif args.command == "plan":
        run_plan(db, config)
    elif args.command == "execute":
        run_exec(db, config, not args.live)
    else:
        parser.print_help()

# --- Phase Wrappers (Same as before) ---
def run_scan(db, config, roots):
    crawler = FileCrawler(db, config)
    count = crawler.scan_roots(roots)
    print(f"Phase 2 Complete. Scanned {count} files.")

def run_hash(db, config):
    hasher = Fingerprinter(db, config)
    hasher.process_database()
    print("Phase 3 Complete. Fingerprinting done.")

def run_analyze(db, config):
    analyzer = Analyzer(db, config)
    analyzer.process_metadata()
    analyzer.process_duplicates()
    print("Phase 4 Complete. Analysis done.")

def run_plan(db, config):
    librarian = Librarian(db, config)
    librarian.generate_organization_plan()
    print("Phase 5 Complete. Plan generated.")

def perform_audit(db, logger) -> bool:
    logger.info("--- PRE-FLIGHT SANITY CHECK ---")
    with db.get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM media_files").fetchone()[0]
        keeps = conn.execute("SELECT COUNT(*) FROM media_files WHERE disposition='KEEP'").fetchone()[0]
        deletes = conn.execute("SELECT COUNT(*) FROM media_files WHERE disposition='DELETE'").fetchone()[0]
        unhandled = conn.execute("SELECT COUNT(*) FROM media_files WHERE disposition IS NULL").fetchone()[0]
    
    logger.info(f"Total Files: {total}")
    logger.info(f"To Keep:     {keeps}")
    logger.info(f"To Trash:    {deletes}")
    
    if unhandled > 0:
        logger.error(f"AUDIT FAIL: {unhandled} files have no disposition!")
        return False
        
    if (keeps + deletes) != total:
        logger.error("AUDIT FAIL: Keep + Trash does not equal Total!")
        return False
        
    logger.info("AUDIT PASS: All files accounted for.")
    return True

def run_exec(db, config, dry_run):
    executioner = Executioner(db, config, dry_run=dry_run)
    executioner.execute()
    print("Phase 6 Complete.")

if __name__ == "__main__":
    main()