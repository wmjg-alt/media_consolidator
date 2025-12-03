"""Media consolidation and deduplication orchestrator."""

import argparse
import logging
import os
from typing import Any

from src.utils import load_config, setup_logger, normalize_path
from src.db import DatabaseManager
from src.crawler import FileCrawler
from src.hasher import Fingerprinter
from src.analyzer import Analyzer
from src.librarian import Librarian
from src.executioner import Executioner

CONFIG_PATH = "config/settings.yaml"


def main() -> None:
    """Execute media consolidation workflow based on command-line arguments."""
    logger = logging.getLogger("MediaConsolidator.Main")
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
    
    if not os.path.exists(CONFIG_PATH):
        print("ERROR: config/settings.yaml not found.")
        return

    config = load_config(CONFIG_PATH) 
    logger = setup_logger(config)
    
    db_path = config['app']['db_name']

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            logger.info("Previous database cleared for fresh run.")
        except OSError as e:
            logger.error(f"Could not clear database: {e}")
            return

    db = DatabaseManager(db_path)
    db.initialize_schema()
    
    scan_roots: list[str] = []
    
    target_root = config['organization']['target_root']
    if not os.path.exists(target_root):
        try:
            os.makedirs(target_root)
            logger.info(f"Created Target Directory: {target_root}")
        except OSError:
            pass
    scan_roots.append(target_root)
    
    if hasattr(args, 'roots') and args.roots:
        scan_roots.extend([normalize_path(p) for p in args.roots])
    else:
        cfg_sources = config['organization'].get('source_dirs', [])
        scan_roots.extend(cfg_sources)
        
    scan_roots = list(set(scan_roots))

    logger.info("=== Media Consolidator Started ===")
    logger.info(f"Target Root: {target_root}")
    logger.info(f"Scan Scope: {scan_roots}")

    if args.command == "all":
        run_scan(db, config, scan_roots, logger)
        run_hash(db, config, logger)
        run_analyze(db, config, logger)
        run_plan(db, config, logger)
        
        if perform_audit(db, logger):
            run_exec(db, config, not args.live, logger)
        else:
            logger.error("AUDIT FAILED. Aborting.")
            
    elif args.command == "scan":
        run_scan(db, config, scan_roots, logger)
    elif args.command == "hash":
        run_hash(db, config, logger)
    elif args.command == "analyze":
        run_analyze(db, config, logger)
    elif args.command == "plan":
        run_plan(db, config, logger)
    elif args.command == "execute":
        run_exec(db, config, not args.live, logger)
    else:
        parser.print_help()


def run_scan(
    db: DatabaseManager,
    config: dict[str, Any],
    roots: list[str],
    logger: logging.Logger,
) -> None:
    """Scan specified root directories and index media files.
    
    Args:
        db: Database manager instance.
        config: Configuration dictionary.
        roots: List of root directories to scan.
        logger: Logger instance.
    """
    crawler = FileCrawler(db, config)
    count = crawler.scan_roots(roots)
    logger.info(f"File scan complete. Indexed {count} files.")


def run_hash(
    db: DatabaseManager,
    config: dict[str, Any],
    logger: logging.Logger,
) -> None:
    """Generate fingerprints for indexed files.
    
    Args:
        db: Database manager instance.
        config: Configuration dictionary.
        logger: Logger instance.
    """
    hasher = Fingerprinter(db, config)
    hasher.process_database()
    logger.info("Fingerprinting complete.")


def run_analyze(
    db: DatabaseManager,
    config: dict[str, Any],
    logger: logging.Logger,
) -> None:
    """Analyze file metadata and identify duplicates.
    
    Args:
        db: Database manager instance.
        config: Configuration dictionary.
        logger: Logger instance.
    """
    analyzer = Analyzer(db, config)
    analyzer.process_metadata()
    analyzer.process_duplicates()
    logger.info("Analysis complete.")


def run_plan(
    db: DatabaseManager,
    config: dict[str, Any],
    logger: logging.Logger,
) -> None:
    """Generate file organization plan.
    
    Args:
        db: Database manager instance.
        config: Configuration dictionary.
        logger: Logger instance.
    """
    librarian = Librarian(db, config)
    librarian.generate_organization_plan()
    logger.info("Organization plan generated.")


def perform_audit(db: DatabaseManager, logger: logging.Logger) -> bool:
    """Verify that all files have valid dispositions before execution.
    
    Args:
        db: Database manager instance.
        logger: Logger instance.
        
    Returns:
        True if audit passes, False otherwise.
    """
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


def run_exec(
    db: DatabaseManager,
    config: dict[str, Any],
    dry_run: bool,
    logger: logging.Logger,
) -> None:
    """Execute file organization plan.
    
    Args:
        db: Database manager instance.
        config: Configuration dictionary.
        dry_run: If True, simulate execution without moving files.
        logger: Logger instance.
    """
    executioner = Executioner(db, config, dry_run=dry_run)
    executioner.execute()
    logger.info("Execution complete.")

if __name__ == "__main__":
    main()