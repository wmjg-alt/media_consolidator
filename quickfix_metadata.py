import os
import re
import sys
import logging
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
from ctypes import windll, wintypes, byref

# --- CONFIG ---
TARGET_ROOT = "C:/OrganizedPhotos"
TRASH_NAME = "_TRASH"
DRY_RUN = False
# --------------

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("MetaFixer")

DATE_PATTERN = re.compile(r'^(\d{4})-(\d{2})-(\d{2})')

def get_windows_handle(path):
    GENERIC_WRITE = 0x40000000
    OPEN_EXISTING = 3
    FILE_ATTRIBUTE_NORMAL = 0x80
    hfile = windll.kernel32.CreateFileW(
        path, GENERIC_WRITE, 0, None, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, None
    )
    if hfile == -1: return None
    return hfile

def set_creation_time(path, timestamp):
    wintime = int((timestamp * 10000000) + 116444736000000000)
    ft = wintypes.FILETIME()
    ft.dwLowDateTime = wintime & 0xFFFFFFFF
    ft.dwHighDateTime = wintime >> 32
    handle = get_windows_handle(path)
    if not handle: return False
    result = windll.kernel32.SetFileTime(handle, byref(ft), None, None)
    windll.kernel32.CloseHandle(handle)
    return result != 0

def is_time_midnight(timestamp):
    dt = datetime.fromtimestamp(timestamp)
    return dt.hour == 0 and dt.minute == 0 and dt.second == 0

def process_file(path_str):
    path = Path(path_str)
    filename = path.name
    
    match = DATE_PATTERN.match(filename)
    if not match: return 
        
    y, m, d = map(int, match.groups())
    
    try:
        # 1. Calculate Jittered Target (Noon +/- 4 hours)
        base_dt = datetime(y, m, d, 12, 0, 0)
        jitter_seconds = random.randint(-14400, 14400)
        final_dt = base_dt + timedelta(seconds=jitter_seconds)
        jittered_ts = final_dt.timestamp()
    except ValueError:
        return 

    try:
        stat = path.stat()
        current_mtime = stat.st_mtime
        current_atime = stat.st_atime
        current_ctime = getattr(stat, 'st_birthtime', stat.st_ctime)
    except OSError:
        return

    # 2. Logic: Determine Effective Target
    # Start with our Jittered Time
    effective_target_ts = jittered_ts
    
    # Check if Modified Time is "Older"
    if current_mtime < jittered_ts:
        # The Modified Time is older than Noon.
        # But is it ARTIFICIAL (Midnight)?
        mtime_is_midnight = is_time_midnight(current_mtime)
        
        # Calculate if it's on the SAME DAY as the filename
        m_dt = datetime.fromtimestamp(current_mtime)
        same_day = (m_dt.year == y and m_dt.month == m and m_dt.day == d)
        
        if same_day and mtime_is_midnight:
            # Case: Modified Time is 00:00:00 on the correct day.
            # This is likely artificial. Ignore it and use our Jittered Time (e.g. 13:45).
            effective_target_ts = jittered_ts
        else:
            # Case: Modified Time is distinct (e.g., previous year, or specific time like 09:00).
            # Trust it as legacy data.
            effective_target_ts = current_mtime

    # 3. Check for Fix
    # If current creation is exactly midnight OR significantly newer than target
    ctime_is_midnight = is_time_midnight(current_ctime)
    file_is_too_new = current_ctime > (effective_target_ts + 5.0)
    
    should_fix = False
    reason = ""

    if ctime_is_midnight:
        should_fix = True
        reason = "MIDNIGHT DETECTED"
    elif file_is_too_new:
        should_fix = True
        reason = "DATE MISMATCH"

    if should_fix:
        t_str = datetime.fromtimestamp(effective_target_ts).strftime('%Y-%m-%d %H:%M:%S')
        c_str = datetime.fromtimestamp(current_ctime).strftime('%Y-%m-%d %H:%M:%S')
        
        # Only log if there is an actual change to report
        if t_str != c_str:
            logger.info(f"FIX [{reason}]: {filename}")
            logger.info(f"  Current: {c_str} -> Target: {t_str}")
            
            if not DRY_RUN:
                if set_creation_time(str(path), effective_target_ts):
                    try:
                        os.utime(path, (current_atime, current_mtime))
                        logger.info("  [SUCCESS] Updated.")
                    except OSError:
                        logger.warning("  [PARTIAL] Failed to restore modified time.")
                else:
                    logger.error("  [FAILED] Win32 API Error.")

def main():
    if not os.path.exists(TARGET_ROOT):
        logger.error(f"Target root not found: {TARGET_ROOT}")
        return

    logger.info(f"Scanning {TARGET_ROOT}...")
    
    count = 0
    for root, dirs, files in os.walk(TARGET_ROOT):
        if TRASH_NAME in dirs:
            dirs.remove(TRASH_NAME)
            
        for file in files:
            full_path = os.path.join(root, file)
            process_file(full_path)
            count += 1
            
    logger.info(f"Scanned {count} files.")

if __name__ == "__main__":
    main()