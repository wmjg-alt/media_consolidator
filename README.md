# Media Consolidator

A high-performance, stateless Python application designed to centralize, deduplicate, and organize massive photo and video libraries. 

It solves the "Digital Hoarding" problem: having terabytes of data scattered across old hard drives, USB sticks, and nested backup folders, with no idea what is a duplicate and what is original.

## Tech Stack

*   **Core:** Python 3.11+
*   **Database:** SQLite (Ephemeral state management + Persistent Sidecar Cache)
*   **Hashing:** `xxhash` (Non-cryptographic, high-speed collision detection)
*   **Imaging:** Pillow (PIL) for metadata extraction
*   **System API:** `ctypes` / Win32 API for low-level timestamp manipulation
*   **Testing:** Pytest

## Architecture

The system operates in a strict, modular pipeline. It is **stateless**, meaning the primary index is wiped and rebuilt on every run to ensure the "plan" always reflects the current reality of the disk.

1.  **Crawler:** Scans source directories using `os.scandir` (fast IO).
    *   **Smart Filtering:** Automatically detects and skips Windows System folders (via `FILE_ATTRIBUTE_SYSTEM`), Junction points/Symlinks (to prevent infinite loops), and known OS trash directories (`$RECYCLE.BIN`, `System Volume Information`).
    *   **Prefix Blocking:** Ignores common non-media roots like `Program Files`, `AppData`, and `Windows`.
2.  **Hasher (The Funnel):** Implements a 4-stage deduplication funnel to minimize IO:
    *   *Stage 1:* File Size Check (Instant).
    *   *Stage 2:* 4KB Header/Footer Partial Hash (Fast).
    *   *Stage 3:* **Persistent Sidecar Cache** check (Skips reading files processed in previous runs).
    *   *Stage 4:* Full `xxHash` (Only for new, high-probability collisions).
3.  **Analyzer (The Judge):** Groups duplicates and determines the "Winner" based on:
    *   Metadata Score (EXIF presence).
    *   Age (Oldest effective date wins).
    *   Filename cleanliness (Penalizes "Copy of..." or "IMG (1)").
4.  **Librarian:** Calculates the target directory structure (`YYYY/YYYY-MM`) and handles renaming templates (`{date}_{name}_from_{folder}`). It is idempotent—it detects files already in the target structure and refuses to double-rename them.
5.  **Executioner:** Performs physical moves and generates **Trace Receipts** (`image_trace.txt`) in source folders so context is never lost. It utilizes the Win32 API to enforce creation dates on the file destination.

## Challenges & Solutions

### 1. The "Windows Filesystem Minefield"
*   **Problem:** Scanning an entire drive (`C:\` or `D:\`) usually crashes crawlers because of infinite Symlink loops, locked System directories, and hundreds of thousands of irrelevant icon files in `AppData`.
*   **Solution:** The Crawler implements low-level checks for `stat.FILE_ATTRIBUTE_SYSTEM` and strictly ignores Symlinks/Junctions. It also utilizes a built-in blocklist for `ProgramFiles`, `Windows`, and `node_modules` to ensure it only indexes user content.

### 2. The "Copy Reset" (Metadata Loss)
*   **Problem:** When a file is moved across volumes (e.g., USB to Hard Drive), Windows resets the "Creation Date" to the current time. This destroys the history of the file.
*   **Solution:** The Executioner uses Python's `ctypes` interface to call the **Win32 API** (`kernel32.SetFileTime`). After moving a file, we force its Creation Date to match the oldest known timestamp from the source, effectively "backdating" the new copy to match the original.

### 3. The "Recursion Curse"
*   **Problem:** If the Target Directory was included in the scan scope, the system treated organized files as "new" imports, appending `_from_2024-01` to filenames repeatedly.
*   **Solution:** The Librarian now detects if a file resides within the `Target Root`. If so, it updates the date prefix if necessary but strictly forbids appending source folder metadata.

### 4. The "1979 Epoch" Bug
*   **Problem:** Some files showed a creation date of Dec 31, 1979. This happens when a filesystem returns `0` or `None` for a timestamp, which translates to the 1980 DOS Epoch (adjusted for US timezones).
*   **Solution:** The Analyzer implements a **Sanity Threshold**. Timestamps older than Jan 2, 1980, are treated as errors and ignored in favor of the Modified Date.

### 5. Context Loss
*   **Problem:** Moving a photo from a folder named "2018 Japan Trip" to "2018/2018-05" destroys the context of the trip.
*   **Solution:** 
    1.  **Filename Templates:** Files are renamed `2018-05-20_IMG_001_from_2018_Japan_Trip.jpg`.
    2.  **Trace Receipts:** A text file is left in the source folder logging exactly where every file went, including duplicates that were consolidated. A receipt that you can trace for recovery.

## Setup

1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Configuration:**
    *   Rename `config/settings.example.yaml` to `config/settings.yaml`.
    *   Edit `config/settings.yaml` with your actual source/target paths.
3.  Run:
    ```bash
    # Dry Run (Safe)
    python main.py all
    
    # Live Run
    python main.py all --live
    ```

## Maintenance Tools
The project includes utility scripts for FIXING specific edge cases:

*   **`quick_trace_fix.py`**: Reads the last run's database and retrospectively writes `image_trace.txt` receipts to source folders.
*   **`quick_fix_filenames.py`**: Scans the organized library for recursive naming artifacts (e.g., `_from_2024-01_from_2024-01`) and cleans them.
*   **`fix_1979.py`**: Scans for files incorrectly dated to the DOS Epoch (1979/1980) and repairs them using their Modified Date.
*   **`quickfix_metadata.py`**: Applies natural "Jitter" to artificial timestamps (e.g., exact Midnight) to make file sorting appear more natural.