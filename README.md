# Media Consolidator

A high-performance, stateless Python application designed to centralize, deduplicate, and organize massive photo and video libraries. 

It is designed to solve the "Digital Hoarding" problem: having terabytes of data scattered across old hard drives, USB sticks, and nested backup folders, with no idea what is a duplicate and what is original.

## Tech Stack

*   **Core:** Python 3.11+
*   **Database:** SQLite (Ephemeral state management)
*   **Hashing:** `xxhash` (Non-cryptographic, high-speed collision detection)
*   **Imaging:** Pillow (PIL) for metadata extraction
*   **Testing:** Pytest

## Architecture

The system operates in a strict, modular pipeline. It is **stateless**, meaning the database is wiped and rebuilt on every run to ensure the "plan" always reflects the current reality of the disk.

1.  **Crawler:** Scans source directories using `os.scandir` (fast IO). Normalizes Windows paths and filters system/hidden folders.
2.  **Hasher (The Funnel):** Implements a 3-stage deduplication funnel to minimize IO:
    *   *Stage 1:* File Size Check (Instant).
    *   *Stage 2:* 4KB Header/Footer Partial Hash (Fast).
    *   *Stage 3:* Full `xxHash` (Only for high-probability collisions).
3.  **Analyzer (The Judge):** Groups duplicates and determines the "Winner" based on:
    *   Metadata Score (EXIF presence).
    *   Age (Oldest effective date wins).
    *   Filename cleanliness (Penalizes "Copy of..." or "IMG (1)").
4.  **Librarian:** Calculates the target directory structure (`YYYY/YYYY-MM`) and handles renaming templates (`{date}_{name}_from_{folder}`). It is idempotent—it detects files already in the target structure and refuses to double-rename them.
5.  **Executioner:** Performs physical moves using `shutil`. Generates **Trace Receipts** (`image_trace.txt`) in source folders so context is never lost, even when files are moved.

## Challenges & Solutions

### 1. The "Recursion Curse"
*   **Problem:** If the Target Directory was included in the scan scope, the system treated organized files as "new" imports, appending `_from_2024-01` to filenames repeatedly.
*   **Solution:** The Librarian now detects if a file resides within the `Target Root`. If so, it updates the date prefix if necessary but strictly forbids appending source folder metadata.

### 2. Context Loss
*   **Problem:** Moving a photo from a folder named "2018 Japan Trip" to "2018/2018-05" destroys the context of the trip.
*   **Solution:** 
    1.  **Filename Templates:** Files are renamed `2018-05-20_IMG_001_from_2018_Japan_Trip.jpg`.
    2.  **Trace Receipts:** A text file is left in the source folder logging exactly where every file went, including duplicates that were consolidated.

### 3. Metadata Trust
*   **Problem:** File timestamps are unreliable. Copying a file resets its "Created Date" on Windows.
*   **Solution:** The Analyzer uses a logic of `min(st_birthtime, st_mtime)`. We trust the *oldest* timestamp found on the file, regardless of whether it claims to be "Created" or "Modified."

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
The project includes utility scripts for specific edge cases we ran into along the way:

*   **`quick_fix.py`**: Reads the last run's database and retrospectively writes `image_trace.txt` receipts to source folders. Use this if you ran a migration but forgot to enable receipts.
*   **`fix_filenames.py`**: Scans the organized library for recursive naming artifacts (e.g., `_from_2024-01_from_2024-01`). It strips these artifacts to clean up the filenames.