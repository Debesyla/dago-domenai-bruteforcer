# Domain DAS Scanner — Implementation Plan v2

> Goal: a **simple**, robust scanner that reads a large **text file** of `.lt` domains (60M+), queries the `.lt` DAS service for each domain, and writes results to **both** a SQLite DB and **separate plain `.txt` files** (one file per DAS status). Optimized for large-scale scanning.

---

## Quick summary (requirements)

1. **No generation** — input comes from an existing `assets/input.txt`.
2. **TXT input** — one full domain per line (must include `.lt`).
3. **SQL + TXT output** — every checked domain stored in SQLite with status, and buffered writes to `assets/output/<status>.txt`.
4. **Separate TXT files & DB column for every DAS status** — DB `status` column contains whatever DAS returns (one of official statuses).
5. **Keep it SIMPLE** — minimal dependencies, clear flow, safe to interrupt and resume.
6. **Scale to 60M domains** — batched DB writes, buffered file writes, progress logging.

---

## File structure

```
project-root/
├── assets/
│   ├── input.txt               # required: one domain per line, e.g. example.lt
│   └── output/                 # output directory (created by app)
│       ├── available.txt
│       ├── registered.txt
│       ├── blocked.txt
│       └── ...                 # one file per DAS status seen
├── src/
│   └── das_scanner.py          # main app (single script)
├── scan_state.db               # created by app (SQLite)
└── README.md
```

---

## DAS protocol notes (exact behavior)

* Host: `das.domreg.lt`
* Port: `4343` (TCP)
* Query format: `get 1.0 domain.lt\n`
* Response contains lines; parse the `Status:` line. Example:

  ```
  % .lt registry DAS service
  Domain: domain.lt
  Status: available
  ```
* Valid/expected statuses (canonical list):

  ```
  available
  registered
  blocked
  reserved
  restrictedDisposal
  restrictedRights
  stopped
  pendingCreate
  pendingDelete
  pendingRelease
  outOfService
  ```

  *Implementation note:* treat any unknown/missing `Status:` as `unknown`.

---

## SQLite schema (optimized for bulk operations)

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS domains (
  domain TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'pending',
  last_checked REAL,
  in_progress INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_status ON domains(status);
CREATE INDEX IF NOT EXISTS idx_pending ON domains(status, in_progress) WHERE status='pending';
```

* `domain` is primary key, **normalized to lowercase**.
* `status` stores the DAS status string.
* `in_progress` prevents concurrent processing (0 or 1).
* `last_checked` stores unix timestamp.
* Added compound index on `(status, in_progress)` for faster pending queries.

---

## Behavior & flow (optimized for 60M domains)

### 1. **Import phase**
* Read `assets/input.txt` in chunks
* Batch insert 5000 domains at a time with `INSERT OR IGNORE`
* Log progress every 100K rows: `"Imported 500000/60000000 domains (0.83%)"`

### 2. **Worker loop** (async tasks)
* Each worker:
  1. Requests batch of 15 domains from picker queue
  2. For each domain:
     - Acquire token from rate limiter
     - Query DAS via TCP
     - Push result to writer queue: `(domain, status, timestamp)`
  3. Repeat

### 3. **Picker thread** (dedicated, synchronous)
* Runs in separate thread with own DB connection
* Workers request batches via `asyncio.Queue`
* Atomically picks pending domains in batches:
  ```sql
  BEGIN IMMEDIATE;
  SELECT domain FROM domains 
  WHERE status='pending' AND in_progress=0 
  LIMIT 15;
  UPDATE domains SET in_progress=1 WHERE domain IN (...);
  COMMIT;
  ```
* Feeds batches to worker queue

### 4. **Writer thread** (dedicated, synchronous)
* Runs in separate thread with own DB connection
* Consumes results from writer queue
* Batches 1000 results before committing:
  ```sql
  BEGIN IMMEDIATE;
  UPDATE domains 
  SET status=?, last_checked=?, in_progress=0 
  WHERE domain=?;
  -- repeat 1000 times
  COMMIT;
  ```
* Also maintains per-status buffers (10K domains each)
* Flushes buffers to `assets/output/<status>.txt` when full

### 5. **Progress logging**
* Every 10K completed domains, log:
  ```
  Processed 50000/60000000 (0.08%) | 28.5 req/sec | ETA: 22.3 days
  Status: available=12000 registered=35000 blocked=3000
  ```

### 6. **Resume capability**
* On restart, reset any stuck `in_progress=1` back to pending:
  ```sql
  UPDATE domains SET in_progress=0 WHERE in_progress=1;
  ```
* Continue from where it left off

---

## Concurrency architecture

```
┌─────────────┐
│   Main      │
│  (asyncio)  │
└──────┬──────┘
       │
       ├─► Picker Thread ──► picker_queue (batches of 50 domains)
       │      (sync DB)           │
       │                          ▼
       ├─► Worker Tasks (40x) ──► DAS queries ──► writer_queue (results)
       │      (async TCP)                              │
       │                                               ▼
       └─► Writer Thread ──────────────────► Batched DB updates (1000/commit)
              (sync DB)                      Buffered file writes (10K/flush)
```

* **40 async workers**
* **2 dedicated threads** (picker + writer) with own DB connections
* **Queues** for coordination (asyncio.Queue for async ↔ thread communication)

---

## Rate limiting (token bucket)

* Refills **30 tokens/sec** (adjustable)
* Workers `await bucket.get_token()` before each DAS query
* Simple asyncio implementation with periodic refill task

---

## Text output (buffered writes)

* **Per-status buffers**: dict of lists, max 10K domains per status
* When buffer reaches 10K:
  ```python
  with open(f'assets/output/{status}.txt', 'a') as f:
      f.writelines(f'{domain}\n' for domain in buffer)
  buffer.clear()
  ```
* On shutdown, flush all remaining buffers
* Plain format: one domain per line, no metadata

---

## Implementation tasks

1. **Project skeleton** + directory creation
2. **DB helpers**:
   - `init_db(conn)` — schema + indexes + PRAGMAs
   - `import_domains(conn, input_path)` — chunked batch insert with progress
   - `reset_in_progress(conn)` — cleanup on startup
3. **Picker thread**:
   - Synchronous thread with own DB connection
   - `pick_batch(conn, size=15)` — atomic batch pick
   - Feeds `picker_queue` when workers request batches
4. **Writer thread**:
   - Synchronous thread with own DB connection
   - Consumes from `writer_queue`
   - Batches 1000 updates per transaction
   - Manages file buffers (10K per status)
   - `flush_buffers()` on shutdown
5. **DAS checker**:
   - `async das_check(domain)` — TCP query + response parsing
   - Returns status string or `'unknown'`
   - Timeout: 6 seconds
   - Retry: 1 additional attempt with 2 second backoff
6. **Rate limiter**:
   - `TokenBucket` class with `async get_token()`
   - Background refill task
7. **Worker tasks**:
   - Request batch from picker
   - Process each domain: token → DAS → writer queue
   - Loop until no more pending domains
8. **Progress tracker**:
   - Separate async task
   - Queries DB every 30 seconds for stats
   - Calculates rate and ETA
   - Logs formatted output
9. **Main**:
   - Init DB, import domains
   - Start picker thread, writer thread
   - Spawn 40 worker tasks + progress task
   - Handle Ctrl+C gracefully (flush buffers, close threads)
10. **Signal handling**:
    - Set shutdown flag on SIGINT
    - Workers finish current domain
    - Flush all buffers
    - Clean exit

---

## Hardcoded settings

```python
# Paths
DB_PATH = "scan_state.db"
INPUT_PATH = "assets/input.txt"
OUTPUT_DIR = "assets/output"

# Performance
RATE = 30                    # requests/sec
CONCURRENCY = 40             # worker tasks
CHECK_TIMEOUT = 6            # seconds
MAX_RETRIES = 1              # additional attempt

# Batching
BATCH_IMPORT_SIZE = 5000     # domains per DB insert
PICK_BATCH_SIZE = 15         # domains per picker batch
WRITE_BATCH_SIZE = 1000      # updates per DB commit
FILE_BUFFER_SIZE = 10000     # domains per file flush

# Logging
PROGRESS_INTERVAL = 30       # seconds between progress logs
IMPORT_LOG_INTERVAL = 100000 # domains between import logs
```

---

## Expected performance

* **Rate**: 30 domains/sec (DAS limit)
* **Total time**: 60M ÷ 30 = 2M seconds ≈ **23 days**
* **DB size**: ~6GB final
* **Text files**: ~1-2GB per major status (available/registered)
* **Memory**: ~200MB (40 workers + buffers)
* **CPU**: Low (mostly I/O waiting)

---

## SQL queries (examples)

```sql
-- Progress check
SELECT status, COUNT(*) FROM domains GROUP BY status;

-- Success rate
SELECT 
  SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as remaining,
  SUM(CASE WHEN status!='pending' THEN 1 ELSE 0 END) as completed,
  COUNT(*) as total
FROM domains;

-- Re-queue errors
UPDATE domains SET status='pending', in_progress=0 
WHERE status='unknown';

-- Export specific status (alternative to text files)
SELECT domain FROM domains WHERE status='registered';
```

---

## Example run

```bash
# Initial run
python3 src/das_scanner.py

# Output:
# Importing domains from assets/input.txt...
# Imported 100000/60000000 domains (0.17%)
# Imported 200000/60000000 domains (0.33%)
# ...
# Import complete: 60000000 domains
# Starting scanner: 40 workers, 30 req/sec
# Processed 10000/60000000 (0.02%) | 29.8 req/sec | ETA: 23.2 days
# Status: available=2500 registered=6800 blocked=700
# ...
# ^C
# Shutting down gracefully...
# Flushing buffers...
# Done.

# Resume
python3 src/das_scanner.py

# Output:
# Resetting 0 stuck domains...
# Starting scanner: 40 workers, 30 req/sec
# Processed 10000/60000000 (0.02%) | 30.1 req/sec | ETA: 23.1 days
# ...
```

---

## Edge cases & safety

* **Interrupted during import**: Safe, `INSERT OR IGNORE` is idempotent
* **Interrupted during scan**: Reset `in_progress` on restart, resume
* **Multiple instances**: Don't. SQLite locks will conflict. Use single instance.
* **Disk space**: Monitor. ~8GB total (6GB DB + 2GB text files)
* **DAS rate limits**: If you get errors, reduce `RATE` constant
* **WAL checkpoint**: Runs automatically, but can manual: `PRAGMA wal_checkpoint(TRUNCATE);`

---

## Minimal dependencies

* Python 3.10+
* Built-in only: `sqlite3`, `asyncio`, `threading`, `queue`, `time`, `os`, `pathlib`
* **Zero external packages**

---

## Dependencies

* Python 3.10+
* Built-in only: `sqlite3`, `asyncio`, `threading`, `queue`, `time`, `os`, `pathlib`
* Zero external packages required