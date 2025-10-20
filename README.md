# Domain DAS Scanner

A simple, robust scanner for querying the .lt Domain Availability Service (DAS). It processes a large list of .lt domains from a text file, checks their status via the DAS protocol, and stores results in a SQLite database and separate text files per status.

## Features

- Processes 60M+ domains efficiently
- Asynchronous workers with rate limiting (30 req/sec)
- Resume capability after interruptions
- Outputs to SQLite DB and per-status text files
- Zero external dependencies (Python 3.10+ built-ins only)

## Quick Start

1. Prepare `assets/input.txt` with one domain per line (e.g., `example.lt`).
2. Run: `python3 src/das_scanner.py`
3. Results in `scan_state.db` and `assets/output/`.

## Requirements

- Python 3.10+

## Details

See [IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md) for full specifications, architecture, and usage examples.

## License

Do whatever.

## AI notice

This readme.md and most of the code was AI-generated, human reviewed.