# Application Development and Database Index Structure Implementation

## Folder Structure

```text
College_Social_Media_DB/
|-- .gitignore
|-- README.md
|-- Module_A/
|   |-- requirements.txt
|   |-- report.ipynb
|   `-- database/
|       |-- __init__.py
|       |-- bplustree.py
|       |-- bruteforce.py
|       |-- table.py
|       |-- db_manager.py
|       |-- transaction_manager.py
|       |-- sql_sanity.py
|       |-- performance.py
|       |-- run_performance_tests.py
|       |-- test_acid_validation.py
|       |-- test_acid_multirelation.py
|       |-- visualizations_generator.py
|       |-- performance_results_jpgs/
|       `-- visualizations/
`-- Module_B/
    |-- requirements.txt
    |-- app/
    |   |-- main.py
    |   |-- database.py
    |   |-- test_db.py
    |   `-- static/
    |       |-- login.html
    |       |-- portfolio.html
    |       |-- create-post.html
    |       |-- posts.html
    |       |-- app.js
    |       `-- styles.css
    |-- sql/
    |   |-- schema.sql
    |   `-- sample_data.sql
```

## Setup

Install dependencies from project root:

```bash
python -m pip install -r Module_A/requirements.txt
```

If you use Conda, run with your Conda Python interpreter instead of `python3` from Windows app aliases.

## Run Performance Tests

From project root:

```bash
python Module_A/database/run_performance_tests.py
```

Alternative (from Module_A/database folder):

```bash
python run_performance_tests.py
```

This runs performance testing for different random key set sizes and generates:

- Performance charts in `Module_A/database/performance_results_jpgs/`
- Benchmark JSON in `Module_A/database/visualizations/benchmark_results.json`

## What Is Implemented

- SubTask 1: B+ Tree node/tree classes, insert, delete, search, range query, split/merge
- SubTask 2: PerformanceAnalyzer for timing and memory comparison
- SubTask 3: Graphviz visualization for tree structure and leaf links
- SubTask 4: Performance testing across different random key set sizes with Matplotlib plots
- Additional Layer: In-memory table/database manager API built on top of B+ Tree index
- Assignment 3 Module A Layer: Multi-relation ACID transactions, failure recovery, and SQL sanity-check validation

## Module A ACID Validation (Assignment 3)

### Design Summary

- B+ Tree is the primary storage for each relation.
- `DBManager` holds multiple relations, each backed by a separate B+ Tree.
- `TransactionManager` provides `BEGIN`, `COMMIT`, and `ROLLBACK` across multiple relations.
- Isolation is implemented as serialized execution (single active write transaction).
- Durability and restart recovery use committed database snapshots.
- SQL (`sqlite3`) is used as a reference/sanity-check store to compare final state with B+ Tree state.

### ACID Components

- Multi-relation transaction coordinator: `Module_A/database/transaction_manager.py`
- Database-level snapshot import/export and persistence: `Module_A/database/db_manager.py`
- Table-level state export/restore helpers: `Module_A/database/table.py`
- SQL reference comparator: `Module_A/database/sql_sanity.py`
- Single-table ACID tests: `Module_A/database/test_acid_validation.py`
- Multi-relation ACID tests (users/products/orders): `Module_A/database/test_acid_multirelation.py`

### Run Module A ACID Tests

From project root:

```bash
python -m unittest Module_A.database.test_acid_validation Module_A.database.test_acid_multirelation -v
```

## B+ Tree Implementation (SubTask 1)

- Implemented in: Module_A/database/bplustree.py
- Main classes: BPlusTreeNode, BPlusTree
- Main operations: insert(), delete(), search(), range_query()
- Node balancing: automatic split/merge handled internally during insert/delete

## Performance Analysis (SubTask 2)

- Implemented in: Module_A/database/performance.py
- Main class: PerformanceAnalyzer
- Benchmarks: insert, search, delete, range_query, mixed workload
- Memory measurement: tracemalloc peak memory tracking
- Comparison target: Module_A/database/bruteforce.py (BruteForceDB)

## Graphviz Implementation (SubTask 3)

- Implemented in: Module_A/database/bplustree.py
- Main method: BPlusTree.visualize_tree()
- Helper methods: \_add_nodes() and \_add_edges()
- Current output folder for visualization files: Module_A/database/visualizations/
- Existing generated files: Module_A/database/visualizations/bplustree_demo.png, Module_A/database/visualizations/bplustree_demo_large.png

## Performance Testing Implementation (SubTask 4)

- Implemented in: Module_A/database/visualizations_generator.py
- Main function: run_full_performance_analysis()
- Benchmarks used from: Module_A/database/performance.py (PerformanceAnalyzer)
- Run file: Module_A/database/run_performance_tests.py
- Output folders for generated artifacts:
  - Module_A/database/performance_results_jpgs/
  - Module_A/database/visualizations/
- Generated files include:
  - JPG charts: performance_insert.jpg, performance_search.jpg, performance_delete.jpg, performance_range_query.jpg, performance_random_workload.jpg, performance_memory_usage.jpg, performance_combined_comparison.jpg, performance_speedup_ratio.jpg
  - Benchmark data: benchmark_results.json

## Table and DB Manager Layer (Additional)

- Implemented in:
  - Module_A/database/table.py
  - Module_A/database/db_manager.py
- Purpose:
  - Provide a simple DBMS-style API over the B+ Tree index.
  - Manage multiple in-memory tables cleanly.

### Features

- Table API:
  - insert(row), upsert(row), get(key), update(key, updates), delete(key)
  - range_query(start_key, end_key), all_rows(), count(), truncate()
  - select(predicate=None, columns=None, limit=None)
  - aggregate(operation, column=None, predicate=None) for count/sum/min/max/avg
- DBManager API:
  - create_table(name, ...), get_table(name), drop_table(name)
  - list_tables(), has_table(name)

### Quick Usage

```python
from Module_A.database import DBManager

db = DBManager()
members = db.create_table(
    name="members",
    primary_key="id",
    schema=["id", "name", "dept"],
    bplustree_order=4,
)

members.insert({"id": 1, "name": "Alice", "dept": "CSE"})
members.upsert({"id": 2, "name": "Bob", "dept": "ECE"})
members.update(1, {"dept": "AIML"})

print(members.get(1))
print(members.range_query(1, 10))
print(db.list_tables())
```

### Notes

- Primary key type is integer (`int`) to match B+ Tree indexing.
- Table-level and database-level JSON snapshot persistence is available for recovery testing.


## Module B (Assignment 3): Concurrency, Failure Simulation, and Stress Testing

This module contains the FastAPI + MySQL application and Assignment 3 validation workflow for concurrency, race handling, failure simulation, and stress testing.

### What Is In Module B

```text
Module_B/
|-- requirements.txt
|-- report.ipynb
|-- .gitignore
|-- app/
|   |-- main.py
|   |-- database.py
|   |-- test_db.py
|   `-- static/
|-- sql/
|   |-- schema.sql
|   |-- sample_data.sql
|   `-- sample_passwords.txt
|-- performance/
|   |-- run_module_b_concurrency_stress.py
|   |-- index_benchmark_results.json
|   `-- module_b_concurrency_report.json
`-- logs/
    `-- .gitkeep
```

### Notebook-First Testing (Single Source of Truth)

All Assignment 3 testing for Module B is run from:

- `Module_B/report.ipynb`

The notebook contains an end-to-end flow:

1. environment/path setup
2. test configuration
3. preflight checks (runner + API + auth + feed)
4. expanded workload matrix execution
5. pass/fail summary and assertions
6. artifact export

### Current Notebook State (Latest)

- The notebook has 10 cells total.
- Code cells executed successfully in order.
- Preflight passed (`ready_for_matrix = true`) in the latest run.
- Matrix profiles completed: `smoke`, `medium`, `high`.

Latest profile summary from `Module_B/performance/module_b_notebook_test_matrix_results.json`:

| Profile | Overall | Race | Failure | Stress | Throughput (req/s) | Stress P95 (ms) |
|---|---|---|---|---|---:|---:|
| smoke | PASS | PASS | PASS | PASS | 239.499 | 370.633 |
| medium | PASS | PASS | PASS | PASS | 221.402 | 583.748 |
| high | PASS | PASS | PASS | PASS | 178.665 | 1062.457 |

### Setup

Run from project root (`DB_A3`).

#### 1) Install dependencies

```powershell
python -m pip install -r Module_B/requirements.txt
```

#### 2) Set environment variables

```powershell
$env:DB_HOST = "localhost"
$env:DB_USER = "root"
$env:DB_PASSWORD = "<your-mysql-password>"
$env:DB_NAME = "college_social_media"
$env:JWT_SECRET_KEY = [Convert]::ToBase64String((1..48 | ForEach-Object { Get-Random -Minimum 0 -Maximum 256 }))
```

#### 3) Load SQL schema and sample data

```powershell
mysql -u "$env:DB_USER" -p"$env:DB_PASSWORD" -e "SOURCE Module_B/sql/schema.sql; SOURCE Module_B/sql/sample_data.sql;"
```

### Running Module B

#### Terminal A: Start API

```powershell
Set-Location Module_B/app
python -m uvicorn main:app --host 127.0.0.1 --port 8001
```

#### Terminal B: Run notebook

Open `Module_B/report.ipynb` and run cells top-to-bottom.

### Expanded Testing Range

The notebook uses a profile matrix in its configuration cell:

- smoke: race 200, failure 120, stress 1000
- medium: race 500, failure 300, stress 3000
- high: race 1000, failure 600, stress 5000
- optional extreme profile via `RUN_EXTREME = True`

### CLI Alternative (Optional)

If needed, run the same logic directly from script:

```powershell
python Module_B/performance/run_module_b_concurrency_stress.py --base-url http://127.0.0.1:8001 --usernames "rahul.sharma@iitgn.ac.in,priya.patel@iitgn.ac.in,ananya.singh@iitgn.ac.in,neha.desai@iitgn.ac.in,aditya.verma@iitgn.ac.in" --password password123 --post-id 1 --race-requests 200 --failure-requests 120 --stress-requests 1000
```

### What Is Validated

- Atomicity of critical multi-step write endpoints
- Race safety for concurrent follow operations
- Failure behavior under mixed valid/invalid writes
- Counter consistency (`Post.LikeCount`, `Post.CommentCount`)
- Stress behavior (success rate, throughput, latency percentiles)

### Cleanup Policy Applied

To keep Module B clean and avoid generated-noise commits:

- Python cache folders are removed from `app/` and `performance/`
- `logs/audit.log` is treated as generated runtime output
- Module-specific ignores are in `Module_B/.gitignore`:
  - `app/__pycache__/`
  - `performance/__pycache__/`
  - `logs/*.log`
  - `performance/module_b_concurrency_report*.json`
  - `performance/module_b_notebook_test_matrix_results.json`

If you want to preserve a run permanently, copy or rename the artifact before commit.
