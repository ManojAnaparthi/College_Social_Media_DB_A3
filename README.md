# Application Development and Database Index Structure Implementation

## Folder Structure

```text
College_Social_Media_DB/
	.gitignore
	README.md
	Module_A/
		requirements.txt
		run_performance_tests.py
		database/
			__init__.py
			bplustree.py
			bruteforce.py
			performance.py
			visualizations_generator.py
			visualizations/
```

## Setup

Install dependencies from project root:

```bash
.venv\Scripts\python.exe -m pip install -r Module_A/requirements.txt
```

## Run Performance Tests

From project root:

```bash
.venv\Scripts\python.exe Module_A/run_performance_tests.py
```

This runs performance testing for different random key set sizes and generates plots in:

`Module_A/database/visualizations/`

## What Is Implemented

- SubTask 1: B+ Tree node/tree classes, insert, delete, search, range query, split/merge
- SubTask 2: PerformanceAnalyzer for timing and memory comparison
- SubTask 3: Graphviz visualization for tree structure and leaf links
- SubTask 4: Performance testing across different random key set sizes with Matplotlib plots

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
- Helper methods: _add_nodes() and _add_edges()
- Current output folder for visualization files: Module_A/database/visualizations/
- Existing generated file: Module_A/database/visualizations/bplustree_demo.png

## Performance Testing Implementation (SubTask 4)

- Implemented in: Module_A/database/visualizations_generator.py
- Main function: run_full_performance_analysis()
- Benchmarks used from: Module_A/database/performance.py (PerformanceAnalyzer)
- Run file: Module_A/run_performance_tests.py
- Output folder for generated plots: Module_A/database/visualizations/
- Generated files include: performance_insert.png, performance_search.png, performance_delete.png, performance_range_query.png, performance_memory_usage.png, performance_combined_comparison.png, performance_speedup_ratio.png, benchmark_results.json
