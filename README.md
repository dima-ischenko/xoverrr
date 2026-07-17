# xoverrr (pronounced “crossover”)

A tool for cross-database and intra-source data comparison with detailed discrepancy analysis and reporting.

Supported databases: **Oracle**, **PostgreSQL** (+ Greenplum), **ClickHouse**.

---

## Features

- **Four comparison strategies** — row samples, daily counts, custom SQL, and source-only sniff checks
- **Multi-DBMS** — tables and views; extensible via adapters
- **SQLAlchemy engines** — pass any supported source / target / results connection
- **Replication-lag aware** — optionally skip “fresh” rows that may still be catching up
- **Auto metadata** — primary keys and column types from DBMS catalogs (or supply your own PK)
- **Type conversion** — application-side normalization across databases
- **Column filters** — include / exclude lists; mismatched column names skipped automatically
- **Chunked date ranges** — process long periods in N-day windows
- **Reports** — text or JSON, with example mismatched rows
- **Optional persistence** — write run results to a third engine for dashboards / audit
- **Tests** — unit coverage plus Docker-backed integration tests

---

## Quick start

**Sample comparison** (Greenplum/PostgreSQL → Oracle):

```python
from xoverrr import DataQualityComparator, DataReference, COMPARISON_SUCCESS
from sqlalchemy import create_engine
from datetime import date, timedelta

# 1. Connections
source_engine = create_engine('postgresql://user:pass@localhost:5432/source_db')
target_engine = create_engine('oracle+oracledb://user:pass@localhost:1521/target_db')
results_engine = create_engine('postgresql://user:pass@localhost:5432/dq_audit')

# 2. Comparator
comparator = DataQualityComparator(
    source_engine=source_engine,
    target_engine=target_engine,
    timezone='Europe/Athens',
    results_engine=results_engine,  # optional
)

# 3. Tables + date window
source_table = DataReference("employees", schema="hr")
target_table = DataReference("employees", schema="hr")
end_date = date.today()
start_date = end_date - timedelta(days=7)

# 4. Run
status, report, stats, details = comparator.compare_sample(
    source_table=source_table,
    target_table=target_table,
    date_column="hire_date",
    update_column="modified_at",
    date_range=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
    chunk_size_days=30,
    custom_primary_key=["employee_id"],
    exclude_columns=["audit_log", "temp_field"],
    tolerance_percentage=0.5,
    exclude_recent_hours=3,
    max_examples=5,
    persist_result=DataReference("dq_results", "test"),
    comparison_name="employees_daily",
    comparison_tags={"env": "prod", "domain": "hr"},
    report_output_format='text',  # 'json' or 'text'
)

# 5. Result
print(report)
if status == COMPARISON_SUCCESS:
    print("Data quality check passed")
else:
    print("Data quality check failed")
```

Every comparison method returns the same tuple:

| Value | Meaning |
|-------|---------|
| `status` | `COMPARISON_SUCCESS` / `COMPARISON_FAILED` / `COMPARISON_SKIPPED` |
| `report` | Text report or JSON string (`report_output_format`) |
| `stats` | `ComparisonStats` — scores and row counts |
| `details` | `ComparisonDiffDetails` — examples and per-column diffs |

---

## Which method should I use?

| Method | When to use | Needs target DB? |
|--------|-------------|------------------|
| `compare_sample` | Compare row values between two tables/views | Yes |
| `compare_counts` | Fast volume check by day (missing / extra rows) | Yes |
| `compare_custom_query` | Complex joins, renamed columns, custom SQL | Yes |
| `sniff_query` | Source-only rule: “does this data look wrong?” | No |

---

## Comparison methods

### 1. Data sample (`compare_sample`)

Compares row sets and column values over a date range.

```python
status, report, stats, details = comparator.compare_sample(
    source_table=DataReference("table_name", "schema_name"),
    target_table=DataReference("table_name", "schema_name"),
    date_column="created_at",
    update_column="modified_date",
    date_range=("2024-01-01", "2024-12-31"),
    chunk_size_days=30,
    exclude_columns=["audit_timestamp", "internal_id"],
    include_columns=None,
    custom_primary_key=["id", "user_id"],
    tolerance_percentage=1.0,
    exclude_recent_hours=24,
    max_examples=3,
)
```

**Main parameters**

| Parameter | Description |
|-----------|-------------|
| `source_table`, `target_table` | Tables or views to compare |
| `date_column` | Column for date-range filtering |
| `update_column` | Marks “fresh” rows (excluded on both sides) |
| `date_range` | `(start_date, end_date)` as `YYYY-MM-DD` |
| `chunk_size_days` | Optional N-day windows over the range |
| `exclude_columns` / `include_columns` | Blacklist / whitelist of columns |
| `custom_primary_key` | PK columns; auto-detected if omitted |
| `tolerance_percentage` | Fail if `final_diff_score` exceeds this (0–100) |
| `exclude_recent_hours` | Drop rows modified in the last N hours |
| `max_examples` | Cap on discrepancy examples in the report |
| `persist_result` | `False`, `True` (default table), or `DataReference` |
| `comparison_name` / `comparison_tags` | Labels for dashboards |
| `report_output_format` | `'text'` (default) or `'json'` |

If `custom_primary_key` is omitted, the PK is inferred from metadata (must exist on at least one side).

---

### 2. Counts (`compare_counts`)

Daily aggregates — good for large volumes and spotting missing/extra rows.

```python
status, report, stats, details = comparator.compare_counts(
    source_table=DataReference("users", "schema1"),
    target_table=DataReference("users", "schema2"),
    date_column="created_at",
    date_range=("2024-01-01", "2024-12-31"),
    chunk_size_days=30,
    tolerance_percentage=2.0,
    max_examples=5,
)
```

**Main parameters:** `source_table`, `target_table`, `date_column`, `date_range`, `chunk_size_days`, `tolerance_percentage`, `max_examples`, plus the shared `persist_result` / `comparison_name` / `comparison_tags` / `report_output_format` options described above.

---

### 3. Custom query (`compare_custom_query`)

Compare arbitrary SQL on both sides. Primary key is **required**.

```python
status, report, stats, details = comparator.compare_custom_query(
    source_query="""
        SELECT id AS user_id, name AS user_name, created_at AS created_date
        FROM scott.source_table
        WHERE status = :status
    """,
    source_params={'status': 'active'},
    target_query="""
        SELECT user_id, user_name, created_date
        FROM scott.target_table
        WHERE status = :status
    """,
    target_params={'status': 'active'},
    custom_primary_key=["user_id"],
    exclude_columns=["internal_code"],
    tolerance_percentage=0.5,
    max_examples=3,
)
```

**Chunking:** when both `source_params` and `target_params` include `start_date` / `end_date`, set `chunk_size_days` to split the range:

```python
status, report, stats, details = comparator.compare_custom_query(
    source_query="""
        SELECT id, name, created_at
        FROM scott.source_table
        WHERE created_at >= date_trunc('day', cast(:start_date as date))
          AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 day'
    """,
    source_params={'start_date': '2024-01-01', 'end_date': '2024-12-31'},
    target_query="""
        SELECT id, name, created_at
        FROM scott.target_table
        WHERE created_at >= date_trunc('day', cast(:start_date as date))
          AND created_at < date_trunc('day', cast(:end_date as date)) + interval '1 day'
    """,
    target_params={'start_date': '2024-01-01', 'end_date': '2024-12-31'},
    custom_primary_key=["id"],
    chunk_size_days=30,
    tolerance_percentage=0.5,
)
```

To skip recently changed rows in custom SQL, add the same flag used by sample comparison:

```sql
CASE WHEN updated_at > (sysdate - 3/24) THEN 'y' END AS xrecently_changed
```

---

### 4. Sniff query (`sniff_query`)

Source-only check. Mark bad rows with `xsniff_issue` (`y` = failed, `n` = ok).  
No target engine or primary key required:

```python
comparator = DataQualityComparator(
    source_engine=source_engine,
    timezone='UTC',
)
```

**Row-level** — one flag per row:

```python
status, report, stats, details = comparator.sniff_query(
    source_query="""
        SELECT
            order_id,
            amount,
            CASE
                WHEN amount > 0 AND customer_id IS NOT NULL THEN 'n'
                ELSE 'y'
            END AS xsniff_issue
        FROM sales.orders
        WHERE created_at >= :start_date
    """,
    source_params={'start_date': '2024-01-01'},
    tolerance_percentage=1.0,
)
```

**Scalar pass/fail** — a single `xsniff_issue` value:

```python
status, report, stats, details = comparator.sniff_query(
    source_query="""
        SELECT CASE
            WHEN EXISTS (SELECT 1 FROM sales.orders WHERE amount <= 0) THEN 'y'
            ELSE 'n'
        END AS xsniff_issue
    """,
    tolerance_percentage=0.0,
)
```

**Main parameters:** `source_query`, `source_params`, `chunk_size_days` (when params include dates), `tolerance_percentage`, `max_examples`, plus shared persistence / naming / report format options.

Useful `stats` fields:

| Field | Meaning |
|-------|---------|
| `total_source_rows` | Rows checked |
| `total_matched_rows` | Passed (`n`) |
| `only_source_rows` | Failed (`y`) |
| `final_diff_score` | Failed rows % |

---

## Metric calculation

All methods expose `stats.final_diff_score` and `stats.final_score`.

**Quality score (every method):**

```
final_score = 100 − final_diff_score
```

Scores are 0–100%. Higher `final_score` = better quality.  
Pass/fail uses tolerance:

- `final_diff_score > tolerance_percentage` → `COMPARISON_FAILED`
- otherwise → `COMPARISON_SUCCESS`

### `compare_sample` / `compare_custom_query`

```
final_diff_score =
    (source_dup% × 0.1)
  + (target_dup% × 0.1)
  + (source_only_rows% × 0.15)
  + (target_only_rows% × 0.15)
  + (rows_mismatched_by_any_column% × 0.5)
```

### `compare_counts`

```
sum_of_absolute_differences = abs(source_count − target_count)  per day
sum_of_common_counts        = min(source_count, target_count)   per day

final_diff_score = 100 × sum_of_absolute_differences
                       / (sum_of_absolute_differences + sum_of_common_counts)
```

### `sniff_query`

```
failed_rows%     = (rows with xsniff_issue = 'y') / (checked rows) × 100
final_diff_score = failed_rows%
```

Empty result → `final_diff_score = 0` (and score 100).

---

## Shared behaviour

### Chunked processing (`chunk_size_days`)

Available on all methods. Splits a date range into N-day windows, runs each chunk, then aggregates metrics and examples. Useful for long ranges or large tables.

- `compare_custom_query`: both sides must pass `start_date` and `end_date` in params
- `sniff_query`: chunking uses `start_date` / `end_date` in `source_params`

### Status values

| Status | Meaning |
|--------|---------|
| `COMPARISON_SUCCESS` | Within tolerance |
| `COMPARISON_FAILED` | Over tolerance, or a technical error |
| `COMPARISON_SKIPPED` | Nothing to compare (e.g. both sides empty) |

### Result persistence

With `results_engine` set and `persist_result=True` (or a custom `DataReference`), one row is written per run. The table is created if missing; primary key is `run_id`. Columns cover status, metadata, stats, details JSON, and the text report.

### Logging

Each run has an internal `run_id` (also stored when persistence is on; not in public JSON from `ComparisonResult.to_dict()`):

```
2024-01-15 10:30:45 - INFO - xoverrr.core - Comparison run started: run_id=a3f2c8b91d4e5678 comparison_name=employees_daily comparison_type=sample
2024-01-15 10:30:45 - INFO - xoverrr.core._compare_samples - Query executed in 2.34s
2024-01-15 10:30:46 - INFO - xoverrr.core._compare_samples - Source: 150000 rows, Target: 149950 rows
2024-01-15 10:30:47 - INFO - xoverrr.utils.compare_dataframes - Comparison completed in 1.2s
2024-01-15 10:30:47 - INFO - xoverrr.core - Comparison run finished: run_id=a3f2c8b91d4e5678 status=COMPARISON_SUCCESS
```

### Performance notes

- DataFrame size hard limit: 3 GB per sample
- Rough benchmark: two samples of ~1M rows × 10 columns (~330 MB each) compared in ~3 s (Intel Core i5 / 16 GB RAM)

---

## Example report

Text output (`report_output_format='text'`):

```
================================================================================
2025-11-24 20:09:40
run_id: a3f2c8b91d4e5678
version: *.*.*
source db type: postgresql
target db type: oracle
DATA SAMPLE COMPARISON REPORT:
hr.employees
VS
hr.employees
================================================================================
timezone: Europe/Athens

    SELECT employee_id, first_name, last_name, salary, department_id, hire_date,
           case when updated_at > (now() - INTERVAL '3 hours') then 'y' end as xrecently_changed
    FROM hr.employees
    WHERE 1=1
        AND hire_date >= date_trunc('day', cast(:start_date as date))
        AND hire_date < date_trunc('day', cast(:end_date as date)) + interval '1 day'

    params: {'start_date': '2025-11-17', 'end_date': '2025-11-24'}
----------------------------------------

    SELECT employee_id, first_name, last_name, salary, department_id, hire_date,
           case when updated_at > (sysdate - 3/24) then 'y' end as xrecently_changed
    FROM hr.employees
    WHERE 1=1
        AND hire_date >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
        AND hire_date < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1

    params: {'start_date': '2025-11-17', 'end_date': '2025-11-24'}
----------------------------------------

SUMMARY:
  Source rows: 105
  Target rows: 105
  Duplicated source rows: 0
  Duplicated target rows: 0
  Only source rows: 0
  Only target rows: 0
  Common rows (by primary key): 105
  Totally matched rows: 103
----------------------------------------
  Source only rows %: 0.00000
  Target only rows %: 0.00000
  Duplicated source rows %: 0.00000
  Duplicated target rows %: 0.00000
  Mismatched rows %: 1.90476
  Final discrepancies score: 0.95238
  Final data quality score: 99.04762
  Source-only key examples:
  Target-only key examples:
  Duplicated source key examples:
  Duplicated target key examples:
  Common attribute columns: first_name, last_name, salary, department_id
  Skipped source columns: audit_log, temp_field
  Skipped target columns:

COLUMN DIFFERENCES:
  Discrepancies per column (max %): 1.90476
  Count of mismatches per column:

 column_name  mismatch_count
     salary                2

  Some examples:

 primary_key column_name source_value target_value
         101      salary        50000        51000
         102      salary        60000        60500

DISCREPANT DATA (first pairs):
Sorted by primary key and dataset:

 employee_id first_name last_name salary department_id xflg
         101       John      Doe  50000            10   src
         101       John      Doe  51000            10   trg
         102       Jane      Doe  60000            20   src
         102       Jane      Doe  60500            20   trg

================================================================================
```

---

## Known limitations

### Oracle thin client & `TIMESTAMP WITH TIME ZONE`

With the Oracle thin client and `compare_custom_query`, `TIMESTAMP WITH TIME ZONE` columns lose timezone context in the result set.

**Workaround** — cast to `TIMESTAMP` in SQL:

```python
source_query = """
    SELECT
        order_id,
        CAST(created_at AT TIME ZONE 'Europe/Paris' AS TIMESTAMP) AS created_at,
        amount
    FROM orders
    WHERE status = 'completed'
"""
```
