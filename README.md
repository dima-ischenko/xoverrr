# xoverrr (pronounced “crossover”)

A tool for cross-database and intra-source data quality checks with detailed discrepancy analysis and reporting.

Supported databases: **Oracle**, **PostgreSQL** (+ Greenplum), **ClickHouse**.

---

## Features

- **Five check strategies** — row samples, daily counts, whole-table counts, custom SQL, and source-only sniff checks
- **Multi-DBMS** — tables and views, extensible via adapters
- **SQLAlchemy engines** — pass any supported source, target, or results connection
- **Recent-row exclusion** — optionally skip rows that may still be delayed (batch load, replication, or calculation)
- **Auto metadata** — primary keys and column types from DBMS catalogues (or supply your own primary key)
- **Type conversion** — application-side normalisation across databases
- **Column filters** — include / exclude lists; mismatched column names are skipped automatically
- **Chunked date ranges** — process long periods in N-day windows
- **Reports** — text or JSON, with example mismatched rows
- **Optional persistence** — write run results to a third engine for dashboards and audit
- **Tests** — unit coverage plus Docker-backed integration tests

---

## Quick start

**Sample check** (Greenplum/PostgreSQL → Oracle):

```python
from xoverrr import DataQualityChecker, DataReference, CheckResult, CHECK_SUCCESS
from sqlalchemy import create_engine
from datetime import date, timedelta

# 1. Connections
source_engine = create_engine('postgresql://user:pass@localhost:5432/source_db')
target_engine = create_engine('oracle+oracledb://user:pass@localhost:1521/target_db')
results_engine = create_engine('postgresql://user:pass@localhost:5432/dq_audit')

# 2. Checker
checker = DataQualityChecker(
    source_engine=source_engine,
    target_engine=target_engine,
    timezone='Europe/Athens',
    results_engine=results_engine,  # optional
    max_dataframe_size_gb=3,  # per query / chunk from one DB
)

# 3. Tables + date window
source_table = DataReference("employees", schema="hr")
target_table = DataReference("employees", schema="hr")
end_date = date.today()
start_date = end_date - timedelta(days=7)

# 4. Run
result: CheckResult = checker.check_samples(
    source_table=source_table,
    target_table=target_table,
    date_column="hire_date",
    update_column="modified_at",
    date_range=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
    chunk_size_days=30,
    custom_primary_key=["employee_id"],
    exclude_columns=["audit_log", "temp_field"],
    tolerance_pct=0.5,
    exclude_recent_hours=3,
    max_examples=5,
    persist_result=DataReference("dq_results", "test"),
    check_name="employees_daily",
    check_tags={"env": "prod", "domain": "hr"},
    report_output_format='text',  # 'json' or 'text'
)

# 5. Result
print(result.run_id)
print(result.report)
if result.status == CHECK_SUCCESS:
    print("Data quality check passed")
else:
    print("Data quality check failed")
```

Every check method returns a `CheckResult`:

```python
result = checker.check_samples(...)
result.run_id
result.status
result.report
result.stats
result.details
```

| Field | Meaning |
|-------|---------|
| `result.status` | `CHECK_SUCCESS` / `CHECK_FAILED` / `CHECK_SKIPPED` |
| `result.report` | Text report or JSON string (`report_output_format`) |
| `result.stats` | `CheckStats` — scores and row counts |
| `result.details` | `CheckDetails` — examples and per-column diffs |
| `result.run_id` | Unique id of this run (also in JSON and persistence) |

---

## Which method should I use?

| Method | When to use | Requires a target database? |
|--------|-------------|------------------|
| `check_samples` | Compare row values between two tables/views | Yes |
| `check_counts` | Daily volume check (missing / extra rows) | Yes |
| `check_total_counts` | Whole-table `COUNT(*)` | Yes |
| `check_custom_queries` | Complex joins, renamed columns, custom SQL | Yes |
| `check_sniff_query` | Source-only rule: “does this data look wrong?” | No |

---

## Check methods

### 1. Data sample (`check_samples`)

This method compares row sets and column values over a date range.

```python
result = checker.check_samples(
    source_table=DataReference("table_name", "schema_name"),
    target_table=DataReference("table_name", "schema_name"),
    date_column="created_at",
    update_column="modified_date",
    date_range=("2024-01-01", "2024-12-31"),
    chunk_size_days=30,
    exclude_columns=["audit_timestamp", "internal_id"],
    include_columns=None,
    custom_primary_key=["id", "user_id"],
    tolerance_pct=1.0,
    exclude_recent_hours=24,
    max_examples=3,
)
```

**Main parameters**

| Parameter | Description |
|-----------|-------------|
| `source_table`, `target_table` | Tables or views to compare |
| `date_column` | Column for date-range filtering |
| `update_column` | Timestamp used to detect recently changed rows (excluded on both sides) |
| `date_range` | `(start_date, end_date)` as `YYYY-MM-DD` |
| `chunk_size_days` | Optional N-day windows over the range |
| `exclude_columns` / `include_columns` | A blacklist or whitelist of columns |
| `custom_primary_key` | Primary-key columns; detected automatically if omitted |
| `tolerance_pct` | The check fails if `final_diff_score` exceeds this (0–100) |
| `exclude_recent_hours` | Exclude rows changed in the last N hours (batch load, replication, or calculation delay) |
| `max_examples` | Maximum number of discrepancy examples in the report |
| `persist_result` | `DataReference` of the results table; omit this option to skip persistence |
| `check_name` / `check_tags` | Labels for dashboards |
| `report_output_format` | `'text'` (default) or `'json'` |

If `custom_primary_key` is omitted, the primary key is inferred from metadata (it must exist on at least one side).

---

### 2. Counts (`check_counts`)

Daily aggregates — suitable for large volumes and for spotting missing or extra rows.

```python
result = checker.check_counts(
    source_table=DataReference("users", "schema1"),
    target_table=DataReference("users", "schema2"),
    date_column="created_at",
    date_range=("2024-01-01", "2024-12-31"),
    chunk_size_days=30,
    tolerance_pct=2.0,
    max_examples=5,
)
```

**Main parameters:** `source_table`, `target_table`, `date_column`, `date_range`, `chunk_size_days`, `tolerance_pct`, `max_examples`, plus the shared `persist_result` / `check_name` / `check_tags` / `report_output_format` options described above.

---

### 3. Total counts (`check_total_counts`)

Whole-table `COUNT(*)` on each side. No date column, no metadata lookup, no per-day breakdown, no `chunk_size_days`. For volume over a date range, use `check_counts`.

```python
result = checker.check_total_counts(
    source_table=DataReference("users", "schema1"),
    target_table=DataReference("users", "schema2"),
    tolerance_pct=2.0,
)
```

**Main parameters:** `source_table`, `target_table`, `tolerance_pct`, plus the shared `persist_result` / `check_name` / `check_tags` / `report_output_format` options.

---

### 4. Custom query (`check_custom_queries`)

Compare the results of arbitrary SQL on both sides. A primary key is **required**.

```python
result = checker.check_custom_queries(
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
    tolerance_pct=0.5,
    max_examples=3,
)
```

**Chunking:** when both `source_params` and `target_params` include `start_date` and `end_date`, set `chunk_size_days` to split the range:

```python
result = checker.check_custom_queries(
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
    tolerance_pct=0.5,
)
```

To skip recently changed rows in custom SQL, add the same flag as that used by the sample check:

```sql
CASE WHEN updated_at > (sysdate - 3/24) THEN 'y' END AS xrecently_changed
```

---

### 5. Sniff query (`check_sniff_query`)

A source-only check. Mark each row with `xsniff_passed` (`y` = passed, `n` = failed).  
No target engine or primary key is required:

```python
checker = DataQualityChecker(
    source_engine=source_engine,
    timezone='UTC',
)
```

**Row-level** — one flag per row. Use this when you need an issue *rate* over the full scope;
`tolerance_pct` then means “allow up to N% of rows with `xsniff_passed = n`”:

```python
result = checker.check_sniff_query(
    source_query="""
        SELECT
            order_id,
            amount,
            CASE
                WHEN amount > 0 AND customer_id IS NOT NULL THEN 'y'
                ELSE 'n'
            END AS xsniff_passed
        FROM sales.orders
        WHERE created_at >= :start_date
    """,
    source_params={'start_date': '2024-01-01'},
    tolerance_pct=1.0,
)
```

**Scalar pass/fail** — a single `xsniff_passed` value. The outcome is typically binary
(`final_diff_score` 0 or 100), so leave `tolerance_pct` at the default `0.0`
(fail on any issue):

```python
result = checker.check_sniff_query(
    source_query="""
        SELECT CASE
            WHEN EXISTS (SELECT 1 FROM sales.orders WHERE amount <= 0) THEN 'n'
            ELSE 'y'
        END AS xsniff_passed
    """,
)
```

**Issues-only filter** — `WHERE` keeps only the failing rows and marks every returned row with a literal `'n'`.  
An empty result means a pass (`final_score = 100`). Any returned row means a fail (`issue_rows_pct = 100` for that result set). As with the scalar pattern, keep the default `tolerance_pct=0.0` so that the check fails if any issue is found:

```python
result = checker.check_sniff_query(
    source_query="""
        SELECT
            order_id,
            amount,
            'n' AS xsniff_passed
        FROM sales.orders
        WHERE amount <= 0
          AND created_at >= :start_date
    """,
    source_params={'start_date': '2024-01-01'},
)
```

Use this when you need only to establish that issue rows exist (and to include their keys and attributes in the report). Prefer the row-level `CASE` pattern above when you need a rate over the full checked scope.

**Main parameters:** `source_query`, `source_params`, `chunk_size_days` (when the parameters include dates), `tolerance_pct`, `max_examples`, plus the shared persistence, naming, and report-format options.

Useful `stats` fields:

| Field | Meaning |
|-------|---------|
| `total_source_rows` | Rows checked |
| `passed_rows` | Passed (`xsniff_passed = y`) |
| `issue_rows_pct` | Issue rows % (`xsniff_passed = n`) |
| `final_diff_score` | Same as `issue_rows_pct` |

---

## Metric calculation

All methods expose `result.stats.final_diff_score` and `result.stats.final_score`.

**Quality score (every method):**

```
final_score = 100 − final_diff_score
```

Scores range from 0 to 100%. A higher `final_score` indicates better quality.  
Pass or fail is determined by the tolerance:

- `final_diff_score > tolerance_pct` → `CHECK_FAILED`
- otherwise → `CHECK_SUCCESS`

### `check_samples` / `check_custom_queries`

```
final_diff_score =
    (dup_source_rows_pct × 0.1)
  + (dup_target_rows_pct × 0.1)
  + (source_only_rows_pct × 0.15)
  + (target_only_rows_pct × 0.15)
  + (issue_rows_pct × 0.5)
```

### `check_counts` / `check_total_counts`

```
sum_of_absolute_differences = abs(source_count − target_count)  per day (or one total)
sum_of_common_counts        = min(source_count, target_count)   per day (or one total)

final_diff_score = 100 × sum_of_absolute_differences
                       / (sum_of_absolute_differences + sum_of_common_counts)
```

### `check_sniff_query`

```
issue_rows_pct = (rows with xsniff_passed = 'n') / (checked rows) × 100
final_diff_score = issue_rows_pct
```

An empty result yields `final_diff_score = 0` (and a score of 100).

With the issues-only filter pattern (`WHERE …` plus a literal `'n' AS xsniff_passed`), *checked rows* are only the filtered issue rows, so any non-empty result yields `issue_rows_pct = 100`.

---

## Shared behaviour

### Chunked processing (`chunk_size_days`)

Available on `check_samples`, `check_counts`, `check_custom_queries`, and `check_sniff_query`. It splits a date range into N-day windows, runs each chunk, then aggregates the metrics and examples. This is useful for long ranges or large tables. `check_total_counts` has no date window, so it is not chunked.

- `check_custom_queries`: both sides must supply `start_date` and `end_date` in their parameters
- `check_sniff_query`: chunking uses `start_date` / `end_date` in `source_params`

### Status values

| Status | Meaning |
|--------|---------|
| `CHECK_SUCCESS` | Within tolerance |
| `CHECK_FAILED` | Above tolerance, or a technical error |
| `CHECK_SKIPPED` | Nothing to compare (e.g. both sides empty) |

### Result persistence

With `results_engine` set and `persist_result=DataReference(...)`, one row is written per run to that table. The schema must already exist; the table is created if it is missing. The columns cover status, metadata, statistics, details JSON, and the text report. Persistence is skipped if `persist_result` is omitted. If persistence was requested and the write fails, the check status becomes `failed`.

### Logging

Each run has a `run_id` on the returned `CheckResult` (also stored when persistence is enabled, and included in `CheckResult.to_dict()` / JSON reports):

```
2024-01-15 10:30:45 - INFO - xoverrr.core - Check run started: run_id=a3f2c8b91d4e5678 check_name=employees_daily check_type=samples
2024-01-15 10:30:45 - INFO - xoverrr.core._check_samples - Query executed in 2.34s
2024-01-15 10:30:46 - INFO - xoverrr.core._check_samples - Source: 150000 rows, Target: 149950 rows
2024-01-15 10:30:47 - INFO - xoverrr.utils.compare_dataframes - Comparison completed in 1.2s
2024-01-15 10:30:47 - INFO - xoverrr.core - Check run finished: run_id=a3f2c8b91d4e5678 status=CHECK_SUCCESS
```

### Performance notes

- DataFrame size limit: `max_dataframe_size_gb` on the checker (default 3 GB per query / chunk from one DB)
- Rough benchmark: two samples of about 1 million rows × 10 columns (about 330 MB each) compared in about 3 s (Intel Core i5 / 16 GB RAM)

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
SAMPLES CHECK REPORT:
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
  Comparable rows: 105
  Passed rows: 103
----------------------------------------
  Source only rows %: 0.00000
  Target only rows %: 0.00000
  Duplicated source rows %: 0.00000
  Duplicated target rows %: 0.00000
  Issue rows %: 1.90476
  Final discrepancies score: 0.95238
  Final data quality score: 99.04762
  Source-only key examples:
  Target-only key examples:
  Duplicated source key examples:
  Duplicated target key examples:
  Skipped source columns: audit_log, temp_field
  Skipped target columns:

ISSUE BREAKDOWN:
  Max issue %: 1.90476
  Issue counts by column:

 column_name  issue_count
     salary                2

  Issue examples:

 primary_key column_name source_value target_value
         101      salary        50000        51000
         102      salary        60000        60500

================================================================================
```

---

## Known limitations

### Oracle thin client & `TIMESTAMP WITH TIME ZONE`

With the Oracle thin client and `check_custom_queries`, `TIMESTAMP WITH TIME ZONE` columns lose their time-zone context in the result set.

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
