# xoverrr (pronounced “crossover”)

Compare data between two databases, or between two tables in the same database, and obtain a pass or fail result together with examples of the discrepancies.

Supported databases: **Oracle**, **PostgreSQL** (including Greenplum), and **ClickHouse**.

---

## Features

- **Six check strategies** — row samples, counts grouped by date, whole-table counts, custom SQL, custom SQL aggregates (`MAX` / `SUM` / `COUNT(*)`), and source-only sniff checks
- **Multi-DBMS** — tables and views, extensible by means of adapters
- **SQLAlchemy engines** — any supported source, target, or results connection may be supplied
- **Recent-row exclusion** — rows that may still be delayed by a batch load, replication, or calculation can be omitted
- **Automatic metadata** — primary keys and column types are read from the DBMS catalogues, or a primary key may be supplied
- **Type conversion** — values are normalised on the application side so that they may be compared across databases
- **Column filters** — include and exclude lists; columns present on only one side are skipped automatically
- **Chunked date ranges** — long periods may be processed in windows of *N* days
- **Reports** — text or JSON, with examples of mismatched rows
- **Optional persistence** — the results of each run may be written to a third engine for dashboards and audit
- **Tests** — unit coverage together with Docker-backed integration tests

---

## Installation

```bash
pip install xoverrr
```

Python 3.9 or later is required. The SQLAlchemy driver for the database in use must also be installed (`oracledb`, `psycopg2`, `clickhouse-driver`, and so forth).

---

## Quick start

**Sample check** (Greenplum/PostgreSQL -> Oracle). Only the required arguments are supplied; all others take their default values:

```python
from sqlalchemy import create_engine
from xoverrr import DataQualityChecker, DataReference, CheckResult, CHECK_SUCCESS

source_engine = create_engine('postgresql://user:pass@localhost:5432/source_db')
target_engine = create_engine('oracle+oracledb://user:pass@localhost:1521/target_db')

checker = DataQualityChecker(
    source_engine=source_engine,
    target_engine=target_engine,
)

result: CheckResult = checker.check_samples(
    source_table=DataReference('employees', schema='hr'),
    target_table=DataReference('employees', schema='hr'),
)

print(result.run_id)
print(result.report)
if result.status == CHECK_SUCCESS:
    print('Data quality check passed')
else:
    print('Data quality check failed')
```

The target engine may be the same object as `source_engine` when both tables or schemas reside in a single database.

`DataReference(name, schema=None)` identifies a table or view. The name may contain only letters, digits, and underscores.

Every method returns a `CheckResult`:

| Field | Meaning |
|-------|---------|
| `result.status` | `success`, `failed`, or `skipped` |
| `result.report` | A human-readable text report, or JSON if that format was requested |
| `result.stats` | Scores and counts (`final_score`, `final_diff_score`, and row totals) |
| `result.details` | Examples of mismatches |
| `result.run_id` | Identifier of this run |

`final_score` ranges from 0 to 100; a higher value indicates closer agreement. `final_diff_score` is the complement: `100 - final_score`.

Most methods fail when `final_diff_score` exceeds `tolerance_pct` (the default is `0`, so any difference constitutes a failure). `check_custom_queries_agg` admits no tolerance: every aggregate must match, or the check fails.

| Status | Meaning |
|--------|---------|
| `success` | Within tolerance, or an exact match |
| `failed` | Above tolerance, or the run encountered an error |
| `skipped` | Nothing to compare (for example, when both sides are empty) |

---

## Which method should I use?

| Method | Use this when you wish to… | Target database required? |
|--------|----------------------------|---------------------------|
| `check_total_counts` | Establish whether the row counts agree | Yes |
| `check_counts_group_by_date` | Compare daily volumes in order to detect missing or extra days | Yes |
| `check_samples` | Compare column values, row by row | Yes |
| `check_custom_queries` | Compare the results of SQL (joins, renamed columns) | Yes |
| `check_custom_queries_agg` | Compare `MAX`, `SUM`, or `COUNT(*)` of two queries | Yes |
| `check_sniff_query` | Validate the source against a rule, without a target | No |

The target engine may be the same object as `source_engine`. Two engines are required only when the two sides reside in different databases.

A typical approach is to begin with counts, which are inexpensive, and then to apply sample or custom-SQL checks where discrepancies remain.

---

## Checker constructor

```python
DataQualityChecker(
    source_engine,
    target_engine=None,
    default_exclude_recent_hours=24,
    timezone='UTC',
    results_engine=None,
    max_dataframe_size_gb=3,
)
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `source_engine` | Yes | — | SQLAlchemy engine for the source. Required for every check. |
| `target_engine` | No | `None` | SQLAlchemy engine for the target. Required for every method except `check_sniff_query`. It may be the same object as `source_engine`. |
| `default_exclude_recent_hours` | No | `24` | Default used by `check_samples` when `exclude_recent_hours` is omitted. Set to `None` to disable it. Takes effect only when `update_column` is also set. |
| `timezone` | No | `'UTC'` | Session time zone for queries and type conversion. |
| `results_engine` | No | `None` | Engine used to store a row when a check specifies `persist_result`. |
| `max_dataframe_size_gb` | No | `3` | Maximum size of a single query or chunk from one database. Must be greater than 0. |

---

## Shared parameters

These four options behave identically on every check method. They are repeated in each method table for convenience.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `check_name` | No | `None` | Label attached to the result and, if persistence is enabled, to the stored row. |
| `persist_result` | No | `None` | `DataReference` of a results table. Requires `results_engine`. A failed write marks the check as `failed`. |
| `check_tags` | No | `None` | Additional labels, for example `{"env": "prod"}`. |
| `report_output_format` | No | `'text'` | `'text'` or `'json'`. |

In the tables that follow, **Required** means that the argument must be supplied. Any omitted argument takes the stated default.

---

## 1. Whole-table count — `check_total_counts`

Compares `COUNT(*)` on each side. If a date column is supplied, the scan may be divided into windows; the counts from those windows are then summed. There is no per-day breakdown.

```python
result = checker.check_total_counts(
    source_table=DataReference('users', 'schema1'),
    target_table=DataReference('users', 'schema2'),
)
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `source_table` | Yes | — | Source table or view. |
| `target_table` | Yes | — | Target table or view. |
| `check_name` | No | `None` | Label attached to the result. |
| `date_column` | No | `None` | Required if `date_range` or `chunk_size_days` is set. |
| `date_range` | No | `None` | `(start, end)` as `YYYY-MM-DD`. Either bound may be omitted unless chunking is used. |
| `chunk_size_days` | No | `None` | Windows of *N* days. Requires `date_column` and both bounds. Must be greater than 0. |
| `tolerance_pct` | No | `0.0` | The check fails when `final_diff_score` exceeds this value (0–100). |
| `persist_result` | No | `None` | Results table; omit this argument to skip writing. |
| `check_tags` | No | `None` | Additional labels. |
| `report_output_format` | No | `'text'` | `'text'` or `'json'`. |

**Score.** The check fails when `final_diff_score > tolerance_pct`.

```
diff   = abs(source_count - target_count)
common = min(source_count, target_count)

final_diff_score = 100 * diff / (diff + common)
final_score      = 100 - final_diff_score
```

Equal counts yield `final_score = 100`. If one side is empty and the other is not, `final_score = 0`.

---

## 2. Daily volume — `check_counts_group_by_date`

Compares row counts for each day (not hour, month, or year). This is useful for detecting a missing or extra day.

```python
result = checker.check_counts_group_by_date(
    source_table=DataReference('users', 'schema1'),
    target_table=DataReference('users', 'schema2'),
    date_column='created_at',
)
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `source_table` | Yes | — | Source table or view. |
| `target_table` | Yes | — | Target table or view. |
| `date_column` | Yes | — | Date or timestamp column by which to group (daily). |
| `check_name` | No | `None` | Label attached to the result. |
| `date_range` | No | `None` | `(start, end)` as `YYYY-MM-DD`. Either bound may be omitted unless chunking is used. |
| `chunk_size_days` | No | `None` | Windows of *N* days. Requires both bounds. Must be greater than 0. |
| `tolerance_pct` | No | `0.0` | The check fails when `final_diff_score` exceeds this value. |
| `max_examples` | No | `3` | Maximum number of example dates to retain. |
| `persist_result` | No | `None` | Results table; omit this argument to skip writing. |
| `check_tags` | No | `None` | Additional labels. |
| `report_output_format` | No | `'text'` | `'text'` or `'json'`. |

**Score.** The same formula as for `check_total_counts`, except that `diff` and `common` are summed across days:

```
diff   = sum(abs(source_count - target_count))   per day
common = sum(min(source_count, target_count))    per day

final_diff_score = 100 * diff / (diff + common)
final_score      = 100 - final_diff_score
```

The check fails when `final_diff_score > tolerance_pct`.

---

## 3. Row values — `check_samples`

Compares rows and column values. An optional date window may be applied. If `custom_primary_key` is omitted, it is read from metadata and must exist on at least one side. Columns present on only one side are skipped.

```python
result = checker.check_samples(
    source_table=DataReference('employees', 'hr'),
    target_table=DataReference('employees', 'hr'),
)
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `source_table` | Yes | — | Source table or view. |
| `target_table` | Yes | — | Target table or view. |
| `check_name` | No | `None` | Label attached to the result. |
| `date_column` | No | `None` | Column used for `date_range` and chunking. Required if either of those is set. |
| `date_range` | No | `None` | `(start, end)` as `YYYY-MM-DD`. Either bound may be omitted unless chunking is used. |
| `chunk_size_days` | No | `None` | Divides the range into windows of *N* days. Requires both bounds and `date_column`. Must be greater than 0. |
| `update_column` | No | `None` | Timestamp used with `exclude_recent_hours`. Keys marked `xrecently_changed = 'y'` are excluded from both sides. Ignored if no hour window is active. |
| `exclude_recent_hours` | No | constructor default (`24`) | Hours used with `update_column`. `None` or `0` falls back to the constructor. To disable the window, set the constructor default to `None` and omit this argument. |
| `exclude_columns` | No | `None` (`[]`) | Columns to omit. Primary-key columns named here are nevertheless retained. |
| `include_columns` | No | `None` (`[]`) | If set, only these columns and the primary key are compared. |
| `custom_primary_key` | No | `None` | Join key. If omitted, it is taken from metadata. |
| `tolerance_pct` | No | `0.0` | The check fails when `final_diff_score` exceeds this value (0–100). |
| `max_examples` | No | `3` | Maximum number of mismatch examples to retain per column. |
| `persist_result` | No | `None` | Results table; omit this argument to skip writing. |
| `check_tags` | No | `None` | Additional labels. |
| `report_output_format` | No | `'text'` | `'text'` or `'json'`. |

**Score.** The check fails when `final_diff_score > tolerance_pct`.

```
final_diff_score =
    (dup_source_rows_pct * 0.1)
  + (dup_target_rows_pct * 0.1)
  + (source_only_rows_pct * 0.15)
  + (target_only_rows_pct * 0.15)
  + (issue_rows_pct * 0.5)

final_score = 100 - final_diff_score
```

---

## 4. Custom SQL — `check_custom_queries`

Compares the results of two queries. A primary key is required. Date filters belong in the SQL; values are passed through `source_params` and `target_params`.

To exclude rows that may still be in the process of being updated, add the same flag as that used by `check_samples`:

```sql
CASE WHEN updated_at > (sysdate - 3/24) THEN 'y' END AS xrecently_changed
```

A key marked `'y'` on either side is excluded from both sides.

```python
result = checker.check_custom_queries(
    source_query='SELECT id AS user_id, name AS user_name FROM scott.source_table',
    target_query='SELECT user_id, user_name FROM scott.target_table',
    custom_primary_key=['user_id'],
)
```

To divide a long range into windows, include `start_date` and `end_date` in **both** parameter dictionaries and set `chunk_size_days`.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `source_query` | Yes | — | Source SQL. Bind values with `:name`. |
| `target_query` | Yes | — | Target SQL. |
| `custom_primary_key` | Yes | — | Key columns in the result. An empty list raises `ValueError`. |
| `source_params` | No | `None` (`{}`) | Bind values. For chunking, include `start_date` and `end_date`. |
| `target_params` | No | `None` (`{}`) | Bind values. For chunking, include `start_date` and `end_date`. |
| `check_name` | No | `None` | Label attached to the result. |
| `chunk_size_days` | No | `None` | Windows of *N* days. Used only when both parameter dictionaries contain `start_date` and `end_date`. Must be greater than 0. |
| `exclude_columns` | No | `None` (`[]`) | Columns to omit. |
| `tolerance_pct` | No | `0.0` | The check fails when `final_diff_score` exceeds this value. |
| `max_examples` | No | `3` | Maximum number of mismatch examples to retain per column. |
| `persist_result` | No | `None` | Results table; omit this argument to skip writing. |
| `check_tags` | No | `None` | Additional labels. |
| `report_output_format` | No | `'text'` | `'text'` or `'json'`. |

**Score.** The same formula as for `check_samples`. The check fails when `final_diff_score > tolerance_pct`.

---

## 5. Custom SQL aggregates — `check_custom_queries_agg`

Compares `MAX`, `SUM`, and, optionally, `COUNT(*)` of two queries. The database performs the aggregation:

```sql
SELECT max(amount) AS max_amount, sum(amount) AS sum_amount, count(*) AS cnt
FROM (<your query>) x_subq
```

At least one of `max_columns`, `sum_columns`, or `include_count=True` must be supplied. Date filters remain in the SQL. There is no `tolerance_pct`, no chunking, and no `xrecently_changed`. Every aggregate must match; otherwise the check fails (`final_score` is 100 or 0).

```python
result = checker.check_custom_queries_agg(
    source_query='SELECT amount, created_at FROM scott.source_table WHERE created_at >= :start_date',
    target_query='SELECT amount, created_at FROM scott.target_table WHERE created_at >= :start_date',
    source_params={'start_date': '2024-01-01'},
    target_params={'start_date': '2024-01-01'},
    max_columns=['created_at'],
    sum_columns=['amount'],
    include_count=True,
)
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `source_query` | Yes | — | Source SQL. Bind values with `:name`. |
| `target_query` | Yes | — | Target SQL. |
| `source_params` | No | `None` (`{}`) | Bind values. |
| `target_params` | No | `None` (`{}`) | Bind values. |
| `max_columns` | No | `None` (`[]`) | Wrapped as `max(col) AS max_col`. Simple identifiers only. At least one of `max_columns`, `sum_columns`, or `include_count=True` is required. |
| `sum_columns` | No | `None` (`[]`) | Wrapped as `sum(col) AS sum_col`. |
| `include_count` | No | `False` | Also compares `count(*) AS cnt`. |
| `check_name` | No | `None` | Label attached to the result. |
| `persist_result` | No | `None` | Results table; omit this argument to skip writing. |
| `check_tags` | No | `None` | Additional labels. |
| `report_output_format` | No | `'text'` | `'text'` or `'json'`. |

**Score.** Binary. There is no `tolerance_pct`.

```
all aggregates match  ->  success,  final_score = 100,  final_diff_score = 0
any difference        ->  failed,   final_score = 0,    final_diff_score = 100
```

`SUM` and `MAX` ignore nulls. Two empty or all-null sides count as a match. `count(*)` of an empty side is 0.

---

## 6. Source-only rules — `check_sniff_query`

No target database is required. The query must return `xsniff_passed`: `y` for a row that is acceptable, `n` for an issue.

**Per row** — use this when a rate is required (“allow up to 1 per cent of rows to be unsatisfactory”):

```python
result = checker.check_sniff_query(
    source_query="""
        SELECT
            order_id,
            CASE WHEN amount > 0 THEN 'y' ELSE 'n' END AS xsniff_passed
        FROM sales.orders
    """,
    tolerance_pct=1.0,
)
```

**Single value** — a single `y` or `n`. Leave `tolerance_pct` at `0` so that any issue causes a failure:

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

**Issues only** — return only unsatisfactory rows, each marked `'n'`. An empty result constitutes a pass; any returned row constitutes a failure.

```python
result = checker.check_sniff_query(
    source_query="""
        SELECT order_id, amount, 'n' AS xsniff_passed
        FROM sales.orders
        WHERE amount <= 0
    """,
)
```

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `source_query` | Yes | — | Must include `xsniff_passed` (`y` or `n`). |
| `source_params` | No | `None` (`{}`) | Bind values. For chunking, include `start_date` and `end_date`. |
| `check_name` | No | `None` | Label attached to the result. |
| `chunk_size_days` | No | `None` | Windows of *N* days, taken from `source_params`. Must be greater than 0. |
| `tolerance_pct` | No | `0.0` | The check fails when the issue-row percentage exceeds this value. |
| `max_examples` | No | `3` | Maximum number of issue rows to retain. |
| `persist_result` | No | `None` | Results table; omit this argument to skip writing. |
| `check_tags` | No | `None` | Additional labels. |
| `report_output_format` | No | `'text'` | `'text'` or `'json'`. |

**Score.** The check fails when `final_diff_score > tolerance_pct`.

```
issue_rows_pct   = (rows with xsniff_passed = 'n') / (rows checked) * 100
final_diff_score = issue_rows_pct
final_score      = 100 - final_diff_score
```

An empty result yields a score of 100. If the query returns only issue rows, any non-empty result is treated as 100 per cent issues.

Useful fields on `stats` are `total_source_rows` (rows checked), `passed_rows`, and `issue_rows_pct` (identical to `final_diff_score`).

---

## Date windows (`chunk_size_days`)

On `check_samples`, `check_counts_group_by_date`, `check_total_counts`, `check_custom_queries`, and `check_sniff_query`, a range may be divided into windows of *N* days. Each window is checked; the scores and examples are then combined. This is useful for long ranges. Both the start and the end date are required when chunking is used.

- `check_custom_queries`: both parameter dictionaries must contain `start_date` and `end_date`
- `check_sniff_query`: chunking uses `start_date` and `end_date` in `source_params`
- `check_total_counts`: requires `date_column` and `date_range`; the chunk counts are summed

`check_custom_queries_agg` does not support chunking. The date filter should be expressed in the SQL.

---

## Saving results

Set `results_engine` on the checker and pass `persist_result=DataReference(...)` on the check. The schema must already exist; the table is created if it is missing. One row is written per run (status, statistics, details, and the report). If persistence was requested and the write fails, the check is marked `failed`.

---

## Example report

`report_output_format='text'`:

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
  Source rows: 107
  Target rows: 106
  Duplicated source rows: 1
  Duplicated target rows: 0
  Only source rows: 1
  Only target rows: 1
  Comparable rows: 105
  Passed rows: 102
----------------------------------------
  Source only rows %: 0.95238
  Target only rows %: 0.95238
  Duplicated source rows %: 0.93458
  Duplicated target rows %: 0.00000
  Issue rows %: 2.85714
  Final discrepancies score: 1.80774
  Final data quality score: 98.19226
  Source-only key examples: 205
  Target-only key examples: 310
  Duplicated source key examples: 104
  Duplicated target key examples:
  Skipped source columns: audit_log, temp_field
  Skipped target columns:

ISSUE BREAKDOWN:
  Max issue %: 2.85714
  Issue counts by column:

  column_name  issue_count
       salary            3
department_id            1

  Issue examples:

 primary_key   column_name  source_value  target_value
         101        salary         50000         51000
         102        salary         60000         60500
         103        salary         72000         70000
         101 department_id            10            20

================================================================================
```

---

## Known limitations

**Oracle thin client and `TIMESTAMP WITH TIME ZONE`.** When `check_custom_queries` is used with the Oracle thin client, the time zone may be lost. Cast in SQL:

```sql
CAST(created_at AT TIME ZONE 'Europe/Paris' AS TIMESTAMP) AS created_at
```

A full sample of approximately one million rows by ten columns (about 330 MB on each side) compares in about three seconds on a typical laptop. A single query or chunk is limited by `max_dataframe_size_gb` (the default is 3 GB).

---

## Tests and releases

| When | What is run |
|------|-------------|
| A push to a branch | Unit tests on Python 3.9 and 3.12 |
| A commit message containing `[integration]` | Unit tests together with the Docker integration tests |
| **Actions -> CI / Integration tests -> Run workflow** | A manual run (once the workflow is on `main`) |
| A tag `vX.Y.Z` on `main` | All tests, followed by publication to PyPI |

The tag must match `pyproject.toml` and `src/xoverrr/version.py`. Release notes and the PyPI configuration are in `COTRIBUTE.md`. The local commands are `make test-unit`, `make up-dbs`, and `make build`.
