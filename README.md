# xoverrr (pronounced “crossover”)

A tool for cross-database and intra-source data comparison with detailed discrepancy analysis and reporting.

## Key Features
- **Multi‑DBMS support**: Oracle, PostgreSQL (+ Greenplum), ClickHouse (extensible via adapter layer) — tables and views.
- **Universal connections**: Provide SQLAlchemy Engine objects for source and target databases.
- **Comparison strategies**:
  * Data sample comparison
  * Count‑based comparison with daily aggregates
  * Fully custom (raw) SQL‑query comparison
- **Smart analysis**:
  * Excludes “fresh” data to mitigate replication lag
  * Auto‑detection of primary keys and column types from DBMS metadata (PK must be found on at least one side, or may be supplied manually)
  * Application‑side type conversion
  * Automatic exclusion of columns with mismatched names
- **Optimization**: Two samples of 1 million rows × 10 columns (each ~330 MB) compared in ~3 s (Intel Core i5 / 16 GB RAM)
- **Detailed reporting**: In‑depth column‑level discrepancy analysis with example records (column view / record view)
- **Flexible configuration**: Column exclusion/inclusion, tolerance thresholds, custom primary‑key specification
- **Unit tests**: Coverage for comparison methods, functional and performance validation
- **Integrations tests**: contains integration tests for xoverrr using real databases started via Docker

## Example Report
```
================================================================================
2025-11-24 20:09:40
DATA SAMPLE COMPARISON REPORT:
public.account
VS
stage.account
================================================================================
timezone: Europe/Athens

        SELECT created_at, updated_at, id, code, bank_code, account_type, counterparty_id, special_code, case when updated_at > (now() - INTERVAL '%(exclude_recent_hours)s hours') then 'y' end as xrecently_changed
        FROM public.account
        WHERE 1=1
            AND created_at >= date_trunc('day', %(start_date)s::date)
            AND created_at < date_trunc('day', %(end_date)s::date)  + interval '1 days'

    params: {'exclude_recent_hours': 1, 'start_date': '2025-11-17', 'end_date': '2025-11-24'}
----------------------------------------

        SELECT created_at, updated_at, id, code, bank_code, account_type, counterparty_id, special_code, case when updated_at > (sysdate - :exclude_recent_hours/24) then 'y' end as xrecently_changed
        FROM stage.account
        WHERE 1=1
            AND created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
            AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1

    params: {'exclude_recent_hours': 1, 'start_date': '2025-11-17', 'end_date': '2025-11-24'}
----------------------------------------

SUMMARY:
  Source rows: 10966
  Target rows: 10966
  Duplicated source rows: 0
  Duplicated target rows: 0
  Only source rows: 0
  Only target rows: 0
  Common rows (by primary key): 10966
  Totally matched rows: 10965
----------------------------------------
  Source only rows %: 0.00000
  Target only rows %: 0.00000
  Duplicated source rows %: 0.00000
  Duplicated target rows %: 0.00000
  Mismatched rows %: 0.00912
  Final discrepancies score: 0.00456
  Final data quality score: 99.99544
  Source-only key examples: None
  Target-only key examples: None
  Duplicated source key examples: None
  Duplicated target key examples: None
  Common attribute columns: created_at, updated_at, code, bank_code, account_type, counterparty_id, special_code
  Skipped source columns:
  Skipped target columns: mt_change_date

COLUMN DIFFERENCES:
  Discrepancies per column (max %): 0.00912
  Count of mismatches per column:

 column_name  mismatch_count
special_code               1
  Some examples:

primary_key                          column_name  source_value target_value
f8153447-****-****-****-****** special_code       N/A          XYZ

DISCREPANT DATA (first pairs):
Sorted by primary key and dataset:


created_at          updated_at          id                                   code                 bank_code account_type counterparty_id                      special_code xflg
2025-11-24 18:58:27 2025-11-24 18:58:27 f8153447-****-****-****-****** 42****************87 0********* 11           62aa01a6-****-****-****-f17e2b*****4
N/A       src
2025-11-24 18:58:27 2025-11-24 18:58:27 f8153447-****-****-****-****** 42****************87 0********* 11           62aa01a6-****-****-****-f17e2b*****4 XYZ       trg

================================================================================
```

## Metric Calculation
### for compare_sample/compare_custom_query
```
final_diff_score =
 (source_dup% × 0.1)
 + (target_dup% × 0.1)
 + (source_only_rows% × 0.15)
 + (target_only_rows% × 0.15)
 + (rows_mismatched_by_any_column% × 0.5)
```

### for compare_counts
```
sum_of_absolute_differences = `abs(source_count - target_count)` per each day
sum_of_common_counts = `min(source_count, target_count)` per each day
final_diff_score = 100 × (sum_of_absolute_differences) / (sum_of_absolute_differences + sum_of_common_counts)
```

#### Quality score formula all methods: `100 − final_diff_score`
#### Scores range 0–100%; higher values indicate better data quality.

## Comparison Methods

### 1. Data Sample Comparison (`compare_sample`)
Suitable for comparing row sets and column values over a date range.

```python
status, report, stats, details = comparator.compare_sample(
    source_table=DataReference("table_name", "schema_name"),
    target_table=DataReference("table_name", "schema_name"),
    date_column="created_at",
    update_column="modified_date",
    date_range=("2024-01-01", "2024-01-31"),
    exclude_columns=["audit_timestamp", "internal_id"],
    include_columns=None,
    custom_primary_key=["id", "user_id"],
    tolerance_percentage=1.0,
    exclude_recent_hours=24,
    max_examples=3
)
```

**Parameters:**
- `source_table`, `target_table` – names of the tables or views to compare
- `date_column` – column used for date‑range filtering
- `update_column` – column identifying “fresh” data (excluded from both sides)
- `date_range` – tuple `(start_date, end_date)` in “YYYY‑MM‑DD” format
- `exclude_columns` – list of columns to omit from comparison, aka blacklist
- `include_columns` – list of columns to include, aka whitelist
- `custom_primary_key` – user‑specified primary key (if not provided, auto‑detected)
- `tolerance_percentage` – acceptable discrepancy threshold (0.0–100.0)
- `exclude_recent_hours` – exclude data modified within the last N hours
- `max_examples` – maximum number of discrepancy examples included in the report

### 2. Count‑Based Comparison (`compare_counts`)
Efficient for large‑volume comparisons over extended date ranges, identifying missing rows or duplicates.

```python
status, report, stats, details = comparator.compare_counts(
    source_table=DataReference("users", "schema1"),
    target_table=DataReference("users", "schema2"),
    date_column="created_at",
    date_range=("2024-01-01", "2024-01-31"),
    tolerance_percentage=2.0,
    max_examples=5
)
```

**Parameters:**
- `source_table`, `target_table` – references to the tables/views to compare
- `date_column` – column for daily grouping
- `date_range` – date interval for analysis
- `tolerance_percentage` – acceptable discrepancy threshold
- `max_examples` – maximum number of daily discrepancy examples included in the report

### 3. Custom‑Query Comparison (`compare_custom_query`)
Compares data from arbitrary SQL queries. Suitable for complex scenarios.

```python
status, report, stats, details = comparator.compare_custom_query(
    source_query="""SELECT id as user_id, name as user_name, created_at as created_date FROM scott.source_table WHERE status = %(status)s""",
    source_params={'status': 'active'},
    target_query="""SELECT user_id, user_name, created_date FROM scott.target_table WHERE status = :status""",
    target_params={'status': 'active'},
    custom_primary_key=["id"],
    exclude_columns=["internal_code"],
    tolerance_percentage=0.5,
    max_examples=3
)
```

**Parameters:**
- `source_query`, `target_query` – parameterised SQL queries for the source and target
- `source_params`, `target_params` – query parameters
- `custom_primary_key` – mandatory list of column names constituting the primary key
- `exclude_columns` – columns to omit from comparison
- `tolerance_percentage` – acceptable discrepancy threshold
- `max_examples` – maximum number of discrepancy examples included in the report
- To automatically exclude recently changed records, add the following expression to your SELECT clause in `compare_custom_query`:
  ```sql
  case when updated_at > (sysdate - 3/24) then 'y' end as xrecently_changed
  ```

**Automatic Primary‑Key Detection:**
- If `custom_primary_key` is not supplied, the system automatically infers the PK from metadata.
- When source and target PKs differ, the source PK is used with a warning.

**Performance Considerations:**
- DataFrame size validation (hard limit: 3 GB per sample)
- Efficient comparison via XOR properties
- Configurable limits via constants

**Return Values:**
All methods return a tuple:
- `status` – comparison status (`COMPARISON_SUCCESS` / `COMPARISON_FAILED` / `COMPARISON_SKIPPED`)
- `report` – textual report detailing discrepancies
- `stats` – `ComparisonStats` dataclass instance containing comparison statistics
- `details` – `ComparisonDiffDetails` dataclass instance with discrepancy examples and details

### Status Types
- **COMPARISON_SUCCESS**: Comparison completed within tolerance limits.
- **COMPARISON_FAILED**: Discrepancies exceed tolerance threshold, or a technical error occurred.
- **COMPARISON_SKIPPED**: No data available for comparison (both tables empty).

### Structured Logging
Logs include timing information and structured context:
```
2024-01-15 10:30:45 - INFO - xoverrr.core._compare_samples - Query executed in 2.34s
2024-01-15 10:30:46 - INFO - xoverrr.core._compare_samples - Source: 150000 rows, Target: 149950 rows
2024-01-15 10:30:47 - INFO - xoverrr.utils.compare_dataframes - Comparison completed in 1.2s
```

### Tolerance Percentage
- **tolerance_percentage**: Acceptable discrepancy threshold (0.0–100.0).
- If `final_diff_score > tolerance`: status = `COMPARISON_FAILED`
- If `final_diff_score ≤ tolerance`: status = `COMPARISON_SUCCESS`
- Enables configuration of acceptable discrepancy levels.

---

## Usage Example
**Sample comparison** (Greenplum vs Oracle):

```python
from xoverrr import DataQualityComparator, DataReference, COMPARISON_SUCCESS, COMPARISON_FAILED, COMPARISON_SKIPPED
import os
from datetime import date, timedelta

USER_ORA = os.getenv('USER_ORA', '')
PASSWORD_ORA = os.getenv('PASSWORD_ORA', '')

USER_GP = os.getenv('USER_GP', '')
PASSWORD_GP = os.getenv('PASSWORD_GP', '')

HOST = os.getenv('HOST', '')

def create_src_engine(user, password, host):
    """Source engine (Oracle)"""
    os.environ['NLS_LANG'] = '.AL32UTF8'
    return create_engine(f'oracle+oracledb://{user}:{password}@{host}:1521/?service_name=dwh')

def create_trg_engine(user, password, host):
    """Target engine (Postgres/Greenplum)"""
    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:5432/adb'
    engine = create_engine(connection_string)
    return engine



src_engine = create_src_engine(USER_ORA, PASSWORD_ORA, HOST)
trg_engine = create_trg_engine(USER_GP, PASSWORD_GP, HOST)

comparator = DataQualityComparator(
    source_engine=src_engine,
    target_engine=trg_engine,
    timezone='Asia/Yekaterinburg'
)

source = DataReference("users", "schema1")
target = DataReference("users", "schema2")

FORMAT = '%Y-%m-%d'
recent_range_end = date.today()
recent_range_begin = recent_range_end - timedelta(days=1)

status, report, stats, details = comparator.compare_sample(
    source,
    target,
    date_column="created_at",
    update_column="modified_date",
    exclude_columns=["audit_timestamp", "internal_id"],
    exclude_recent_hours=24,
    date_range=(
        recent_range_begin.strftime(FORMAT),
        recent_range_end.strftime(FORMAT)
    ),
    tolerance_percentage=0
)

print(report)
if status == COMPARISON_FAILED:
    raise Exception("Sample check failed")
```

---

