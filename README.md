# xoverrr (pronounced “crossover”)

A tool for cross-database and intra-source data comparison with detailed discrepancy analysis and reporting.

## Usage Example
**Sample comparison** (Greenplum vs Oracle):

```python
from xoverrr import DataQualityComparator, DataReference, COMPARISON_SUCCESS
from sqlalchemy import create_engine
from datetime import date, timedelta

# 1. Create database connections
source_engine = create_engine('postgresql://user:pass@localhost:5432/source_db')
target_engine = create_engine('oracle+oracledb://user:pass@localhost:1521/target_db')

# 2. Initialize comparator
comparator = DataQualityComparator(
    source_engine=source_engine,
    target_engine=target_engine,
    timezone='Europe/Moscow'
)

# 3. Define tables to compare
source_table = DataReference("employees", schema="hr")
target_table = DataReference("employees", schema="hr")

# 4. Set date range (last 7 days)
end_date = date.today()
start_date = end_date - timedelta(days=7)

# 5. Run comparison
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
    max_examples=5
)

# 6. Check results
print(report)

if status == COMPARISON_SUCCESS:
    print("Data quality check passed")
else:
    print("Data quality check failed")
```

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
hr.employees
VS
hr.employees
================================================================================
timezone: Europe/Moscow

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
  Source-only key examples: None
  Target-only key examples: None
  Duplicated source key examples: None
  Duplicated target key examples: None
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
    date_range=("2024-01-01", "2025-01-31"),
    chunk_size_days=30,
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
- `chunk_size_days` – optional chunk size (in days) for iterative processing across the date range
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
    date_range=("2024-01-01", "2025-01-31"),
    chunk_size_days=30,
    tolerance_percentage=2.0,
    max_examples=5
)
```

**Parameters:**
- `source_table`, `target_table` – references to the tables/views to compare
- `date_column` – column for daily grouping
- `date_range` – date interval for analysis
- `chunk_size_days` – optional chunk size (in days) for iterative processing across the date range
- `tolerance_percentage` – acceptable discrepancy threshold
- `max_examples` – maximum number of daily discrepancy examples included in the report

### 3. Custom‑Query Comparison (`compare_custom_query`)
Compares data from arbitrary SQL queries. Suitable for complex scenarios.

```python
status, report, stats, details = comparator.compare_custom_query(
    source_query="""SELECT id as user_id, name as user_name, created_at as created_date FROM scott.source_table WHERE status = :status""",
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
- `chunk_size_days` – optional chunk size (in days) for iterative processing when source and target params include `start_date` and `end_date`
- `exclude_columns` – columns to omit from comparison
- `tolerance_percentage` – acceptable discrepancy threshold
- `max_examples` – maximum number of discrepancy examples included in the report
- To automatically exclude recently changed records, add the following expression to your SELECT clause in `compare_custom_query`:
  ```sql
  case when updated_at > (sysdate - 3/24) then 'y' end as xrecently_changed
  ```

### Chunked Processing (`chunk_size_days`)
- Available in all methods: `compare_sample`, `compare_counts`, `compare_custom_query`.
- Splits `date_range` into N-day windows and compares chunk-by-chunk, then aggregates final metrics and examples.
- Useful for long ranges or large tables to reduce peak query/dataframe size.
- For `compare_custom_query`, chunking is applied only when both `source_params` and `target_params` contain `start_date` and `end_date`.

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

## Known Limitations

### Oracle Thin Client & TIMESTAMP WITH TIME ZONE

When using the Oracle thin client with `compare_custom_query`, columns of type `TIMESTAMP WITH TIME ZONE` lose timezone information in the result set. The thin driver returns them as without timezone context.

**Workaround:** Explicitly cast such columns to `TIMESTAMP` in your custom query:

```python
# Instead of:
source_query = """
    select order_id, created_at, amount
    from orders
    where status = 'completed'
"""

# Do this:
source_query = """
    select 
        order_id, 
        cast(created_at at time zone 'Europe/Paris' as timestamp) as created_at,
        amount
    from orders
    where status = 'completed'
"""