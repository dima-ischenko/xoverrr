### xoverrr (pronounced "crossover")
Designed for comparing data between sources, with detailed analysis and discrepancy reporting.

### Key Features
- **Multiple DBMS Support**: Oracle, PostgreSQL (+ GreenPlum), ClickHouse (extensible list via adapter layer) -- tables/views
- **Connection Versatility**: Requires passing SQLAlchemy Engine objects for the source and target databases to work with connections.
- **Comparison Strategies**: Data comparison via sampling, count-only comparison with daily aggregates, and fully custom (raw) SQL queries.
- **Smart Analysis**:
  * Excludes "recent" data to mitigate replication lag, for example.
  * Auto-detection of primary keys and column types from the DBMS metadata catalog (PK must be found on at least one side, or you can specify your own via parameter).
  * Application-side type conversion.
  * Auto-exclusion of columns with mismatched names from comparison.
- **Optimization**: Two samples of 1 million rows with 10 columns each (each 330 MB), compared in 3 seconds (inter core i5/16GB).
- **Detailed Reporting**: In-depth analysis of column discrepancies, output with examples (column view/record view).
- **Flexible Configuration**: Field exclusion, tolerance thresholds, ability to specify a "custom" primary key.
- **Unit Tests**: For comparison methods, functional, and performance.

### Sample Report
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

        SELECT created_at, updated_at, id, code, bank_code, account_type, counterparty_id, special_code, case when updated_at > (sysdate - :exclude_recent_hours) then 'y' end as xrecently_changed
        FROM stage.account
        WHERE 1=1
            AND created_at >= trunc(to_date(:start_date, 'YYYY-MM-DD'), 'dd')
            AND created_at < trunc(to_date(:end_date, 'YYYY-MM-DD'), 'dd') + 1

    params: {'exclude_recent_hours': 0.041666666666666664, 'start_date': '2025-11-17', 'end_date': '2025-11-24'}
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

### Weighted Metrics Calculation
**Final Score Formula**: `100 - final_diff_score`
- **final_diff_score** = (source_dup% × 0.1) + (target_dup% × 0.1) + (source_only_rows% × 0.15) + (target_only_rows% × 0.15) + (rows_mismatched_by_any_column% × 0.5)
- Scores 0-100%, higher = better data quality.

## Comparison Methods

### 1. Data Sample Comparison (`compare_sample`) suitable when you need to compare by a set of rows and column values, over a date range

Compares data by a set of rows and column values over a specified date range.

```python
status, report, stats, details = comparator.compare_sample(
    source_table=DataReference("table_name", "schema_name"),
    target_table=DataReference("table_name", "schema_name"),
    date_column="created_at",
    update_column="modified_date",
    date_range=("2024-01-01", "2024-01-31"),
    exclude_columns=["audit_timestamp", "internal_id"],
    custom_primary_key=["id", "user_id"],
    tolerance_percentage=1.0,
    exclude_recent_hours=24,
    max_examples=3
)
```

**Parameters:**
- `source_table`, `target_table` - names of the compared tables or views
- `date_column` - column for filtering by date range
- `update_column` - column for identifying "recent" data (excluded from comparison on both sides)
- `date_range` - tuple `(start_date, end_date)` in "YYYY-MM-DD" format
- `exclude_columns` - list of columns to exclude from comparison
- `custom_primary_key` - custom primary key (if not specified, detected automatically)
- `tolerance_percentage` - acceptable discrepancy percentage (0.0-100.0)
- `exclude_recent_hours` - exclude data modified within the last N hours
- `max_examples` - maximum number of discrepancy examples included in the report

### 2. Count Comparison (`compare_counts`), for efficient processing of large volumes (over a large date range) and locating missing rows or duplicates

Compares daily record count aggregates. Efficient for large data volumes.

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
- `source_table`, `target_table` - references to the compared tables/views
- `date_column` - column for daily grouping
- `date_range` - date range for analysis
- `tolerance_percentage` - acceptable discrepancy percentage
- `max_examples` - maximum number of daily discrepancy examples included in the report

### 3. Custom Query Comparison (`compare_custom_query`)

Compares data from arbitrary SQL queries. Suitable for complex scenarios.

```python
status, report, stats, details = comparator.compare_custom_query(
    source_query="SELECT id as user_id, name as user_name, created_at as created_date FROM scott.source_table WHERE status = 'active'",
    source_params={},
    target_query="SELECT user_id, user_name, created_date FROM scott.target_table WHERE status = :status",
    target_params={'status': 'active'},
    custom_primary_key=["id"],
    exclude_columns=["internal_code"],
    tolerance_percentage=0.5,
    max_examples=3
)
```

**Parameters:**
- `source_query`, `target_query` - SQL queries for sources (support parameterization)
- `source_params`, `target_params` - parameters for the queries
- `custom_primary_key` - mandatory parameter, list of columns
- `exclude_columns` - columns to exclude from comparison
- `tolerance_percentage` - acceptable discrepancy percentage
- `max_examples` - maximum number of discrepancy examples included in the report
- To automatically exclude recently modified records, add to the SELECT query in the `compare_custom_query` method:
```sql
case when updated_at > (sysdate - 3/24) then 'y' end as xrecently_changed
```

**Automatic Primary Key Detection:**
- If `custom_primary_key` is not specified, the system automatically detects the PK from metadata.
- If source and target have different PKs, the source PK will be used with a warning.

**Performance:**
- DataFrame size check (hard limit of 3GB per sample)
- Efficient comparison via XOR properties
- Configurable limits via constants

**Return Values:**
All methods return a tuple:
- `status` - comparison status (`COMPARISON_SUCCESS`/`COMPARISON_FAILED`/`COMPARISON_SKIPPED`)
- `report` - text report detailing discrepancies
- `stats` - `ComparisonStats` object with comparison statistics, a dataclass instance
- `details` - `ComparisonDiffDetails` object with discrepancy details and examples, a dataclass instance

### Status Types
- **COMPARISON_SUCCESS**: Comparison passed within tolerance limits
- **COMPARISON_FAILED**: Discrepancies exceed tolerance threshold or technical error occurred
- **COMPARISON_SKIPPED**: No data to compare (both tables are empty)

### Convenient Logging
Structured logging with timings:
```
2024-01-15 10:30:45 - INFO - xoverrr.core._compare_samples - Query executed in 2.34s
2024-01-15 10:30:46 - INFO - xoverrr.core._compare_samples - Source: 150000 rows, Target: 149950 rows
2024-01-15 10:30:47 - INFO - xoverrr.utils.compare_dataframes - Comparison completed in 1.2s
```

### Tolerance Percentage
- **tolerance_percentage**: Threshold for acceptable discrepancies (0.0-100.0)
- If final_diff_score > tolerance: status = COMPARISON_FAILED
- If final_diff_score ≤ tolerance: status = COMPARISON_SUCCESS
- Allows configuring an acceptable level of discrepancies

### Usage Examples
**Sample Comparison:**
```python
from xoverrr import DataQualityComparator, DataReference, COMPARISON_SUCCESS, COMPARISON_FAILED, COMPARISON_SKIPPED


def create_src_engine():

    connection_string = 'tbd'
    return engine

def create_trg_engine():

    connection_string ='tbd'
    engine = create_engine(connection_string)

    return engine

src_engine, trg_engine = create_src_engine(), create_trg_engine()

comparator = DataQualityComparator(
      source_engine=src_engine,
      target_engine=trg_engine,
      timezone='Europe/Athens'
  )


source = DataReference("users", "schema1")
target = DataReference("users", "schema2")

status, report, stats, details = comparator.compare_sample(
    source,
    target,
    date_column="created_at",
    update_column="modified_date",
    exclude_columns=["audit_timestamp", "internal_id"],
    exclude_recent_hours=24,
    tolerance_percentage=0
)
print(report)
if status == COMPARISON_FAILED:
    raise Exception(f"Sample check failed")

```

### Running Unit Tests from Project Directory
**Selectively:**
```
python -m unittest run_unit_tests.TestUtils.test_compound_primary_key
```

**Full Suite:**
```
python -m unittest run_unit_tests.TestUtils
```