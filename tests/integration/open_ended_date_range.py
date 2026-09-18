"""Shared assertions for one-sided date_range without chunking."""

from xoverrr.constants import CHECK_FAILED, CHECK_SUCCESS
from xoverrr.core import DataQualityChecker, DataReference

# 2024 leap year: Jan-Jun = 182 days, Jul-Dec = 183 days.
START_ONLY_RANGE = ('2024-07-01', None)
END_ONLY_RANGE = (None, '2024-06-30')
START_ONLY_ROWS = 183
END_ONLY_ROWS = 182


def assert_open_ended_date_range_without_chunking(
    engine, source_name: str, target_name: str, schema: str = 'test'
) -> None:
    checker = DataQualityChecker(
        source_engine=engine,
        target_engine=engine,
        timezone='UTC',
    )
    source_ref = DataReference(source_name, schema)
    target_ref = DataReference(target_name, schema)

    cases = [
        (START_ONLY_RANGE, START_ONLY_ROWS, 1),
        (END_ONLY_RANGE, END_ONLY_ROWS, 2),
    ]
    for date_range, expected_rows, expected_name_issues in cases:
        total = checker.check_total_counts(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=date_range,
            tolerance_pct=0.0,
        )
        daily = checker.check_counts_group_by_date(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            date_range=date_range,
            tolerance_pct=0.0,
        )
        samples = checker.check_samples(
            source_table=source_ref,
            target_table=target_ref,
            date_column='created_at',
            update_column='updated_at',
            date_range=date_range,
            tolerance_pct=0.0,
        )

        assert total.status == CHECK_SUCCESS
        assert daily.status == CHECK_SUCCESS
        assert samples.status == CHECK_FAILED
        assert total.stats.total_source_rows == expected_rows
        assert total.stats.total_target_rows == expected_rows
        assert daily.stats.total_source_rows == expected_rows
        assert daily.stats.total_target_rows == expected_rows
        assert samples.stats.total_source_rows == expected_rows
        assert samples.stats.total_target_rows == expected_rows
        assert 'chunks processed' not in total.report
        assert 'chunks processed' not in daily.report
        assert 'chunks processed' not in samples.report
        mismatch = samples.details.issue_breakdown.set_index('column_name')
        assert int(mismatch.loc['name', 'issue_count']) == expected_name_issues
