import sys
import os
import unittest
import pandas as pd
import numpy as np
import time
from utils import (
    compare_dataframes,
    prepare_dataframe,
    cross_fill_missing_dates,
    ComparisonStats,
    ComparisonDiffDetails,
    validate_dataframe_size,
    get_dataframe_size_gb
)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

class TestUtils(unittest.TestCase):

    def test_prepare_dataframe_basic(self):
        """Test basic dataframe preparation with null handling"""
        df = pd.DataFrame({
            'col1': [1, 2, np.nan, 4],
            'col2': ['a', ' ', None, 'd'],
            'col3': [1.0, 2.5, 3.0, 4.0]
        })

        result = prepare_dataframe(df)

        self.assertEqual(result.shape, df.shape)
        self.assertEqual(result['col1'].iloc[2], 'N/A')
        self.assertEqual(result['col2'].iloc[1], 'N/A')
        self.assertEqual(result['col2'].iloc[2], 'N/A')
        self.assertTrue(all(result.dtypes == 'object'))

    def test_compare_dataframes_identical(self):
        """Test comparison of identical dataframes"""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })

        df2 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })

        stats, details = compare_dataframes(df1, df2, ['id'], 3)

        self.assertEqual(stats.total_source_rows, 3)
        self.assertEqual(stats.total_target_rows, 3)
        self.assertEqual(stats.common_pk_rows, 3)
        self.assertEqual(stats.total_matched_rows, 3)
        self.assertAlmostEqual(stats.final_diff_score, 0.0, places=5)

    def test_compare_dataframes_different_values(self):
        """Test comparison with different values"""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })

        df2 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Robert', 'Charlie'],
            'age': [25, 31, 36]
        })

        stats, details = compare_dataframes(df1, df2, ['id'], 3)

        self.assertEqual(stats.common_pk_rows, 3)
        # Expected: 2 mismatched rows out of 3 common rows = 66.66667%
        # Final score = 66.66667% * 0.5 = 33.33333%
        expected_score = (2/3) * 100 * 0.5
        self.assertAlmostEqual(stats.final_diff_score, expected_score, places=5)
        self.assertEqual(len(details.discrepancies_per_col_examples), 3)

    def test_compare_dataframes_different_keys(self):
        """Test comparison with different primary keys"""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })

        df2 = pd.DataFrame({
            'id': [1, 2, 4],
            'name': ['Alice', 'Bob', 'David']
        })

        stats, details = compare_dataframes(df1, df2, ['id'], 3)

        self.assertEqual(stats.only_source_rows, 1)
        self.assertEqual(stats.only_target_rows, 1)
        self.assertEqual(stats.common_pk_rows, 2)
        # Expected: 1 source-only row (50%) + 1 target-only row (50%) out of 2 common rows
        # Final score = 50% * 0.15 + 50% * 0.15 = 15.0%
        expected_score = 50.0 * 0.15 + 50.0 * 0.15
        self.assertAlmostEqual(stats.final_diff_score, expected_score, places=5)

    def test_compare_dataframes_empty(self):
        """Test comparison with empty dataframes"""
        df1 = pd.DataFrame({'id': [], 'name': []})
        df2 = pd.DataFrame({'id': [], 'name': []})

        stats, details = compare_dataframes(df1, df2, ['id'], 3)
        self.assertIsNone(stats)
        self.assertIsNone(details)

    def test_compare_dataframes_missing_columns(self):
        """Test comparison with missing key columns"""
        df1 = pd.DataFrame({'id': [1], 'name': ['Alice']})
        df2 = pd.DataFrame({'name': ['Alice']})  # Missing id column

        with self.assertRaises(ValueError):
            compare_dataframes(df1, df2, ['id'], 3)

    def test_cross_fill_missing_dates(self):
        """Test cross-filling missing dates"""
        df1 = pd.DataFrame({
            'dt': pd.to_datetime(['2023-01-01', '2023-01-02']),
            'cnt': [10, 20]
        })

        df2 = pd.DataFrame({
            'dt': pd.to_datetime(['2023-01-02', '2023-01-03']),
            'cnt': [15, 25]
        })

        result1, result2 = cross_fill_missing_dates(df1, df2)

        self.assertEqual(len(result1), 3)
        self.assertEqual(len(result2), 3)
        self.assertEqual(result1['cnt'].sum(), 30)
        self.assertEqual(result2['cnt'].sum(), 40)

    def test_get_dataframe_size_gb(self):
        """Test dataframe size calculation"""
        df = pd.DataFrame({'col': range(1000)})
        size_gb = get_dataframe_size_gb(df)

        self.assertGreater(size_gb, 0.0)
        self.assertLess(size_gb, 0.1)

    def test_performance_small_dataframe(self):
        """Performance test for small dataframes"""
        n_records = 10000

        df1 = pd.DataFrame({
            'id': range(n_records),
            'value': [f'text_{i}' for i in range(n_records)],
            'value2': [f'text_{i}' for i in range(n_records)],
            'value3': [f'text_{i}' for i in range(n_records)],
            'value4': [f'text_{i}' for i in range(n_records)],
            'value5': [f'text_{i}' for i in range(n_records)],
            'value6': [f'text_{i}' for i in range(n_records)],
            'value7': [f'text_{i}' for i in range(n_records)],
            'value8': [f'text_{i}' for i in range(n_records)],
            'value9': [f'text_{i}' for i in range(n_records)],
            'value10': [f'text_{i}' for i in range(n_records)],
        })

        df2 = df1.copy()
        # Modify a few records
        df2.loc[10:15, 'value'] = 'modified'

        start_time = time.time()
        stats, details = compare_dataframes(df1, df2, ['id'])
        execution_time = time.time() - start_time

        self.assertLess(execution_time, 1.0)  # Should complete in <1 second
        # Expected: 6 modified rows out of 10000 = 0.06% mismatch
        self.assertAlmostEqual(stats.final_diff_score, 0.03, places=5)

    def test_performance_medium_dataframe(self):
        """Performance test for medium dataframes"""
        n_records = 1000 * 1000

        df1 = pd.DataFrame({
            'id': range(1, n_records + 1),
            'int_col': 1,
            'float_col': np.random.rand(n_records),
            'str_col': [f'text_{i}' for i in range(n_records)],
            'str_col2': [f'text_a_{i}' for i in range(n_records)],
            'str_col3': [f'text_b_{i}' for i in range(n_records)],
            'str_col4': [f'text_c_{i}' for i in range(n_records)],
            'str_col5': [f'text_d_{i}' for i in range(n_records)],
            'str_col6': [f'text_d_{i}' for i in range(n_records)],
            'bool_col': np.random.choice([True, False], n_records)
        })

        df2 = df1.copy()

        # Change few records
        k = 100
        change_indices = np.random.choice(np.arange(1, n_records), size=k, replace=False)
        for idx in change_indices:
            df2.loc[idx, 'float_col'] += 0.1
        df2.loc[change_indices[0], 'str_col'] = 'pink_floyd'

        # Add one record to df2
        new_record = {
            'id': n_records + 1,
            'int_col': 100500,
            'float_col': -0.42,
            'str_col': 'limp_bizkit',
            'str_col2': 'alice_cooper',
            'str_col3': 'rammstein',
            'str_col4': 'him',
            'str_col5': 'nine inch nails',
            'str_col6': 'prodigy',
            'bool_col': True,
        }
        df2 = pd.concat([df2, pd.DataFrame([new_record])], ignore_index=True)

        print(f"Memory df1: {df1.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        print(f"Memory df2: {df2.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

        start_time = time.time()
        stats, details = compare_dataframes(df1, df2, ['id'])
        execution_time = time.time() - start_time
        print(f'{execution_time=}')
        
        self.assertLess(execution_time, 5.0)  # Should complete in <5 seconds
        # Expected: 100 value mismatches + 1 target-only row out of 1M records
        # Very small percentage, but greater than 0
        self.assertGreater(stats.final_diff_score, 0.0)
        self.assertLess(stats.final_diff_score, 0.1)  # Should be very small

    def test_edge_case_all_different(self):
        """Test edge case where all records are different"""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })

        df2 = pd.DataFrame({
            'id': [4, 5, 6],
            'value': ['x', 'y', 'z']
        })

        stats, details = compare_dataframes(df1, df2, ['id'], 3)

        self.assertEqual(stats.only_source_rows, 3)
        self.assertEqual(stats.only_target_rows, 3)
        self.assertEqual(stats.common_pk_rows, 0)
        # Expected: 100% mismatch (all rows are different)
        self.assertAlmostEqual(stats.final_diff_score, 100.0, places=5)

    def test_edge_case_complete_match(self):
        """Test edge case where everything matches perfectly"""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })

        df2 = df1.copy()

        stats, details = compare_dataframes(df1, df2, ['id'], 3)

        self.assertAlmostEqual(stats.final_diff_score, 0.0, places=5)
        self.assertEqual(stats.total_matched_rows, 3)

    def test_compound_primary_key(self):
        """Test comparison with compound primary key"""
        df1 = pd.DataFrame({
            'id1': [1, 1, 2],
            'id2': ['a', 'b', 'a'],
            'value': [10, 20, 30]
        })

        df2 = pd.DataFrame({
            'id1': [1, 2, 2],
            'id2': ['a', 'a', 'b'],
            'value': [10, 30, 40]
        })

        stats, details = compare_dataframes(df1, df2, ['id1', 'id2'])

        self.assertEqual(stats.common_pk_rows, 2)  # (1,a) and (2,a)
        self.assertEqual(stats.only_source_rows, 1)  # (1,b)
        self.assertEqual(stats.only_target_rows, 1)  # (2,b)
        # Expected: 1 source-only (50%) + 1 target-only (50%) out of 2 common rows
        # Final score = 50% * 0.15 + 50% * 0.15 = 15.0%
        expected_score = 50.0 * 0.15 + 50.0 * 0.15
        self.assertAlmostEqual(stats.final_diff_score, expected_score, places=5)

    def test_compound_primary_key_with_duplicates(self):
        """Test comparison with compound primary key and duplicate keys in source data"""
        df1 = pd.DataFrame({
            'id1': [1, 1, 2, 3],
            'id2': ['a', 'a', 'a', 'c'],
            'value': [10, 15, 30, 40]  # Duplicate (1,a) with different values
        })

        df2 = pd.DataFrame({
            'id1': [1, 2, 3],
            'id2': ['a', 'a', 'c'],
            'value': [11, 30, 40]
        })

        stats, details = compare_dataframes(df1, df2, ['id1', 'id2'])
        
        # With duplicates and value mismatches, score should be > 0
        # Duplicate in source contributes to source_dup_percentage
        self.assertGreater(stats.final_diff_score, 0.0)
        # Should be significant due to duplicates
        self.assertGreater(stats.final_diff_score, 10.0)

    def test_compound_primary_key_complex(self):
        """Test complex scenario with compound primary key and various discrepancies"""
        df1 = pd.DataFrame({
            'user_id':    [ 1,   1,   2,   3,   4],
            'session_id': ['A', 'B', 'A', 'A', 'A'],
            'value1': [100, 200, 300, 400, 500],
            'value2': ['X', 'Y', 'Z', 'W', 'V']
        })

        df2 = pd.DataFrame({
            'user_id':    [ 1,   2,   3,   4,   5],
            'session_id': ['A', 'A', 'A', 'B', 'A'],
            'value1': [100, 300, 400, 550, 600],
            'value2': ['X', 'Z', 'W', 'V', 'U']
        })

        stats, details = compare_dataframes(df1, df2, ['user_id', 'session_id'], 3)

        # Verify statistics
        self.assertEqual(stats.total_source_rows, 5)
        self.assertEqual(stats.total_target_rows, 5)
        self.assertEqual(stats.common_pk_rows, 3)  # (1,A), (2,A), (3,A)
        self.assertEqual(stats.only_source_rows, 2)  # (1,B), (4,A)
        self.assertEqual(stats.only_target_rows, 2)  # (4,B), (5,A)
        self.assertEqual(stats.total_matched_rows,3)
        
        # Expected: 2 source-only (66.67%) + 2 target-only (66.67%) out of 3 common rows
        # No value mismatches in common rows
        self.assertAlmostEqual(stats.final_diff_score, 20.0, places=5)

    def test_compound_primary_key_perfect_match(self):
        """Test compound primary key with perfect match"""
        df1 = pd.DataFrame({
            'part1': [1, 1, 2, 2],
            'part2': ['A', 'B', 'A', 'B'],
            'data': ['foo', 'bar', 'baz', 'qux']
        })

        df2 = pd.DataFrame({
            'part1': [1, 1, 2, 2],
            'part2': ['A', 'B', 'A', 'B'],
            'data': ['foo', 'bar', 'baz', 'qux']
        })

        stats, details = compare_dataframes(df1, df2, ['part1', 'part2'], 3)

        self.assertAlmostEqual(stats.final_diff_score, 0.0, places=5)
        self.assertEqual(stats.total_matched_rows, 4)
        self.assertEqual(stats.common_pk_rows, 4)

    def test_compound_primary_key_all_different(self):
        """Test compound primary key with completely different keys"""
        df1 = pd.DataFrame({
            'key1': [1, 2, 3],
            'key2': ['X', 'Y', 'Z'],
            'value': [10, 20, 30]
        })

        df2 = pd.DataFrame({
            'key1': [4, 5, 6],
            'key2': ['A', 'B', 'C'],
            'value': [40, 50, 60]
        })

        stats, details = compare_dataframes(df1, df2, ['key1', 'key2'], 3)

        self.assertEqual(stats.only_source_rows, 3)
        self.assertEqual(stats.only_target_rows, 3)
        self.assertEqual(stats.common_pk_rows, 0)
        # Expected: 100% mismatch
        self.assertAlmostEqual(stats.final_diff_score, 100.0, places=5)

    def test_compound_primary_key_partial_overlap(self):
        """Test compound primary key with partial overlap and value discrepancies"""
        df1 = pd.DataFrame({
            'id': [1, 1, 2, 3, 4],
            'type': ['A', 'B', 'A', 'A', 'B'],
            'amount': [100.0, 200.0, 300.0, 400.0, 500.0],
            'status': ['active', 'inactive', 'active', 'pending', 'active']
        })

        df2 = pd.DataFrame({
            'id': [1, 1, 2, 3, 5],
            'type': ['A', 'B', 'A', 'A', 'A'],
            'amount': [100.0, 250.0, 300.0, 450.0, 600.0],
            'status': ['active', 'inactive', 'active', 'completed', 'active']
        })

        stats, details = compare_dataframes(df1, df2, ['id', 'type'], 3)

        # Verify key statistics
        self.assertEqual(stats.common_pk_rows, 4)  # (1,A), (1,B), (2,A), (3,A)
        self.assertEqual(stats.only_source_rows, 1)  # (4,B)
        self.assertEqual(stats.only_target_rows, 1)  # (5,A)

        # Should have discrepancies in values for some common keys
        # Expected: 1 source-only (25%) + 1 target-only (25%) + value mismatches
        self.assertGreater(stats.final_diff_score, 10.0)
        self.assertLess(stats.final_diff_score, 50.0)

    def test_duplicate_primary_keys_in_source(self):
        """Test handling of duplicate primary keys within source dataframe"""
        df1 = pd.DataFrame({
            'pk': [1, 1, 2, 3],  # Duplicate PK=1
            'value': ['A', 'B', 'C', 'D']
        })

        df2 = pd.DataFrame({
            'pk': [1, 2, 3, 4],
            'value': ['A', 'C', 'D', 'E']
        })
       

        stats, details = compare_dataframes(df1, df2, ['pk'], 3)
        # With duplicates and only target rows
        self.assertAlmostEqual(stats.final_diff_score, 7.5, places = 5)

    def test_duplicate_primary_keys_in_target(self):
        """Test handling of duplicate primary keys within target dataframe"""
        df1 = pd.DataFrame({
            'pk': [1, 2, 3, 4],
            'value': ['A', 'C', 'D', 'E']
        })

        df2 = pd.DataFrame({
            'pk': [1, 1, 2, 3],  # Duplicate PK=1
            'value': ['A', 'B', 'C', 'D']
        })

        stats, details = compare_dataframes(df1, df2, ['pk'], 3)

        self.assertAlmostEqual(stats.final_diff_score, 7.5,places = 5)

    def test_duplicate_compound_primary_keys(self):
        """Test handling of duplicate compound primary keys"""
        df1 = pd.DataFrame({
            'key1': [1, 1, 1, 2],
            'key2': ['A', 'A', 'B', 'A'],  # Duplicate (1,A)
            'value': [10, 20, 30, 40]
        })

        df2 = pd.DataFrame({
            'key1': [1, 1, 2, 3],
            'key2': ['A', 'B', 'A', 'A'],
            'value': [10, 30, 40, 50]
        })

        stats, details = compare_dataframes(df1, df2, ['key1', 'key2'], 3)
        
        # With duplicates and only trg rows mismatches
        # 1/4*0.1 + 1/3 * 0.15
        self.assertAlmostEqual(stats.final_diff_score, 7.5, places=5)


if __name__ == '__main__':
    unittest.main(verbosity=2)
    # or from shell
    #python3 -m unittest run_unit_tests.TestUtils.test_compare_dataframes_different_values -v