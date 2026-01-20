import pytest
import pandas as pd
import numpy as np
import time
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.utils import (
    compare_dataframes,
    prepare_dataframe,
    cross_fill_missing_dates,
    ComparisonStats,
    ComparisonDiffDetails,
    validate_dataframe_size,
    get_dataframe_size_gb,
    clean_recently_changed_data,
    generate_comparison_sample_report
)


class TestUtils:
    """Unit tests for utility functions"""
    
    def test_prepare_dataframe_basic(self):
        """Test basic dataframe preparation with null handling"""
        df = pd.DataFrame({
            'col1': [1, 2, np.nan, 4],
            'col2': ['a', ' ', None, 'd'],
            'col3': [1.0, 2.5, 3.0, 4.0]
        })
        
        result = prepare_dataframe(df)
        
        assert result.shape == df.shape
        assert result['col1'].iloc[2] == 'N/A'
        assert result['col2'].iloc[1] == 'N/A'
        assert result['col2'].iloc[2] == 'N/A'
        assert all(result.dtypes == 'object')
    
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
        
        assert stats.total_source_rows == 3
        assert stats.total_target_rows == 3
        assert stats.common_pk_rows == 3
        assert stats.total_matched_rows == 3
        assert stats.final_diff_score == pytest.approx(0.0, rel=1e-5)
    
    @pytest.mark.parametrize("df1_ids, df2_ids, expected_score", [
        ([1, 2, 3], [1, 2, 3], 0.0),  # Идентичные
        ([1, 2, 3], [1, 2, 4], 15.0), # Разные ключи
        ([1, 2, 3], [1, 2, 3], 0.0),  # Идентичные значения
    ])
    def test_compare_dataframes_parametrized(self, df1_ids, df2_ids, expected_score):
        """Parametrized test for dataframe comparison"""
        df1 = pd.DataFrame({
            'id': df1_ids,
            'value': ['A', 'B', 'C']
        })
        
        df2 = pd.DataFrame({
            'id': df2_ids,
            'value': ['A', 'B', 'C' if df1_ids == df2_ids else 'X']
        })
        
        stats, details = compare_dataframes(df1, df2, ['id'])
        assert stats.final_diff_score == pytest.approx(expected_score, rel=1e-5)
    
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
        
        assert stats.common_pk_rows == 3
        expected_score = (2/3) * 100 * 0.5
        assert stats.final_diff_score == pytest.approx(expected_score, rel=1e-5)
        assert len(details.discrepancies_per_col_examples) == 3
    
    def test_compare_dataframes_empty(self):
        """Test comparison with empty dataframes"""
        df1 = pd.DataFrame({'id': [], 'name': []})
        df2 = pd.DataFrame({'id': [], 'name': []})
        
        stats, details = compare_dataframes(df1, df2, ['id'], 3)
        assert stats is None
        assert details is None
    
    def test_compare_dataframes_missing_columns_raises(self):
        """Test comparison with missing key columns raises error"""
        df1 = pd.DataFrame({'id': [1], 'name': ['Alice']})
        df2 = pd.DataFrame({'name': ['Alice']})  # Missing id column
        
        with pytest.raises(ValueError, match="Key columns missing in target"):
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
        
        assert len(result1) == 3
        assert len(result2) == 3
        assert result1['cnt'].sum() == 30
        assert result2['cnt'].sum() == 40
    
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
        df2.loc[10:15, 'value'] = 'modified'
        
        start_time = time.time()
        stats, details = compare_dataframes(df1, df2, ['id'])
        execution_time = time.time() - start_time
        
        assert execution_time < 1.0  # Should complete in <1 second
        assert stats.final_diff_score == pytest.approx(0.03, rel=1e-5)
    
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
        
        start_time = time.time()
        stats, details = compare_dataframes(df1, df2, ['id'])
        execution_time = time.time() - start_time
        
        assert execution_time < 5.0  # Should complete in <5 seconds
        assert stats.final_diff_score > 0.0
        assert stats.final_diff_score < 0.1
    
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
        
        assert stats.common_pk_rows == 2  # (1,a) and (2,a)
        assert stats.only_source_rows == 1  # (1,b)
        assert stats.only_target_rows == 1  # (2,b)
        expected_score = 50.0 * 0.15 + 50.0 * 0.15
        assert stats.final_diff_score == pytest.approx(expected_score, rel=1e-5)
    
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
        assert stats.final_diff_score == pytest.approx(7.5, rel=1e-5)
    
    def test_get_dataframe_size_gb(self):
        """Test dataframe size calculation"""
        df = pd.DataFrame({'col': range(1000)})
        size_gb = get_dataframe_size_gb(df)
        
        assert size_gb > 0.0
        assert size_gb < 0.1
    
    def test_validate_dataframe_size_raises_on_exceed(self):
        """Test validation raises exception when size exceeds limit"""
        df = pd.DataFrame({'col': range(10_000_000)})  # Large dataframe
        
        with pytest.raises(ValueError, match="DataFrame size.*exceeds limit"):
            validate_dataframe_size(df, max_size_gb=0.01)  # 10MB limit
    
    def test_clean_recently_changed_data(self):
        """Test cleaning recently changed data"""
        df1 = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'value': ['A', 'B', 'C', 'D'],
            'xrecently_changed': ['y', 'n', 'y', 'n']
        })
        
        df2 = pd.DataFrame({
            'id': [1, 2, 3, 5],
            'value': ['A', 'B', 'X', 'E'],
            'xrecently_changed': ['n', 'y', 'n', 'n']
        })
        
        df1_clean, df2_clean = clean_recently_changed_data(df1, df2, ['id'])
        
        # IDs with 'y' in either dataframe should be removed
        assert 2 not in df1_clean['id'].values  # 'y' in df2
        assert 3 not in df1_clean['id'].values  # 'y' in df1
        assert 1 not in df1_clean['id'].values  # 'y' in df1
        assert 'xrecently_changed' not in df1_clean.columns


class TestModels:
    """Unit tests for models module"""
    
    def test_data_reference_validation(self):
        """Test DataReference validation"""
        from src.models import DataReference
        
        # Valid names
        ref1 = DataReference("table_name", "schema_name")
        assert ref1.name == "table_name"
        assert ref1.schema == "schema_name"
        assert ref1.full_name == "schema_name.table_name"
        
        # Invalid names should raise
        with pytest.raises(ValueError):
            DataReference("table-name", "schema")  # hyphen
        
        with pytest.raises(ValueError):
            DataReference("table", "schema.name")  # dot in schema
    
    def test_dbms_type_from_engine(self):
        """Test DBMS type detection from engine"""
        from src.models import DBMSType
        from sqlalchemy import create_engine
        
        # Test Oracle
        oracle_engine = create_engine("oracle+oracledb://user:pass@host/service")
        assert DBMSType.from_engine(oracle_engine) == DBMSType.ORACLE
        
        # Test PostgreSQL
        pg_engine = create_engine("postgresql://user:pass@host/db")
        assert DBMSType.from_engine(pg_engine) == DBMSType.POSTGRESQL


@pytest.fixture
def sample_dataframe():
    """Fixture providing sample dataframe for tests"""
    return pd.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
        'score': [85.5, 92.0, 78.5, 95.0]
    })


def test_with_fixture(sample_dataframe):
    """Test using pytest fixture"""
    assert len(sample_dataframe) == 4
    assert list(sample_dataframe.columns) == ['id', 'name', 'score']