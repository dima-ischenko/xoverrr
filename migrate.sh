#!/bin/bash
# simple_rename_tests.sh
# Простое переименование файлов тестов

set -e

echo "=== Простое переименование файлов тестов ==="

# Переименовываем cross_db тесты
rename_files_in_dir() {
    local dir="$1"
    echo "Обрабатываем: $dir"
    
    find "$dir" -name "test_*.py" | while IFS= read -r file; do
        dir_name=$(basename "$(dirname "$file")")
        old_name=$(basename "$file")
        
        # Создаем новое имя
        new_name=""
        if [[ "$dir_name" == "clickhouse_oracle" ]]; then
            new_name=$(echo "$old_name" | sed 's/test_clickhouse_oracle_/test_ch_ora_/')
        elif [[ "$dir_name" == "clickhouse_postgres" ]]; then
            new_name=$(echo "$old_name" | sed 's/test_clickhouse_postgres_/test_ch_pg_/')
        elif [[ "$dir_name" == "oracle_postgres" ]]; then
            new_name=$(echo "$old_name" | sed 's/test_oracle_postgres_/test_ora_pg_/')
        else
            new_name="$old_name"
        fi
        
        if [[ "$new_name" != "$old_name" ]]; then
            echo "  $old_name -> $new_name"
            mv "$file" "$(dirname "$file")/$new_name"
        fi
    done
}

# Обрабатываем все директории
rename_files_in_dir "tests/integration/cross_db/clickhouse_oracle"
rename_files_in_dir "tests/integration/cross_db/clickhouse_postgres"
rename_files_in_dir "tests/integration/cross_db/oracle_postgres"

# Также переименовываем self_db тесты
find "tests/integration/self_db" -name "test_*.py" | while IFS= read -r file; do
    old_name=$(basename "$file")
    new_name=$(echo "$old_name" | sed -e 's/test_//' -e 's/_identical//' -e 's/_complex//' -e 's/_table_vs_table//' -e 's/_table_vs_view//')
    new_name="test_${new_name}"
    
    if [[ "$new_name" != "$old_name" ]]; then
        echo "  $old_name -> $new_name"
        mv "$file" "$(dirname "$file")/$new_name"
    fi
done

echo "=== Переименование завершено ==="
echo ""
echo "Новые имена файлов:"
find "tests/integration" -name "test_*.py" | head -20

echo ""
echo "Примеры новых имен:"
echo "- test_ch_ora_sample_identical.py (было test_clickhouse_oracle_sample_identical.py)"
echo "- test_ora_pg_sample_identical_boolean.py (было test_oracle_postgres_sample_identical_boolean.py)"
echo "- test_ch_pg_count_identical.py (было test_clickhouse_postgres_count_identical.py)"