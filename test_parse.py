#!/usr/bin/env python3
# test_parse.py - тестовый скрипт для проверки формата patch.txt

import re

def test_parse():
    print("Тестирование парсинга patch.txt")
    print("="*60)
    
    with open('patch.txt', 'r', encoding='utf-8') as f:
        content = f.read()
    
    print(f"Общий размер файла: {len(content)} символов")
    print(f"Первые 1000 символов:")
    print("-"*60)
    print(content[:1000])
    print("-"*60)
    
    # Ищем все строки с "ФАЙЛ:"
    print("\nПоиск всех вхождений 'ФАЙЛ:' в файле:")
    lines = content.split('\n')
    file_lines = []
    
    for i, line in enumerate(lines):
        if 'ФАЙЛ:' in line:
            file_lines.append((i, line))
            print(f"Строка {i:4}: {line[:100]}...")
    
    print(f"\nНайдено строк с 'ФАЙЛ:': {len(file_lines)}")
    
    # Ищем разделители
    print("\nПоиск разделителей (===):")
    separator_lines = []
    for i, line in enumerate(lines):
        if '==' in line and len(line.strip()) > 20:
            separator_lines.append((i, line))
            if len(separator_lines) <= 5:
                print(f"Строка {i:4}: {line[:50]}...")
    
    print(f"Найдено строк с разделителями: {len(separator_lines)}")
    
    # Ищем паттерн "ФАЙЛ: path" между разделителями
    print("\nПоиск структуры патчей:")
    for i in range(len(lines)):
        if i < len(lines) - 2:
            # Проверяем, есть ли разделитель, затем ФАЙЛ:, затем разделитель
            if ('==' in lines[i] and len(lines[i].strip()) > 20 and
                'ФАЙЛ:' in lines[i+1] and
                '==' in lines[i+2] and len(lines[i+2].strip()) > 20):
                print(f"\nНайден патч на строке {i}:")
                print(f"  Строка {i}: {lines[i][:50]}...")
                print(f"  Строка {i+1}: {lines[i+1]}")
                print(f"  Строка {i+2}: {lines[i+2][:50]}...")
                
                # Извлекаем путь к файлу
                file_line = lines[i+1]
                file_path = file_line.replace('ФАЙЛ:', '').strip()
                print(f"  Путь к файлу: {file_path}")

def extract_patches():
    """Попытка извлечь патчи вручную"""
    print("\n" + "="*60)
    print("Попытка извлечь патчи:")
    print("="*60)
    
    with open('patch.txt', 'r', encoding='utf-8') as f:
        content = f.read()
    
    lines = content.split('\n')
    patches = []
    current_patch = None
    current_content = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Ищем начало патча
        if '==' in line and len(line.strip()) > 20:
            # Проверяем следующие строки
            if i + 1 < len(lines) and 'ФАЙЛ:' in lines[i + 1]:
                # Сохраняем предыдущий патч, если есть
                if current_patch:
                    patch_text = '\n'.join(current_content).strip()
                    if patch_text:
                        patches.append((current_patch, patch_text))
                    
                # Начинаем новый патч
                file_path = lines[i + 1].replace('ФАЙЛ:', '').strip()
                current_patch = file_path
                current_content = []
                
                # Пропускаем разделитель и строку с ФАЙЛ:
                i += 2
                continue
        
        # Собираем содержимое
        if current_patch is not None:
            # Проверяем, не начинается ли новый патч
            if i < len(lines) - 2:
                if ('==' in lines[i] and len(lines[i].strip()) > 20 and
                    i + 1 < len(lines) and 'ФАЙЛ:' in lines[i + 1]):
                    # Это начало нового патча
                    continue
                else:
                    current_content.append(line)
        
        i += 1
    
    # Сохраняем последний патч
    if current_patch:
        patch_text = '\n'.join(current_content).strip()
        if patch_text:
            patches.append((current_patch, patch_text))
    
    # Выводим результаты
    print(f"\nНайдено патчей: {len(patches)}")
    for i, (file_path, patch_text) in enumerate(patches[:5]):  # Показываем первые 5
        print(f"\nПатч {i+1}:")
        print(f"  Файл: {file_path}")
        print(f"  Размер: {len(patch_text)} символов")
        print(f"  Первые 200 символов:")
        print("  " + "-"*40)
        print(f"  {patch_text[:200]}...")
    
    if len(patches) > 5:
        print(f"\n... и еще {len(patches) - 5} патчей")

if __name__ == "__main__":
    test_parse()
    extract_patches()