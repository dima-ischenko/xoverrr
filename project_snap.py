import os
import re
import fnmatch
from pathlib import Path
from typing import Set, List, Pattern, Optional, Dict
import argparse

class GitIgnorePatterns:
    """Класс для обработки шаблонов в стиле .gitignore"""

    @staticmethod
    def pattern_to_regex(pattern: str) -> Pattern:
        """
        Конвертирует .gitignore-like паттерн в регулярное выражение
        """
        # Убираем пробелы в начале/конце
        pattern = pattern.strip()

        # Игнорируем комментарии и пустые строки
        if not pattern or pattern.startswith('#'):
            return re.compile(r'(?!)')  # Ничего не матчит

        # Экранируем специальные символы regex, кроме *
        pattern = re.escape(pattern)

        # Заменяем экранированные * на .*
        pattern = pattern.replace(r'\*', '.*')

        # Обработка паттернов начинающихся с /
        if pattern.startswith(r'\/'):
            pattern = pattern[2:]  # Убираем \/
        else:
            pattern = '.*' + pattern  # Матчит в любой директории

        # Обработка паттернов заканчивающихся на /
        if pattern.endswith(r'\/'):
            pattern = pattern[:-2] + '(/.*)?'

        # Добавляем якоря
        pattern = f'^{pattern}$'

        return re.compile(pattern)

    @staticmethod
    def should_ignore(path: Path, patterns: List[str], base_dir: Path) -> bool:
        """
        Проверяет, должен ли путь быть проигнорирован на основе шаблонов
        """
        relative_path = str(path.relative_to(base_dir))

        for pattern_str in patterns:
            pattern_regex = GitIgnorePatterns.pattern_to_regex(pattern_str)
            if pattern_regex.match(relative_path):
                return True

        return False

class ProjectSnapshot:
    def __init__(self, root_dir: str, output_file: str):
        self.root_dir = Path(root_dir).resolve()
        self.output_file = output_file

        # Дефолтные настройки
        self.default_whitelist = ['*.sql', '*.py', '*.js', '*.html', '*.md', '*.json', '*.yml', '*.toml']
        self.default_blacklist = ['python3.13','__pycache__', '.pytest_cache', '.git', 'node_modules', '.venv', '*.pyc']

    def should_include_file(self, file_path: Path,
                          whitelist: Optional[List[str]] = None,
                          blacklist: Optional[List[str]] = None) -> bool:
        """
        Определяет, нужно ли включать файл на основе whitelist/blacklist
        """
        if whitelist is None:
            whitelist = self.default_whitelist
        if blacklist is None:
            blacklist = self.default_blacklist

        relative_path = str(file_path.relative_to(self.root_dir))

        # Сначала проверяем blacklist
        for pattern in blacklist:
            if fnmatch.fnmatch(relative_path, pattern) or fnmatch.fnmatch(file_path.name, pattern):
                return False

        # Если whitelist пустой, включаем все (кроме blacklist)
        if not whitelist:
            return True

        # Проверяем whitelist
        for pattern in whitelist:
            if fnmatch.fnmatch(relative_path, pattern) or fnmatch.fnmatch(file_path.name, pattern):
                return True

        return False

    def should_include_dir(self, dir_path: Path,
                         blacklist: Optional[List[str]] = None) -> bool:
        """
        Определяет, нужно ли включать директорию
        """
        if blacklist is None:
            blacklist = self.default_blacklist

        relative_path = str(dir_path.relative_to(self.root_dir))

        for pattern in blacklist:
            if fnmatch.fnmatch(relative_path, pattern) or fnmatch.fnmatch(dir_path.name, pattern):
                return False

        return True

    def load_patterns_from_file(self, file_path: str) -> List[str]:
        """Загружает паттерны из файла (как .gitignore)"""
        patterns = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        patterns.append(line)
        except FileNotFoundError:
            pass
        return patterns

    def get_filtered_structure(self, whitelist: List[str], blacklist: List[str]) -> Dict[Path, List[Path]]:
        """
        Возвращает отфильтрованную структуру проекта: {директория: [файлы]}
        """
        structure = {}

        for root, dirs, files in os.walk(self.root_dir):
            root_path = Path(root)

            # Фильтруем директории для обхода
            dirs[:] = [d for d in dirs
                      if self.should_include_dir(root_path / d, blacklist)]

            # Получаем файлы, которые прошли фильтрацию
            included_files = []
            for file in files:
                file_path = root_path / file
                if self.should_include_file(file_path, whitelist, blacklist):
                    included_files.append(file_path)

            # Добавляем директорию в структуру только если в ней есть файлы
            if included_files:
                structure[root_path] = included_files

        return structure

    def has_files_in_subtree(self, dir_path: Path, whitelist: List[str], blacklist: List[str]) -> bool:
        """
        Проверяет, есть ли в поддереве директории файлы, которые прошли фильтрацию
        """
        for root, dirs, files in os.walk(dir_path):
            root_path = Path(root)

            # Фильтруем директории для обхода
            dirs[:] = [d for d in dirs
                      if self.should_include_dir(root_path / d, blacklist)]

            for file in files:
                file_path = root_path / file
                if self.should_include_file(file_path, whitelist, blacklist):
                    return True

        return False

    def get_directory_tree(self, whitelist: List[str], blacklist: List[str]) -> List[Path]:
        """
        Возвращает список директорий, которые содержат файлы или поддиректории с файлами
        """
        relevant_dirs = set()

        # Сначала находим все директории с файлами
        structure = self.get_filtered_structure(whitelist, blacklist)
        for dir_path in structure.keys():
            # Добавляем все родительские директории, включая корневую
            current = dir_path
            while current != self.root_dir.parent:  # Изменено условие
                relevant_dirs.add(current)
                current = current.parent

        # Добавляем корневую директорию, если в ней есть файлы
        if self.root_dir in structure:
            relevant_dirs.add(self.root_dir)

        return sorted(relevant_dirs, key=lambda x: len(x.parts))

    def create_snapshot(self,
                       whitelist: Optional[List[str]] = None,
                       blacklist: Optional[List[str]] = None,
                       whitelist_file: Optional[str] = None,
                       blacklist_file: Optional[str] = None):
        """
        Создает снимок проекта с поддержкой whitelist/blacklist
        """
        # Загружаем паттерны из файлов если указаны
        final_whitelist = whitelist or self.default_whitelist.copy()
        final_blacklist = blacklist or self.default_blacklist.copy()

        if whitelist_file:
            final_whitelist.extend(self.load_patterns_from_file(whitelist_file))

        if blacklist_file:
            final_blacklist.extend(self.load_patterns_from_file(blacklist_file))

        with open(self.output_file, 'w', encoding='utf-8') as f:
            self._write_header(f)
            self._write_structure(f, final_whitelist, final_blacklist)
            self._write_contents(f, final_whitelist, final_blacklist)

    def _write_header(self, file_obj):
        """Записывает заголовок файла"""
        file_obj.write(f"СНИМОК ПРОЕКТА: {self.root_dir}\n")
        file_obj.write(f"СОЗДАН: {os.path.basename(self.output_file)}\n")
        file_obj.write("=" * 60 + "\n\n")

    def _write_structure(self, file_obj, whitelist, blacklist):
        """Записывает структуру проекта с учетом фильтрации"""
        file_obj.write("СТРУКТУРА ПРОЕКТА (отфильтрованная):\n")
        file_obj.write("-" * 50 + "\n\n")

        # Получаем структуру файлов для быстрой проверки
        structure = self.get_filtered_structure(whitelist, blacklist)
        files_by_dir = {dir: [f.name for f in files] for dir, files in structure.items()}
        
        # Рекурсивная функция для обхода директорий
        def walk_directory(current_dir, level=0):
            indent = '  ' * level
            
            # Отображаем текущую директорию
            if current_dir == self.root_dir:
                file_obj.write(f"./\n")
            else:
                file_obj.write(f"{indent}{current_dir.name}/\n")
            
            # Получаем содержимое директории
            items = []
            try:
                for item in current_dir.iterdir():
                    # Пропускаем скрытые файлы/папки, начинающиеся с точки
                    if item.name.startswith('.'):
                        continue
                        
                    # Проверяем директории
                    if item.is_dir():
                        if self.should_include_dir(item, blacklist):
                            # Проверяем, есть ли в этой директории или ее поддиректориях файлы
                            if self.has_files_in_subtree(item, whitelist, blacklist):
                                items.append((item, 'dir'))
                    # Проверяем файлы
                    elif item.is_file():
                        if self.should_include_file(item, whitelist, blacklist):
                            items.append((item, 'file'))
            except (PermissionError, OSError):
                return
            
            # Сортируем: сначала директории, потом файлы
            dirs = [item for item in items if item[1] == 'dir']
            files = [item for item in items if item[1] == 'file']
            dirs.sort(key=lambda x: x[0].name.lower())
            files.sort(key=lambda x: x[0].name.lower())
            sorted_items = dirs + files
            
            # Обрабатываем элементы
            for item, item_type in sorted_items:
                if item_type == 'dir':
                    walk_directory(item, level + 1)
                else:
                    subindent = '  ' * (level + 1)
                    file_obj.write(f"{subindent}{item.name}\n")
        
        # Начинаем обход с корневой директории
        walk_directory(self.root_dir)

    def _write_structure_old(self, file_obj, whitelist, blacklist):
        """Записывает структуру проекта с учетом фильтрации"""
        file_obj.write("СТРУКТУРА ПРОЕКТА (отфильтрованная):\n")
        file_obj.write("-" * 50 + "\n\n")

        # Получаем все релевантные директории
        relevant_dirs = self.get_directory_tree(whitelist, blacklist)

        # Сортируем директории по уровню вложенности
        relevant_dirs.sort(key=lambda x: len(x.relative_to(self.root_dir).parts))

        # Создаем словарь для быстрого доступа к файлам в директориях
        structure = self.get_filtered_structure(whitelist, blacklist)

        for dir_path in relevant_dirs:
            level = len(dir_path.relative_to(self.root_dir).parts)
            indent = '  ' * level

            # Отображаем директорию
            file_obj.write(f"{indent}{dir_path.name}/\n")

            # Отображаем файлы в этой директории, если они есть
            if dir_path in structure:
                subindent = '  ' * (level + 1)
                for file_path in structure[dir_path]:
                    file_obj.write(f"{subindent}{file_path.name}\n")

    def _write_contents(self, file_obj, whitelist, blacklist):
        """Записывает содержимое файлов"""
        file_obj.write("\n" + "=" * 60 + "\n")
        file_obj.write("СОДЕРЖИМОЕ ФАЙЛОВ:\n")
        file_obj.write("=" * 60 + "\n\n")

        structure = self.get_filtered_structure(whitelist, blacklist)

        for dir_path, files in structure.items():
            for file_path in files:
                try:
                    content = file_path.read_text(encoding='utf-8')
                    relative_path = file_path.relative_to(self.root_dir)

                    file_obj.write(f"\n{'='*50}\n")
                    file_obj.write(f"ФАЙЛ: {relative_path}\n")
                    file_obj.write(f"РАЗМЕР: {len(content)} символов\n")
                    file_obj.write(f"{'='*50}\n\n")
                    file_obj.write(content)
                    file_obj.write("\n\n")

                except UnicodeDecodeError:
                    relative_path = file_path.relative_to(self.root_dir)
                    file_obj.write(f"\n[БИНАРНЫЙ ФАЙЛ: {relative_path}]\n\n")
                except Exception as e:
                    relative_path = file_path.relative_to(self.root_dir)
                    file_obj.write(f"\n[ОШИБКА ЧТЕНИЯ {relative_path}: {e}]\n\n")

def main():
    parser = argparse.ArgumentParser(
        description='Создание структуры проекта и объединение файлов с поддержкой whitelist/blacklist'
    )
    parser.add_argument('directory', help='Корневая директория проекта')
    parser.add_argument('-o', '--output', default='project_snapshot.txt',
                       help='Выходной файл (по умолчанию: project_snapshot.txt)')

    # Whitelist/Blacklist аргументы
    parser.add_argument('-w', '--whitelist', nargs='+',
                       help='Паттерны для включения файлов (например: "*.py" "src/*" "*.json")')
    parser.add_argument('-b', '--blacklist', nargs='+',
                       help='Паттерны для исключения файлов/директорий')

    # Файлы с паттернами
    parser.add_argument('--whitelist-file',
                       help='Файл с паттернами whitelist (в стиле .gitignore)')
    parser.add_argument('--blacklist-file',
                       help='Файл с паттернами blacklist (в стиле .gitignore)')

    # Дополнительные опции
    parser.add_argument('--no-defaults', action='store_true',
                       help='Не использовать дефолтные whitelist/blacklist')
    parser.add_argument('--show-empty-dirs', action='store_true',
                       help='Показывать пустые директории в структуре')

    args = parser.parse_args()

    snapshot = ProjectSnapshot(args.directory, args.output)

    if args.no_defaults:
        snapshot.default_whitelist = []
        snapshot.default_blacklist = []

    # Переопределяем метод для показа пустых директорий если нужно
    if args.show_empty_dirs:
        original_get_directory_tree = snapshot.get_directory_tree
        def get_directory_tree_with_empty(whitelist, blacklist):
            # Включаем все директории, которые не в blacklist
            all_dirs = set()
            for root, dirs, _ in os.walk(snapshot.root_dir):
                root_path = Path(root)
                dirs[:] = [d for d in dirs if snapshot.should_include_dir(root_path / d, blacklist)]
                for d in dirs:
                    all_dirs.add(root_path / d)
            # Добавляем корневую директорию
            all_dirs.add(snapshot.root_dir)
            return sorted(all_dirs, key=lambda x: len(x.parts))

        snapshot.get_directory_tree = get_directory_tree_with_empty

    snapshot.create_snapshot(
        whitelist=args.whitelist,
        blacklist=args.blacklist,
        whitelist_file=args.whitelist_file,
        blacklist_file=args.blacklist_file
    )

    print(f"Снимок проекта сохранен в: {args.output}")
    print(f"Размер: {os.path.getsize(args.output)} байт")

if __name__ == "__main__":
    main()