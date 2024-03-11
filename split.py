import os
import sys

import os
import sys

def split_file(file_path, max_size):
    chunk_size = max_size * 1024 * 1024  # Максимальный размер в байтах
    part_number = 1

    with open(file_path, 'rb') as file:  # Открываем файл в бинарном режиме
        while True:
            lines = []
            current_size = 0
            while current_size < chunk_size:
                line = file.readline()
                if not line:
                    break
                try:
                    # Пытаемся декодировать строку в UTF-8
                    line = line.decode('utf-8')
                except UnicodeDecodeError as e:
                    # Обработка исключения, если строка не может быть декодирована
                    print(f"Ошибка декодирования: {e}")
                    continue  # Пропускаем строку или прерываем обработку
                lines.append(line)
                current_size += len(line.encode('utf-8'))

            if not lines:
                break

            part_file_name = f"{os.path.splitext(file_path)[0]}_{part_number}.txt"
            with open(part_file_name, 'w', encoding='utf-8') as part_file:
                part_file.writelines(lines)
                print(f"Файл {part_file_name} создан.")

            part_number += 1

    print("Разделение файла завершено.")

def main():
    if len(sys.argv) != 3:
        print("Использование: python split_script.py <путь_к_файлу> <размер_части_в_мегабайтах>")
        sys.exit(1)

    file_path = sys.argv[1]
    max_size = int(sys.argv[2])

    if not os.path.isfile(file_path):
        print("Указанный файл не существует.")
        sys.exit(1)

    split_file(file_path, max_size)

if __name__ == "__main__":
    main()