import os
import sys

def split_file(file_path, max_size):
    chunk_size = max_size * 1024 * 1024  # 50 мегабайт
    part_number = 1

    with open(file_path, 'r') as file:
        while True:
            lines = []
            current_size = 0
            # Читаем строки до тех пор, пока не достигнем желаемого размера файла
            while current_size < chunk_size:
                line = file.readline()
                # Если достигнут конец файла, прерываем цикл
                if not line:
                    break
                lines.append(line)
                current_size += len(line.encode('utf-8'))

            # Если больше нет данных для чтения, прерываем цикл
            if not lines:
                break

            # Создаем новый файл для текущей части
            part_file_name = f"{os.path.splitext(file_path)[0]}_{part_number}.txt"
            with open(part_file_name, 'w') as part_file:
                part_file.writelines(lines)
                print(f"Файл {part_file_name} создан.")

            # Увеличиваем номер части
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