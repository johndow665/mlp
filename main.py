from pymongo import MongoClient
import sys
import os
import argparse

# Создаем парсер аргументов командной строки
parser = argparse.ArgumentParser(description='Загрузка файлов в MongoDB')
parser.add_argument('directory', help='Путь к директории с файлами')
parser.add_argument('-d', '--database', required=True, help='Имя базы данных MongoDB')
parser.add_argument('-c', '--collection', required=True, help='Имя коллекции MongoDB')

# Парсим аргументы
args = parser.parse_args()

# Путь к директории из аргумента командной строки
dir_path = args.directory

# Подключение к MongoDB
try:
    client = MongoClient('localhost', 27017)
    db = client[args.database]
    collection = db[args.collection]
    print(f"Успешное подключение к MongoDB. База данных: {args.database}, Коллекция: {args.collection}")
except Exception as e:
    print(f"Не удалось подключиться к MongoDB: {e}")
    sys.exit(1)

# Путь к файлу loaded.txt
loaded_file_path = os.path.join(os.path.dirname(__file__), 'loaded.txt')

# Создаем файл loaded.txt, если он не существует
if not os.path.exists(loaded_file_path):
    open(loaded_file_path, 'w').close()
    print("Создан файл loaded.txt.")

# Получение списка всех файлов в директории
all_files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]

# Обработка файлов в цикле
for filename in all_files:
    # Проверяем, есть ли файл в loaded.txt
    with open(loaded_file_path, 'r') as loaded_file:
        if filename in (line.strip() for line in loaded_file):
            continue  # Файл уже обработан, переходим к следующему

    # Записываем имя файла в loaded.txt перед обработкой
    with open(loaded_file_path, 'a') as loaded_file:
        loaded_file.write(filename + '\n')

    # Файл не обработан, начинаем загрузку
    file_path = os.path.join(dir_path, filename)
    print(f"Начинаем обработку файла {filename}...")

    try:
        with open(file_path, 'r') as file:
            count = 0
            for line in file:
                record = line.strip()
                if record:
                    try:
                        collection.insert_one({'data': record})
                        count += 1
                        if count % 10000 == 0:
                            print(f"Файл {filename}: добавлено {count} записей в базу данных.")
                    except Exception as e:
                        print(f"Ошибка при добавлении записи в базу данных: {e}")
            print(f"Закончил чтение и запись файла {filename}. Всего добавлено {count} записей.")
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
    except Exception as e:
        print(f"Произошла ошибка при чтении файла {filename}: {e}")

# Закрываем соединение с MongoDB
client.close()