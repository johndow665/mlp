from pymongo import MongoClient
import sys
import os
import argparse
import random

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

# Чтение списка уже загруженных файлов
loaded_files = set()
with open(loaded_file_path, 'r') as loaded_file:
    loaded_files = {line.strip() for line in loaded_file}

# Получение списка всех файлов в директории и исключение уже загруженных
all_files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f)) and f not in loaded_files]

# Проверяем, есть ли незагруженные файлы
if not all_files:
    print("Все файлы уже были обработаны.")
    sys.exit(0)

# Обработка файлов в цикле
for filename in all_files:
    file_path = os.path.join(dir_path, filename)

    # Записываем имя файла в loaded.txt перед обработкой
    with open(loaded_file_path, 'a') as loaded_file:
        loaded_file.write(filename + '\n')

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