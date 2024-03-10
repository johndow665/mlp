from pymongo import MongoClient
import sys
import os

# Проверяем, передан ли путь к директории в аргументах командной строки
if len(sys.argv) != 2:
    print("Пожалуйста, укажите путь к директории в качестве аргумента.")
    sys.exit(1)

# Путь к директории из аргумента командной строки
dir_path = sys.argv[1]

# Подключение к MongoDB
try:
    client = MongoClient('localhost', 27017)
    db = client['pass']
    collection = db['pass']
    print("Успешное подключение к MongoDB.")
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

# Обработка файлов в директории
for filename in os.listdir(dir_path):
    if filename not in loaded_files:
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
                # Записываем имя файла в loaded.txt
                with open(loaded_file_path, 'a') as loaded_file:
                    loaded_file.write(filename + '\n')
                print(f"Имя файла {filename} записано в loaded.txt.")
        except FileNotFoundError:
            print(f"Файл {file_path} не найден.")
        except Exception as e:
            print(f"Произошла ошибка при чтении файла {filename}: {e}")
    else:
        print(f"Файл {filename} уже был обработан.")

# Закрываем соединение с MongoDB
client.close()