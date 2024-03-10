from pymongo import MongoClient
import sys

# Проверяем, передан ли путь к файлу в аргументах командной строки
if len(sys.argv) != 2:
    print("Пожалуйста, укажите путь к файлу в качестве аргумента.")
    sys.exit(1)

# Путь к файлу из аргумента командной строки
file_path = sys.argv[1]

# Подключение к MongoDB
try:
    client = MongoClient('localhost', 27017)  # Подключение к локальному MongoDB
    db = client['pass']  # База данных 'pass'
    collection = db['pass']  # Коллекция 'pass'
    print("Успешное подключение к MongoDB.")
except Exception as e:
    print(f"Не удалось подключиться к MongoDB: {e}")
    sys.exit(1)

# Чтение файла и запись в MongoDB
try:
    with open(file_path, 'r') as file:
        count = 0  # Счетчик для отслеживания количества обработанных строк
        for line in file:
            record = line.strip()  # Убираем пробельные символы в начале и конце строки
            if record:  # Проверяем, что строка не пустая
                try:
                    collection.insert_one({'data': record})
                    count += 1
                    if count % 10000 == 0:  # Каждые 10 000 строк выводим сообщение
                        print(f"Добавлено {count} записей в базу данных.")
                except Exception as e:
                    print(f"Ошибка при добавлении записи в базу данных: {e}")
        print(f"Всего добавлено {count} записей в базу данных.")
except FileNotFoundError:
    print(f"Файл {file_path} не найден.")
except Exception as e:
    print(f"Произошла ошибка при чтении файла: {e}")

# Закрываем соединение с MongoDB
client.close()