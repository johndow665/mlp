from pymongo import MongoClient
import sys

# Проверяем, передан ли путь к файлу в аргументах командной строки
if len(sys.argv) != 2:
    print("Пожалуйста, укажите путь к файлу в качестве аргумента.")
    sys.exit(1)

# Путь к файлу из аргумента командной строки
file_path = sys.argv[1]

# Подключение к MongoDB
client = MongoClient('localhost', 27017)  # Подключение к локальному MongoDB
db = client['pass']  # База данных 'pass'
collection = db['pass']  # Коллекция 'pass'

# Чтение файла и запись в MongoDB
try:
    with open(file_path, 'r') as file:
        for line in file:
            # Предполагаем, что каждая строка файла - это отдельная запись
            record = line.strip()  # Убираем пробельные символы в начале и конце строки
            if record:  # Проверяем, что строка не пустая
                # Вставляем запись в коллекцию как документ
                collection.insert_one({'data': record})
    print("Данные успешно добавлены в MongoDB.")
except FileNotFoundError:
    print(f"Файл {file_path} не найден.")
except Exception as e:
    print(f"Произошла ошибка: {e}")

# Закрываем соединение с MongoDB
client.close()