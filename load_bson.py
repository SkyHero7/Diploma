from pymongo import MongoClient
from bson import decode_all

# Путь к вашему BSON-файлу
bson_file_path = 'sample_collection.bson'

# Подключение к MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['DB_diplom']
collection = db['Collection_diplom']

# Чтение BSON-файла и вставка данных в коллекцию
with open(bson_file_path, 'rb') as f:
    data = decode_all(f.read())
    if data:
        collection.insert_many(data)
        print("Данные успешно загружены в MongoDB")
    else:
        print("Файл BSON пустой или данные не найдены")
