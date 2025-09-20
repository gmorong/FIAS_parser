# -*- coding: utf-8 -*-
"""
Конфигурация для нового ФИАС парсера
"""

# НАСТРОЙКИ БАЗЫ ДАННЫХ PostgreSQL
DB_HOST = 'localhost'           # Адрес сервера БД
DB_PORT = 5432                  # Порт PostgreSQL
DB_USER = 'postgres'            # Имя пользователя БД
DB_PASSWORD = 'postgres'            # Пароль БД
DB_NAME = 'f_66'                # Имя базы данных
DB_SCHEMA = 'fias'          # Схема в БД (будет создана автоматически)

# НАСТРОЙКИ ФАЙЛОВ ФИАС
XML_DIRECTORY = 'C:\GAR_DATA\XML'  # Путь к XML файлам ФИАС
REGION_CODE = '66'              # Код региона (66 - Свердловская область)

# ДОПОЛНИТЕЛЬНЫЕ НАСТРОЙКИ
BATCH_SIZE = 5000               # Размер батча для вставки данных
DEBUG_MODE = False              # Включить отладочный режим

print("Конфигурация загружена:")
print(f"  Регион: {REGION_CODE}")
print(f"  БД: {DB_HOST}:{DB_PORT}/{DB_NAME}.{DB_SCHEMA}")
print(f"  XML: {XML_DIRECTORY}")