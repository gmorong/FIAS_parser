"""
Модуль для работы с подключением к PostgreSQL
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from contextlib import contextmanager
import logging

load_dotenv()
logger = logging.getLogger(__name__)

class DatabaseConfig:
    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = int(os.getenv('DB_PORT', '5432'))
        self.database = os.getenv('DB_NAME', 'fias_66')
        self.username = os.getenv('DB_USER', 'd_kamkov')
        self.password = os.getenv('DB_PASSWORD', '1221')
    
    def get_connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

@contextmanager
def get_db_connection():
    """Контекстный менеджер для подключения к БД"""
    config = DatabaseConfig()
    conn = None
    try:
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            user=config.username,
            password=config.password,
            database=config.database
        )
        conn.autocommit = False
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка подключения к БД: {e}")
        raise
    finally:
        if conn:
            conn.close()

def test_connection():
    """Тестирование подключения к базе данных"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                print(f"Подключение успешно! PostgreSQL версия: {version}")
                return True
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        return False