#!/usr/bin/env python3
"""
Создание и настройка базы данных для 66 региона
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from pathlib import Path
from dotenv import load_dotenv
import logging

# Загружаем переменные окружения
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_admin_credentials():
    """Получение учетных данных администратора PostgreSQL"""
    print("Настройка базы данных FIAS для 66 региона")
    print("=" * 50)
    
    return {
        'host': input("Host PostgreSQL (localhost): ").strip() or "localhost",
        'port': int(input("Port PostgreSQL (5432): ").strip() or "5432"),
        'user': input("Пользователь-администратор (postgres): ").strip() or "postgres",
        'password': input("Пароль администратора: ").strip()
    }

def create_database_and_user(admin_creds, db_config):
    """Создание базы данных и пользователя"""
    try:
        # Подключение как администратор
        conn = psycopg2.connect(
            host=admin_creds['host'],
            port=admin_creds['port'],
            user=admin_creds['user'],
            password=admin_creds['password'],
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cursor:
            # Проверяем существование БД
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_config['name'],))
            if cursor.fetchone():
                recreate = input(f"База данных '{db_config['name']}' существует. Пересоздать? (y/N): ")
                if recreate.lower() in ['y', 'yes']:
                    cursor.execute(f"DROP DATABASE IF EXISTS {db_config['name']}")
                    logger.info(f"База данных '{db_config['name']}' удалена")
                else:
                    logger.info("Используем существующую базу данных")
                    return True
            
            # Создаем базу данных
            cursor.execute(f"""
                CREATE DATABASE {db_config['name']} 
                WITH ENCODING = 'UTF8' 
                LC_COLLATE = 'ru_RU.UTF-8' 
                LC_CTYPE = 'ru_RU.UTF-8'
                TEMPLATE = template0
            """)
            logger.info(f"База данных '{db_config['name']}' создана")
            
            # Создаем пользователя
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (db_config['user'],))
            if not cursor.fetchone():
                cursor.execute(f"CREATE USER {db_config['user']} WITH PASSWORD '{db_config['password']}'")
                logger.info(f"Пользователь '{db_config['user']}' создан")
            
            # Выдаем права
            cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db_config['name']} TO {db_config['user']}")
            logger.info("Права пользователя настроены")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка создания базы данных: {e}")
        return False

def get_schema_sql():
    """SQL схема для 66 региона"""
    return """
-- Расширения PostgreSQL
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- Справочник статусов объектов
CREATE TABLE object_statuses (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

-- Справочник типов домов
CREATE TABLE house_types (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    short_name VARCHAR(20),
    category VARCHAR(20)
);

-- Населенные пункты 66 региона
CREATE TABLE settlements (
    object_id BIGINT PRIMARY KEY,
    object_guid UUID,
    name VARCHAR(255) NOT NULL,
    type_name VARCHAR(100),
    full_name VARCHAR(255),
    level_id INTEGER,
    parent_id BIGINT,
    oktmo VARCHAR(20),
    search_name VARCHAR(255),
    name_parts TEXT[],
    start_date DATE,
    end_date DATE DEFAULT '2079-06-06',
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    status_id INTEGER DEFAULT 1 REFERENCES object_statuses(id)
);

-- Улицы в населенных пунктах
CREATE TABLE streets (
    object_id BIGINT PRIMARY KEY,
    object_guid UUID,
    name VARCHAR(255) NOT NULL,
    type_name VARCHAR(50),
    full_name VARCHAR(255),
    settlement_id BIGINT NOT NULL REFERENCES settlements(object_id),
    search_name VARCHAR(255),
    settlement_name VARCHAR(255),
    start_date DATE,
    end_date DATE DEFAULT '2079-06-06',
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    status_id INTEGER DEFAULT 1 REFERENCES object_statuses(id)
);

-- Дома на улицах
CREATE TABLE houses (
    object_id BIGINT PRIMARY KEY,
    object_guid UUID,
    house_num VARCHAR(50) NOT NULL,
    house_type_id INTEGER REFERENCES house_types(id),
    add_num1 VARCHAR(20),
    add_num2 VARCHAR(20),
    add_type1 INTEGER,
    add_type2 INTEGER,
    street_id BIGINT NOT NULL REFERENCES streets(object_id),
    settlement_id BIGINT NOT NULL REFERENCES settlements(object_id),
    full_number VARCHAR(100),
    display_number VARCHAR(100),
    sort_number NUMERIC(10,2),
    cadastral_number VARCHAR(50),
    apartment_count INTEGER DEFAULT 0,
    building_category VARCHAR(50),
    street_name VARCHAR(255),
    settlement_name VARCHAR(255),
    start_date DATE,
    end_date DATE DEFAULT '2079-06-06',
    update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    status_id INTEGER DEFAULT 1 REFERENCES object_statuses(id)
);

-- Индексы для быстрого поиска
CREATE INDEX idx_settlements_search ON settlements USING GIN(search_name gin_trgm_ops);
CREATE INDEX idx_settlements_active ON settlements(is_active, status_id) WHERE is_active = true;

CREATE INDEX idx_streets_settlement ON streets(settlement_id, is_active) WHERE is_active = true;
CREATE INDEX idx_streets_search ON streets USING GIN(search_name gin_trgm_ops);

CREATE INDEX idx_houses_street ON houses(street_id, sort_number) WHERE is_active = true;
CREATE INDEX idx_houses_cadastral ON houses(cadastral_number) WHERE cadastral_number IS NOT NULL;

-- Заполнение справочников
INSERT INTO object_statuses (id, name) VALUES (1, 'active'), (2, 'deleted');

INSERT INTO house_types (id, name, short_name, category) VALUES
(1, 'Владение', 'влд.', 'residential'),
(2, 'Дом', 'д.', 'residential'),
(3, 'Домовладение', 'домовлад.', 'residential'),
(4, 'Гараж', 'гар.', 'other'),
(5, 'Здание', 'зд.', 'residential');
-- Функции для автоматизации
CREATE OR REPLACE FUNCTION update_settlement_search()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_name := lower(NEW.name);
    NEW.name_parts := string_to_array(lower(NEW.name), ' ');
    NEW.full_name := CASE 
        WHEN NEW.type_name IS NOT NULL THEN NEW.type_name || '. ' || NEW.name
        ELSE NEW.name
    END;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_settlement_search
    BEFORE INSERT OR UPDATE OF name, type_name ON settlements
    FOR EACH ROW EXECUTE FUNCTION update_settlement_search();

CREATE OR REPLACE FUNCTION update_street_search()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_name := lower(NEW.name);
    NEW.full_name := CASE 
        WHEN NEW.type_name IS NOT NULL THEN NEW.type_name || '. ' || NEW.name
        ELSE NEW.name
    END;
    SELECT name INTO NEW.settlement_name 
    FROM settlements 
    WHERE object_id = NEW.settlement_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_street_search
    BEFORE INSERT OR UPDATE OF name, type_name, settlement_id ON streets
    FOR EACH ROW EXECUTE FUNCTION update_street_search();

CREATE OR REPLACE FUNCTION update_house_number()
RETURNS TRIGGER AS $$
BEGIN
    NEW.full_number := NEW.house_num;
    
    IF NEW.add_num1 IS NOT NULL THEN
        NEW.full_number := NEW.full_number || 
            CASE NEW.add_type1
                WHEN 1 THEN NEW.add_num1
                WHEN 2 THEN ' стр.' || NEW.add_num1
                WHEN 3 THEN NEW.add_num1
                ELSE '/' || NEW.add_num1
            END;
    END IF;
    
    NEW.sort_number := COALESCE(
        (regexp_match(NEW.house_num, '^(\\d+)'))[1]::numeric, 
        0
    );
    
    NEW.display_number := NEW.full_number;
    
    SELECT s.name, st.name 
    INTO NEW.settlement_name, NEW.street_name
    FROM streets st
    JOIN settlements s ON st.settlement_id = s.object_id
    WHERE st.object_id = NEW.street_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_house_number
    BEFORE INSERT OR UPDATE OF house_num, add_num1, add_num2, add_type1, add_type2, street_id ON houses
    FOR EACH ROW EXECUTE FUNCTION update_house_number();
"""

def apply_schema(admin_creds, db_config):
    """Применение SQL схемы к базе данных"""
    try:
        conn = psycopg2.connect(
            host=admin_creds['host'],
            port=admin_creds['port'],
            user=admin_creds['user'],
            password=admin_creds['password'],
            database=db_config['name']
        )
        
        with conn.cursor() as cursor:
            cursor.execute(get_schema_sql())
            conn.commit()
            logger.info("SQL схема применена успешно")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка применения схемы: {e}")
        return False

def main():
    """Основная функция настройки"""
    # Получаем конфигурацию из .env
    db_config = {
        'name': os.getenv('DB_NAME', 'fias_66'),
        'user': os.getenv('DB_USER', 'fias_user'),
        'password': os.getenv('DB_PASSWORD', 'fias123')
    }
    
    # Получаем учетные данные администратора
    admin_creds = get_admin_credentials()
    
    # Создаем базу данных
    if not create_database_and_user(admin_creds, db_config):
        return False
    
    # Применяем схему
    if not apply_schema(admin_creds, db_config):
        return False
    
    print("\n" + "=" * 50)
    print("НАСТРОЙКА ЗАВЕРШЕНА УСПЕШНО!")
    print(f"База данных: {db_config['name']}")
    print(f"Пользователь: {db_config['user']}")
    print("Теперь можно запустить парсер данных")
    
    return True

if __name__ == "__main__":
    main()