-- SQL скрипт для создания всех таблиц ФИАС с полной иерархией
-- Запускать в PostgreSQL

-- Подключитесь к PostgreSQL и выполните SQL скрипт
-- psql -d your_database_name -f create_tables.sql

-- Удаление существующих таблиц (осторожно!)
-- DROP TABLE IF EXISTS houses, cadastral_plots, streets, settlements, municipal_formations, house_types, param_types CASCADE;

-- Таблица типов домов
CREATE TABLE IF NOT EXISTS house_types (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    short_name VARCHAR(20) NOT NULL,
    category VARCHAR(20) NOT NULL
);

-- Таблица типов параметров
CREATE TABLE IF NOT EXISTS param_types (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    code VARCHAR(50) NOT NULL,
    description TEXT
);

-- Таблица муниципальных образований (уровни 1-3)
-- УБИРАЕМ СТРОГИЙ FOREIGN KEY для parent_id
CREATE TABLE IF NOT EXISTS municipal_formations (
    object_id BIGINT PRIMARY KEY,
    object_guid UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    type_name VARCHAR(100) NOT NULL,
    level_id INTEGER NOT NULL,
    parent_id BIGINT, -- Убран REFERENCES для избежания циклических ссылок
    oktmo VARCHAR(20),
    okato VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    status_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица населенных пунктов (уровни 4-7)
CREATE TABLE IF NOT EXISTS settlements (
    object_id BIGINT PRIMARY KEY,
    object_guid UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    type_name VARCHAR(100) NOT NULL,
    level_id INTEGER NOT NULL,
    parent_id BIGINT,
    municipal_formation_id BIGINT, -- Убран REFERENCES для гибкости
    oktmo VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    status_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица улиц (уровень 8)
CREATE TABLE IF NOT EXISTS streets (
    object_id BIGINT PRIMARY KEY,
    object_guid UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    type_name VARCHAR(50) NOT NULL,
    settlement_id BIGINT NOT NULL, -- Оставляем, но не REFERENCES для скорости
    is_active BOOLEAN DEFAULT TRUE,
    status_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица кадастровых участков
CREATE TABLE IF NOT EXISTS cadastral_plots (
    object_id BIGINT PRIMARY KEY,
    object_guid UUID,
    cadastral_number VARCHAR(50),
    name VARCHAR(255),
    type_name VARCHAR(100),
    level_id INTEGER NOT NULL,
    settlement_id BIGINT, -- Убран REFERENCES
    street_id BIGINT, -- Убран REFERENCES  
    area DECIMAL(15,2),
    purpose VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    status_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица домов
CREATE TABLE IF NOT EXISTS houses (
    object_id BIGINT PRIMARY KEY,
    object_guid UUID NOT NULL,
    house_num VARCHAR(50) NOT NULL,
    house_type_id INTEGER, -- Убран REFERENCES для гибкости
    add_num1 VARCHAR(20),
    add_num2 VARCHAR(20),
    add_type1 INTEGER,
    add_type2 INTEGER,
    street_id BIGINT NOT NULL, -- Убран REFERENCES
    settlement_id BIGINT NOT NULL, -- Убран REFERENCES
    cadastral_plot_id BIGINT, -- Убран REFERENCES
    is_active BOOLEAN DEFAULT TRUE,
    status_id INTEGER NOT NULL,
    -- Параметры домов
    postal_code VARCHAR(10),
    cadastral_number VARCHAR(50),
    oktmo VARCHAR(20),
    kladr VARCHAR(20),
    egrn_number VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов для производительности (ВАЖНО!)
CREATE INDEX IF NOT EXISTS idx_mf_name ON municipal_formations(name);
CREATE INDEX IF NOT EXISTS idx_mf_level ON municipal_formations(level_id);
CREATE INDEX IF NOT EXISTS idx_mf_parent ON municipal_formations(parent_id);
CREATE INDEX IF NOT EXISTS idx_mf_active ON municipal_formations(is_active);

CREATE INDEX IF NOT EXISTS idx_settlements_name ON settlements(name);
CREATE INDEX IF NOT EXISTS idx_settlements_mf ON settlements(municipal_formation_id);
CREATE INDEX IF NOT EXISTS idx_settlements_active ON settlements(is_active);
CREATE INDEX IF NOT EXISTS idx_settlements_level ON settlements(level_id);

CREATE INDEX IF NOT EXISTS idx_streets_name ON streets(name);
CREATE INDEX IF NOT EXISTS idx_streets_settlement ON streets(settlement_id);
CREATE INDEX IF NOT EXISTS idx_streets_active ON streets(is_active);

CREATE INDEX IF NOT EXISTS idx_plots_cadastral ON cadastral_plots(cadastral_number);
CREATE INDEX IF NOT EXISTS idx_plots_settlement ON cadastral_plots(settlement_id);
CREATE INDEX IF NOT EXISTS idx_plots_street ON cadastral_plots(street_id);
CREATE INDEX IF NOT EXISTS idx_plots_active ON cadastral_plots(is_active);

CREATE INDEX IF NOT EXISTS idx_houses_street ON houses(street_id);
CREATE INDEX IF NOT EXISTS idx_houses_settlement ON houses(settlement_id);
CREATE INDEX IF NOT EXISTS idx_houses_plot ON houses(cadastral_plot_id);
CREATE INDEX IF NOT EXISTS idx_houses_cadastral ON houses(cadastral_number);
CREATE INDEX IF NOT EXISTS idx_houses_active ON houses(is_active);
CREATE INDEX IF NOT EXISTS idx_houses_postal ON houses(postal_code);
CREATE INDEX IF NOT EXISTS idx_houses_type ON houses(house_type_id);

-- Заполнение справочных данных
INSERT INTO house_types (id, name, short_name, category) VALUES
(1, 'Владение', 'влд.', 'building'),
(2, 'Дом', 'д.', 'building'),
(3, 'Домовладение', 'домовл.', 'building'),
(4, 'Корпус', 'корп.', 'building'),
(5, 'Здание', 'зд.', 'building'),
(6, 'Строение', 'стр.', 'building'),
(7, 'Сооружение', 'соор.', 'building'),
(8, 'Участок', 'уч.', 'plot'),
(9, 'Помещение', 'пом.', 'room'),
(10, 'Объект', 'об.', 'object')
ON CONFLICT (id) DO NOTHING;

INSERT INTO param_types (id, name, code, description) VALUES
(1, 'Почтовый индекс', 'POSTALCODE', 'Почтовый индекс адресного объекта'),
(2, 'Код ИФНС', 'IFNS', 'Код территориального участка ИФНС'),
(3, 'Код ИФНС территориальный', 'TERRIFNS', 'Код территориального участка ИФНС'),
(4, 'Код ОКАТО', 'OKATO', 'Код ОКАТО'),
(5, 'Код ОКТМО', 'OKTMO', 'Код ОКТМО'),
(6, 'Код КЛАДР', 'KLADR', 'Код КЛАДР'),
(7, 'Кадастровый номер', 'CADNUM', 'Кадастровый номер земельного участка/здания'),
(8, 'Номер в ЕГРН', 'EGRN', 'Номер записи в ЕГРН'),
(9, 'Код статуса', 'STATUS', 'Код статуса объекта'),
(10, 'Признак центра', 'CENTER', 'Признак центра административного объекта'),
(11, 'Код региона', 'REGION', 'Код региона'),
(12, 'Автономный округ', 'AUTONOMY', 'Код автономного округа'),
(13, 'Уникальный номер записи', 'UNIQUENUMBER', 'Уникальный номер записи адресообразующего элемента'),
(14, 'Код города', 'CITYCODE', 'Код города в регионе'),
(15, 'Уровень детализации', 'DETAILLEVEL', 'Уровень детализации адресного объекта'),
(16, 'Код района', 'DISTRICTCODE', 'Код района в регионе'),
(17, 'Координаты', 'COORDINATES', 'Координаты объекта'),
(18, 'Площадь', 'AREA', 'Площадь территории'),
(19, 'Статус записи', 'RECORDSTATUS', 'Статус записи в реестре'),
(20, 'Дата актуализации', 'ACTUALDATE', 'Дата актуализации записи')
ON CONFLICT (id) DO NOTHING;

-- Создание уникального индекса для кадастровых номеров
CREATE UNIQUE INDEX IF NOT EXISTS idx_cadastral_plots_number_unique 
ON cadastral_plots(cadastral_number) 
WHERE cadastral_number IS NOT NULL AND cadastral_number != '';

-- Комментарии к таблицам
COMMENT ON TABLE municipal_formations IS 'Муниципальные образования (субъекты, районы, поселения) - уровни 1-3 ФИАС';
COMMENT ON TABLE settlements IS 'Населенные пункты (города, поселки, села) - уровни 4-7 ФИАС';
COMMENT ON TABLE streets IS 'Улицы, проспекты, переулки - уровень 8 ФИАС';
COMMENT ON TABLE cadastral_plots IS 'Кадастровые участки - уровни 9+ ФИАС и участки из параметров домов';
COMMENT ON TABLE houses IS 'Дома, здания, строения с их параметрами';
COMMENT ON TABLE house_types IS 'Справочник типов домов/строений';
COMMENT ON TABLE param_types IS 'Справочник типов параметров ФИАС';

-- Создание триггеров для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_municipal_formations_updated_at BEFORE UPDATE ON municipal_formations 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_settlements_updated_at BEFORE UPDATE ON settlements 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_streets_updated_at BEFORE UPDATE ON streets 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_cadastral_plots_updated_at BEFORE UPDATE ON cadastral_plots 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_houses_updated_at BEFORE UPDATE ON houses 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Проверка созданной структуры
SELECT 
    schemaname,
    tablename,
    tableowner
FROM pg_tables 
WHERE schemaname = 'public' 
  AND tablename IN ('municipal_formations', 'settlements', 'streets', 'cadastral_plots', 'houses', 'house_types', 'param_types')
ORDER BY tablename;