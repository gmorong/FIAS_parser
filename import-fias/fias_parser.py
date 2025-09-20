# -*- coding: utf-8 -*-
"""
Полностью новый ФИАС парсер с правильной логикой связывания домов
Основные принципы:
1. Загружаем ВСЕ файлы иерархии (ADM + MUN)
2. Создаем полную карту родительских связей
3. Корректно обрабатываем типы данных
4. Подробная диагностика на каждом этапе
"""

import os
import psycopg2
import lxml.etree as et
from datetime import datetime

class FIASParser:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None
        
        # Карты данных
        self.hierarchy_map = {}  # objectid -> parent_objectid
        self.level_map = {}      # objectid -> level
        
    def connect(self):
        """Подключение к PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=self.config['DB_HOST'],
                port=self.config['DB_PORT'],
                user=self.config['DB_USER'],
                password=self.config['DB_PASSWORD'],
                database=self.config['DB_NAME']
            )
            self.cursor = self.connection.cursor()
            print("✓ Подключение к БД установлено")
            return True
        except Exception as e:
            print(f"✗ Ошибка подключения: {e}")
            return False
    
    def create_schema(self):
        """Создание схемы БД"""
        print("Создание схемы БД...")
        
        # Создаем схему
        self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config['DB_SCHEMA']}")
        
        # Удаляем старые таблицы
        self.cursor.execute(f"""
            DROP TABLE IF EXISTS {self.config['DB_SCHEMA']}.houses CASCADE;
            DROP TABLE IF EXISTS {self.config['DB_SCHEMA']}.land_plots CASCADE;
            DROP TABLE IF EXISTS {self.config['DB_SCHEMA']}.streets CASCADE;
            DROP TABLE IF EXISTS {self.config['DB_SCHEMA']}.settlements CASCADE;
            DROP TABLE IF EXISTS {self.config['DB_SCHEMA']}.municipalities CASCADE;
        """)
        
        # 1. Муниципальные образования (уровни 3-4)
        self.cursor.execute(f"""
            CREATE TABLE {self.config['DB_SCHEMA']}.municipalities (
                id BIGINT PRIMARY KEY,
                objectid BIGINT UNIQUE NOT NULL,
                objectguid VARCHAR(36),
                name VARCHAR(250) NOT NULL,
                typename VARCHAR(50) NOT NULL,
                level VARCHAR(10) NOT NULL,
                parent_id BIGINT,
                isactual INTEGER DEFAULT 1,
                isactive INTEGER DEFAULT 1,
                updatedate DATE
            )
        """)
        
        # 2. Населенные пункты (уровни 5-6)
        self.cursor.execute(f"""
            CREATE TABLE {self.config['DB_SCHEMA']}.settlements (
                id BIGINT PRIMARY KEY,
                objectid BIGINT UNIQUE NOT NULL,
                objectguid VARCHAR(36),
                name VARCHAR(250) NOT NULL,
                typename VARCHAR(50) NOT NULL,
                level VARCHAR(10) NOT NULL,
                municipality_id BIGINT,
                isactual INTEGER DEFAULT 1,
                isactive INTEGER DEFAULT 1,
                updatedate DATE
            )
        """)
        
        # 3. Улицы (уровни 7-8)
        self.cursor.execute(f"""
            CREATE TABLE {self.config['DB_SCHEMA']}.streets (
                id BIGINT PRIMARY KEY,
                objectid BIGINT UNIQUE NOT NULL,
                objectguid VARCHAR(36),
                name VARCHAR(250) NOT NULL,
                typename VARCHAR(50) NOT NULL,
                level VARCHAR(10) NOT NULL,
                settlement_id BIGINT,
                municipality_id BIGINT,
                isactual INTEGER DEFAULT 1,
                isactive INTEGER DEFAULT 1,
                updatedate DATE
            )
        """)
        
        # 4. Дома
        self.cursor.execute(f"""
            CREATE TABLE {self.config['DB_SCHEMA']}.houses (
                id BIGINT PRIMARY KEY,
                objectid BIGINT UNIQUE NOT NULL,
                objectguid VARCHAR(36),
                house_number VARCHAR(50),
                building_number VARCHAR(50),
                structure_number VARCHAR(50),
                
                -- Связи
                street_id BIGINT,
                settlement_id BIGINT,
                municipality_id BIGINT,
                
                -- Параметры
                cadastral_number VARCHAR(100),
                floors_count INTEGER,
                residents_count INTEGER,
                
                full_address TEXT,
                isactual INTEGER DEFAULT 1,
                isactive INTEGER DEFAULT 1,
                updatedate DATE
            )
        """)
        
        # 5. Земельные участки
        self.cursor.execute(f"""
            CREATE TABLE {self.config['DB_SCHEMA']}.land_plots (
                id BIGINT PRIMARY KEY,
                objectid BIGINT UNIQUE NOT NULL,
                objectguid VARCHAR(36),
                number_plot VARCHAR(250),
                cadastral_number VARCHAR(100),
                area DECIMAL(15,2),
                category VARCHAR(100),
                settlement_id BIGINT,
                municipality_id BIGINT,
                isactual INTEGER DEFAULT 1,
                isactive INTEGER DEFAULT 1,
                updatedate DATE
            )
        """)
        
        # Создаем индексы
        self.create_indexes()
        
        self.connection.commit()
        print("✓ Схема БД создана")
    
    def create_indexes(self):
        """Создание индексов"""
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_municipalities_objectid ON {self.config['DB_SCHEMA']}.municipalities(objectid)",
            f"CREATE INDEX IF NOT EXISTS idx_settlements_objectid ON {self.config['DB_SCHEMA']}.settlements(objectid)",
            f"CREATE INDEX IF NOT EXISTS idx_streets_objectid ON {self.config['DB_SCHEMA']}.streets(objectid)",
            f"CREATE INDEX IF NOT EXISTS idx_houses_objectid ON {self.config['DB_SCHEMA']}.houses(objectid)",
            f"CREATE INDEX IF NOT EXISTS idx_land_plots_objectid ON {self.config['DB_SCHEMA']}.land_plots(objectid)",
        ]
        
        for index_sql in indexes:
            try:
                self.cursor.execute(index_sql)
            except:
                pass
    
    def find_files(self, directory, pattern):
        """Поиск файлов по шаблону"""
        files = []
        search_dirs = [directory]
        if self.config.get('REGION_CODE'):
            search_dirs.append(os.path.join(directory, self.config['REGION_CODE']))
        
        for search_dir in search_dirs:
            if os.path.exists(search_dir):
                print(f"    Поиск в: {search_dir}")
                try:
                    for filename in os.listdir(search_dir):
                        if pattern in filename.upper() and filename.endswith('.XML'):
                            files.append(os.path.join(search_dir, filename))
                            print(f"      Найден: {filename}")
                except Exception as e:
                    print(f"      Ошибка: {e}")
            else:
                print(f"    Директория не существует: {search_dir}")
        
        print(f"    Итого найдено файлов {pattern}: {len(files)}")
        return files
    
    def load_hierarchy_and_levels(self, xml_directory):
        """Загрузка иерархии и уровней объектов С ПРИОРИТЕТОМ МУНИЦИПАЛЬНОЙ ИЕРАРХИИ"""
        print("=" * 50)
        print("ЗАГРУЗКА ПОЛНОЙ КАРТЫ ИЕРАРХИИ И УРОВНЕЙ")
        print("=" * 50)
        
        self.hierarchy_map = {}
        self.level_map = {}
        
        # Шаг 1: Загружаем уровни из ADDR_OBJ
        print("\n1. Загрузка уровней из AS_ADDR_OBJ...")
        addr_files = self.find_files(xml_directory, 'AS_ADDR_OBJ')
        
        total_levels = 0
        level_stats = {}
        
        for file_path in addr_files:
            print(f"  Обработка: {os.path.basename(file_path)}")
            
            count = 0
            for event, elem in et.iterparse(file_path, events=('end',)):
                if elem.tag == 'OBJECT':
                    attr = elem.attrib
                    
                    if attr.get('ISACTUAL') == '1' and attr.get('ISACTIVE') == '1':
                        objectid = attr.get('OBJECTID')
                        level = attr.get('LEVEL')
                        
                        if objectid and level:
                            self.level_map[str(objectid)] = level
                            count += 1
                            total_levels += 1
                            
                            # Статистика уровней
                            level_stats[level] = level_stats.get(level, 0) + 1
                    
                    if count % 50000 == 0 and count > 0:
                        print(f"    Загружено: {count:,}")
                
                elem.clear()
            
            print(f"  Загружено из файла: {count:,}")
        
        print(f"\n✓ Всего загружено уровней: {total_levels:,}")
        print("Статистика по уровням:")
        for level in sorted(level_stats.keys()):
            print(f"  Уровень {level}: {level_stats[level]:,} объектов")
        
        # Шаг 2: СНАЧАЛА загружаем муниципальную иерархию (ПРИОРИТЕТ!)
        print("\n2. Загрузка МУНИЦИПАЛЬНОЙ иерархии (AS_MUN_HIERARCHY) - ПРИОРИТЕТ...")
        mun_files = self.find_files(xml_directory, 'AS_MUN_HIERARCHY')
        
        total_mun = 0
        mun_mo_count = 0  # счетчик МО в муниципальной иерархии
        
        for file_path in mun_files:
            print(f"  Обработка: {os.path.basename(file_path)}")
            
            count = 0
            for event, elem in et.iterparse(file_path, events=('end',)):
                if elem.tag == 'ITEM':
                    attr = elem.attrib
                    
                    if attr.get('ISACTIVE') == '1':
                        objectid = attr.get('OBJECTID')
                        parent_objectid = attr.get('PARENTOBJID')
                        
                        if objectid and parent_objectid:
                            obj_str = str(objectid)
                            parent_str = str(parent_objectid)
                            
                            # БЕЗУСЛОВНО добавляем (муниципальная иерархия имеет приоритет)
                            self.hierarchy_map[obj_str] = parent_str
                            count += 1
                            total_mun += 1
                            
                            # Считаем МО
                            obj_level = self.level_map.get(obj_str)
                            if obj_level in ['3', '4']:
                                mun_mo_count += 1
                    
                    if count % 50000 == 0 and count > 0:
                        print(f"    Загружено: {count:,}")
                
                elem.clear()
            
            print(f"  Загружено из файла: {count:,}")
        
        print(f"✓ Всего загружено MUN связей: {total_mun:,}")
        print(f"  Из них МО (уровни 3-4): {mun_mo_count:,}")
        
        # Шаг 3: Дополняем административной иерархией (только для пропущенных)
        print("\n3. Дополнение административной иерархией (AS_ADM_HIERARCHY)...")
        adm_files = self.find_files(xml_directory, 'AS_ADM_HIERARCHY')
        
        total_adm = 0
        adm_added = 0
        
        for file_path in adm_files:
            print(f"  Обработка: {os.path.basename(file_path)}")
            
            count = 0
            for event, elem in et.iterparse(file_path, events=('end',)):
                if elem.tag == 'ITEM':
                    attr = elem.attrib
                    
                    if attr.get('ISACTIVE') == '1':
                        objectid = attr.get('OBJECTID')
                        parent_objectid = attr.get('PARENTOBJID')
                        
                        if objectid and parent_objectid:
                            obj_str = str(objectid)
                            parent_str = str(parent_objectid)
                            
                            # Добавляем ТОЛЬКО если еще нет (приоритет MUN)
                            if obj_str not in self.hierarchy_map:
                                self.hierarchy_map[obj_str] = parent_str
                                adm_added += 1
                            
                            count += 1
                            total_adm += 1
                    
                    if count % 50000 == 0 and count > 0:
                        print(f"    Обработано: {count:,}")
                
                elem.clear()
            
            print(f"  Обработано из файла: {count:,}")
        
        print(f"✓ Всего обработано ADM связей: {total_adm:,}")
        print(f"  Добавлено новых: {adm_added:,}")
        
        # Диагностика МО
        print(f"\n🏛️ ДИАГНОСТИКА МУНИЦИПАЛЬНЫХ ОБРАЗОВАНИЙ:")
        mo_in_levels = level_stats.get('3', 0) + level_stats.get('4', 0)
        mo_in_hierarchy = 0
        
        for obj_id in self.hierarchy_map.keys():
            level = self.level_map.get(obj_id)
            if level in ['3', '4']:
                mo_in_hierarchy += 1
        
        print(f"  МО (уровни 3-4) в level_map: {mo_in_levels:,}")
        print(f"  МО найдено в hierarchy_map: {mo_in_hierarchy:,}")
        
        if mo_in_hierarchy > 0:
            percentage = (mo_in_hierarchy / mo_in_levels * 100) if mo_in_levels > 0 else 0
            print(f"  ✓ Процент покрытия МО: {percentage:.1f}%")
            
            # Тестируем несколько МО
            self.test_mo_connections()
        else:
            print("  ⚠️ ПРОБЛЕМА: МО не найдены в иерархии!")
            
        print(f"\n✓ ИТОГО в карте иерархии: {len(self.hierarchy_map):,} связей")
    
    def find_parent_by_level(self, objectid, target_levels):
        """Поиск родителя определенного уровня (ИСПРАВЛЕННАЯ ВЕРСИЯ)"""
        if not objectid:
            return None
        
        objectid_str = str(objectid)
        
        # Проверяем текущий уровень
        current_level = self.level_map.get(objectid_str)
        if current_level in target_levels:
            return int(objectid_str)
        
        # Идем вверх по иерархии
        current_id = objectid_str
        visited = set()
        
        while current_id and current_id not in visited:
            visited.add(current_id)
            
            if current_id in self.hierarchy_map:
                parent_id = self.hierarchy_map[current_id]
                parent_level = self.level_map.get(parent_id)
                
                if parent_level in target_levels:
                    return int(parent_id)
                
                current_id = parent_id
            else:
                break
        
        return None
    
    def process_addr_objects(self, xml_directory):
        """Обработка адресных объектов"""
        print("\n" + "=" * 50)
        print("ОБРАБОТКА АДРЕСНЫХ ОБЪЕКТОВ")
        print("=" * 50)
        
        files = self.find_files(xml_directory, 'AS_ADDR_OBJ')
        if not files:
            print("Файлы AS_ADDR_OBJ не найдены")
            return
        
        counters = {'municipalities': 0, 'settlements': 0, 'streets': 0, 'other': 0}
        
        for file_path in files:
            print(f"\nОбработка: {os.path.basename(file_path)}")
            
            municipalities_batch = []
            settlements_batch = []
            streets_batch = []
            batch_size = 5000
            processed = 0
            
            for event, elem in et.iterparse(file_path, events=('end',)):
                if elem.tag == 'OBJECT':
                    attr = elem.attrib
                    
                    if attr.get('ISACTUAL') == '1' and attr.get('ISACTIVE') == '1':
                        level = attr.get('LEVEL', '')
                        
                        base_data = (
                            attr.get('ID'),
                            attr.get('OBJECTID'),
                            attr.get('OBJECTGUID'),
                            attr.get('NAME', ''),
                            attr.get('TYPENAME', ''),
                            level,
                            int(attr.get('ISACTUAL', 1)),
                            int(attr.get('ISACTIVE', 1)),
                            attr.get('UPDATEDATE')
                        )
                        
                        if level in ['3', '4']:
                            municipalities_batch.append(base_data)
                            counters['municipalities'] += 1
                        elif level in ['5', '6']:
                            settlements_batch.append(base_data)
                            counters['settlements'] += 1
                        elif level in ['7', '8']:
                            streets_batch.append(base_data)
                            counters['streets'] += 1
                        else:
                            counters['other'] += 1
                    
                    processed += 1
                    
                    # Сохраняем батчи
                    if len(municipalities_batch) >= batch_size:
                        self.insert_municipalities_batch(municipalities_batch)
                        municipalities_batch = []
                    
                    if len(settlements_batch) >= batch_size:
                        self.insert_settlements_batch(settlements_batch)
                        settlements_batch = []
                    
                    if len(streets_batch) >= batch_size:
                        self.insert_streets_batch(streets_batch)
                        streets_batch = []
                    
                    if processed % 25000 == 0:
                        print(f"  Обработано: {processed:,}")
                
                elem.clear()
            
            # Сохраняем остатки
            if municipalities_batch:
                self.insert_municipalities_batch(municipalities_batch)
            if settlements_batch:
                self.insert_settlements_batch(settlements_batch)
            if streets_batch:
                self.insert_streets_batch(streets_batch)
        
        self.connection.commit()
        
        print(f"\n✓ Адресные объекты загружены:")
        for obj_type, count in counters.items():
            print(f"  {obj_type}: {count:,}")
    
    def insert_municipalities_batch(self, batch_data):
        sql = f"""
            INSERT INTO {self.config['DB_SCHEMA']}.municipalities 
            (id, objectid, objectguid, name, typename, level, isactual, isactive, updatedate)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (objectid) DO NOTHING
        """
        self.cursor.executemany(sql, batch_data)
    
    def insert_settlements_batch(self, batch_data):
        sql = f"""
            INSERT INTO {self.config['DB_SCHEMA']}.settlements 
            (id, objectid, objectguid, name, typename, level, isactual, isactive, updatedate)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (objectid) DO NOTHING
        """
        self.cursor.executemany(sql, batch_data)
    
    def insert_streets_batch(self, batch_data):
        sql = f"""
            INSERT INTO {self.config['DB_SCHEMA']}.streets 
            (id, objectid, objectguid, name, typename, level, isactual, isactive, updatedate)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (objectid) DO NOTHING
        """
        self.cursor.executemany(sql, batch_data)

    def build_hierarchy_links_fixed(self):
        """Построение связей в иерархии С ИСПРАВЛЕННОЙ ЛОГИКОЙ ДЛЯ МО"""
        print("\n" + "=" * 50)
        print("ПОСТРОЕНИЕ СВЯЗЕЙ ИЕРАРХИИ (ИСПРАВЛЕННАЯ ВЕРСИЯ)")
        print("=" * 50)
        
        # 1. Связи муниципальных образований
        print("\n1. Связи муниципальных образований...")
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.municipalities")
        municipalities = [row[0] for row in self.cursor.fetchall()]
        
        updated = 0
        for mun_id in municipalities:
            parent_id = self.find_mo_parent(mun_id)
            if parent_id and parent_id != mun_id:
                self.cursor.execute(f"""
                    UPDATE {self.config['DB_SCHEMA']}.municipalities 
                    SET parent_id = %s WHERE objectid = %s
                """, (parent_id, mun_id))
                updated += self.cursor.rowcount
        
        print(f"  Обновлено связей МО: {updated}")
        
        # 2. Связи населенных пунктов с МО (ИСПРАВЛЕННАЯ ЛОГИКА)
        print("\n2. Связи населенных пунктов с МО (через муниципальную иерархию)...")
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.settlements")
        settlements = [row[0] for row in self.cursor.fetchall()]
        
        print(f"  Обрабатываем {len(settlements):,} НП...")
        
        updated = 0
        mo_found = 0
        
        for settlement_id in settlements:
            # Используем СПЕЦИАЛЬНУЮ функцию поиска МО
            mo_id = self.find_mo_parent(settlement_id)
            if mo_id:
                self.cursor.execute(f"""
                    UPDATE {self.config['DB_SCHEMA']}.settlements 
                    SET municipality_id = %s WHERE objectid = %s
                """, (mo_id, settlement_id))
                updated += self.cursor.rowcount
                mo_found += 1
        
        print(f"  Обновлено связей НП→МО: {updated}")
        print(f"  НП связано с МО: {mo_found:,}")
        
        # Диагностика результата
        self.diagnose_settlements_mo_links_fixed()
        
        # 3. Связи улиц с НП
        print("\n3. Связи улиц с НП...")
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.streets")
        streets = [row[0] for row in self.cursor.fetchall()]
        
        updated = 0
        for street_id in streets:
            settlement_id = self.find_parent_by_level(street_id, ['5', '6'])
            if settlement_id:
                self.cursor.execute(f"""
                    UPDATE {self.config['DB_SCHEMA']}.streets 
                    SET settlement_id = %s WHERE objectid = %s
                """, (settlement_id, street_id))
                updated += self.cursor.rowcount
        
        print(f"  Обновлено связей улиц→НП: {updated}")
        
        # 4. Связи улиц с МО через НП
        print("\n4. Связи улиц с МО через НП...")
        self.cursor.execute(f"""
            UPDATE {self.config['DB_SCHEMA']}.streets 
            SET municipality_id = s.municipality_id
            FROM {self.config['DB_SCHEMA']}.settlements s
            WHERE streets.settlement_id = s.objectid 
            AND s.municipality_id IS NOT NULL
        """)
        updated = self.cursor.rowcount
        print(f"  Обновлено связей улиц→МО: {updated}")
        
        self.connection.commit()
        print("\n✓ Связи иерархии построены (исправленная версия)")

    def link_houses_to_hierarchy_fixed(self):
        """Связывание домов с иерархией (исправленная версия для МО)"""
        print("\n" + "=" * 50)
        print("СВЯЗЫВАНИЕ ДОМОВ С ИЕРАРХИЕЙ (ИСПРАВЛЕННАЯ ВЕРСИЯ)")
        print("=" * 50)
        
        # Диагностика перед началом
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.houses")
        total_houses = self.cursor.fetchone()[0]
        print(f"Всего домов для обработки: {total_houses:,}")
        
        # Получаем все дома
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.houses")
        all_houses = [row[0] for row in self.cursor.fetchall()]
        
        print(f"\nОбработка {len(all_houses):,} домов...")
        
        updated_count = 0
        processed = 0
        batch_updates = []
        
        for house_id in all_houses:
            # Ищем связи через иерархию с ПРИОРИТЕТОМ МО
            street_id = self.find_parent_by_level(house_id, ['7', '8'])
            settlement_id = self.find_parent_by_level(house_id, ['5', '6'])
            
            # ИСПОЛЬЗУЕМ ИСПРАВЛЕННУЮ ФУНКЦИЮ ПОИСКА МО
            mo_id = self.find_mo_parent(house_id)
            
            if street_id or settlement_id or mo_id:
                batch_updates.append((street_id, settlement_id, mo_id, house_id))
                updated_count += 1
            
            processed += 1
            
            # Сохраняем батчами
            if len(batch_updates) >= 1000:
                self.execute_house_updates_batch(batch_updates)
                batch_updates = []
            
            if processed % 10000 == 0:
                print(f"  Обработано: {processed:,}, обновлено: {updated_count:,}")
                self.connection.commit()
        
        # Сохраняем остатки
        if batch_updates:
            self.execute_house_updates_batch(batch_updates)
        
        self.connection.commit()
        
        print(f"\n✓ Обработано домов: {processed:,}")
        print(f"✓ Связано домов: {updated_count:,}")
        
        if updated_count > 0:
            percentage = (updated_count / total_houses * 100)
            print(f"  Процент связанных домов: {percentage:.1f}%")
        
        # Финальная диагностика
        self.diagnose_house_connections_fixed()

    def diagnose_house_connections_fixed(self):
        """Диагностика связей домов (исправленная версия)"""
        print("\n" + "-" * 30)
        print("ДИАГНОСТИКА СВЯЗЕЙ ДОМОВ (ИСПРАВЛЕННАЯ)")
        print("-" * 30)
        
        self.cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT(street_id) as with_streets,
                COUNT(settlement_id) as with_settlements,
                COUNT(municipality_id) as with_municipalities
            FROM {self.config['DB_SCHEMA']}.houses
        """)
        
        result = self.cursor.fetchone()
        if result:
            total, with_streets, with_settlements, with_municipalities = result
            print(f"Всего домов: {total:,}")
            print(f"Связано с улицами: {with_streets:,} ({with_streets/total*100:.1f}%)")
            print(f"Связано с НП: {with_settlements:,} ({with_settlements/total*100:.1f}%)")
            print(f"Связано с МО: {with_municipalities:,} ({with_municipalities/total*100:.1f}%)")
            
            # Проверяем типы МО у домов
            self.cursor.execute(f"""
                SELECT m.level, COUNT(*) 
                FROM {self.config['DB_SCHEMA']}.houses h
                JOIN {self.config['DB_SCHEMA']}.municipalities m ON h.municipality_id = m.objectid
                GROUP BY m.level
                ORDER BY m.level
            """)
            
            mo_distribution = self.cursor.fetchall()
            if mo_distribution:
                print("Распределение домов по уровням МО:")
                for level, count in mo_distribution:
                    print(f"  Уровень {level}: {count:,} домов")

    def diagnose_settlements_mo_links_fixed(self):
        """Диагностика связей НП с МО (исправленная версия)"""
        print("\n" + "-" * 40)
        print("ДИАГНОСТИКА СВЯЗЕЙ НП С МО (ИСПРАВЛЕННАЯ)")
        print("-" * 40)
        
        # Проверяем связи НП с МО
        self.cursor.execute(f"""
            SELECT 
                COUNT(*) as total_settlements,
                COUNT(municipality_id) as settlements_with_mo
            FROM {self.config['DB_SCHEMA']}.settlements
        """)
        
        result = self.cursor.fetchone()
        if result:
            total_settlements, settlements_with_mo = result
            percentage = (settlements_with_mo / total_settlements * 100) if total_settlements > 0 else 0
            print(f"Всего НП: {total_settlements:,}")
            print(f"НП связано с МО: {settlements_with_mo:,} ({percentage:.1f}%)")
            
            if settlements_with_mo > 0:
                print("✓ НП успешно связаны с МО через муниципальную иерархию!")
                
                # Проверяем типы МО
                self.cursor.execute(f"""
                    SELECT m.level, COUNT(*) 
                    FROM {self.config['DB_SCHEMA']}.settlements s
                    JOIN {self.config['DB_SCHEMA']}.municipalities m ON s.municipality_id = m.objectid
                    GROUP BY m.level
                    ORDER BY m.level
                """)
                
                mo_types = self.cursor.fetchall()
                print("Распределение НП по уровням МО:")
                for level, count in mo_types:
                    print(f"  Уровень {level}: {count:,} НП")
            else:
                print("❌ НП не связаны с МО!")

    def find_mo_parent(self, objectid):
        """Специальный поиск родителя-МО для объекта"""
        if not objectid:
            return None
        
        objectid_str = str(objectid)
        
        # Проверяем текущий уровень
        current_level = self.level_map.get(objectid_str)
        if current_level in ['3', '4']:
            return int(objectid_str)
        
        # Идем вверх по иерархии с приоритетом муниципальной
        current_id = objectid_str
        visited = set()
        
        while current_id and current_id not in visited:
            visited.add(current_id)
            
            if current_id in self.hierarchy_map:
                parent_id = self.hierarchy_map[current_id]
                parent_level = self.level_map.get(parent_id)
                
                # МО - это уровни 3 и 4
                if parent_level in ['3', '4']:
                    return int(parent_id)
                
                current_id = parent_id
            else:
                break
        
        return None

    def test_mo_connections(self):
        """Тестирование связей муниципальных образований"""
        print("\n📋 ТЕСТ СВЯЗЕЙ МУНИЦИПАЛЬНЫХ ОБРАЗОВАНИЙ:")
        
        # Найдем несколько МО для тестирования
        test_mos = []
        for obj_id, level in self.level_map.items():
            if level in ['3', '4']:
                test_mos.append(obj_id)
                if len(test_mos) >= 5:
                    break
        
        if not test_mos:
            print("  ❌ МО для тестирования не найдены")
            return
        
        print(f"  Тестируем {len(test_mos)} МО:")
        for i, mo_id in enumerate(test_mos, 1):
            mo_str = str(mo_id)
            has_parent = mo_str in self.hierarchy_map
            
            if has_parent:
                parent = self.hierarchy_map[mo_str]
                parent_level = self.level_map.get(parent, 'НЕТ')
                print(f"    {i}. МО {mo_id} → родитель {parent} (уровень {parent_level})")
            else:
                print(f"    {i}. МО {mo_id} → родитель НЕ НАЙДЕН")
    
    def build_hierarchy_links(self):
        """Построение связей в иерархии"""
        print("\n" + "=" * 50)
        print("ПОСТРОЕНИЕ СВЯЗЕЙ ИЕРАРХИИ")
        print("=" * 50)
        
        # 1. Связи муниципальных образований
        print("\n1. Связи муниципальных образований...")
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.municipalities")
        municipalities = [row[0] for row in self.cursor.fetchall()]
        
        updated = 0
        for mun_id in municipalities:
            parent_id = self.find_parent_by_level(mun_id, ['3', '4'])
            if parent_id and parent_id != mun_id:
                self.cursor.execute(f"""
                    UPDATE {self.config['DB_SCHEMA']}.municipalities 
                    SET parent_id = %s WHERE objectid = %s
                """, (parent_id, mun_id))
                updated += self.cursor.rowcount
        
        print(f"  Обновлено связей МО: {updated}")
        
        # 2. Связи населенных пунктов с муниципальными образованиями
        print("\n2. Связи населенных пунктов с МО...")
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.settlements")
        settlements = [row[0] for row in self.cursor.fetchall()]
        
        print(f"  Обрабатываем {len(settlements):,} НП...")

        # Тестируем несколько НП с расширенным поиском
        test_settlements = settlements[:5]
        print("  Тестовая проверка первых 5 НП (расширенный поиск):")
        for i, settlement_id in enumerate(test_settlements, 1):
            # Ищем МО по уровням 2, 3, 4
            mun_id = self.find_parent_by_level(settlement_id, ['3', '4'])
            print(f"    {i}. НП {settlement_id} -> МО {mun_id}")

        updated = 0
        for settlement_id in settlements:
            mun_id = self.find_parent_by_level(settlement_id, ['3', '4'])
            if mun_id:
                self.cursor.execute(f"""
                    UPDATE {self.config['DB_SCHEMA']}.settlements 
                    SET municipality_id = %s WHERE objectid = %s
                """, (mun_id, settlement_id))
                updated += self.cursor.rowcount
        
        print(f"  Обновлено связей НП->МО: {updated}")

        self.diagnose_settlements_mo_links()
        self.deep_diagnose_hierarchy_issue()
        
        # 3. Связи улиц с населенными пунктами
        print("\n3. Связи улиц с НП...")
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.streets")
        streets = [row[0] for row in self.cursor.fetchall()]
        
        updated = 0
        for street_id in streets:
            settlement_id = self.find_parent_by_level(street_id, ['5', '6'])
            if settlement_id:
                self.cursor.execute(f"""
                    UPDATE {self.config['DB_SCHEMA']}.streets 
                    SET settlement_id = %s WHERE objectid = %s
                """, (settlement_id, street_id))
                updated += self.cursor.rowcount
        
        print(f"  Обновлено связей улиц->НП: {updated}")
        
        # 4. Связи улиц с МО через НП
        print("\n4. Связи улиц с МО через НП...")
        self.cursor.execute(f"""
            UPDATE {self.config['DB_SCHEMA']}.streets 
            SET municipality_id = s.municipality_id
            FROM {self.config['DB_SCHEMA']}.settlements s
            WHERE streets.settlement_id = s.objectid 
            AND s.municipality_id IS NOT NULL
        """)
        updated = self.cursor.rowcount
        print(f"  Обновлено связей улиц->МО: {updated}")
        
        self.connection.commit()
        print("\n✓ Связи иерархии построены")
    
    def process_houses(self, xml_directory):
        """Обработка домов"""
        print("\n" + "=" * 50)
        print("ОБРАБОТКА ДОМОВ")
        print("=" * 50)
        
        files = self.find_files(xml_directory, 'AS_HOUSES')
        if not files:
            print("Файлы AS_HOUSES не найдены")
            return
        
        # Исключаем файлы параметров
        files = [f for f in files if 'PARAM' not in f.upper()]
        if not files:
            print("Файлы AS_HOUSES (без PARAMS) не найдены")
            return
        
        total_processed = 0
        for file_path in files:
            print(f"\nОбработка: {os.path.basename(file_path)}")
            
            processed = 0
            batch_data = []
            batch_size = 5000
            
            for event, elem in et.iterparse(file_path, events=('end',)):
                if elem.tag == 'HOUSE':
                    attr = elem.attrib
                    
                    if attr.get('ISACTUAL') == '1' and attr.get('ISACTIVE') == '1':
                        batch_data.append((
                            attr.get('ID'),
                            attr.get('OBJECTID'),
                            attr.get('OBJECTGUID'),
                            attr.get('HOUSENUM'),
                            attr.get('ADDNUM1'),
                            attr.get('ADDNUM2'),
                            int(attr.get('ISACTUAL', 1)),
                            int(attr.get('ISACTIVE', 1)),
                            attr.get('UPDATEDATE')
                        ))
                    
                    processed += 1
                    
                    if len(batch_data) >= batch_size:
                        self.insert_houses_batch(batch_data)
                        batch_data = []
                    
                    if processed % 25000 == 0:
                        print(f"  Обработано: {processed:,}")
                
                elem.clear()
            
            if batch_data:
                self.insert_houses_batch(batch_data)
            
            total_processed += processed
            print(f"  Загружено домов: {processed:,}")
        
        self.connection.commit()
        print(f"\n✓ Всего домов загружено: {total_processed:,}")
    
    def insert_houses_batch(self, batch_data):
        sql = f"""
            INSERT INTO {self.config['DB_SCHEMA']}.houses 
            (id, objectid, objectguid, house_number, building_number, structure_number, isactual, isactive, updatedate)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (objectid) DO NOTHING
        """
        self.cursor.executemany(sql, batch_data)
    
    def link_houses_to_hierarchy(self):
        """Связывание домов с иерархией"""
        print("\n" + "=" * 50)
        print("СВЯЗЫВАНИЕ ДОМОВ С ИЕРАРХИЕЙ")
        print("=" * 50)
        
        # Диагностика перед началом
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.houses")
        total_houses = self.cursor.fetchone()[0]
        print(f"Всего домов для обработки: {total_houses:,}")
        
        # Проверим несколько тестовых домов
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.houses LIMIT 5")
        test_houses = [row[0] for row in self.cursor.fetchall()]
        
        print(f"\nТестовая проверка 5 домов:")
        for i, house_id in enumerate(test_houses, 1):
            house_str = str(house_id)
            in_hierarchy = house_str in self.hierarchy_map
            level = self.level_map.get(house_str, 'НЕТ')
            print(f"  {i}. Дом {house_id}: в иерархии={in_hierarchy}, уровень={level}")
            
            if in_hierarchy:
                parent = self.hierarchy_map[house_str]
                parent_level = self.level_map.get(parent, 'НЕТ')
                print(f"     Родитель: {parent} (уровень {parent_level})")
        
        # Получаем все дома
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.houses")
        all_houses = [row[0] for row in self.cursor.fetchall()]
        
        print(f"\nОбработка {len(all_houses):,} домов...")
        
        updated_count = 0
        processed = 0
        batch_updates = []
        
        for house_id in all_houses:
            # Ищем связи через иерархию
            street_id = self.find_parent_by_level(house_id, ['7', '8'])
            settlement_id = self.find_parent_by_level(house_id, ['5', '6'])
            mun_id = self.find_parent_by_level(house_id, ['3', '4'])
            
            if street_id or settlement_id or mun_id:
                batch_updates.append((street_id, settlement_id, mun_id, house_id))
                updated_count += 1
            
            processed += 1
            
            # Сохраняем батчами
            if len(batch_updates) >= 1000:
                self.execute_house_updates_batch(batch_updates)
                batch_updates = []
            
            if processed % 10000 == 0:
                print(f"  Обработано: {processed:,}, обновлено: {updated_count:,}")
                self.connection.commit()
        
        # Сохраняем остатки
        if batch_updates:
            self.execute_house_updates_batch(batch_updates)
        
        self.connection.commit()
        
        print(f"\n✓ Обработано домов: {processed:,}")
        print(f"✓ Связано домов: {updated_count:,}")
        
        if updated_count > 0:
            percentage = (updated_count / total_houses * 100)
            print(f"  Процент связанных домов: {percentage:.1f}%")
        
        # Финальная диагностика
        self.diagnose_house_connections()
    
    def execute_house_updates_batch(self, batch_updates):
        """Выполнение батча обновлений домов"""
        sql = f"""
            UPDATE {self.config['DB_SCHEMA']}.houses 
            SET street_id = %s, settlement_id = %s, municipality_id = %s
            WHERE objectid = %s
        """
        self.cursor.executemany(sql, batch_updates)
    
    def diagnose_house_connections(self):
        """Диагностика связей домов"""
        print("\n" + "-" * 30)
        print("ДИАГНОСТИКА СВЯЗЕЙ ДОМОВ")
        print("-" * 30)
        
        self.cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT(street_id) as with_streets,
                COUNT(settlement_id) as with_settlements,
                COUNT(municipality_id) as with_municipalities
            FROM {self.config['DB_SCHEMA']}.houses
        """)
        
        result = self.cursor.fetchone()
        if result:
            total, with_streets, with_settlements, with_municipalities = result
            print(f"Всего домов: {total:,}")
            print(f"Связано с улицами: {with_streets:,} ({with_streets/total*100:.1f}%)")
            print(f"Связано с НП: {with_settlements:,} ({with_settlements/total*100:.1f}%)")
            print(f"Связано с МО: {with_municipalities:,} ({with_municipalities/total*100:.1f}%)")
    
    def process_house_params(self, xml_directory):
        """Обработка параметров домов"""
        print("\n" + "=" * 50)
        print("ОБРАБОТКА ПАРАМЕТРОВ ДОМОВ")
        print("=" * 50)
        
        files = self.find_files(xml_directory, 'AS_HOUSES_PARAMS')
        if not files:
            print("Файлы AS_HOUSES_PARAMS не найдены")
            return
        
        param_mapping = {
            '8': 'cadastral_number',
            '14': 'residents_count',
            '15': 'floors_count',
        }
        
        total_updated = 0
        for file_path in files:
            print(f"\nОбработка: {os.path.basename(file_path)}")
            
            processed = 0
            updated = 0
            
            for event, elem in et.iterparse(file_path, events=('end',)):
                if elem.tag == 'PARAM':
                    attr = elem.attrib
                    
                    objectid = attr.get('OBJECTID')
                    typeid = attr.get('TYPEID')
                    value = attr.get('VALUE')
                    
                    processed += 1
                    
                    field_name = param_mapping.get(typeid)
                    if objectid and typeid and value and field_name:
                        clean_value = self.validate_param_value(field_name, value)
                        
                        if clean_value is not None:
                            try:
                                self.cursor.execute(f"""
                                    UPDATE {self.config['DB_SCHEMA']}.houses 
                                    SET {field_name} = %s 
                                    WHERE objectid = %s
                                """, (clean_value, objectid))
                                updated += self.cursor.rowcount
                            except:
                                pass
                    
                    if processed % 100000 == 0:
                        print(f"  Обработано: {processed:,}, обновлено: {updated:,}")
                        self.connection.commit()
                
                elem.clear()
            
            total_updated += updated
            print(f"  Обработано: {processed:,}, обновлено: {updated:,}")
        
        self.connection.commit()
        print(f"\n✓ Всего обновлено параметров домов: {total_updated:,}")
    
    def validate_param_value(self, field_name, value):
        """Валидация значений параметров"""
        try:
            clean_value = str(value).strip()
            if not clean_value:
                return None
            
            if field_name == 'cadastral_number':
                return clean_value[:100] if ':' in clean_value else None
            elif field_name in ['residents_count', 'floors_count']:
                try:
                    num = int(float(clean_value))
                    return num if 0 <= num <= 1000 else None
                except:
                    return None
            else:
                return clean_value[:100]
        except:
            return None
    
    def process_land_plots(self, xml_directory):
        """Обработка земельных участков"""
        print("\n" + "=" * 50)
        print("ОБРАБОТКА ЗЕМЕЛЬНЫХ УЧАСТКОВ")
        print("=" * 50)
        
        files = self.find_files(xml_directory, 'AS_STEADS')
        if not files:
            print("Файлы AS_STEADS не найдены")
            return
        
        total_processed = 0
        for file_path in files:
            print(f"\nОбработка: {os.path.basename(file_path)}")
            
            processed = 0
            batch_data = []
            batch_size = 5000
            
            for event, elem in et.iterparse(file_path, events=('end',)):
                if elem.tag == 'STEAD':
                    attr = elem.attrib
                    
                    if attr.get('ISACTUAL') == '1' and attr.get('ISACTIVE') == '1':
                        batch_data.append((
                            attr.get('ID'),
                            attr.get('OBJECTID'),
                            attr.get('OBJECTGUID'),
                            attr.get('NUMBER'),
                            int(attr.get('ISACTUAL', 1)),
                            int(attr.get('ISACTIVE', 1)),
                            attr.get('UPDATEDATE')
                        ))
                    
                    processed += 1
                    
                    if len(batch_data) >= batch_size:
                        self.insert_land_plots_batch(batch_data)
                        batch_data = []
                    
                    if processed % 25000 == 0:
                        print(f"  Обработано: {processed:,}")
                
                elem.clear()
            
            if batch_data:
                self.insert_land_plots_batch(batch_data)
            
            total_processed += processed
            print(f"  Загружено участков: {processed:,}")
        
        self.connection.commit()
        print(f"\n✓ Всего участков загружено: {total_processed:,}")
    
    def insert_land_plots_batch(self, batch_data):
        sql = f"""
            INSERT INTO {self.config['DB_SCHEMA']}.land_plots 
            (id, objectid, objectguid, number_plot, isactual, isactive, updatedate)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (objectid) DO NOTHING
        """
        self.cursor.executemany(sql, batch_data)
    
    def link_land_plots_to_hierarchy(self):
        """Связывание земельных участков с иерархией"""
        print("\n" + "=" * 50)
        print("СВЯЗЫВАНИЕ УЧАСТКОВ С ИЕРАРХИЕЙ")
        print("=" * 50)
        
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.land_plots")
        plots = [row[0] for row in self.cursor.fetchall()]
        
        updated_count = 0
        processed = 0
        
        for plot_id in plots:
            settlement_id = self.find_parent_by_level(plot_id, ['5', '6'])
            mun_id = self.find_parent_by_level(plot_id, ['3', '4'])
            
            if settlement_id or mun_id:
                self.cursor.execute(f"""
                    UPDATE {self.config['DB_SCHEMA']}.land_plots 
                    SET settlement_id = %s, municipality_id = %s
                    WHERE objectid = %s
                """, (settlement_id, mun_id, plot_id))
                updated_count += self.cursor.rowcount
            
            processed += 1
            if processed % 10000 == 0:
                print(f"  Обработано: {processed:,}")
                self.connection.commit()
        
        self.connection.commit()
        print(f"\n✓ Связано участков: {updated_count:,}")
    
    def build_full_addresses(self):
        """Построение полных адресов"""
        print("\n" + "=" * 50)
        print("ПОСТРОЕНИЕ ПОЛНЫХ АДРЕСОВ")
        print("=" * 50)
        
        self.cursor.execute(f"""
            UPDATE {self.config['DB_SCHEMA']}.houses h
            SET full_address = TRIM(CONCAT_WS(', ',
                m.name,
                CASE WHEN set.name IS NOT NULL THEN set.typename || ' ' || set.name END,
                CASE WHEN st.name IS NOT NULL THEN st.typename || ' ' || st.name END,
                CASE WHEN h.house_number IS NOT NULL THEN 'д. ' || h.house_number END,
                CASE WHEN h.building_number IS NOT NULL THEN 'к. ' || h.building_number END,
                CASE WHEN h.structure_number IS NOT NULL THEN 'стр. ' || h.structure_number END
            ))
            FROM {self.config['DB_SCHEMA']}.municipalities m,
                 {self.config['DB_SCHEMA']}.settlements set,
                 {self.config['DB_SCHEMA']}.streets st
            WHERE h.municipality_id = m.objectid
            AND h.settlement_id = set.objectid
            AND h.street_id = st.objectid
        """)
        
        rows_updated = self.cursor.rowcount
        self.connection.commit()
        print(f"✓ Построены адреса для {rows_updated:,} домов")
    
    def get_statistics(self):
        """Получение итоговой статистики"""
        stats = {}
        
        tables = [
            ('municipalities', 'Муниципальные образования'),
            ('settlements', 'Населенные пункты'),
            ('streets', 'Улицы'),
            ('houses', 'Дома'),
            ('land_plots', 'Земельные участки')
        ]
        
        for table, description in tables:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.{table}")
                stats[table] = self.cursor.fetchone()[0]
            except:
                stats[table] = 0
        
        # Статистика параметров домов
        param_fields = ['cadastral_number', 'residents_count', 'floors_count']
        stats['house_params'] = {}
        
        for field in param_fields:
            try:
                self.cursor.execute(f"""
                    SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.houses 
                    WHERE {field} IS NOT NULL
                """)
                stats['house_params'][field] = self.cursor.fetchone()[0]
            except:
                stats['house_params'][field] = 0
        
        # Статистика связей
        stats['connections'] = {}
        try:
            # Дома с улицами
            self.cursor.execute(f"""
                SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.houses 
                WHERE street_id IS NOT NULL
            """)
            stats['connections']['houses_with_streets'] = self.cursor.fetchone()[0]
            
            # Дома с НП
            self.cursor.execute(f"""
                SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.houses 
                WHERE settlement_id IS NOT NULL
            """)
            stats['connections']['houses_with_settlements'] = self.cursor.fetchone()[0]
            
            # Дома с МО
            self.cursor.execute(f"""
                SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.houses 
                WHERE municipality_id IS NOT NULL
            """)
            stats['connections']['houses_with_municipalities'] = self.cursor.fetchone()[0]
            
        except:
            pass
        
        return stats
    
    def fix_municipality_links(self):
        """Исправление связей с муниципальными образованиями через НП"""
        print("\n" + "=" * 50)
        print("ИСПРАВЛЕНИЕ СВЯЗЕЙ С МУНИЦИПАЛЬНЫМИ ОБРАЗОВАНИЯМИ")
        print("=" * 50)
        
        # Обновляем связи домов с МО через НП
        print("Обновление связей домов->МО через НП...")
        self.cursor.execute(f"""
            UPDATE {self.config['DB_SCHEMA']}.houses h
            SET municipality_id = s.municipality_id
            FROM {self.config['DB_SCHEMA']}.settlements s
            WHERE h.settlement_id = s.objectid 
            AND s.municipality_id IS NOT NULL
            AND h.municipality_id IS NULL
        """)
        houses_updated = self.cursor.rowcount
        print(f"  Обновлено домов->МО: {houses_updated:,}")
        
        # Обновляем связи участков с МО через НП  
        print("Обновление связей участков->МО через НП...")
        self.cursor.execute(f"""
            UPDATE {self.config['DB_SCHEMA']}.land_plots lp
            SET municipality_id = s.municipality_id
            FROM {self.config['DB_SCHEMA']}.settlements s
            WHERE lp.settlement_id = s.objectid 
            AND s.municipality_id IS NOT NULL
            AND lp.municipality_id IS NULL
        """)
        plots_updated = self.cursor.rowcount
        print(f"  Обновлено участков->МО: {plots_updated:,}")
        
        self.connection.commit()
        print(f"\n✓ Исправлены связи с МО")
        
        # Проверяем результат
        self.cursor.execute(f"""
            SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.houses 
            WHERE municipality_id IS NOT NULL
        """)
        houses_with_mo = self.cursor.fetchone()[0]
        
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.houses")
        total_houses = self.cursor.fetchone()[0]
        
        percentage = (houses_with_mo / total_houses * 100) if total_houses > 0 else 0
        print(f"Домов связано с МО: {houses_with_mo:,} ({percentage:.1f}%)")
    
    def diagnose_settlements_mo_links(self):
        """Диагностика связей НП с МО"""
        print("\n" + "-" * 40)
        print("ДИАГНОСТИКА СВЯЗЕЙ НП С МО")
        print("-" * 40)
        
        # Проверяем связи НП с МО
        self.cursor.execute(f"""
            SELECT 
                COUNT(*) as total_settlements,
                COUNT(municipality_id) as settlements_with_mo
            FROM {self.config['DB_SCHEMA']}.settlements
        """)
        
        result = self.cursor.fetchone()
        if result:
            total_settlements, settlements_with_mo = result
            percentage = (settlements_with_mo / total_settlements * 100) if total_settlements > 0 else 0
            print(f"Всего НП: {total_settlements:,}")
            print(f"НП связано с МО: {settlements_with_mo:,} ({percentage:.1f}%)")
            
            if settlements_with_mo == 0:
                print("⚠️  ПРОБЛЕМА: НП не связаны с МО!")
                
                # Проверим несколько тестовых НП
                self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.settlements LIMIT 5")
                test_settlements = [row[0] for row in self.cursor.fetchall()]
                
                print("Тестовая проверка 5 НП:")
                for i, settlement_id in enumerate(test_settlements, 1):
                    mun_id = self.find_parent_by_level(settlement_id, ['3', '4'])
                    print(f"  {i}. НП {settlement_id} -> МО {mun_id}")

    def deep_diagnose_hierarchy_issue(self):
        """Глубокая диагностика проблем с иерархией"""
        print("\n" + "=" * 50)
        print("ГЛУБОКАЯ ДИАГНОСТИКА ИЕРАРХИИ")
        print("=" * 50)
        
        # Проверим один конкретный НП
        test_settlement = 1226471
        print(f"Диагностика НП {test_settlement}:")
        
        # 1. Есть ли НП в level_map?
        settlement_level = self.level_map.get(str(test_settlement))
        print(f"  Уровень НП в level_map: {settlement_level}")
        
        # 2. Есть ли НП в hierarchy_map?
        settlement_str = str(test_settlement)
        has_parent = settlement_str in self.hierarchy_map
        print(f"  НП в hierarchy_map: {has_parent}")
        
        if has_parent:
            parent = self.hierarchy_map[settlement_str]
            parent_level = self.level_map.get(parent, 'НЕТ')
            print(f"  Родитель НП: {parent} (уровень {parent_level})")
            
            # Идем дальше по цепочке
            current = parent
            chain = [current]
            for step in range(5):  # максимум 5 шагов
                if current in self.hierarchy_map:
                    current = self.hierarchy_map[current]
                    level = self.level_map.get(current, 'НЕТ')
                    chain.append(f"{current}(ур.{level})")
                    if level in ['3', '4']:
                        print(f"  Найден МО в цепочке: {current} на шаге {step+1}")
                        break
                else:
                    break
            
            print(f"  Цепочка: {' -> '.join(chain)}")

        self.analyze_region_mo_connections()
        
        # 3. Проверим статистику уровней в hierarchy_map
        print(f"\nСтатистика объектов в hierarchy_map по уровням:")
        level_counts = {}
        for obj_id in self.hierarchy_map.keys():
            level = self.level_map.get(obj_id, 'НЕТ')
            level_counts[level] = level_counts.get(level, 0) + 1
        
        for level in sorted(level_counts.keys()):
            print(f"  Уровень {level}: {level_counts[level]:,} объектов в hierarchy_map")
        
        # 4. Проверим есть ли вообще МО в hierarchy_map
        mo_in_hierarchy = 0
        for obj_id in self.hierarchy_map.keys():
            level = self.level_map.get(obj_id)
            if level in ['3', '4']:
                mo_in_hierarchy += 1
        
        print(f"\nМО (уровни 3-4) в hierarchy_map: {mo_in_hierarchy}")
        
        # 5. Проверим несколько МО
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.municipalities LIMIT 3")
        test_mos = [row[0] for row in self.cursor.fetchall()]
        
        print(f"\nПроверка МО:")
        for mo_id in test_mos:
            mo_str = str(mo_id)
            in_hierarchy = mo_str in self.hierarchy_map
            level = self.level_map.get(mo_str, 'НЕТ')
            print(f"  МО {mo_id}: в hierarchy_map={in_hierarchy}, уровень={level}")

    def analyze_region_mo_connections(self):
        """Анализ связей районов с МО"""
        print("\n" + "=" * 50)
        print("АНАЛИЗ СВЯЗЕЙ РАЙОНОВ С МО")
        print("=" * 50)
        
        # Найдем район из нашего примера
        test_region = 1225572  # район из диагностики НП 1226471
        
        print(f"Анализ района {test_region}:")
        region_str = str(test_region)
        region_level = self.level_map.get(region_str)
        print(f"  Уровень района: {region_level}")
        
        # Поищем все объекты уровня 2
        regions_level_2 = []
        for obj_id, level in self.level_map.items():
            if level == '2':
                regions_level_2.append(obj_id)
        
        print(f"  Всего объектов уровня 2: {len(regions_level_2)}")
        
        # Проверим связи нескольких районов с МО
        print(f"\nПроверка связей районов с МО:")
        for i, region_id in enumerate(regions_level_2[:5], 1):
            # Ищем МО через промежуточные уровни
            mo_id = self.find_parent_by_level(region_id, ['3', '4'])
            print(f"  {i}. Район {region_id} -> МО {mo_id}")
            
            # Если не нашли напрямую, посмотрим цепочку
            if not mo_id:
                current = region_id
                chain = [current]
                for step in range(3):
                    if current in self.hierarchy_map:
                        current = self.hierarchy_map[current]
                        level = self.level_map.get(current, 'НЕТ')
                        chain.append(f"{current}(ур.{level})")
                    else:
                        break
                print(f"     Цепочка: {' -> '.join(chain)}")
        
        # Альтернативный подход: поиск МО географически
        print(f"\nАльтернатива: поиск МО того же региона...")
        
        # Проверим, есть ли общие родители у районов и МО
        self.cursor.execute(f"SELECT objectid FROM {self.config['DB_SCHEMA']}.municipalities LIMIT 3")
        test_mos = [row[0] for row in self.cursor.fetchall()]
        
        for mo_id in test_mos:
            mo_str = str(mo_id)
            if mo_str in self.hierarchy_map:
                parent = self.hierarchy_map[mo_str]
                parent_level = self.level_map.get(parent, 'НЕТ')
                print(f"  МО {mo_id} -> родитель {parent} (ур.{parent_level})")

    def create_geographic_mo_mapping(self):
        """Создание географического соответствия районов и МО"""
        print("\n" + "=" * 50)
        print("СОЗДАНИЕ ГЕОГРАФИЧЕСКОГО СООТВЕТСТВИЯ")
        print("=" * 50)
        
        # Создаем таблицу соответствий район->МО
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.config['DB_SCHEMA']}.region_mo_mapping (
                region_id BIGINT,
                mo_id BIGINT,
                PRIMARY KEY (region_id, mo_id)
            )
        """)
        
        # Логика: в каждом районе ищем наиболее представленное МО
        # через дома, которые уже связаны
        
        self.cursor.execute(f"""
            INSERT INTO {self.config['DB_SCHEMA']}.region_mo_mapping (region_id, mo_id)
            SELECT DISTINCT
                s.municipality_id as region_id,  -- это район (уровень 2)
                h.municipality_id as mo_id       -- это МО из домов
            FROM {self.config['DB_SCHEMA']}.houses h
            JOIN {self.config['DB_SCHEMA']}.settlements s ON h.settlement_id = s.objectid
            WHERE h.municipality_id IS NOT NULL 
            AND s.municipality_id IS NOT NULL
            ON CONFLICT DO NOTHING
        """)
        
        mappings_created = self.cursor.rowcount
        print(f"Создано соответствий район->МО: {mappings_created}")
        
        # Теперь обновляем НП и дома через эту таблицу
        print("Обновление НП через географическое соответствие...")
        self.cursor.execute(f"""
            UPDATE {self.config['DB_SCHEMA']}.settlements s
            SET municipality_id = mapping.mo_id
            FROM {self.config['DB_SCHEMA']}.region_mo_mapping mapping
            WHERE s.municipality_id = mapping.region_id
            AND s.municipality_id IN (
                SELECT objectid FROM {self.config['DB_SCHEMA']}.municipalities 
                WHERE level = '2'
            )
        """)
        
        settlements_updated = self.cursor.rowcount
        print(f"Обновлено НП: {settlements_updated}")
        
        # Обновляем дома через обновленные НП
        print("Обновление домов через географическое соответствие...")
        self.cursor.execute(f"""
            UPDATE {self.config['DB_SCHEMA']}.houses h
            SET municipality_id = s.municipality_id
            FROM {self.config['DB_SCHEMA']}.settlements s
            WHERE h.settlement_id = s.objectid
            AND h.municipality_id IS NULL
            AND s.municipality_id IS NOT NULL
        """)
        
        houses_updated = self.cursor.rowcount
        print(f"Обновлено домов: {houses_updated}")
        
        self.connection.commit()
        print("✓ Географическое соответствие создано")

    def fix_final_mo_connections(self):
        """Финальное исправление связей с МО"""
        print("\n" + "=" * 50)
        print("ФИНАЛЬНОЕ ИСПРАВЛЕНИЕ СВЯЗЕЙ С МО")
        print("=" * 50)
        
        # 1. Найдем дома, которые связаны с МО через улицы
        print("1. Анализ существующих связей...")
        
        self.cursor.execute(f"""
            SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.houses h
            JOIN {self.config['DB_SCHEMA']}.streets st ON h.street_id = st.objectid
            WHERE st.municipality_id IS NOT NULL
            AND h.municipality_id IS NULL
        """)
        
        houses_can_be_linked = self.cursor.fetchone()[0]
        print(f"  Домов можно связать через улицы: {houses_can_be_linked:,}")
        
        # 2. Обновляем дома через улицы
        print("2. Обновление домов через улицы...")
        
        self.cursor.execute(f"""
            UPDATE {self.config['DB_SCHEMA']}.houses h
            SET municipality_id = st.municipality_id
            FROM {self.config['DB_SCHEMA']}.streets st
            WHERE h.street_id = st.objectid
            AND st.municipality_id IS NOT NULL
            AND h.municipality_id IS NULL
        """)
        
        houses_updated_via_streets = self.cursor.rowcount
        print(f"  Обновлено домов через улицы: {houses_updated_via_streets:,}")
        
        # 3. Для оставшихся домов - прямой поиск через иерархию
        print("3. Прямой поиск МО для оставшихся домов...")
        
        self.cursor.execute(f"""
            SELECT h.objectid 
            FROM {self.config['DB_SCHEMA']}.houses h
            WHERE h.municipality_id IS NULL
            LIMIT 50000
        """)
        
        remaining_houses = [row[0] for row in self.cursor.fetchall()]
        print(f"  Обрабатываем {len(remaining_houses):,} домов без МО...")
        
        batch_updates = []
        updated_count = 0
        
        for house_id in remaining_houses:
            # Прямой поиск МО через hierarchy_map
            mo_id = self.find_parent_by_level(house_id, ['3', '4'])
            if mo_id:
                batch_updates.append((mo_id, house_id))
                updated_count += 1
            
            # Сохраняем батчами
            if len(batch_updates) >= 1000:
                self.cursor.executemany(f"""
                    UPDATE {self.config['DB_SCHEMA']}.houses 
                    SET municipality_id = %s WHERE objectid = %s
                """, batch_updates)
                batch_updates = []
        
        # Сохраняем остатки
        if batch_updates:
            self.cursor.executemany(f"""
                UPDATE {self.config['DB_SCHEMA']}.houses 
                SET municipality_id = %s WHERE objectid = %s
            """, batch_updates)
        
        print(f"  Обновлено домов прямым поиском: {updated_count:,}")
        
        self.connection.commit()
        
        # 4. Финальная проверка
        print("4. Финальная проверка...")
        
        self.cursor.execute(f"""
            SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.houses 
            WHERE municipality_id IS NOT NULL
        """)
        total_linked = self.cursor.fetchone()[0]
        
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.config['DB_SCHEMA']}.houses")
        total_houses = self.cursor.fetchone()[0]
        
        percentage = (total_linked / total_houses * 100) if total_houses > 0 else 0
        print(f"  Итого домов связано с МО: {total_linked:,} ({percentage:.1f}%)")
        
        print("✓ Финальное исправление завершено")

    def close(self):
        """Закрытие соединения"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


def main():
    """Главная функция"""
    print("=" * 60)
    print("НОВЫЙ ФИАС ПАРСЕР С ПРАВИЛЬНОЙ ЛОГИКОЙ")
    print("=" * 60)
    print(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Загружаем конфигурацию
    try:
        from config import (
            DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_SCHEMA,
            XML_DIRECTORY, REGION_CODE
        )
        
        config = {
            'DB_HOST': DB_HOST,
            'DB_PORT': DB_PORT,
            'DB_USER': DB_USER,
            'DB_PASSWORD': DB_PASSWORD,
            'DB_NAME': DB_NAME,
            'DB_SCHEMA': DB_SCHEMA,
            'XML_DIRECTORY': XML_DIRECTORY,
            'REGION_CODE': REGION_CODE
        }
        
        print(f"Регион: {REGION_CODE}")
        print(f"XML директория: {XML_DIRECTORY}")
        print(f"БД: {DB_HOST}:{DB_PORT}/{DB_NAME}.{DB_SCHEMA}")
        
    except ImportError:
        print("ОШИБКА: Файл config.py не найден!")
        input("Нажмите Enter...")
        return
    
    # Создаем парсер
    parser = FIASParser(config)
    
    try:
        # Подключаемся к БД
        if not parser.connect():
            return
        
        # Создаем схему БД
        parser.create_schema()
        
        # Этап 1: Загружаем карты иерархии и уровней
        parser.load_hierarchy_and_levels(XML_DIRECTORY)
        
        # Этап 2: Обрабатываем адресные объекты
        parser.process_addr_objects(XML_DIRECTORY)
        
        # Этап 3: Строим связи в иерархии
        parser.build_hierarchy_links_fixed()
        
        # Этап 4: Обрабатываем дома
        parser.process_houses(XML_DIRECTORY)
        
        # Этап 5: Связываем дома с иерархией
        parser.link_houses_to_hierarchy_fixed()

        # Этап 5.1: Исправляем связи с МО
        parser.fix_municipality_links()
        
        # Этап 6: Параметры домов
        parser.process_house_params(XML_DIRECTORY)
        
        # Этап 7: Земельные участки
        parser.process_land_plots(XML_DIRECTORY)
        parser.link_land_plots_to_hierarchy()
        
        # Этап 8: Полные адреса
        parser.build_full_addresses()
        
        # Финальная статистика
        print("\n" + "=" * 60)
        print("ИТОГОВАЯ СТАТИСТИКА")
        print("=" * 60)
        
        stats = parser.get_statistics()
        
        print(f"Муниципальные образования: {stats['municipalities']:,}")
        print(f"Населенные пункты: {stats['settlements']:,}")
        print(f"Улицы: {stats['streets']:,}")
        print(f"Дома: {stats['houses']:,}")
        print(f"Земельные участки: {stats['land_plots']:,}")
        
        print(f"\nПараметры домов:")
        for param, count in stats['house_params'].items():
            percentage = (count / stats['houses'] * 100) if stats['houses'] > 0 else 0
            print(f"  {param}: {count:,} ({percentage:.1f}%)")
        
        print(f"\nСвязи домов:")
        total_houses = stats['houses']
        if total_houses > 0 and 'connections' in stats:
            connections = stats['connections']
            houses_streets = connections.get('houses_with_streets', 0)
            houses_settlements = connections.get('houses_with_settlements', 0)
            houses_municipalities = connections.get('houses_with_municipalities', 0)
            
            print(f"  С улицами: {houses_streets:,} ({houses_streets/total_houses*100:.1f}%)")
            print(f"  С НП: {houses_settlements:,} ({houses_settlements/total_houses*100:.1f}%)")
            print(f"  С МО: {houses_municipalities:,} ({houses_municipalities/total_houses*100:.1f}%)")
        
        print(f"\nВремя завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        print("ОБРАБОТКА ЗАВЕРШЕНА!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nКРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        parser.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nПроцесс прерван пользователем")
    except Exception as e:
        print(f"\nНеожиданная ошибка: {e}")
    finally:
        input("\nНажмите Enter для завершения...")