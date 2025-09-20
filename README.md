# 🏠 ФИАС Парсер с Автообновлениями

<div align="center">

![Python](https://img.shields.io/badge/Python-3.8+-blue?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue?style=for-the-badge&logo=postgresql&logoColor=white)
![Status](https://img.shields.io/badge/Status-Production-brightgreen?style=for-the-badge)

**Система парсинга и автообновления данных Федеральной Информационной Адресной Системы**

</div>

## О проекте

**ФИАС** - это федеральная база всех адресов России (квартиры, дома, улицы, населенные пункты). Данный парсер загружает XML файлы ФИАС в PostgreSQL с правильной обработкой иерархии муниципальных образований и автоматическими обновлениями.

**Создан летом 2025** в рамках работы над корпоративным приложением с интеграцией к 1С.

### Ключевые возможности

- 🔄 **Автообновления** - ежедневная проверка и применение дельта-обновлений
- 🏛️ **Корректная иерархия** - правильное связывание МО → НП → Улицы → Дома  
- 📊 **Статистика** - детальные отчеты о качестве данных
- 🗄️ **Оптимизированная БД** - индексы для быстрого поиска адресов
- 💾 **Резервное копирование** - автобэкапы перед обновлениями

## Структура данных

| Уровень | Описание | Примерное количество |
|---------|----------|---------------------|
| Муниципальные образования | Районы, округа | ~3,000 |
| Населенные пункты | Города, деревни, села | ~150,000 |
| Улицы | Улицы, проспекты, переулки | ~1,500,000 |
| Дома | Здания и строения | ~40,000,000 |

## Быстрый старт

### Установка и настройка

```bash
# Клонирование и установка
git clone https://github.com/your-username/fias-parser.git
cd fias-parser
pip install -r requirements.txt

# Настройка config.py
DB_HOST = 'localhost'
DB_USER = 'fias_user'
DB_PASSWORD = 'password'
DB_NAME = 'fias_db'
XML_DIRECTORY = './fias_xml'
REGION_CODE = '77'  # Опционально: только Москва
```

### Использование

```bash
# Первоначальная загрузка ФИАС
python fias_parser.py

# Разовое обновление
python fias_console_updater.py

# Автообновления (демон)
python fias_console_updater.py --daemon

# Только проверка обновлений
python fias_console_updater.py --check-only
```

## Структура проекта

```
fias-parser/
├── fias_parser.py              # Основной парсер
├── fias_console_updater.py     # Система автообновлений  
├── config.py                   # Конфигурация
├── requirements.txt            # Зависимости
└── fias_xml/                   # Рабочая директория
    ├── *.xml                   # XML файлы ФИАС
    ├── updates/                # Скачанные обновления
    ├── backups/                # Резервные копии
    └── logs/                   # Логи операций
```

## Примеры использования данных

### SQL запросы

```sql
-- Поиск домов по адресу
SELECT h.full_address, m.name as municipality 
FROM fias.houses h
LEFT JOIN fias.municipalities m ON h.municipality_id = m.objectid
WHERE h.full_address ILIKE '%Ленина%'
LIMIT 10;

-- Статистика по региону  
SELECT m.name, COUNT(h.objectid) as houses_count
FROM fias.municipalities m
LEFT JOIN fias.houses h ON h.municipality_id = m.objectid
GROUP BY m.name
ORDER BY houses_count DESC;
```

### Интеграция с 1С

Данные ФИАС используются для:
- Валидации адресов в CRM системах
- Автодополнения адресов в формах
- Сверки адресной информации с государственными реестрами
- Интеграции с почтовыми сервисами

## Мониторинг

После загрузки система выводит статистику:

```
=== СТАТИСТИКА ЗАГРУЗКИ ===
Муниципальные образования: 2,847
Населенные пункты: 157,123  
Улицы: 1,647,892
Дома: 41,235,678

Связи домов:
  С улицами: 93.3%
  С населенными пунктами: 97.3%
  С муниципальными образованиями: 96.7%

Время выполнения: 2ч 15мин
```

## Автоматизация

### Linux (systemd)
```bash
# Создание сервиса для автообновлений
sudo cp fias-updater.service /etc/systemd/system/
sudo systemctl enable fias-updater.service
sudo systemctl start fias-updater.service
```

### Планировщик задач
```bash
# Добавить в crontab для ежедневного обновления в 2:00
0 2 * * * /usr/bin/python3 /path/to/fias_console_updater.py
```

## Требования

- **Python 3.8+** с библиотеками psycopg2, lxml, requests
- **PostgreSQL 13+** с 50+ GB свободного места
- **RAM**: минимум 8GB, рекомендуется 16GB  
- **Интернет** для скачивания обновлений ФИАС

## Решение проблем

**Медленная загрузка:**
- Увеличить `shared_buffers` и `work_mem` в PostgreSQL
- Использовать SSD диски
- Настроить `maintenance_work_mem = 1GB`

**Ошибки памяти:**
- Обрабатывать XML файлы по частям
- Уменьшить размер batch для вставки данных

**Восстановление:**
```bash
# Откат к предыдущей версии
cp -r fias_xml/backups/backup_YYYYMMDD/* fias_xml/
python fias_parser.py
```

## Практическое применение

Проект решает реальные бизнес-задачи:
- **Валидация адресов** в корпоративных системах
- **Автодополнение** при вводе адресной информации  
- **Сверка данных** с государственными реестрами
- **Интеграция** с логистическими системами
- **Стандартизация** адресных справочников

---

<div align="center">

**Создано для интеграции с корпоративными системами и 1С**

*Автор: Дмитрий Камков | [Telegram](https://t.me/dkamkov) | [Email](mailto:dmitry.kamkov@gmail.com)*

</div>
