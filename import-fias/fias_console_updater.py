# -*- coding: utf-8 -*-
"""
КОНСОЛЬНОЕ ПРИЛОЖЕНИЕ ДЛЯ АВТОМАТИЧЕСКОГО ОБНОВЛЕНИЯ ФИАС
Запуск: python fias_console_updater.py [--force] [--daemon] [--check-only]
"""

import os
import sys
import time
import json
import zipfile
import shutil
import hashlib
import logging
import argparse
import requests
import schedule
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any
import subprocess
import signal


class FIASConsoleUpdater:
    def __init__(self, config_file="config.py"):
        """Инициализация консольного обновляльщика ФИАС"""
        
        # Загружаем конфигурацию
        self.load_config(config_file)
        
        # Настраиваем пути
        self.base_dir = Path(self.config['XML_DIRECTORY'])
        self.update_dir = self.base_dir / 'updates'
        self.backup_dir = self.base_dir / 'backups'
        self.logs_dir = self.base_dir / 'logs'
        
        # Создаем директории
        for directory in [self.update_dir, self.backup_dir, self.logs_dir]:
            directory.mkdir(exist_ok=True)
        
        # Настраиваем логирование
        self.setup_logging()
        
        # API endpoints
        self.api_base = "http://fias.nalog.ru/WebServices/Public"
        
        # Флаг для остановки демона
        self.running = True
        
        self.logger.info("=" * 60)
        self.logger.info("ФИАС КОНСОЛЬНЫЙ ОБНОВЛЯЛЬЩИК ЗАПУЩЕН")
        self.logger.info("=" * 60)
    
    def load_config(self, config_file):
        """Загрузка конфигурации"""
        try:
            # Импортируем конфигурацию
            config_path = Path(config_file)
            if config_path.exists():
                spec = {}
                with open(config_path, 'r', encoding='utf-8') as f:
                    exec(f.read(), spec)
                
                self.config = {
                    'XML_DIRECTORY': spec.get('XML_DIRECTORY', './fias_xml'),
                    'DB_HOST': spec.get('DB_HOST', 'localhost'),
                    'DB_PORT': spec.get('DB_PORT', 5432),
                    'DB_USER': spec.get('DB_USER', 'postgres'),
                    'DB_PASSWORD': spec.get('DB_PASSWORD', ''),
                    'DB_NAME': spec.get('DB_NAME', 'fias'),
                    'DB_SCHEMA': spec.get('DB_SCHEMA', 'fias'),
                    'REGION_CODE': spec.get('REGION_CODE', ''),
                }
            else:
                raise FileNotFoundError(f"Файл конфигурации {config_file} не найден")
                
        except Exception as e:
            print(f"ОШИБКА ЗАГРУЗКИ КОНФИГУРАЦИИ: {e}")
            sys.exit(1)
    
    def setup_logging(self):
        """Настройка системы логирования"""
        
        # Создаем файл лога с текущей датой
        log_filename = f"fias_updater_{datetime.now().strftime('%Y%m')}.log"
        log_file = self.logs_dir / log_filename
        
        # Настраиваем форматирование
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Настраиваем логгер
        self.logger = logging.getLogger('FIASUpdater')
        self.logger.setLevel(logging.INFO)
        
        # Очищаем существующие обработчики
        self.logger.handlers.clear()
        
        # Файловый обработчик
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Консольный обработчик
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def get_version_info(self) -> Optional[Dict[str, Any]]:
        """Получение информации о последней версии ФИАС"""
        try:
            self.logger.info("Получение информации о последней версии ФИАС...")
            
            response = requests.get(
                f"{self.api_base}/GetLastDownloadFileInfo", 
                timeout=30
            )
            response.raise_for_status()
            
            version_info = response.json()
            self.logger.info(f"Последняя версия: {version_info.get('VersionId')}")
            return version_info
            
        except requests.RequestException as e:
            self.logger.error(f"Ошибка сети при получении информации о версии: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Ошибка парсинга JSON ответа: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при получении версии: {e}")
            return None
    
    def get_current_version(self) -> Optional[str]:
        """Получение текущей установленной версии"""
        version_file = self.base_dir / 'current_version.txt'
        try:
            if version_file.exists():
                current = version_file.read_text(encoding='utf-8').strip()
                self.logger.info(f"Текущая версия: {current}")
                return current
            else:
                self.logger.info("Файл текущей версии не найден")
                return None
        except Exception as e:
            self.logger.error(f"Ошибка чтения текущей версии: {e}")
            return None
    
    def save_current_version(self, version: str):
        """Сохранение информации о текущей версии"""
        version_file = self.base_dir / 'current_version.txt'
        try:
            version_file.write_text(version, encoding='utf-8')
            self.logger.info(f"Версия {version} сохранена")
        except Exception as e:
            self.logger.error(f"Ошибка сохранения версии: {e}")
    
    def download_update(self, url: str, filename: str) -> Optional[Path]:
        """Скачивание файла обновления"""
        try:
            self.logger.info(f"Скачивание обновления: {filename}")
            
            # Создаем временный файл
            temp_file = self.update_dir / f"temp_{filename}"
            final_file = self.update_dir / filename
            
            # Скачиваем с прогрессом
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Показываем прогресс каждые 10MB
                        if downloaded_size % (10 * 1024 * 1024) == 0:
                            if total_size > 0:
                                progress = (downloaded_size / total_size) * 100
                                self.logger.info(f"Скачано: {progress:.1f}%")
            
            # Переименовываем во финальное имя
            shutil.move(temp_file, final_file)
            
            self.logger.info(f"Скачивание завершено: {final_file}")
            return final_file
            
        except Exception as e:
            self.logger.error(f"Ошибка скачивания: {e}")
            return None
    
    def extract_archive(self, archive_path: Path) -> Optional[Path]:
        """Извлечение архива с обновлением"""
        try:
            extract_dir = self.update_dir / f"extracted_{archive_path.stem}"
            
            # Удаляем старую папку если есть
            if extract_dir.exists():
                shutil.rmtree(extract_dir)
            
            extract_dir.mkdir()
            
            self.logger.info(f"Извлечение архива: {archive_path}")
            
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            self.logger.info(f"Архив извлечен в: {extract_dir}")
            return extract_dir
            
        except Exception as e:
            self.logger.error(f"Ошибка извлечения архива: {e}")
            return None
    
    def backup_current_data(self) -> bool:
        """Создание резервной копии текущих данных"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"backup_{timestamp}"
            backup_path = self.backup_dir / backup_name
            
            self.logger.info("Создание резервной копии...")
            
            # Ищем XML файлы
            xml_files = list(self.base_dir.glob("*.xml"))
            
            if xml_files:
                backup_path.mkdir()
                
                for xml_file in xml_files:
                    shutil.copy2(xml_file, backup_path)
                
                # Копируем также файл версии
                version_file = self.base_dir / 'current_version.txt'
                if version_file.exists():
                    shutil.copy2(version_file, backup_path)
                
                self.logger.info(f"Резервная копия создана: {backup_path}")
                return True
            else:
                self.logger.warning("XML файлы для резервного копирования не найдены")
                return True
                
        except Exception as e:
            self.logger.error(f"Ошибка создания резервной копии: {e}")
            return False
    
    def apply_update(self, extracted_dir: Path) -> bool:
        """Применение обновления"""
        try:
            self.logger.info("Применение обновления...")
            
            # Ищем XML файлы в извлеченной директории
            xml_files = []
            for xml_file in extracted_dir.rglob("*.xml"):
                xml_files.append(xml_file)
            
            if not xml_files:
                self.logger.error("В обновлении не найдено XML файлов")
                return False
            
            self.logger.info(f"Найдено {len(xml_files)} XML файлов для обновления")
            
            # Копируем файлы
            for xml_file in xml_files:
                target_file = self.base_dir / xml_file.name
                shutil.copy2(xml_file, target_file)
                self.logger.info(f"Обновлен файл: {xml_file.name}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка применения обновления: {e}")
            return False
    
    def update_database(self) -> bool:
        """Обновление базы данных через парсер"""
        try:
            self.logger.info("Запуск обновления базы данных...")
            
            # Путь к основному парсеру
            parser_script = Path(__file__).parent / 'fias_parser.py'
            
            if not parser_script.exists():
                self.logger.error(f"Скрипт парсера не найден: {parser_script}")
                return False
            
            # Запускаем парсер
            cmd = [sys.executable, str(parser_script)]
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                encoding='utf-8',
                timeout=3600  # таймаут 1 час
            )
            
            if result.returncode == 0:
                self.logger.info("База данных успешно обновлена")
                
                # Логируем последние строки вывода парсера
                if result.stdout:
                    lines = result.stdout.strip().split('\n')[-10:]
                    for line in lines:
                        self.logger.info(f"ПАРСЕР: {line}")
                
                return True
            else:
                self.logger.error("Ошибка обновления базы данных")
                self.logger.error(f"Код возврата: {result.returncode}")
                
                if result.stderr:
                    error_lines = result.stderr.strip().split('\n')[-5:]
                    for line in error_lines:
                        self.logger.error(f"ОШИБКА ПАРСЕРА: {line}")
                
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("Таймаут при обновлении базы данных")
            return False
        except Exception as e:
            self.logger.error(f"Ошибка запуска парсера: {e}")
            return False
    
    def cleanup_old_files(self):
        """Очистка старых файлов"""
        try:
            self.logger.info("Очистка старых файлов...")
            
            current_time = time.time()
            
            # Удаляем старые обновления (старше 7 дней)
            week_ago = current_time - (7 * 24 * 3600)
            
            for file_path in self.update_dir.glob("*.zip"):
                if file_path.stat().st_mtime < week_ago:
                    file_path.unlink()
                    self.logger.info(f"Удален старый файл обновления: {file_path.name}")
            
            # Удаляем старые папки извлечения
            for dir_path in self.update_dir.glob("extracted_*"):
                if dir_path.is_dir() and dir_path.stat().st_mtime < week_ago:
                    shutil.rmtree(dir_path)
                    self.logger.info(f"Удалена старая папка: {dir_path.name}")
            
            # Удаляем старые бэкапы (старше 30 дней)
            month_ago = current_time - (30 * 24 * 3600)
            
            for backup_path in self.backup_dir.glob("backup_*"):
                if backup_path.stat().st_mtime < month_ago:
                    if backup_path.is_dir():
                        shutil.rmtree(backup_path)
                    else:
                        backup_path.unlink()
                    self.logger.info(f"Удален старый бэкап: {backup_path.name}")
            
            # Удаляем старые логи (старше 90 дней)
            three_months_ago = current_time - (90 * 24 * 3600)
            
            for log_path in self.logs_dir.glob("fias_updater_*.log"):
                if log_path.stat().st_mtime < three_months_ago:
                    log_path.unlink()
                    self.logger.info(f"Удален старый лог: {log_path.name}")
                    
        except Exception as e:
            self.logger.error(f"Ошибка очистки файлов: {e}")
    
    def check_and_update(self, force=False) -> bool:
        """Основная функция проверки и обновления"""
        try:
            self.logger.info("🔍 НАЧАЛО ПРОВЕРКИ ОБНОВЛЕНИЙ ФИАС")
            
            # Получаем информацию о версиях
            version_info = self.get_version_info()
            if not version_info:
                self.logger.error("Не удалось получить информацию о версии")
                return False
            
            latest_version = version_info.get('VersionId')
            current_version = self.get_current_version()
            
            self.logger.info(f"Текущая версия: {current_version or 'НЕ УСТАНОВЛЕНА'}")
            self.logger.info(f"Последняя версия: {latest_version}")
            
            # Проверяем необходимость обновления
            if not force and current_version == latest_version:
                self.logger.info("✅ Обновление не требуется")
                self.cleanup_old_files()
                return True
            
            # Определяем URL для скачивания
            if current_version is None:
                # Первая установка - полная версия
                download_url = version_info.get('GarXMLFullURL')
                filename = f"fias_full_{latest_version}.zip"
                self.logger.info("🔽 Скачивание полной версии ФИАС")
            else:
                # Дельта обновление
                download_url = version_info.get('GarXMLDeltaURL')
                filename = f"fias_delta_{latest_version}.zip"
                self.logger.info("🔄 Скачивание дельта-обновления")
            
            if not download_url:
                self.logger.error("URL для скачивания не найден")
                return False
            
            # Скачиваем обновление
            archive_path = self.download_update(download_url, filename)
            if not archive_path:
                return False
            
            # Извлекаем архив
            extracted_dir = self.extract_archive(archive_path)
            if not extracted_dir:
                return False
            
            # Создаем резервную копию
            if not self.backup_current_data():
                self.logger.error("Не удалось создать резервную копию")
                return False
            
            # Применяем обновление
            if not self.apply_update(extracted_dir):
                self.logger.error("Не удалось применить обновление")
                return False
            
            # Обновляем базу данных
            if not self.update_database():
                self.logger.error("Не удалось обновить базу данных")
                return False
            
            # Сохраняем новую версию
            self.save_current_version(latest_version)
            
            # Очищаем временные файлы
            self.cleanup_old_files()
            
            self.logger.info("🎉 ОБНОВЛЕНИЕ ЗАВЕРШЕНО УСПЕШНО!")
            self.logger.info(f"Новая версия: {latest_version}")
            return True
            
        except Exception as e:
            self.logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА ПРИ ОБНОВЛЕНИИ: {e}")
            return False
    
    def schedule_daily_updates(self):
        """Планирование ежедневных обновлений"""
        # Планируем на 00:00
        schedule.every().day.at("00:00").do(self.check_and_update)
        self.logger.info("📅 Запланированы ежедневные обновления в 00:00")
    
    def run_daemon(self):
        """Запуск в режиме демона"""
        self.logger.info("🔄 ЗАПУСК В РЕЖИМЕ ДЕМОНА")
        
        # Настраиваем обработку сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Планируем обновления
        self.schedule_daily_updates()
        
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # проверка каждую минуту
                
        except KeyboardInterrupt:
            self.logger.info("Получен сигнал остановки от пользователя")
        finally:
            self.logger.info("🔴 ДЕМОН ОСТАНОВЛЕН")
    
    def signal_handler(self, signum, frame):
        """Обработчик сигналов для корректной остановки"""
        self.logger.info(f"Получен сигнал {signum}, остановка...")
        self.running = False
    
    def check_only(self) -> bool:
        """Только проверка доступности обновлений без применения"""
        try:
            self.logger.info("🔍 ПРОВЕРКА ДОСТУПНОСТИ ОБНОВЛЕНИЙ")
            
            version_info = self.get_version_info()
            if not version_info:
                return False
            
            latest_version = version_info.get('VersionId')
            current_version = self.get_current_version()
            
            self.logger.info(f"Текущая версия: {current_version or 'НЕ УСТАНОВЛЕНА'}")
            self.logger.info(f"Последняя версия: {latest_version}")
            
            if current_version == latest_version:
                self.logger.info("✅ Система актуальна")
                return True
            else:
                self.logger.info("⚠️ Доступно обновление")
                return False
                
        except Exception as e:
            self.logger.error(f"Ошибка проверки: {e}")
            return False


def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Консольный обновляльщик ФИАС')
    
    parser.add_argument('--force', '-f', action='store_true',
                       help='Принудительное обновление')
    parser.add_argument('--daemon', '-d', action='store_true',
                       help='Запуск в режиме демона')
    parser.add_argument('--check-only', '-c', action='store_true',
                       help='Только проверка обновлений')
    parser.add_argument('--config', default='config.py',
                       help='Путь к файлу конфигурации')
    
    args = parser.parse_args()
    
    try:
        # Создаем обновляльщик
        updater = FIASConsoleUpdater(args.config)
        
        if args.check_only:
            # Только проверка
            success = updater.check_only()
            sys.exit(0 if success else 1)
            
        elif args.daemon:
            # Режим демона
            updater.run_daemon()
            
        else:
            # Одноразовое обновление
            success = updater.check_and_update(force=args.force)
            sys.exit(0 if success else 1)
            
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()