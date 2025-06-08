import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pytz
from telegram import Bot
from telegram.error import TelegramError
import schedule
import time
from threading import Thread

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MessageScheduler:
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.config = self.load_config()
        self.bot = Bot(token=self.config["bot_token"])
        self.jobs = {}
        self.message_counters = {}
        
    def load_config(self) -> Dict:
        """Загружает конфигурацию из файла"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"Конфигурация загружена из {self.config_file}")
            return config
        except FileNotFoundError:
            logger.error(f"Файл конфигурации {self.config_file} не найден")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON: {e}")
            raise
    
    def validate_config(self) -> bool:
        """Валидация конфигурации"""
        required_fields = ["bot_token", "scheduled_messages"]
        
        for field in required_fields:
            if field not in self.config:
                logger.error(f"Отсутствует обязательное поле: {field}")
                return False
        
        for i, msg_config in enumerate(self.config["scheduled_messages"]):
            required_msg_fields = ["chat_id", "message", "schedule"]
            for field in required_msg_fields:
                if field not in msg_config:
                    logger.error(f"Отсутствует поле {field} в сообщении {i}")
                    return False
        
        return True
    
    async def send_message(self, chat_id: str, message: str, job_id: str):
        """Отправляет сообщение в указанный чат"""
        try:
            await self.bot.send_message(chat_id=chat_id, text=message)
            logger.info(f"Сообщение отправлено в чат {chat_id}: {message[:50]}...")
            
            # Увеличиваем счетчик отправленных сообщений
            if job_id in self.message_counters:
                self.message_counters[job_id] += 1
                
                # Проверяем лимит отправок
                job_config = next((job for job in self.config["scheduled_messages"] 
                                 if job.get("job_id", str(hash(str(job)))) == job_id), None)
                
                if job_config and "max_count" in job_config:
                    if self.message_counters[job_id] >= job_config["max_count"]:
                        logger.info(f"Достигнут лимит отправок для задачи {job_id}")
                        schedule.cancel_job(self.jobs[job_id])
                        del self.jobs[job_id]
                        
        except TelegramError as e:
            logger.error(f"Ошибка отправки сообщения в чат {chat_id}: {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке сообщения: {e}")
    
    def create_job_wrapper(self, chat_id: str, message: str, job_id: str):
        """Создает обертку для задачи с поддержкой asyncio"""
        def job_wrapper():
            asyncio.run(self.send_message(chat_id, message, job_id))
        return job_wrapper
    
    def parse_schedule(self, schedule_config: Dict, timezone_str: str) -> bool:
        """Парсит конфигурацию расписания и создает задачи"""
        try:
            tz = pytz.timezone(timezone_str)
            schedule_type = schedule_config.get("type", "daily")
            time_str = schedule_config.get("time", "12:00")
            
            # Парсим время
            try:
                hour, minute = map(int, time_str.split(":"))
            except ValueError:
                logger.error(f"Неверный формат времени: {time_str}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка парсинга расписания: {e}")
            return False
    
    def setup_scheduled_messages(self):
        """Настраивает все задачи из конфигурации"""
        for i, msg_config in enumerate(self.config["scheduled_messages"]):
            try:
                # Генерируем уникальный ID для задачи
                job_id = msg_config.get("job_id", f"job_{i}_{hash(str(msg_config))}")
                
                chat_id = msg_config["chat_id"]
                message = msg_config["message"]
                schedule_config = msg_config["schedule"]
                timezone_str = msg_config.get("timezone", "UTC")
                
                # Проверяем временную зону
                try:
                    tz = pytz.timezone(timezone_str)
                except pytz.exceptions.UnknownTimeZoneError:
                    logger.error(f"Неизвестная временная зона: {timezone_str}")
                    continue
                
                # Инициализируем счетчик сообщений
                self.message_counters[job_id] = 0
                
                # Парсим расписание
                schedule_type = schedule_config.get("type", "daily")
                time_str = schedule_config.get("time", "12:00")
                
                try:
                    hour, minute = map(int, time_str.split(":"))
                except ValueError:
                    logger.error(f"Неверный формат времени: {time_str}")
                    continue
                
                # Проверяем дату окончания
                end_date = None
                if "end_date" in msg_config:
                    try:
                        end_date = datetime.strptime(msg_config["end_date"], "%Y-%m-%d")
                        end_date = tz.localize(end_date.replace(hour=23, minute=59, second=59))
                        
                        # Проверяем, не истекла ли дата
                        current_time = datetime.now(tz)
                        if current_time > end_date:
                            logger.info(f"Задача {job_id} пропущена - дата окончания истекла")
                            continue
                    except ValueError:
                        logger.error(f"Неверный формат даты окончания: {msg_config['end_date']}")
                        continue
                
                # Создаем задачу
                job_wrapper = self.create_job_wrapper(chat_id, message, job_id)
                
                if schedule_type == "daily":
                    job = schedule.every().day.at(time_str).do(job_wrapper)
                elif schedule_type == "weekly":
                    day_of_week = schedule_config.get("day_of_week", "monday")
                    job = getattr(schedule.every(), day_of_week.lower()).at(time_str).do(job_wrapper)
                elif schedule_type == "interval":
                    interval = schedule_config.get("interval", 60)  # в минутах
                    job = schedule.every(interval).minutes.do(job_wrapper)
                elif schedule_type == "hourly":
                    job = schedule.every().hour.at(f":{minute:02d}").do(job_wrapper)
                else:
                    logger.error(f"Неподдерживаемый тип расписания: {schedule_type}")
                    continue
                
                # Добавляем задачу в словарь
                self.jobs[job_id] = job
                
                logger.info(f"Задача {job_id} создана: {schedule_type} в {time_str} для чата {chat_id}")
                
            except Exception as e:
                logger.error(f"Ошибка создания задачи {i}: {e}")
                continue
    
    def cleanup_expired_jobs(self):
        """Удаляет задачи с истекшей датой окончания"""
        current_time = datetime.now(pytz.UTC)
        expired_jobs = []
        
        for i, msg_config in enumerate(self.config["scheduled_messages"]):
            if "end_date" in msg_config:
                job_id = msg_config.get("job_id", f"job_{i}_{hash(str(msg_config))}")
                timezone_str = msg_config.get("timezone", "UTC")
                
                try:
                    tz = pytz.timezone(timezone_str)
                    end_date = datetime.strptime(msg_config["end_date"], "%Y-%m-%d")
                    end_date = tz.localize(end_date.replace(hour=23, minute=59, second=59))
                    
                    if current_time > end_date and job_id in self.jobs:
                        expired_jobs.append(job_id)
                        
                except Exception as e:
                    logger.error(f"Ошибка проверки даты окончания для задачи {job_id}: {e}")
        
        # Удаляем истекшие задачи
        for job_id in expired_jobs:
            schedule.cancel_job(self.jobs[job_id])
            del self.jobs[job_id]
            logger.info(f"Задача {job_id} удалена - дата окончания истекла")
    
    def run_scheduler(self):
        """Запускает планировщик задач"""
        logger.info("Запуск планировщика задач...")
        
        while True:
            try:
                schedule.run_pending()
                
                # Периодически проверяем истекшие задачи
                if datetime.now().minute % 1 == 0:  # каждая 1 минута
                    self.cleanup_expired_jobs()
                
                time.sleep(1)
                
            except KeyboardInterrupt:
                logger.info("Получен сигнал остановки")
                break
            except Exception as e:
                logger.error(f"Ошибка в планировщике: {e}")
                time.sleep(5)
    
    async def test_bot_connection(self):
        """Тестирует подключение к боту"""
        try:
            bot_info = await self.bot.get_me()
            logger.info(f"Бот подключен: @{bot_info.username}")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к боту: {e}")
            return False
    
    async def start(self):
        """Запускает бота"""
        logger.info("Запуск Telegram бота...")
        
        # Валидация конфигурации
        if not self.validate_config():
            logger.error("Ошибка валидации конфигурации")
            return
        
        # Тест подключения
        if not await self.test_bot_connection():
            logger.error("Не удалось подключиться к боту")
            return
        
        # Настройка задач
        self.setup_scheduled_messages()
        
        if not self.jobs:
            logger.warning("Нет активных задач для выполнения")
            return
        
        logger.info(f"Настроено {len(self.jobs)} задач")
        
        # Запуск планировщика в отдельном потоке
        scheduler_thread = Thread(target=self.run_scheduler, daemon=True)
        scheduler_thread.start()
        
        # Основной цикл
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки")
        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {e}")

def main():
    """Главная функция"""
    try:
        scheduler = MessageScheduler()
        asyncio.run(scheduler.start())
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    main()