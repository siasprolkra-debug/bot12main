import asyncio
import logging
import sqlite3
import json
import os
import random
import time
from datetime import datetime
from pathlib import Path
import re
import sys

from telethon import TelegramClient, functions
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneNumberInvalidError,
    PhoneCodeInvalidError,
    PasswordHashInvalidError,
    FloodWaitError
)
from telethon.tl.types import InputPeerEmpty, MessageEntityTextUrl, MessageEntityUrl, MessageEntityMention, \
    MessageEntityHashtag, MessageEntityBold, MessageEntityItalic, MessageEntityCode, MessageEntityPre, \
    MessageEntityStrike, MessageMediaPhoto, MessageMediaDocument
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.utils import get_input_location

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    FSInputFile,
    InputMediaPhoto,
    InputMediaVideo
)
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

# ==================== КОНФИГУРАЦИЯ ====================
BOT_TOKEN = "8645316560:AAHY7LkVz8LQVN1Z6b_0bZNMS79qS-_VZdU"

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== ИНИЦИАЛИЗАЦИЯ БОТА ====================
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


# ==================== СОСТОЯНИЯ FSM ====================
class AccountAuth(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    code = State()
    two_fa = State()


class PresetCreation(StatesGroup):
    name = State()
    content_type = State()
    message_text = State()
    media_path = State()
    delay = State()
    duration = State()
    forward_from = State()
    forwarded_message = State()
    preset_id = State()


class PresetEdit(StatesGroup):
    waiting_for_new_text = State()
    waiting_for_new_delay = State()
    waiting_for_new_duration = State()
    waiting_for_chat_input = State()
    waiting_for_account = State()
    waiting_for_username = State()
    waiting_for_media = State()
    waiting_for_forwarded = State()


# ==================== УПРАВЛЕНИЕ РАССЫЛКАМИ ====================
class BroadcastManager:
    def __init__(self):
        self.active = {}
        self.stopped = set()

    def is_active(self, preset_id):
        return preset_id in self.active and self.active[preset_id]['status'] == 'running'

    def start(self, preset_id, total_chats):
        self.active[preset_id] = {
            'sent': 0,
            'errors': 0,
            'total': total_chats,
            'start_time': time.time(),
            'status': 'running',
            'current_index': 0,
            'loop_count': 0
        }
        if preset_id in self.stopped:
            self.stopped.remove(preset_id)

    def stop(self, preset_id):
        if preset_id in self.active:
            self.active[preset_id]['status'] = 'stopped'
            self.stopped.add(preset_id)

    def update(self, preset_id, sent=0, errors=0, current_index=None):
        if preset_id in self.active:
            self.active[preset_id]['sent'] += sent
            self.active[preset_id]['errors'] += errors
            if current_index is not None:
                self.active[preset_id]['current_index'] = current_index

    def increment_loop(self, preset_id):
        if preset_id in self.active:
            self.active[preset_id]['loop_count'] += 1
            self.active[preset_id]['current_index'] = 0

    def remove(self, preset_id):
        if preset_id in self.active:
            del self.active[preset_id]
        if preset_id in self.stopped:
            self.stopped.remove(preset_id)

    def get_info(self, preset_id):
        return self.active.get(preset_id)

    def get_all_active(self):
        return [(pid, data) for pid, data in self.active.items() if data['status'] == 'running']


broadcast_manager = BroadcastManager()


# ==================== БАЗА ДАННЫХ ====================
def adapt_datetime(dt):
    return dt.isoformat()


def convert_datetime(s):
    try:
        return datetime.fromisoformat(s.decode())
    except:
        return datetime.now()


sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("timestamp", convert_datetime)


class Database:
    def __init__(self):
        self.conn = None
        self._init_db()

    def _init_db(self):
        self.conn = sqlite3.connect('bot.db', check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = self.conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT UNIQUE,
                api_id INTEGER,
                api_hash TEXT,
                session_file TEXT,
                two_fa TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS presets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                content_type TEXT DEFAULT 'text',
                message_text TEXT,
                media_path TEXT,
                delay INTEGER DEFAULT 5,
                duration INTEGER DEFAULT 60,
                forward_from TEXT,
                is_formatted BOOLEAN DEFAULT 0,
                forwarded_data TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                preset_id INTEGER,
                chat_id TEXT,
                chat_title TEXT,
                UNIQUE(preset_id, chat_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                preset_id INTEGER,
                account_id INTEGER,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                sent INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                total INTEGER DEFAULT 0,
                loops INTEGER DEFAULT 0,
                status TEXT,
                FOREIGN KEY (preset_id) REFERENCES presets (id),
                FOREIGN KEY (account_id) REFERENCES accounts (id)
            )
        ''')

        self.conn.commit()
        print("✅ База данных инициализирована")

    def migrate_database(self):
        """Обновляет структуру базы данных, добавляя новые столбцы"""
        cursor = self.conn.cursor()

        cursor.execute("PRAGMA table_info(presets)")
        columns = [col[1] for col in cursor.fetchall()]

        if 'content_type' not in columns:
            print("➕ Добавляем столбец content_type...")
            cursor.execute("ALTER TABLE presets ADD COLUMN content_type TEXT DEFAULT 'text'")

        if 'media_path' not in columns:
            print("➕ Добавляем столбец media_path...")
            cursor.execute("ALTER TABLE presets ADD COLUMN media_path TEXT")

        if 'forward_from' not in columns:
            print("➕ Добавляем столбец forward_from...")
            cursor.execute("ALTER TABLE presets ADD COLUMN forward_from TEXT")

        if 'is_formatted' not in columns:
            print("➕ Добавляем столбец is_formatted...")
            cursor.execute("ALTER TABLE presets ADD COLUMN is_formatted BOOLEAN DEFAULT 0")

        if 'forwarded_data' not in columns:
            print("➕ Добавляем столбец forwarded_data...")
            cursor.execute("ALTER TABLE presets ADD COLUMN forwarded_data TEXT")

        cursor.execute("PRAGMA table_info(history)")
        history_columns = [col[1] for col in cursor.fetchall()]

        if 'loops' not in history_columns:
            print("➕ Добавляем столбец loops в history...")
            cursor.execute("ALTER TABLE history ADD COLUMN loops INTEGER DEFAULT 0")

        self.conn.commit()
        print("✅ База данных обновлена")

    # ========== АККАУНТЫ ==========
    def add_account(self, phone, api_id, api_hash, session_file, two_fa=None):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO accounts (phone, api_id, api_hash, session_file, two_fa) VALUES (?, ?, ?, ?, ?)",
            (phone, api_id, api_hash, session_file, two_fa)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_accounts(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, phone, api_id, api_hash, session_file, two_fa FROM accounts ORDER BY id DESC")
        return cursor.fetchall()

    def get_account(self, account_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, phone, api_id, api_hash, session_file, two_fa FROM accounts WHERE id = ?",
                       (account_id,))
        return cursor.fetchone()

    def delete_account(self, account_id):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        self.conn.commit()

    # ========== ПРЕСЕТЫ ==========
    def add_preset(self, name, content_type='text', message_text=None, media_path=None, delay=5, duration=60,
                   forward_from=None, is_formatted=False, forwarded_data=None):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO presets (name, content_type, message_text, media_path, delay, duration, forward_from, is_formatted, forwarded_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (name, content_type, message_text, media_path, delay, duration, forward_from, is_formatted, forwarded_data)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_presets(self):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, name, content_type, message_text, media_path, delay, duration, forward_from, is_formatted, forwarded_data FROM presets ORDER BY id DESC")
        return cursor.fetchall()

    def get_preset(self, preset_id):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, name, content_type, message_text, media_path, delay, duration, forward_from, is_formatted, forwarded_data FROM presets WHERE id = ?",
            (preset_id,))
        return cursor.fetchone()

    def update_preset_name(self, preset_id, name):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE presets SET name = ? WHERE id = ?", (name, preset_id))
        self.conn.commit()

    def update_preset_text(self, preset_id, new_text):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE presets SET message_text = ? WHERE id = ?", (new_text, preset_id))
        self.conn.commit()

    def update_preset_media(self, preset_id, media_path):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE presets SET media_path = ? WHERE id = ?", (media_path, preset_id))
        self.conn.commit()

    def update_preset_delay(self, preset_id, new_delay):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE presets SET delay = ? WHERE id = ?", (new_delay, preset_id))
        self.conn.commit()

    def update_preset_duration(self, preset_id, new_duration):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE presets SET duration = ? WHERE id = ?", (new_duration, preset_id))
        self.conn.commit()

    def update_preset_formatting(self, preset_id, is_formatted):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE presets SET is_formatted = ? WHERE id = ?", (is_formatted, preset_id))
        self.conn.commit()

    def update_preset_forwarded_data(self, preset_id, forwarded_data):
        cursor = self.conn.cursor()
        cursor.execute("UPDATE presets SET forwarded_data = ? WHERE id = ?", (forwarded_data, preset_id))
        self.conn.commit()

    def delete_preset(self, preset_id):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM presets WHERE id = ?", (preset_id,))
        self.conn.commit()

    # ========== ЧАТЫ ==========
    def add_chat(self, preset_id, chat_id, chat_title):
        """Добавляет чат в пресет, если его еще нет"""
        cursor = self.conn.cursor()
        try:
            # Проверяем существование
            cursor.execute("SELECT id FROM chats WHERE preset_id = ? AND chat_id = ?", (preset_id, chat_id))
            if cursor.fetchone():
                return False  # уже существует

            cursor.execute(
                "INSERT INTO chats (preset_id, chat_id, chat_title) VALUES (?, ?, ?)",
                (preset_id, chat_id, chat_title)
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            logger.error(f"Error adding chat: {e}")
            return False

    def get_chats(self, preset_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, preset_id, chat_id, chat_title FROM chats WHERE preset_id = ?", (preset_id,))
        return cursor.fetchall()

    def chat_exists(self, preset_id, chat_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM chats WHERE preset_id = ? AND chat_id = ?", (preset_id, chat_id))
        return cursor.fetchone() is not None

    def delete_chat(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM chats WHERE id = ?", (chat_id,))
        self.conn.commit()

    def clear_chats(self, preset_id):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM chats WHERE preset_id = ?", (preset_id,))
        self.conn.commit()

    # ========== ИСТОРИЯ ==========
    def add_history(self, preset_id, account_id, total):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO history (preset_id, account_id, start_time, total, status, loops) VALUES (?, ?, ?, ?, 'running', 0)",
            (preset_id, account_id, datetime.now(), total)
        )
        self.conn.commit()
        return cursor.lastrowid

    def update_history(self, history_id, sent, errors, loops=0, status='completed'):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE history SET sent = ?, errors = ?, loops = ?, end_time = ?, status = ? WHERE id = ?",
            (sent, errors, loops, datetime.now(), status, history_id)
        )
        self.conn.commit()


# ==================== ИНИЦИАЛИЗАЦИЯ И МИГРАЦИЯ ====================
db = Database()
db.migrate_database()


# ==================== TELEGRAM CLIENT MANAGER ====================
class ClientManager:
    def __init__(self):
        self.clients = {}

    async def get_client(self, phone, api_id, api_hash, session_file, two_fa=None):
        if phone in self.clients:
            return self.clients[phone]

        try:
            session_path = f"sessions/{session_file}"
            os.makedirs("sessions", exist_ok=True)

            client = TelegramClient(session_path, api_id, api_hash)
            await client.connect()

            if not await client.is_user_authorized():
                if two_fa:
                    await client.sign_in(phone=phone, password=two_fa)
                else:
                    return None

            self.clients[phone] = client
            return client
        except Exception as e:
            logger.error(f"Error creating client for {phone}: {e}")
            return None

    async def close_client(self, phone):
        if phone in self.clients:
            await self.clients[phone].disconnect()
            del self.clients[phone]

    async def parse_chats(self, account_id):
        account = db.get_account(account_id)
        if not account:
            return None

        client = await self.get_client(account[1], account[2], account[3], account[4], account[5])
        if not client:
            return None

        try:
            dialogs = await client.get_dialogs()
            chats = []
            for dialog in dialogs:
                if dialog.is_group or dialog.is_channel:
                    chat_id = str(dialog.entity.id)
                    if hasattr(dialog.entity, 'megagroup') and dialog.entity.megagroup:
                        chat_id = f"-100{chat_id}"
                    chats.append({
                        'id': chat_id,
                        'title': dialog.name
                    })
            return chats
        except Exception as e:
            logger.error(f"Error parsing chats: {e}")
            return None

    async def parse_folders(self, account_id):
        account = db.get_account(account_id)
        if not account:
            return None

        client = await self.get_client(account[1], account[2], account[3], account[4], account[5])
        if not client:
            return None

        try:
            result = await client(functions.messages.GetDialogFiltersRequest())
            folders = []

            filters = []
            if hasattr(result, 'filters'):
                filters = result.filters
            elif isinstance(result, list):
                filters = result

            for folder in filters:
                if hasattr(folder, 'title') and folder.title:
                    folder_title = folder.title
                    if hasattr(folder_title, 'text'):
                        folder_title = folder_title.text
                    elif hasattr(folder_title, '__str__'):
                        folder_title = str(folder_title)

                    folder_info = {
                        'title': folder_title,
                        'chats': []
                    }

                    peers = getattr(folder, 'include_peers', [])
                    for peer in peers:
                        try:
                            entity = await client.get_entity(peer)
                            chat_id = str(entity.id)
                            if hasattr(entity, 'megagroup') and entity.megagroup:
                                chat_id = f"-100{chat_id}"
                            folder_info['chats'].append({
                                'id': chat_id,
                                'title': getattr(entity, 'title', getattr(entity, 'first_name', 'Unknown'))
                            })
                        except:
                            continue

                    if folder_info['chats']:
                        folders.append(folder_info)

            return folders
        except Exception as e:
            logger.error(f"Error parsing folders: {e}")
            return None

    async def resolve_username(self, account_id, username):
        account = db.get_account(account_id)
        if not account:
            return None

        client = await self.get_client(account[1], account[2], account[3], account[4], account[5])
        if not client:
            return None

        try:
            entity = await client.get_entity(username)
            chat_id = str(entity.id)
            if hasattr(entity, 'megagroup') and entity.megagroup:
                chat_id = f"-100{chat_id}"
            return {
                'id': chat_id,
                'title': getattr(entity, 'title', getattr(entity, 'first_name', 'Unknown'))
            }
        except Exception as e:
            logger.error(f"Error resolving username {username}: {e}")
            return None

    async def save_forwarded_message(self, message: Message, preset_id: int):
        """Сохраняет пересланное сообщение в пресет"""
        try:
            forwarded_data = {
                'text': message.text or message.caption or '',
                'has_media': False,
                'media_type': None,
                'media_path': None
            }

            if message.photo:
                file = message.photo[-1]
                file_path = f"media/forwarded_{datetime.now().timestamp()}_{file.file_unique_id}.jpg"
                await bot.download(file, file_path)
                forwarded_data['has_media'] = True
                forwarded_data['media_type'] = 'photo'
                forwarded_data['media_path'] = file_path

            elif message.video:
                file = message.video
                file_path = f"media/forwarded_{datetime.now().timestamp()}_{file.file_unique_id}.mp4"
                await bot.download(file, file_path)
                forwarded_data['has_media'] = True
                forwarded_data['media_type'] = 'video'
                forwarded_data['media_path'] = file_path

            elif message.document:
                file = message.document
                file_path = f"media/forwarded_{datetime.now().timestamp()}_{file.file_unique_id}_{file.file_name}"
                await bot.download(file, file_path)
                forwarded_data['has_media'] = True
                forwarded_data['media_type'] = 'document'
                forwarded_data['media_path'] = file_path

            db.update_preset_forwarded_data(preset_id, json.dumps(forwarded_data))

            return True, forwarded_data
        except Exception as e:
            return False, str(e)

    async def send_message(self, client, chat_id, preset):
        try:
            entity = None
            try:
                entity = await client.get_entity(int(chat_id))
            except:
                try:
                    entity = await client.get_entity(str(chat_id))
                except:
                    try:
                        entity = await client.get_entity(chat_id)
                    except:
                        return False, f"Cannot find entity {chat_id}"

            content_type = preset[2]
            forwarded_data = preset[9] if len(preset) > 9 else None

            if content_type == 'forwarded' and forwarded_data:
                try:
                    data = json.loads(forwarded_data)

                    if data.get('has_media') and data.get('media_path') and os.path.exists(data['media_path']):
                        await client.send_file(entity, data['media_path'], caption=data.get('text', ''))
                    else:
                        await client.send_message(entity, data.get('text', ''))
                    return True, None
                except FloodWaitError as e:
                    wait_time = e.seconds
                    print(f"⚠️ Flood wait! Waiting {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                    data = json.loads(forwarded_data)
                    if data.get('has_media') and data.get('media_path') and os.path.exists(data['media_path']):
                        await client.send_file(entity, data['media_path'], caption=data.get('text', ''))
                    else:
                        await client.send_message(entity, data.get('text', ''))
                    return True, None
                except Exception as e:
                    return False, f"Ошибка отправки: {e}"

            message_text = preset[3] if preset[3] else ""
            media_path = preset[4]

            try:
                if content_type == 'text':
                    await client.send_message(entity, message_text)
                    return True, None

                elif content_type == 'photo':
                    if media_path and os.path.exists(media_path):
                        await client.send_file(entity, media_path, caption=message_text)
                    else:
                        return False, "Файл не найден"
                    return True, None

                elif content_type == 'video':
                    if media_path and os.path.exists(media_path):
                        await client.send_file(entity, media_path, caption=message_text)
                    else:
                        return False, "Файл не найден"
                    return True, None

                elif content_type == 'document':
                    if media_path and os.path.exists(media_path):
                        await client.send_file(entity, media_path, caption=message_text)
                    else:
                        return False, "Файл не найден"
                    return True, None

                elif content_type == 'forward':
                    if preset[7]:
                        try:
                            source_entity = await client.get_entity(preset[7])
                            messages = await client.get_messages(source_entity, limit=1)
                            if messages:
                                await client.forward_messages(entity, messages)
                                return True, None
                            else:
                                return False, "Нет сообщений для пересылки"
                        except Exception as e:
                            return False, f"Ошибка пересылки: {e}"
                    else:
                        return False, "Не указан источник пересылки"

                else:
                    return False, f"Неизвестный тип контента: {content_type}"

            except FloodWaitError as e:
                wait_time = e.seconds
                print(f"⚠️ Flood wait! Waiting {wait_time} seconds...")
                await asyncio.sleep(wait_time)
                if content_type == 'text':
                    await client.send_message(entity, message_text)
                else:
                    await client.send_file(entity, media_path, caption=message_text)
                return True, None

        except Exception as e:
            return False, str(e)

    async def run_broadcast(self, preset_id, account_id):
        preset = db.get_preset(preset_id)
        account = db.get_account(account_id)
        chats = db.get_chats(preset_id)

        if not preset or not account or not chats:
            print(f"DEBUG: Ошибка - нет данных для рассылки")
            return

        client = await self.get_client(account[1], account[2], account[3], account[4], account[5])
        if not client:
            print(f"DEBUG: Не удалось получить клиент")
            return

        delay = preset[5] if preset[5] is not None else 5
        duration = preset[6] if preset[6] is not None else 60

        print(f"DEBUG: Запуск рассылки: тип={preset[2]}, задержка={delay}, длительность={duration}")

        history_id = db.add_history(preset_id, account_id, len(chats))

        broadcast_manager.start(preset_id, len(chats))

        start_time = time.time()
        duration_seconds = duration * 60
        total_sent = 0
        total_errors = 0
        loops = 0

        try:
            while True:
                info = broadcast_manager.get_info(preset_id)
                if not info or info['status'] == 'stopped':
                    print(f"DEBUG: Рассылка остановлена пользователем")
                    break

                elapsed = time.time() - start_time
                if elapsed > duration_seconds:
                    print(f"DEBUG: Время вышло после {loops} циклов")
                    break

                for i, chat in enumerate(chats):
                    info = broadcast_manager.get_info(preset_id)
                    if not info or info['status'] == 'stopped':
                        break

                    if time.time() - start_time > duration_seconds:
                        break

                    chat_id = chat[2]
                    chat_title = chat[3]

                    print(f"DEBUG: Отправка в {chat_title} (ID: {chat_id})")

                    success, error = await self.send_message(client, chat_id, preset)
                    if success:
                        total_sent += 1
                        broadcast_manager.update(preset_id, sent=1, current_index=i)
                        print(f"DEBUG: ✓ Отправлено в {chat_title}")
                    else:
                        total_errors += 1
                        broadcast_manager.update(preset_id, errors=1, current_index=i)
                        print(f"DEBUG: ✗ Ошибка отправки в {chat_title}: {error}")

                    await asyncio.sleep(delay)

                loops += 1
                broadcast_manager.increment_loop(preset_id)
                print(f"DEBUG: Завершен цикл {loops}")
                await asyncio.sleep(1)

        except Exception as e:
            print(f"DEBUG: Ошибка в рассылке: {e}")
        finally:
            info = broadcast_manager.get_info(preset_id)
            if info and info['status'] == 'stopped':
                db.update_history(history_id, total_sent, total_errors, loops, 'stopped')
            else:
                db.update_history(history_id, total_sent, total_errors, loops, 'completed')
            broadcast_manager.remove(preset_id)
            print(f"DEBUG: Рассылка завершена. Циклов: {loops}, Отправлено: {total_sent}, Ошибок: {total_errors}")


client_manager = ClientManager()


# ==================== КЛАВИАТУРЫ ====================
def main_keyboard():
    kb = [
        [KeyboardButton(text="➕ Добавить аккаунт"), KeyboardButton(text="👤 Аккаунты")],
        [KeyboardButton(text="📦 Пресеты"), KeyboardButton(text="🚀 Активные рассылки")],
        [KeyboardButton(text="📊 Статус"), KeyboardButton(text="📈 Статистика")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def cancel_keyboard():
    kb = [[KeyboardButton(text="❌ Отмена")]]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def content_type_keyboard():
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="📝 Текст", callback_data="content_text"),
        InlineKeyboardButton(text="🖼 Фото", callback_data="content_photo")
    )
    kb.row(
        InlineKeyboardButton(text="🎬 Видео", callback_data="content_video"),
        InlineKeyboardButton(text="📄 Документ", callback_data="content_document")
    )
    kb.row(
        InlineKeyboardButton(text="🔄 Переслать из чата", callback_data="content_forward"),
        InlineKeyboardButton(text="📎 Переслать боту", callback_data="content_forwarded")
    )
    return kb.as_markup()


def preset_menu_keyboard(preset_id):
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="📝 Редактировать текст", callback_data=f"edit_text_{preset_id}"),
        InlineKeyboardButton(text="🖼 Редактировать медиа", callback_data=f"edit_media_{preset_id}")
    )
    kb.row(
        InlineKeyboardButton(text="⏱ Задержка", callback_data=f"edit_delay_{preset_id}"),
        InlineKeyboardButton(text="⏳ Длительность", callback_data=f"edit_duration_{preset_id}")
    )
    kb.row(
        InlineKeyboardButton(text="📋 Чаты", callback_data=f"view_chats_{preset_id}"),
        InlineKeyboardButton(text="➕ Добавить чат", callback_data=f"add_chat_menu_{preset_id}")
    )
    kb.row(
        InlineKeyboardButton(text="📂 Из папки", callback_data=f"from_folder_{preset_id}"),
        InlineKeyboardButton(text="🔄 Обновить пересланное", callback_data=f"update_forwarded_{preset_id}")
    )
    kb.row(
        InlineKeyboardButton(text="🚀 Запустить", callback_data=f"run_preset_{preset_id}"),
        InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete_preset_{preset_id}")
    )
    kb.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_presets")
    )
    return kb.as_markup()


# ==================== СТАРТ ====================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "🚀 **Бот для рассылок**\n\nВыберите действие:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_keyboard()
    )


# ==================== ДОБАВЛЕНИЕ АККАУНТА ====================
@dp.message(F.text == "➕ Добавить аккаунт")
async def add_account_start(message: Message, state: FSMContext):
    await message.answer(
        "Введите API ID (число):",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(AccountAuth.api_id)


@dp.message(AccountAuth.api_id)
async def process_api_id(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    try:
        api_id = int(message.text)
        await state.update_data(api_id=api_id)
        await message.answer("Введите API HASH:")
        await state.set_state(AccountAuth.api_hash)
    except:
        await message.answer("❌ Нужно число!")


@dp.message(AccountAuth.api_hash)
async def process_api_hash(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    await state.update_data(api_hash=message.text)
    await message.answer("Введите номер телефона (например +79123456789):")
    await state.set_state(AccountAuth.phone)


@dp.message(AccountAuth.phone)
async def process_phone(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    phone = message.text.strip()
    if not re.match(r'^\+?\d{10,15}$', phone):
        await message.answer("❌ Неверный формат номера")
        return

    data = await state.get_data()
    session_file = f"{phone.replace('+', '')}.session"

    client = TelegramClient(f"sessions/{session_file}", data['api_id'], data['api_hash'])
    await client.connect()

    try:
        await client.send_code_request(phone)
        await state.update_data(phone=phone, session_file=session_file, client=client)
        await message.answer("Введите код из Telegram:")
        await state.set_state(AccountAuth.code)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        await state.clear()


@dp.message(AccountAuth.code)
async def process_code(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    data = await state.get_data()
    client = data['client']

    try:
        await client.sign_in(phone=data['phone'], code=message.text.strip())
        db.add_account(data['phone'], data['api_id'], data['api_hash'], data['session_file'])
        await client.disconnect()
        await state.clear()
        await message.answer("✅ Аккаунт добавлен!", reply_markup=main_keyboard())
    except SessionPasswordNeededError:
        await message.answer("🔐 Введите пароль 2FA:")
        await state.set_state(AccountAuth.two_fa)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        await state.clear()


@dp.message(AccountAuth.two_fa)
async def process_2fa(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    data = await state.get_data()
    client = data['client']

    try:
        await client.sign_in(password=message.text)
        db.add_account(data['phone'], data['api_id'], data['api_hash'], data['session_file'], message.text)
        await client.disconnect()
        await state.clear()
        await message.answer("✅ Аккаунт добавлен!", reply_markup=main_keyboard())
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        await state.clear()


# ==================== УПРАВЛЕНИЕ АККАУНТАМИ ====================
@dp.message(F.text == "👤 Аккаунты")
async def list_accounts(message: Message):
    accounts = db.get_accounts()
    if not accounts:
        await message.answer("Нет аккаунтов")
        return

    text = "👤 **Аккаунты:**\n\n"
    kb = InlineKeyboardBuilder()

    for acc in accounts:
        text += f"📱 {acc[1]}\n"
        kb.row(InlineKeyboardButton(
            text=f"📱 {acc[1]}",
            callback_data=f"acc_{acc[0]}"
        ))

    await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb.as_markup())


@dp.callback_query(lambda c: c.data.startswith("acc_"))
async def account_details(query: CallbackQuery):
    acc_id = int(query.data.split("_")[1])
    acc = db.get_account(acc_id)

    if not acc:
        await query.answer("Не найден")
        return

    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="❌ Удалить", callback_data=f"delacc_{acc_id}"),
        InlineKeyboardButton(text="📁 Чаты", callback_data=f"chats_{acc_id}"),
        InlineKeyboardButton(text="📂 Папки", callback_data=f"folders_{acc_id}")
    )
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="back_accounts"))

    await query.message.edit_text(
        f"📱 **Аккаунт**\n\nНомер: {acc[1]}\nID: {acc[0]}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("delacc_"))
async def delete_account_confirm(query: CallbackQuery):
    acc_id = int(query.data.split("_")[1])

    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Да", callback_data=f"delacc_yes_{acc_id}"),
        InlineKeyboardButton(text="❌ Нет", callback_data=f"acc_{acc_id}")
    )

    await query.message.edit_text(
        "❓ **Удалить аккаунт?**",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("delacc_yes_"))
async def delete_account(query: CallbackQuery):
    acc_id = int(query.data.split("_")[2])
    db.delete_account(acc_id)
    await query.answer("✅ Удалено")
    await list_accounts(query.message)


@dp.callback_query(lambda c: c.data == "back_accounts")
async def back_to_accounts(query: CallbackQuery):
    await list_accounts(query.message)
    await query.answer()


# ==================== ПАРСИНГ ЧАТОВ ====================
@dp.callback_query(lambda c: c.data.startswith("chats_"))
async def parse_chats(query: CallbackQuery):
    acc_id = int(query.data.split("_")[1])

    await query.message.edit_text("🔄 Парсинг чатов...")
    await query.answer()

    chats = await client_manager.parse_chats(acc_id)

    if not chats:
        await query.message.edit_text(
            "❌ Нет чатов",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"acc_{acc_id}")]
            ])
        )
        return

    text = f"📊 **Найдено чатов: {len(chats)}**\n\n"
    for chat in chats[:10]:
        text += f"• {chat['title'][:30]}...\n"

    await query.message.edit_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data=f"acc_{acc_id}")]
        ])
    )


# ==================== ПАРСИНГ ПАПОК ====================
@dp.callback_query(lambda c: c.data.startswith("folders_"))
async def parse_folders(query: CallbackQuery):
    acc_id = int(query.data.split("_")[1])

    await query.message.edit_text("🔄 Парсинг папок...")
    await query.answer()

    folders = await client_manager.parse_folders(acc_id)

    if not folders:
        await query.message.edit_text(
            "❌ Нет папок",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"acc_{acc_id}")]
            ])
        )
        return

    text = f"📂 **Найдено папок: {len(folders)}**\n\n"
    for folder in folders:
        text += f"• {folder['title']} ({len(folder['chats'])} чатов)\n"

    await query.message.edit_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data=f"acc_{acc_id}")]
        ])
    )


# ==================== УПРАВЛЕНИЕ ПРЕСЕТАМИ ====================
@dp.message(F.text == "📦 Пресеты")
async def list_presets(message: Message):
    presets = db.get_presets()
    if not presets:
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="➕ Создать пресет", callback_data="new_preset"))
        await message.answer("📦 **Нет пресетов**\n\nСоздайте первый пресет:",
                             parse_mode=ParseMode.MARKDOWN,
                             reply_markup=kb.as_markup())
        return

    text = "📦 **Пресеты:**\n\n"
    kb = InlineKeyboardBuilder()

    for p in presets:
        chats = db.get_chats(p[0])
        content_emoji = {
            'text': '📝',
            'photo': '🖼',
            'video': '🎬',
            'document': '📄',
            'forward': '🔄',
            'forwarded': '📎'
        }.get(p[2], '📝')
        text += f"{content_emoji} **{p[1]}**\n"
        text += f"   Чатов: {len(chats)} | Задержка: {p[5]}с | Длит: {p[6]}мин\n\n"
        kb.row(InlineKeyboardButton(
            text=f"{content_emoji} {p[1]}",
            callback_data=f"preset_menu_{p[0]}"
        ))

    kb.row(InlineKeyboardButton(text="➕ Создать пресет", callback_data="new_preset"))

    await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb.as_markup())


@dp.callback_query(lambda c: c.data == "new_preset")
async def new_preset(query: CallbackQuery, state: FSMContext):
    await query.message.edit_text("📝 **Введите название пресета:**")
    await state.set_state(PresetCreation.name)
    await query.answer()


@dp.message(PresetCreation.name)
async def preset_name(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    await state.update_data(name=message.text)

    await message.answer(
        "📦 **Выберите тип контента:**\n\n"
        "• 📝 Текст - простое текстовое сообщение\n"
        "• 🖼 Фото - отправка фото с подписью\n"
        "• 🎬 Видео - отправка видео с подписью\n"
        "• 📄 Документ - отправка документа\n"
        "• 🔄 Переслать из чата - пересылка последнего сообщения из другого чата\n"
        "• 📎 Переслать боту - перешлите любое сообщение боту для рассылки",
        reply_markup=content_type_keyboard()
    )
    await state.set_state(PresetCreation.content_type)


@dp.callback_query(lambda c: c.data.startswith("content_"))
async def preset_content_type(query: CallbackQuery, state: FSMContext):
    content_type = query.data.split("_")[1]
    await state.update_data(content_type=content_type)

    if content_type == 'forward':
        await query.message.edit_text(
            "🔄 **Введите username или ID чата, из которого будет пересылаться сообщение:**\n\n"
            "Например: @channel_name или -1001234567890\n\n"
            "Будет переслано последнее сообщение из этого чата"
        )
        await state.set_state(PresetCreation.forward_from)
    elif content_type == 'forwarded':
        await query.message.edit_text(
            "📎 **Перешлите сообщение, которое нужно рассылать:**\n\n"
            "Просто перешлите любое сообщение (текст, фото, видео) в этот чат\n\n"
            "Сообщение будет сохранено и использовано для рассылки"
        )
        await state.set_state(PresetCreation.forwarded_message)
    else:
        await query.message.edit_text(
            "📝 **Введите текст сообщения (подпись для медиа):**\n\n"
            "Поддерживается форматирование:\n"
            "**жирный** __курсив__ ~~зачеркнутый~~ `моноширинный`\n"
            "Ссылки автоматически определяются"
        )
        await state.set_state(PresetCreation.message_text)

    await query.answer()


@dp.message(PresetCreation.forwarded_message)
async def preset_forwarded_message(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    data = await state.get_data()

    preset_id = db.add_preset(
        name=data['name'],
        content_type='forwarded',
        delay=5,
        duration=60,
        is_formatted=False
    )

    success, forwarded_data = await client_manager.save_forwarded_message(message, preset_id)

    if not success:
        await message.answer(f"❌ Ошибка сохранения сообщения: {forwarded_data}")
        await state.clear()
        return

    await state.update_data(
        preset_id=preset_id,
        forwarded_data=json.dumps(forwarded_data)
    )

    await message.answer(
        "✅ **Сообщение сохранено!**\n\n"
        f"📝 Текст: {forwarded_data['text'][:100]}...\n"
        f"{'📎 Есть медиа' if forwarded_data['has_media'] else '📎 Без медиа'}\n\n"
        "⏱ **Введите задержку между сообщениями (в секундах):**\n\n(по умолчанию 5)"
    )
    await state.set_state(PresetCreation.delay)


@dp.message(PresetCreation.forward_from)
async def preset_forward_from(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    await state.update_data(forward_from=message.text.strip())
    await message.answer("⏱ **Введите задержку между сообщениями (в секундах):**\n\n(по умолчанию 5)")
    await state.set_state(PresetCreation.delay)


@dp.message(PresetCreation.message_text)
async def preset_text(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    await state.update_data(message_text=message.text)
    await state.update_data(is_formatted=True)

    data = await state.get_data()
    content_type = data.get('content_type')

    if content_type in ['photo', 'video', 'document']:
        await message.answer(
            f"📎 **Отправьте {content_type} файл:**\n\n"
            "Просто отправьте файл в этот чат"
        )
        await state.set_state(PresetCreation.media_path)
    else:
        await message.answer("⏱ **Введите задержку между сообщениями (в секундах):**\n\n(по умолчанию 5)")
        await state.set_state(PresetCreation.delay)


@dp.message(PresetCreation.media_path)
async def preset_media(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    if message.photo:
        file = message.photo[-1]
        file_path = f"media/{datetime.now().timestamp()}_{file.file_unique_id}.jpg"
    elif message.video:
        file = message.video
        file_path = f"media/{datetime.now().timestamp()}_{file.file_unique_id}.mp4"
    elif message.document:
        file = message.document
        file_path = f"media/{datetime.now().timestamp()}_{file.file_unique_id}_{file.file_name}"
    else:
        await message.answer("❌ Отправьте фото, видео или документ")
        return

    os.makedirs("media", exist_ok=True)
    await bot.download(file, file_path)
    await state.update_data(media_path=file_path)

    await message.answer("⏱ **Введите задержку между сообщениями (в секундах):**\n\n(по умолчанию 5)")
    await state.set_state(PresetCreation.delay)


@dp.message(PresetCreation.delay)
async def preset_delay(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    try:
        delay = int(message.text) if message.text.strip() else 5
        data = await state.get_data()

        if 'preset_id' in data:
            db.update_preset_delay(data['preset_id'], delay)
        else:
            await state.update_data(delay=delay)

        await state.update_data(delay=delay)
        await message.answer("⏳ **Введите длительность рассылки (в минутах):**\n\n(по умолчанию 60)")
        await state.set_state(PresetCreation.duration)
    except ValueError:
        await message.answer("❌ Введите число")


@dp.message(PresetCreation.duration)
async def preset_duration(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    try:
        duration = int(message.text) if message.text.strip() else 60
        data = await state.get_data()

        if 'preset_id' in data:
            preset_id = data['preset_id']
            db.update_preset_duration(preset_id, duration)
            if 'name' in data:
                db.update_preset_name(preset_id, data['name'])
        else:
            preset_id = db.add_preset(
                name=data['name'],
                content_type=data.get('content_type', 'text'),
                message_text=data.get('message_text', ''),
                media_path=data.get('media_path'),
                delay=data['delay'],
                duration=duration,
                forward_from=data.get('forward_from'),
                is_formatted=data.get('is_formatted', False),
                forwarded_data=data.get('forwarded_data')
            )

        await state.clear()

        await message.answer(
            f"✅ **Пресет создан!**\n\n"
            f"Название: {data['name']}\n"
            f"Тип: {data.get('content_type', 'text')}\n"
            f"Задержка: {data.get('delay', 5)} сек\n"
            f"Длительность: {duration} мин",
            reply_markup=main_keyboard()
        )
    except ValueError:
        await message.answer("❌ Введите число")


# ==================== ОТОБРАЖЕНИЕ МЕНЮ ПРЕСЕТА ====================
async def preset_menu_display(target, preset):
    chats = db.get_chats(preset[0])
    is_active = broadcast_manager.is_active(preset[0])
    status_text = "🟢 **Активна**" if is_active else "⚪ **Неактивна**"

    info = broadcast_manager.get_info(preset[0])
    progress_text = ""
    if info and info['status'] == 'running':
        progress = info['sent'] / info['total'] * 100 if info['total'] > 0 else 0
        progress_text = f"\n**Прогресс:** {info['sent']}/{info['total']} ({progress:.1f}%)\n"
        progress_text += f"❌ Ошибок: {info['errors']}\n"
        progress_text += f"🔄 Циклов: {info['loop_count']}"

    content_emoji = {
        'text': '📝',
        'photo': '🖼',
        'video': '🎬',
        'document': '📄',
        'forward': '🔄',
        'forwarded': '📎'
    }.get(preset[2], '📝')

    text = f"{content_emoji} **Пресет: {preset[1]}**\n\n"
    text += f"{status_text}\n"

    if preset[2] == 'forward':
        text += f"🔄 Пересылка из: {preset[7]}\n"
    elif preset[2] == 'forwarded':
        if preset[9]:
            try:
                data = json.loads(preset[9])
                text += f"📎 Пересланное сообщение\n"
                text += f"📝 Текст: {data.get('text', '')[:100]}...\n"
                text += f"{'📎 Есть медиа' if data.get('has_media') else '📎 Без медиа'}\n"
            except:
                text += f"📎 Пересланное сообщение (ошибка парсинга)\n"
    elif preset[2] in ['photo', 'video', 'document']:
        if preset[4]:
            text += f"📎 Медиа: {os.path.basename(preset[4])}\n"
        if preset[3]:
            text += f"📝 Подпись: {preset[3][:100]}...\n"
    else:
        if preset[8]:
            text += f"📝 Текст (с форматированием): {preset[3][:100]}...\n"
        else:
            text += f"📝 Текст: {preset[3][:100]}...\n"

    text += f"⏱ Задержка: {preset[5]} сек\n"
    text += f"⏳ Длительность: {preset[6]} мин\n"
    text += f"📊 Чатов в списке: {len(chats)}\n"
    text += progress_text

    if isinstance(target, CallbackQuery):
        try:
            await target.message.edit_text(
                text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=preset_menu_keyboard(preset[0])
            )
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e):
                raise e
    else:
        await target.answer(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=preset_menu_keyboard(preset[0])
        )


@dp.callback_query(lambda c: c.data.startswith("preset_menu_"))
async def preset_menu(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])
    preset = db.get_preset(preset_id)

    if not preset:
        await query.answer("Пресет не найден")
        return

    await preset_menu_display(query, preset)
    await query.answer()


# ==================== РЕДАКТИРОВАНИЕ ПРЕСЕТА ====================
@dp.callback_query(lambda c: c.data.startswith("update_forwarded_"))
async def update_forwarded(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[2])
    await state.update_data(edit_id=preset_id)
    await query.message.edit_text(
        "📎 **Перешлите новое сообщение для рассылки:**\n\n"
        "Просто перешлите любое сообщение (текст, фото, видео) в этот чат"
    )
    await state.set_state(PresetEdit.waiting_for_forwarded)
    await query.answer()


@dp.message(PresetEdit.waiting_for_forwarded)
async def process_new_forwarded(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    data = await state.get_data()
    preset_id = data['edit_id']

    success, forwarded_data = await client_manager.save_forwarded_message(message, preset_id)

    if not success:
        await message.answer(f"❌ Ошибка сохранения сообщения: {forwarded_data}")
        await state.clear()
        return

    await state.clear()
    await message.answer("✅ **Пересланное сообщение обновлено!**")

    preset = db.get_preset(preset_id)
    if preset:
        await preset_menu_display(message, preset)


@dp.callback_query(lambda c: c.data.startswith("edit_text_"))
async def edit_text(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[2])
    await state.update_data(edit_id=preset_id)
    await query.message.edit_text("📝 **Введите новый текст сообщения:**")
    await state.set_state(PresetEdit.waiting_for_new_text)
    await query.answer()


@dp.message(PresetEdit.waiting_for_new_text)
async def process_new_text(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    data = await state.get_data()
    preset_id = data['edit_id']
    db.update_preset_text(preset_id, message.text)
    await state.clear()

    preset = db.get_preset(preset_id)
    if preset:
        await preset_menu_display(message, preset)


@dp.callback_query(lambda c: c.data.startswith("edit_media_"))
async def edit_media(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[2])
    await state.update_data(edit_id=preset_id)
    await query.message.edit_text("📎 **Отправьте новый медиа-файл:**")
    await state.set_state(PresetEdit.waiting_for_media)
    await query.answer()


@dp.message(PresetEdit.waiting_for_media)
async def process_new_media(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    data = await state.get_data()
    preset_id = data['edit_id']

    if message.photo:
        file = message.photo[-1]
        file_path = f"media/{datetime.now().timestamp()}_{file.file_unique_id}.jpg"
    elif message.video:
        file = message.video
        file_path = f"media/{datetime.now().timestamp()}_{file.file_unique_id}.mp4"
    elif message.document:
        file = message.document
        file_path = f"media/{datetime.now().timestamp()}_{file.file_unique_id}_{file.file_name}"
    else:
        await message.answer("❌ Отправьте фото, видео или документ")
        return

    os.makedirs("media", exist_ok=True)
    await bot.download(file, file_path)
    db.update_preset_media(preset_id, file_path)

    await state.clear()

    preset = db.get_preset(preset_id)
    if preset:
        await preset_menu_display(message, preset)


@dp.callback_query(lambda c: c.data.startswith("edit_delay_"))
async def edit_delay(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[2])
    await state.update_data(edit_id=preset_id)
    await query.message.edit_text("⏱ **Введите новую задержку (в секундах):**")
    await state.set_state(PresetEdit.waiting_for_new_delay)
    await query.answer()


@dp.message(PresetEdit.waiting_for_new_delay)
async def process_new_delay(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    try:
        delay = int(message.text)
        data = await state.get_data()
        preset_id = data['edit_id']
        db.update_preset_delay(preset_id, delay)
        await state.clear()

        preset = db.get_preset(preset_id)
        if preset:
            await preset_menu_display(message, preset)
    except ValueError:
        await message.answer("❌ Введите число")


@dp.callback_query(lambda c: c.data.startswith("edit_duration_"))
async def edit_duration(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[2])
    await state.update_data(edit_id=preset_id)
    await query.message.edit_text("⏳ **Введите новую длительность (в минутах):**")
    await state.set_state(PresetEdit.waiting_for_new_duration)
    await query.answer()


@dp.message(PresetEdit.waiting_for_new_duration)
async def process_new_duration(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    try:
        duration = int(message.text)
        data = await state.get_data()
        preset_id = data['edit_id']
        db.update_preset_duration(preset_id, duration)
        await state.clear()

        preset = db.get_preset(preset_id)
        if preset:
            await preset_menu_display(message, preset)
    except ValueError:
        await message.answer("❌ Введите число")


# ==================== УПРАВЛЕНИЕ ЧАТАМИ ====================
@dp.callback_query(lambda c: c.data.startswith("view_chats_"))
async def view_chats(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])
    chats = db.get_chats(preset_id)

    text = f"📋 **Список чатов в пресете**\n\n"
    text += f"Всего чатов: {len(chats)}\n\n"

    kb = InlineKeyboardBuilder()

    if chats:
        for i, chat in enumerate(chats[:10]):
            text += f"{i + 1}. {chat[3][:30]}...\n"
            kb.row(InlineKeyboardButton(
                text=f"❌ Удалить {i + 1}",
                callback_data=f"delchat_{chat[0]}_{preset_id}"
            ))
        if len(chats) > 10:
            text += f"... и еще {len(chats) - 10} чатов\n"
    else:
        text += "Список чатов пуст.\n"

    kb.row(
        InlineKeyboardButton(text="➕ Добавить чат", callback_data=f"add_chat_menu_{preset_id}"),
        InlineKeyboardButton(text="🗑 Очистить все", callback_data=f"clear_chats_{preset_id}")
    )
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"preset_menu_{preset_id}"))

    await query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb.as_markup())
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("delchat_"))
async def delete_chat(query: CallbackQuery):
    parts = query.data.split("_")
    chat_id = int(parts[1])
    preset_id = int(parts[2])

    db.delete_chat(chat_id)
    await query.answer("✅ Чат удален")
    await view_chats(query)


@dp.callback_query(lambda c: c.data.startswith("clear_chats_"))
async def clear_chats_confirm(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])

    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_clear_{preset_id}"),
        InlineKeyboardButton(text="❌ Нет", callback_data=f"view_chats_{preset_id}")
    )

    await query.message.edit_text(
        "❓ **Очистить все чаты?**",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("confirm_clear_"))
async def clear_chats(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])
    db.clear_chats(preset_id)
    await query.answer("✅ Все чаты удалены")
    await view_chats(query)


# ==================== ДОБАВЛЕНИЕ ЧАТА ====================
@dp.callback_query(lambda c: c.data.startswith("add_chat_menu_"))
async def add_chat_menu(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[3])
    await state.update_data(preset_id=preset_id)

    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="🔤 По username", callback_data="add_by_username"),
        InlineKeyboardButton(text="📋 Из аккаунта", callback_data="add_from_account")
    )
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"view_chats_{preset_id}"))

    await query.message.edit_text(
        "📥 **Выберите способ добавления чата:**",
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data == "add_by_username")
async def add_by_username(query: CallbackQuery, state: FSMContext):
    accounts = db.get_accounts()
    if not accounts:
        await query.answer("❌ Нет аккаунтов")
        return

    data = await state.get_data()
    preset_id = data['preset_id']

    kb = InlineKeyboardBuilder()
    for acc in accounts:
        kb.row(InlineKeyboardButton(
            text=f"📱 {acc[1]}",
            callback_data=f"select_acc_username_{acc[0]}"
        ))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"add_chat_menu_{preset_id}"))

    await query.message.edit_text(
        "📱 **Выберите аккаунт для поиска:**",
        reply_markup=kb.as_markup()
    )
    await state.set_state(PresetEdit.waiting_for_account)
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("select_acc_username_"))
async def select_acc_username(query: CallbackQuery, state: FSMContext):
    account_id = int(query.data.split("_")[3])
    await state.update_data(account_id=account_id)
    await query.message.edit_text(
        "🔤 **Введите username чата** (например @channel_name):"
    )
    await state.set_state(PresetEdit.waiting_for_username)
    await query.answer()


@dp.message(PresetEdit.waiting_for_username)
async def process_username(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    data = await state.get_data()
    preset_id = data['preset_id']
    account_id = data['account_id']

    username = message.text.strip()
    if not username.startswith('@'):
        username = '@' + username

    await message.answer("🔄 **Поиск чата...**")

    chat = await client_manager.resolve_username(account_id, username)

    if not chat:
        await message.answer(
            "❌ **Чат не найден** или нет доступа",
            reply_markup=main_keyboard()
        )
        await state.clear()
        return

    if db.chat_exists(preset_id, chat['id']):
        await message.answer(
            "⚠️ **Этот чат уже добавлен в пресет!**",
            reply_markup=main_keyboard()
        )
        await state.clear()
        return

    db.add_chat(preset_id, chat['id'], chat['title'])
    await state.clear()

    await message.answer(
        f"✅ **Чат добавлен!**\n\n"
        f"Название: {chat['title']}",
        reply_markup=main_keyboard()
    )


@dp.callback_query(lambda c: c.data == "add_from_account")
async def add_from_account(query: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    preset_id = data['preset_id']

    accounts = db.get_accounts()
    if not accounts:
        await query.answer("❌ Нет аккаунтов")
        return

    kb = InlineKeyboardBuilder()
    for acc in accounts:
        kb.row(InlineKeyboardButton(
            text=f"📱 {acc[1]}",
            callback_data=f"import_chats_{preset_id}_{acc[0]}"
        ))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"add_chat_menu_{preset_id}"))

    await query.message.edit_text(
        "📱 **Выберите аккаунт для импорта чатов:**",
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("import_chats_"))
async def import_chats(query: CallbackQuery):
    parts = query.data.split("_")
    preset_id = int(parts[2])
    account_id = int(parts[3])

    await query.message.edit_text("🔄 **Импорт чатов...**")
    await query.answer()

    chats = await client_manager.parse_chats(account_id)

    if not chats:
        await query.message.edit_text(
            "❌ **Нет чатов**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"add_chat_menu_{preset_id}")]
            ])
        )
        return

    added = 0
    skipped = 0
    for chat in chats:
        try:
            if db.add_chat(preset_id, chat['id'], chat['title']):
                added += 1
            else:
                skipped += 1
        except:
            pass

    await query.message.edit_text(
        f"✅ **Импорт завершен!**\n\n"
        f"➕ Добавлено: {added}\n"
        f"⏭️ Пропущено (уже есть): {skipped}\n"
        f"📊 Всего в аккаунте: {len(chats)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 К списку чатов", callback_data=f"view_chats_{preset_id}")]
        ])
    )


# ==================== ИМПОРТ ПАПОК ====================
@dp.callback_query(lambda c: c.data.startswith("from_folder_"))
async def from_folder(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[2])
    await state.update_data(preset_id=preset_id)

    accounts = db.get_accounts()
    if not accounts:
        await query.answer("❌ Нет аккаунтов")
        return

    kb = InlineKeyboardBuilder()
    for acc in accounts:
        kb.row(InlineKeyboardButton(
            text=f"📱 {acc[1]}",
            callback_data=f"select_folder_{acc[0]}"
        ))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"preset_menu_{preset_id}"))

    await query.message.edit_text(
        "📱 **Выберите аккаунт для просмотра папок:**",
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("select_folder_"))
async def select_folder(query: CallbackQuery, state: FSMContext):
    account_id = int(query.data.split("_")[2])
    data = await state.get_data()
    preset_id = data['preset_id']

    await query.message.edit_text("🔄 **Загрузка папок...**")
    await query.answer()

    folders = await client_manager.parse_folders(account_id)

    if not folders:
        await query.message.edit_text(
            "❌ **Нет папок с чатами**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"preset_menu_{preset_id}")]
            ])
        )
        return

    kb = InlineKeyboardBuilder()
    for i, folder in enumerate(folders):
        if folder['chats']:
            kb.row(InlineKeyboardButton(
                text=f"📁 {folder['title']} ({len(folder['chats'])} чатов)",
                callback_data=f"import_folder_{preset_id}_{account_id}_{i}"
            ))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"preset_menu_{preset_id}"))

    await query.message.edit_text(
        f"📂 **Найдено папок: {len([f for f in folders if f['chats']])}**\n\n"
        "Выберите папку для импорта:",
        reply_markup=kb.as_markup()
    )


@dp.callback_query(lambda c: c.data.startswith("import_folder_"))
async def import_folder(query: CallbackQuery):
    parts = query.data.split("_")
    preset_id = int(parts[2])
    account_id = int(parts[3])
    folder_index = int(parts[4])

    # Показываем, что процесс идет
    await query.message.edit_text("🔄 **Импорт чатов из папки...**\n\nПожалуйста, подождите.")
    await query.answer()

    folders = await client_manager.parse_folders(account_id)

    if not folders or folder_index >= len(folders):
        await query.message.edit_text(
            "❌ **Папка не найдена**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"preset_menu_{preset_id}")]
            ])
        )
        return

    folder = folders[folder_index]
    added = 0
    already_exists = 0
    errors = 0

    # Получаем список существующих чатов в пресете (для быстрой проверки)
    existing_chats = db.get_chats(preset_id)
    existing_ids = {chat[2] for chat in existing_chats}  # chat_id это индекс 2

    for chat in folder['chats']:
        try:
            # Проверяем, есть ли уже такой чат в пресете
            if chat['id'] in existing_ids:
                already_exists += 1
                continue

            if db.add_chat(preset_id, chat['id'], chat['title']):
                added += 1
                existing_ids.add(chat['id'])  # добавляем в set для следующих проверок
            else:
                errors += 1
        except Exception as e:
            logger.error(f"Ошибка при добавлении чата {chat.get('title', 'Unknown')}: {e}")
            errors += 1

    # Формируем результат
    result_text = f"📁 **Импорт из папки: {folder['title']}**\n\n"
    result_text += f"✅ **Добавлено:** {added}\n"
    result_text += f"⏭️ **Уже существовали:** {already_exists}\n"
    if errors > 0:
        result_text += f"⚠️ **Ошибок:** {errors}\n"
    result_text += f"\n📊 **Всего чатов в папке:** {len(folder['chats'])}"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 К списку чатов", callback_data=f"view_chats_{preset_id}")],
        [InlineKeyboardButton(text="🔙 Назад в меню", callback_data=f"preset_menu_{preset_id}")]
    ])

    await query.message.edit_text(result_text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    await query.answer(f"✅ Добавлено: {added}, уже было: {already_exists}")

# ==================== ЗАПУСК РАССЫЛКИ ====================
@dp.callback_query(lambda c: c.data.startswith("run_preset_"))
async def run_preset(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])

    if broadcast_manager.is_active(preset_id):
        await query.answer("❌ Рассылка уже запущена!")
        return

    chats = db.get_chats(preset_id)
    if not chats:
        await query.answer("❌ Нет чатов в пресете!")
        return

    accounts = db.get_accounts()
    if not accounts:
        await query.answer("❌ Нет аккаунтов!")
        return

    kb = InlineKeyboardBuilder()
    for acc in accounts:
        kb.row(InlineKeyboardButton(
            text=f"📱 {acc[1]}",
            callback_data=f"start_broadcast_{preset_id}_{acc[0]}"
        ))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"preset_menu_{preset_id}"))

    await query.message.edit_text(
        "👤 **Выберите аккаунт для рассылки:**",
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("start_broadcast_"))
async def start_broadcast(query: CallbackQuery):
    parts = query.data.split("_")
    preset_id = int(parts[2])
    account_id = int(parts[3])

    await query.message.edit_text("🚀 **Запуск рассылки...**")
    await query.answer()

    asyncio.create_task(client_manager.run_broadcast(preset_id, account_id))

    await query.message.answer(
        "✅ **Рассылка запущена!**\n\n"
        "Следите за статусом в разделе '🚀 Активные рассылки'",
        reply_markup=main_keyboard()
    )

    await query.message.delete()


# ==================== АКТИВНЫЕ РАССЫЛКИ ====================
@dp.message(F.text == "🚀 Активные рассылки")
async def active_broadcasts(message: Message):
    active = broadcast_manager.get_all_active()
    if not active:
        await message.answer("📭 **Нет активных рассылок**")
        return

    text = "🚀 **Активные рассылки:**\n\n"
    kb = InlineKeyboardBuilder()

    for preset_id, data in active:
        preset = db.get_preset(preset_id)
        if preset:
            progress = data['sent'] / data['total'] * 100 if data['total'] > 0 else 0
            text += f"📁 **{preset[1]}**\n"
            text += f"Прогресс: {data['sent']}/{data['total']} ({progress:.1f}%)\n"
            text += f"❌ Ошибок: {data['errors']}\n"
            text += f"🔄 Циклов: {data['loop_count']}\n\n"
            kb.row(InlineKeyboardButton(
                text=f"⏹ Остановить {preset[1]}",
                callback_data=f"stop_broadcast_{preset_id}"
            ))

    await message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(lambda c: c.data.startswith("stop_broadcast_"))
async def stop_broadcast(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])

    if broadcast_manager.is_active(preset_id):
        broadcast_manager.stop(preset_id)
        await query.answer("⏹ Рассылка остановлена")
    else:
        await query.answer("❌ Рассылка не найдена")

    await active_broadcasts(query.message)


# ==================== УДАЛЕНИЕ ПРЕСЕТА ====================
@dp.callback_query(lambda c: c.data.startswith("delete_preset_"))
async def delete_preset_confirm(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])

    if broadcast_manager.is_active(preset_id):
        await query.answer("❌ Сначала остановите рассылку!")
        return

    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_delete_{preset_id}"),
        InlineKeyboardButton(text="❌ Нет", callback_data=f"preset_menu_{preset_id}")
    )

    await query.message.edit_text(
        "❓ **Удалить пресет?**\n\nВсе чаты будут удалены.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("confirm_delete_"))
async def delete_preset(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])
    db.delete_preset(preset_id)
    await query.answer("✅ Пресет удален")
    await list_presets(query.message)


# ==================== СТАТУС ====================
@dp.message(F.text == "📊 Статус")
async def status(message: Message):
    active = broadcast_manager.get_all_active()
    if active:
        text = "📊 **Активные рассылки:**\n\n"
        for preset_id, data in active:
            preset = db.get_preset(preset_id)
            if preset:
                progress = data['sent'] / data['total'] * 100 if data['total'] > 0 else 0
                text += f"📁 {preset[1]}: {data['sent']}/{data['total']} ({progress:.1f}%) "
                text += f"🔄 {data['loop_count']} циклов\n"
    else:
        text = "📊 **Нет активных рассылок**"

    await message.answer(text)


# ==================== СТАТИСТИКА ====================
@dp.message(F.text == "📈 Статистика")
async def statistics(message: Message):
    accounts = db.get_accounts()
    presets = db.get_presets()

    total_chats = 0
    for p in presets:
        chats = db.get_chats(p[0])
        total_chats += len(chats)

    text = "📈 **Общая статистика**\n\n"
    text += f"👤 Аккаунтов: {len(accounts)}\n"
    text += f"📦 Пресетов: {len(presets)}\n"
    text += f"💬 Всего чатов: {total_chats}\n"

    await message.answer(text, parse_mode=ParseMode.MARKDOWN)


# ==================== ОТМЕНА ====================
@dp.message(F.text == "❌ Отмена")
async def cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено", reply_markup=main_keyboard())


# ==================== ВОЗВРАТ К ПРЕСЕТАМ ====================
@dp.callback_query(lambda c: c.data == "back_to_presets")
async def back_to_presets(query: CallbackQuery):
    await list_presets(query.message)
    await query.answer()


# ==================== НЕИЗВЕСТНОЕ ====================
@dp.message()
async def unknown(message: Message):
    await message.answer("❓ Используйте меню", reply_markup=main_keyboard())


# ==================== ЗАПУСК ====================
async def main():
    print("🚀 Запуск бота...")
    os.makedirs("sessions", exist_ok=True)
    os.makedirs("media", exist_ok=True)

    print("✅ Бот работает!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("❌ Бот остановлен")
    except Exception as e:
        print(f"❌ Ошибка: {e}")