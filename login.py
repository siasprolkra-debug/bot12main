import asyncio
import logging
import sqlite3
import json
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
import re
import sys
from contextlib import contextmanager

from telethon import TelegramClient, functions
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneNumberInvalidError,
    PhoneCodeInvalidError,
    PasswordHashInvalidError
)
from telethon.tl.types import InputPeerEmpty
from telethon.tl.functions.messages import GetDialogsRequest

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
    FSInputFile
)
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# ==================== КОНФИГУРАЦИЯ ====================
BOT_TOKEN = "8645316560:AAHY7LkVz8LQVN1Z6b_0bZNMS79qS-_VZdU"
API_ID = 30376776
API_HASH = "ba37e163f990eba78cd11423b885dab7"

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
    text = State()
    delay = State()
    duration = State()


class PresetEdit(StatesGroup):
    waiting_for_new_text = State()
    waiting_for_new_delay = State()
    waiting_for_new_duration = State()
    waiting_for_chat_input = State()


class BroadcastControl:
    def __init__(self):
        self.active_broadcasts = {}  # preset_id: {task, status, sent, total, start_time}


broadcast_control = BroadcastControl()


# ==================== БАЗА ДАННЫХ (исправленная) ====================
class Database:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        with self._get_conn() as conn:
            cursor = conn.cursor()

            # Таблица аккаунтов
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

            # Таблица пресетов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS presets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    message_text TEXT,
                    delay INTEGER DEFAULT 5,
                    duration INTEGER DEFAULT 60,
                    status TEXT DEFAULT 'active'
                )
            ''')

            # Таблица чатов - БЕЗ chat_type
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS chats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    preset_id INTEGER,
                    chat_id TEXT,
                    chat_title TEXT,
                    FOREIGN KEY (preset_id) REFERENCES presets (id) ON DELETE CASCADE
                )
            ''')

            # Таблица истории рассылок
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS broadcast_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    preset_id INTEGER,
                    account_id INTEGER,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    messages_sent INTEGER DEFAULT 0,
                    errors INTEGER DEFAULT 0,
                    status TEXT,
                    FOREIGN KEY (preset_id) REFERENCES presets (id),
                    FOREIGN KEY (account_id) REFERENCES accounts (id)
                )
            ''')

            conn.commit()

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect('bot.db', timeout=30, check_same_thread=False)
        try:
            yield conn
        finally:
            conn.close()

    async def _execute(self, query, params=None, fetchone=False, fetchall=False, commit=False):
        async with self._lock:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                try:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)

                    if commit:
                        conn.commit()

                    if fetchone:
                        return cursor.fetchone()
                    elif fetchall:
                        return cursor.fetchall()
                    else:
                        return cursor.lastrowid if commit else None
                except Exception as e:
                    logger.error(f"SQL Error: {e}, Query: {query}")
                    raise

    async def add_account(self, phone, api_id, api_hash, session_file, two_fa=None):
        return await self._execute(
            "INSERT OR REPLACE INTO accounts (phone, api_id, api_hash, session_file, two_fa) VALUES (?, ?, ?, ?, ?)",
            (phone, api_id, api_hash, session_file, two_fa),
            commit=True
        )

    async def get_accounts(self):
        return await self._execute("SELECT * FROM accounts", fetchall=True)

    async def get_account(self, account_id):
        return await self._execute("SELECT * FROM accounts WHERE id = ?", (account_id,), fetchone=True)

    async def delete_account(self, account_id):
        await self._execute("DELETE FROM accounts WHERE id = ?", (account_id,), commit=True)

    async def add_preset(self, name, message_text, delay=5, duration=60):
        return await self._execute(
            "INSERT INTO presets (name, message_text, delay, duration) VALUES (?, ?, ?, ?)",
            (name, message_text, delay, duration),
            commit=True
        )

    async def get_presets(self):
        return await self._execute("SELECT * FROM presets", fetchall=True)

    async def get_preset(self, preset_id):
        return await self._execute("SELECT * FROM presets WHERE id = ?", (preset_id,), fetchone=True)

    async def update_preset_text(self, preset_id, new_text):
        await self._execute(
            "UPDATE presets SET message_text = ? WHERE id = ?",
            (new_text, preset_id),
            commit=True
        )

    async def update_preset_delay(self, preset_id, new_delay):
        await self._execute(
            "UPDATE presets SET delay = ? WHERE id = ?",
            (new_delay, preset_id),
            commit=True
        )

    async def update_preset_duration(self, preset_id, new_duration):
        await self._execute(
            "UPDATE presets SET duration = ? WHERE id = ?",
            (new_duration, preset_id),
            commit=True
        )

    async def delete_preset(self, preset_id):
        await self._execute("DELETE FROM presets WHERE id = ?", (preset_id,), commit=True)

    async def add_chat(self, preset_id, chat_id, chat_title):
        await self._execute(
            "INSERT INTO chats (preset_id, chat_id, chat_title) VALUES (?, ?, ?)",
            (preset_id, chat_id, chat_title),
            commit=True
        )

    async def get_chats(self, preset_id):
        return await self._execute("SELECT * FROM chats WHERE preset_id = ?", (preset_id,), fetchall=True)

    async def delete_chat(self, chat_id):
        await self._execute("DELETE FROM chats WHERE id = ?", (chat_id,), commit=True)

    async def clear_chats(self, preset_id):
        await self._execute("DELETE FROM chats WHERE preset_id = ?", (preset_id,), commit=True)

    async def add_broadcast_history(self, preset_id, account_id, total_chats):
        return await self._execute(
            "INSERT INTO broadcast_history (preset_id, account_id, start_time, total_chats, status) VALUES (?, ?, ?, ?, 'running')",
            (preset_id, account_id, datetime.now(), total_chats),
            commit=True
        )

    async def update_broadcast_history(self, history_id, sent, errors, status='completed'):
        await self._execute(
            "UPDATE broadcast_history SET messages_sent = ?, errors = ?, end_time = ?, status = ? WHERE id = ?",
            (sent, errors, datetime.now(), status, history_id),
            commit=True
        )


db = Database()


# ==================== TELEGRAM CLIENT MANAGER ====================
class ClientManager:
    def __init__(self):
        self.clients = {}
        self._locks = {}

    async def get_client(self, phone, api_id, api_hash, session_file, two_fa=None):
        if phone in self.clients:
            return self.clients[phone]

        if phone not in self._locks:
            self._locks[phone] = asyncio.Lock()

        async with self._locks[phone]:
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
        account = await db.get_account(account_id)
        if not account:
            return None

        try:
            client = await self.get_client(account[1], account[2], account[3], account[4], account[5])
            if not client:
                return None

            dialogs = await client.get_dialogs()
            chats = []
            for dialog in dialogs:
                if dialog.is_group or dialog.is_channel:
                    chats.append({
                        'id': str(dialog.entity.id),
                        'title': dialog.name
                    })
            return chats
        except Exception as e:
            logger.error(f"Error parsing chats: {e}")
            return None

    async def parse_folders(self, account_id):
        account = await db.get_account(account_id)
        if not account:
            return None

        try:
            client = await self.get_client(account[1], account[2], account[3], account[4], account[5])
            if not client:
                return None

            result = await client(functions.messages.GetDialogFiltersRequest())
            folders = []

            filters = []
            if hasattr(result, 'filters'):
                filters = result.filters
            elif isinstance(result, list):
                filters = result

            for folder in filters:
                if hasattr(folder, 'title') and folder.title:
                    folder_info = {
                        'title': folder.title,
                        'chats': []
                    }

                    peers = getattr(folder, 'include_peers', [])
                    for peer in peers:
                        try:
                            entity = await client.get_entity(peer)
                            folder_info['chats'].append({
                                'id': str(entity.id),
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
        account = await db.get_account(account_id)
        if not account:
            return None

        try:
            client = await self.get_client(account[1], account[2], account[3], account[4], account[5])
            if not client:
                return None

            entity = await client.get_entity(username)
            return {
                'id': str(entity.id),
                'title': getattr(entity, 'title', getattr(entity, 'first_name', 'Unknown'))
            }
        except Exception as e:
            logger.error(f"Error resolving username {username}: {e}")
            return None

    async def send_message(self, client, chat_id, text):
        try:
            entity = await client.get_entity(chat_id)
            await client.send_message(entity, text)
            return True, None
        except Exception as e:
            return False, str(e)

    async def run_broadcast(self, preset_id, account_id):
        preset = await db.get_preset(preset_id)
        account = await db.get_account(account_id)
        chats = await db.get_chats(preset_id)

        if not preset or not account or not chats:
            return

        client = await self.get_client(account[1], account[2], account[3], account[4], account[5])
        if not client:
            return

        history_id = await db.add_broadcast_history(preset_id, account_id, len(chats))

        broadcast_control.active_broadcasts[preset_id] = {
            'status': 'running',
            'sent': 0,
            'errors': 0,
            'total': len(chats),
            'start_time': time.time(),
            'history_id': history_id
        }

        delay = preset[3]
        duration = preset[4] * 60
        start_time = time.time()
        sent = 0
        errors = 0

        for chat in chats:
            if preset_id not in broadcast_control.active_broadcasts or \
                    broadcast_control.active_broadcasts[preset_id]['status'] == 'stopped':
                break

            if time.time() - start_time > duration:
                break

            success, error = await self.send_message(client, chat[2], preset[2])
            if success:
                sent += 1
                broadcast_control.active_broadcasts[preset_id]['sent'] = sent
            else:
                errors += 1
                broadcast_control.active_broadcasts[preset_id]['errors'] = errors

            await asyncio.sleep(delay)

        if preset_id in broadcast_control.active_broadcasts:
            broadcast_control.active_broadcasts[preset_id]['status'] = 'completed'
            await db.update_broadcast_history(history_id, sent, errors, 'completed')
            del broadcast_control.active_broadcasts[preset_id]


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


def preset_menu_keyboard(preset_id):
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="📝 Редактировать текст", callback_data=f"edit_text_{preset_id}"),
        InlineKeyboardButton(text="⏱ Изменить задержку", callback_data=f"edit_delay_{preset_id}")
    )
    kb.row(
        InlineKeyboardButton(text="⏳ Изменить длительность", callback_data=f"edit_duration_{preset_id}"),
        InlineKeyboardButton(text="📋 Список чатов", callback_data=f"view_chats_{preset_id}")
    )
    kb.row(
        InlineKeyboardButton(text="➕ Добавить чат", callback_data=f"add_chat_menu_{preset_id}"),
        InlineKeyboardButton(text="📂 Из папки", callback_data=f"from_folder_{preset_id}")
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
async def add_account_step1(message: Message, state: FSMContext):
    await message.answer(
        "Введите API ID (число):",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(AccountAuth.api_id)


@dp.message(AccountAuth.api_id)
async def add_account_step2(message: Message, state: FSMContext):
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
async def add_account_step3(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    await state.update_data(api_hash=message.text)
    await message.answer("Введите номер телефона (например +79123456789):")
    await state.set_state(AccountAuth.phone)


@dp.message(AccountAuth.phone)
async def add_account_step4(message: Message, state: FSMContext):
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
async def add_account_step5(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    data = await state.get_data()
    client = data['client']

    try:
        await client.sign_in(phone=data['phone'], code=message.text.strip())
        await db.add_account(data['phone'], data['api_id'], data['api_hash'], data['session_file'])
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
async def add_account_step6(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    data = await state.get_data()
    client = data['client']

    try:
        await client.sign_in(password=message.text)
        await db.add_account(data['phone'], data['api_id'], data['api_hash'], data['session_file'], message.text)
        await client.disconnect()
        await state.clear()
        await message.answer("✅ Аккаунт добавлен!", reply_markup=main_keyboard())
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        await state.clear()


# ==================== УПРАВЛЕНИЕ АККАУНТАМИ ====================
@dp.message(F.text == "👤 Аккаунты")
async def list_accounts(message: Message):
    accounts = await db.get_accounts()
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
    try:
        acc_id = int(query.data.split("_")[1])
        acc = await db.get_account(acc_id)

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
    except Exception as e:
        await query.answer(f"Ошибка: {str(e)[:30]}")
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("delacc_") and len(c.data.split("_")) == 2)
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


@dp.callback_query(lambda c: c.data.startswith("delacc_yes_") and len(c.data.split("_")) == 3)
async def delete_account(query: CallbackQuery):
    acc_id = int(query.data.split("_")[2])
    await db.delete_account(acc_id)
    await query.answer("✅ Удалено")
    await list_accounts(query.message)


@dp.callback_query(lambda c: c.data == "back_accounts")
async def back_to_accounts(query: CallbackQuery):
    await list_accounts(query.message)
    await query.answer()


# ==================== ПАРСИНГ ЧАТОВ И ПАПОК ====================
@dp.callback_query(lambda c: c.data.startswith("chats_") and len(c.data.split("_")) == 2)
async def parse_account_chats(query: CallbackQuery):
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


@dp.callback_query(lambda c: c.data.startswith("folders_") and len(c.data.split("_")) == 2)
async def parse_account_folders(query: CallbackQuery):
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
    presets = await db.get_presets()
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
        chats = await db.get_chats(p[0])
        text += f"📁 **{p[1]}**\n"
        text += f"   Чатов: {len(chats)} | Задержка: {p[3]}с | Длит: {p[4]}мин\n\n"
        kb.row(InlineKeyboardButton(
            text=f"📁 {p[1]}",
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
    await message.answer("📝 **Введите текст сообщения для рассылки:**")
    await state.set_state(PresetCreation.text)


@dp.message(PresetCreation.text)
async def preset_text(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    await state.update_data(text=message.text)
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
        preset_id = await db.add_preset(data['name'], data['text'], data['delay'], duration)
        await state.clear()

        await message.answer(
            f"✅ **Пресет создан!**\n\n"
            f"Название: {data['name']}\n"
            f"Задержка: {data['delay']} сек\n"
            f"Длительность: {duration} мин",
            reply_markup=main_keyboard()
        )
    except ValueError:
        await message.answer("❌ Введите число")


@dp.callback_query(lambda c: c.data.startswith("preset_menu_") and len(c.data.split("_")) == 3)
async def preset_menu(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])
    preset = await db.get_preset(preset_id)
    chats = await db.get_chats(preset_id)

    is_active = preset_id in broadcast_control.active_broadcasts
    status_text = "🟢 **Активна**" if is_active else "⚪ **Неактивна**"

    text = f"📁 **Пресет: {preset[1]}**\n\n"
    text += f"{status_text}\n"
    text += f"📝 Текст: {preset[2][:100]}...\n"
    text += f"⏱ Задержка: {preset[3]} сек\n"
    text += f"⏳ Длительность: {preset[4]} мин\n"
    text += f"📊 Чатов в списке: {len(chats)}\n"

    if is_active:
        bcast = broadcast_control.active_broadcasts[preset_id]
        text += f"\n**Прогресс:** {bcast['sent']}/{bcast['total']} "
        text += f"({bcast['sent'] / bcast['total'] * 100:.1f}%)\n"
        text += f"❌ Ошибок: {bcast['errors']}"

    await query.message.edit_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=preset_menu_keyboard(preset_id)
    )
    await query.answer()


# ==================== РЕДАКТИРОВАНИЕ ПРЕСЕТА ====================
@dp.callback_query(lambda c: c.data.startswith("edit_text_") and len(c.data.split("_")) == 3)
async def edit_preset_text(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[2])
    await state.update_data(edit_preset_id=preset_id)
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
    preset_id = data['edit_preset_id']
    await db.update_preset_text(preset_id, message.text)
    await state.clear()
    await message.answer("✅ **Текст обновлен!**", reply_markup=main_keyboard())

    # Создаем новый callback query для возврата в меню
    await show_preset_menu(message, preset_id)


async def show_preset_menu(message: Message, preset_id: int):
    preset = await db.get_preset(preset_id)
    chats = await db.get_chats(preset_id)

    is_active = preset_id in broadcast_control.active_broadcasts
    status_text = "🟢 **Активна**" if is_active else "⚪ **Неактивна**"

    text = f"📁 **Пресет: {preset[1]}**\n\n"
    text += f"{status_text}\n"
    text += f"📝 Текст: {preset[2][:100]}...\n"
    text += f"⏱ Задержка: {preset[3]} сек\n"
    text += f"⏳ Длительность: {preset[4]} мин\n"
    text += f"📊 Чатов в списке: {len(chats)}\n"

    if is_active:
        bcast = broadcast_control.active_broadcasts[preset_id]
        text += f"\n**Прогресс:** {bcast['sent']}/{bcast['total']} "
        text += f"({bcast['sent'] / bcast['total'] * 100:.1f}%)\n"
        text += f"❌ Ошибок: {bcast['errors']}"

    await message.answer(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=preset_menu_keyboard(preset_id)
    )


@dp.callback_query(lambda c: c.data.startswith("edit_delay_") and len(c.data.split("_")) == 3)
async def edit_preset_delay(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[2])
    await state.update_data(edit_preset_id=preset_id)
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
        preset_id = data['edit_preset_id']
        await db.update_preset_delay(preset_id, delay)
        await state.clear()
        await message.answer(f"✅ **Задержка обновлена!** Теперь {delay} сек", reply_markup=main_keyboard())
        await show_preset_menu(message, preset_id)
    except ValueError:
        await message.answer("❌ Введите число")


@dp.callback_query(lambda c: c.data.startswith("edit_duration_") and len(c.data.split("_")) == 3)
async def edit_preset_duration(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[2])
    await state.update_data(edit_preset_id=preset_id)
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
        preset_id = data['edit_preset_id']
        await db.update_preset_duration(preset_id, duration)
        await state.clear()
        await message.answer(f"✅ **Длительность обновлена!** Теперь {duration} мин", reply_markup=main_keyboard())
        await show_preset_menu(message, preset_id)
    except ValueError:
        await message.answer("❌ Введите число")


# ==================== УПРАВЛЕНИЕ ЧАТАМИ ====================
@dp.callback_query(lambda c: c.data.startswith("view_chats_") and len(c.data.split("_")) == 3)
async def view_chats(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])
    chats = await db.get_chats(preset_id)

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
        InlineKeyboardButton(text="➕ Добавить", callback_data=f"add_chat_menu_{preset_id}"),
        InlineKeyboardButton(text="🗑 Очистить все", callback_data=f"clear_chats_{preset_id}")
    )
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"preset_menu_{preset_id}"))

    await query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb.as_markup())
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("delchat_") and len(c.data.split("_")) == 3)
async def delete_chat(query: CallbackQuery):
    parts = query.data.split("_")
    chat_id = int(parts[1])
    preset_id = int(parts[2])

    await db.delete_chat(chat_id)
    await query.answer("✅ Чат удален")

    # Создаем новый callback data для обновления списка
    await view_chats(query)


@dp.callback_query(lambda c: c.data.startswith("clear_chats_") and len(c.data.split("_")) == 3)
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


@dp.callback_query(lambda c: c.data.startswith("confirm_clear_") and len(c.data.split("_")) == 3)
async def clear_chats(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])
    await db.clear_chats(preset_id)
    await query.answer("✅ Все чаты удалены")
    await view_chats(query)


@dp.callback_query(lambda c: c.data.startswith("add_chat_menu_") and len(c.data.split("_")) == 3)
async def add_chat_menu(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[2])

    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="🔤 По username", callback_data=f"add_by_username_{preset_id}"),
        InlineKeyboardButton(text="📋 Из аккаунта", callback_data=f"add_from_account_{preset_id}")
    )
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"view_chats_{preset_id}"))

    await query.message.edit_text(
        "📥 **Выберите способ добавления чата:**",
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("add_by_username_") and len(c.data.split("_")) == 4)
async def add_by_username(query: CallbackQuery, state: FSMContext):
    preset_id = int(query.data.split("_")[3])
    await state.update_data(add_chat_preset_id=preset_id)

    accounts = await db.get_accounts()
    if not accounts:
        await query.answer("❌ Нет аккаунтов")
        return

    kb = InlineKeyboardBuilder()
    for acc in accounts:
        kb.row(InlineKeyboardButton(
            text=f"📱 {acc[1]}",
            callback_data=f"select_acc_for_username_{acc[0]}"
        ))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"add_chat_menu_{preset_id}"))

    await query.message.edit_text(
        "📱 **Сначала выберите аккаунт для поиска:**",
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("select_acc_for_username_") and len(c.data.split("_")) == 5)
async def select_account_for_username(query: CallbackQuery, state: FSMContext):
    account_id = int(query.data.split("_")[4])
    data = await state.get_data()
    preset_id = data['add_chat_preset_id']

    await state.update_data(username_account_id=account_id)
    await query.message.edit_text(
        "🔤 **Введите username чата** (например @channel_name):"
    )
    await state.set_state(PresetEdit.waiting_for_chat_input)
    await query.answer()


@dp.message(PresetEdit.waiting_for_chat_input)
async def process_chat_username(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено", reply_markup=main_keyboard())
        return

    data = await state.get_data()
    preset_id = data['add_chat_preset_id']
    account_id = data['username_account_id']

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

    await db.add_chat(preset_id, chat['id'], chat['title'])
    await state.clear()

    await message.answer(
        f"✅ **Чат добавлен!**\n\n"
        f"Название: {chat['title']}",
        reply_markup=main_keyboard()
    )


@dp.callback_query(lambda c: c.data.startswith("add_from_account_") and len(c.data.split("_")) == 4)
async def add_from_account(query: CallbackQuery):
    preset_id = int(query.data.split("_")[3])
    accounts = await db.get_accounts()

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


@dp.callback_query(lambda c: c.data.startswith("import_chats_") and len(c.data.split("_")) == 4)
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
    for chat in chats:
        try:
            await db.add_chat(preset_id, chat['id'], chat['title'])
            added += 1
        except:
            pass

    await query.message.edit_text(
        f"✅ **Импортировано:** {added} из {len(chats)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 К списку чатов", callback_data=f"view_chats_{preset_id}")]
        ])
    )


# ==================== ИМПОРТ ПАПОК ====================
@dp.callback_query(lambda c: c.data.startswith("from_folder_") and len(c.data.split("_")) == 3)
async def from_folder(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])
    accounts = await db.get_accounts()

    if not accounts:
        await query.answer("❌ Нет аккаунтов")
        return

    kb = InlineKeyboardBuilder()
    for acc in accounts:
        kb.row(InlineKeyboardButton(
            text=f"📱 {acc[1]}",
            callback_data=f"select_folder_acc_{preset_id}_{acc[0]}"
        ))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"preset_menu_{preset_id}"))

    await query.message.edit_text(
        "📱 **Выберите аккаунт для просмотра папок:**",
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("select_folder_acc_") and len(c.data.split("_")) == 5)
async def select_folder_account(query: CallbackQuery):
    parts = query.data.split("_")
    preset_id = int(parts[3])
    account_id = int(parts[4])

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


@dp.callback_query(lambda c: c.data.startswith("import_folder_") and len(c.data.split("_")) == 5)
async def import_folder(query: CallbackQuery):
    parts = query.data.split("_")
    preset_id = int(parts[2])
    account_id = int(parts[3])
    folder_index = int(parts[4])

    folders = await client_manager.parse_folders(account_id)

    if not folders or folder_index >= len(folders):
        await query.answer("❌ Папка не найдена")
        return

    folder = folders[folder_index]
    added = 0

    for chat in folder['chats']:
        try:
            await db.add_chat(preset_id, chat['id'], chat['title'])
            added += 1
        except:
            pass

    await query.answer(f"✅ Добавлено {added} чатов из папки")

    # Возвращаемся к списку чатов
    await view_chats(query)


# ==================== ЗАПУСК РАССЫЛКИ ====================
@dp.callback_query(lambda c: c.data.startswith("run_preset_") and len(c.data.split("_")) == 3)
async def run_preset(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])

    if preset_id in broadcast_control.active_broadcasts:
        await query.answer("❌ Рассылка уже запущена!")
        return

    chats = await db.get_chats(preset_id)
    if not chats:
        await query.answer("❌ Нет чатов в пресете!")
        return

    accounts = await db.get_accounts()
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


@dp.callback_query(lambda c: c.data.startswith("start_broadcast_") and len(c.data.split("_")) == 4)
async def start_broadcast(query: CallbackQuery):
    parts = query.data.split("_")
    preset_id = int(parts[2])
    account_id = int(parts[3])

    await query.message.edit_text("🚀 **Запуск рассылки...**")
    await query.answer()

    asyncio.create_task(client_manager.run_broadcast(preset_id, account_id))

    await query.message.edit_text(
        "✅ **Рассылка запущена!**\n\n"
        "Следите за статусом в разделе '🚀 Активные рассылки'",
        reply_markup=main_keyboard()
    )


# ==================== АКТИВНЫЕ РАССЫЛКИ ====================
@dp.message(F.text == "🚀 Активные рассылки")
async def active_broadcasts(message: Message):
    if not broadcast_control.active_broadcasts:
        await message.answer("📭 **Нет активных рассылок**")
        return

    text = "🚀 **Активные рассылки:**\n\n"
    kb = InlineKeyboardBuilder()

    for preset_id, data in broadcast_control.active_broadcasts.items():
        preset = await db.get_preset(preset_id)
        if preset:
            progress = data['sent'] / data['total'] * 100 if data['total'] > 0 else 0
            text += f"📁 **{preset[1]}**\n"
            text += f"Прогресс: {data['sent']}/{data['total']} ({progress:.1f}%)\n"
            text += f"❌ Ошибок: {data['errors']}\n\n"
            kb.row(InlineKeyboardButton(
                text=f"⏹ Остановить {preset[1]}",
                callback_data=f"stop_broadcast_{preset_id}"
            ))

    await message.answer(text, reply_markup=kb.as_markup())


@dp.callback_query(lambda c: c.data.startswith("stop_broadcast_") and len(c.data.split("_")) == 3)
async def stop_broadcast(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])

    if preset_id in broadcast_control.active_broadcasts:
        broadcast_control.active_broadcasts[preset_id]['status'] = 'stopped'
        await query.answer("⏹ Рассылка остановлена")
    else:
        await query.answer("❌ Рассылка не найдена")

    await active_broadcasts(query.message)


# ==================== УДАЛЕНИЕ ПРЕСЕТА ====================
@dp.callback_query(lambda c: c.data.startswith("delete_preset_") and len(c.data.split("_")) == 3)
async def delete_preset_confirm(query: CallbackQuery):
    preset_id = int(query.data.split("_")[2])

    if preset_id in broadcast_control.active_broadcasts:
        await query.answer("❌ Сначала остановите рассылку!")
        return

    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_delete_preset_{preset_id}"),
        InlineKeyboardButton(text="❌ Нет", callback_data=f"preset_menu_{preset_id}")
    )

    await query.message.edit_text(
        "❓ **Удалить пресет?**\n\nВсе чаты будут удалены.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb.as_markup()
    )
    await query.answer()


@dp.callback_query(lambda c: c.data.startswith("confirm_delete_preset_") and len(c.data.split("_")) == 4)
async def delete_preset(query: CallbackQuery):
    preset_id = int(query.data.split("_")[3])
    await db.delete_preset(preset_id)
    await query.answer("✅ Пресет удален")
    await list_presets(query.message)


# ==================== СТАТУС И СТАТИСТИКА ====================
@dp.message(F.text == "📊 Статус")
async def status(message: Message):
    if broadcast_control.active_broadcasts:
        text = "📊 **Активные рассылки:**\n\n"
        for preset_id, data in broadcast_control.active_broadcasts.items():
            preset = await db.get_preset(preset_id)
            if preset:
                progress = data['sent'] / data['total'] * 100 if data['total'] > 0 else 0
                text += f"📁 {preset[1]}: {data['sent']}/{data['total']} ({progress:.1f}%)\n"
    else:
        text = "📊 **Нет активных рассылок**"

    await message.answer(text)


@dp.message(F.text == "📈 Статистика")
async def statistics(message: Message):
    accounts = await db.get_accounts()
    presets = await db.get_presets()

    total_chats = 0
    for p in presets:
        chats = await db.get_chats(p[0])
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
    print("✅ Бот работает!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("❌ Бот остановлен")
    except Exception as e:
        print(f"❌ Ошибка: {e}")