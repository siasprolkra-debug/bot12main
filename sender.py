import asyncio
from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChatWriteForbiddenError
import database
import config

# Создаём Telethon клиент для личного аккаунта
client = TelegramClient("myaccount", config.API_ID, config.API_HASH)


async def start():
    await client.start()
    me = await client.get_me()
    print(f"✅ Клиент авторизован: {me.first_name} ({getattr(me, 'username', '')})")


async def add_chat_by_link(link: str):
    """
    Добавляет чат в базу и возвращает entity
    """
    link = link.replace("https://t.me/", "").replace("@", "")
    try:
        entity = await client.get_entity(link)
        db.add_chat(entity.id, entity.title)
        return entity
    except Exception as e:
        return f"❌ Ошибка добавления чата: {e}"


async def send_message_all(text):
    chats = db.get_chats()
    for chat_id, title in chats:
        try:
            entity = await client.get_entity(chat_id)
            # Не проверяем участников для публичных групп
            await client.send_message(entity, text)
            print(f"✅ Сообщение отправлено в: {title}")
            await asyncio.sleep(config.DELAY)
        except FloodWaitError as e:
            print(f"⏱ FloodWait: {e.seconds} сек. ждем...")
            await asyncio.sleep(e.seconds)
        except ChatWriteForbiddenError:
            print(f"❌ Нет прав на отправку сообщений в: {title}. Пропускаем.")
        except Exception as e:
            print(f"❌ Не удалось отправить в {title}: {e}")