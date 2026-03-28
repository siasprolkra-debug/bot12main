import os
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import config

# создаем папку sessions если ее нет
os.makedirs("sessions", exist_ok=True)


async def send_code(phone):
    client = TelegramClient(f"sessions/{phone}", config.API_ID, config.API_HASH)
    await client.connect()
    await client.send_code_request(phone)
    return client


async def sign_in(client, phone, code, password=None):
    """
    Выполняет вход в аккаунт.
    Если включена двухфакторная аутентификация, требуется передать password.
    """
    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        if password is None:
            raise ValueError("Аккаунт требует пароль 2FA, но он не был передан")
        await client.sign_in(password=password)

    return client