import asyncio
import time
from telethon import TelegramClient

running_broadcasts = {}

async def broadcast(phone, account, preset):

    client = TelegramClient(
        f"sessions/{phone}",
        account["api_id"],
        account["api_hash"]
    )

    await client.start()

    start = time.time()
    sent = 0
    errors = 0

    running_broadcasts[phone] = True

    while running_broadcasts.get(phone):

        if time.time() - start > preset["duration"]:
            break

        for chat in preset["chats"]:

            if not running_broadcasts.get(phone):
                break

            try:
                await client.send_message(chat, preset["message"])
                sent += 1
            except Exception:
                errors += 1

            await asyncio.sleep(preset["frequency"])

    await client.disconnect()

    return sent, errors


def stop_broadcast(phone):

    running_broadcasts[phone] = False