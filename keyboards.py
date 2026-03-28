from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import database


def main_menu():

    return InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="add_account")],

        [InlineKeyboardButton(text="👤 Аккаунты", callback_data="accounts")]

    ])


def accounts_menu():

    accounts = database.get_accounts()

    buttons = []

    for acc in accounts:

        buttons.append(
            [InlineKeyboardButton(text=acc[1], callback_data=f"acc_{acc[0]}")]
        )

    buttons.append(
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back")]
    )

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def account_panel(acc_id):

    return InlineKeyboardMarkup(inline_keyboard=[

        [InlineKeyboardButton(text="📂 Диалоги", callback_data=f"dialogs_{acc_id}")],

        [InlineKeyboardButton(text="✏ Отправить сообщение", callback_data=f"send_{acc_id}")],

        [InlineKeyboardButton(text="⬅ Назад", callback_data="accounts")]

    ])