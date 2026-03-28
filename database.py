import sqlite3

conn = sqlite3.connect("database.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS accounts(
id INTEGER PRIMARY KEY AUTOINCREMENT,
phone TEXT,
session TEXT
)
""")

conn.commit()


def add_account(phone, session):

    cursor.execute(
        "INSERT INTO accounts(phone,session) VALUES(?,?)",
        (phone, session)
    )

    conn.commit()


def get_accounts():

    cursor.execute("SELECT id,phone,session FROM accounts")

    return cursor.fetchall()