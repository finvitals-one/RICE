import os
import asyncio
import sqlite3
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# -------------------------
# ENV
# -------------------------
TOKEN = os.getenv("TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

if not TOKEN:
    raise ValueError("TOKEN missing")

bot = Bot(token=TOKEN)
dp = Dispatcher()

# -------------------------
# DATABASE
# -------------------------
conn = sqlite3.connect("rice.db")
cursor = conn.cursor()

# Groups
cursor.execute("""
CREATE TABLE IF NOT EXISTS groups (
    group_id INTEGER PRIMARY KEY,
    member_count INTEGER
)
""")

# Users
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER,
    group_id INTEGER,
    name TEXT,
    username TEXT,
    monthly_points INTEGER DEFAULT 0,
    PRIMARY KEY (user_id, group_id)
)
""")

# Posts
cursor.execute("""
CREATE TABLE IF NOT EXISTS posts (
    post_id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER,
    telegram_message_id INTEGER,
    type TEXT,
    question TEXT,
    correct_option INTEGER,
    created_at TEXT
)
""")

# Responses
cursor.execute("""
CREATE TABLE IF NOT EXISTS responses (
    post_id INTEGER,
    user_id INTEGER,
    selected_option INTEGER,
    correct INTEGER,
    PRIMARY KEY (post_id, user_id)
)
""")

# Monthly archive
cursor.execute("""
CREATE TABLE IF NOT EXISTS monthly_archive (
    group_id INTEGER,
    user_id INTEGER,
    month TEXT,
    points INTEGER
)
""")

conn.commit()

# -------------------------
# HELPERS
# -------------------------
def is_admin(user_id):
    return user_id == ADMIN_ID

def ensure_group(group_id):
    cursor.execute("INSERT OR IGNORE INTO groups (group_id, member_count) VALUES (?, ?)", (group_id, 0))
    conn.commit()

def ensure_user(user, group_id):
    cursor.execute("""
        INSERT OR IGNORE INTO users (user_id, group_id, name, username, monthly_points)
        VALUES (?, ?, ?, ?, 0)
    """, (user.id, group_id, user.full_name, user.username))
    conn.commit()

def add_points(user_id, group_id, points):
    cursor.execute("""
        UPDATE users
        SET monthly_points = monthly_points + ?
        WHERE user_id = ? AND group_id = ?
    """, (points, user_id, group_id))
    conn.commit()

# -------------------------
# SET MEMBER COUNT
# -------------------------
@dp.message(Command("setmembers"))
async def set_members(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        count = int(message.text.split()[1])
    except:
        await message.reply("Usage: /setmembers 180")
        return

    group_id = message.chat.id
    ensure_group(group_id)

    cursor.execute("UPDATE groups SET member_count=? WHERE group_id=?", (count, group_id))
    conn.commit()

    await message.reply(f"Member count set to {count}")

# -------------------------
# QUIZ / POLL / CTA
# -------------------------
async def create_structured_post(message: Message, post_type: str):
    if not is_admin(message.from_user.id):
        return

    group_id = message.chat.id
    ensure_group(group_id)

    parts = message.text.split("|")
    if len(parts) < 3:
        await message.reply("Invalid format.")
        return

    header = parts[0].split(maxsplit=1)
    if len(header) < 2:
        await message.reply("Question missing.")
        return

    question = header[1].strip()
    options = [p.strip() for p in parts[1:-1] if p.strip()] if post_type == "quiz" else [p.strip() for p in parts[1:] if p.strip()]

    correct_option = None
    if post_type == "quiz":
        try:
            correct_option = int(parts[-1])
            options = [p.strip() for p in parts[1:-1]]
        except:
            await message.reply("Quiz must end with correct option number.")
            return

    if not 2 <= len(options) <= 4:
        await message.reply("Options must be 2-4.")
        return

    builder = InlineKeyboardBuilder()
    for idx, opt in enumerate(options, start=1):
        builder.button(text=opt, callback_data=f"{post_type}:{idx}")
    builder.adjust(1)

    sent = await message.answer(question, reply_markup=builder.as_markup())

    cursor.execute("""
        INSERT INTO posts (group_id, telegram_message_id, type, question, correct_option, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (group_id, sent.message_id, post_type, question, correct_option, datetime.now().isoformat()))
    conn.commit()

    try:
        await message.delete()
    except:
        pass

@dp.message(Command("quiz"))
async def quiz(message: Message):
    await create_structured_post(message, "quiz")

@dp.message(Command("poll"))
async def poll(message: Message):
    await create_structured_post(message, "poll")

@dp.message(Command("cta"))
async def cta(message: Message):
    await create_structured_post(message, "cta")

# -------------------------
# LINK (NO POINTS)
# -------------------------
@dp.message(Command("link"))
async def link(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.split("|")
    if len(parts) < 2:
        return

    header = parts[0].split(maxsplit=1)
    if len(header) < 2:
        return

    text = header[1].strip()
    url = parts[1].strip()

    builder = InlineKeyboardBuilder()
    builder.button(text="Open Link", url=url)

    await message.answer(text, reply_markup=builder.as_markup())

    try:
        await message.delete()
    except:
        pass

# -------------------------
# HANDLE RESPONSES
# -------------------------
@dp.callback_query(F.data.contains(":"))
async def handle_response(callback: CallbackQuery):
    group_id = callback.message.chat.id
    user = callback.from_user

    post_type, option_index = callback.data.split(":")
    option_index = int(option_index)

    cursor.execute("""
        SELECT post_id, correct_option
        FROM posts
        WHERE telegram_message_id=? AND group_id=?
    """, (callback.message.message_id, group_id))
    post = cursor.fetchone()

    if not post:
        return

    post_id, correct_option = post

    cursor.execute("""
        SELECT 1 FROM responses WHERE post_id=? AND user_id=?
    """, (post_id, user.id))
    if cursor.fetchone():
        await callback.answer("Already responded.")
        return

    ensure_user(user, group_id)

    correct = 0
    points = 1

    if post_type == "quiz":
        if option_index == correct_option:
            correct = 1
            points += 2

    add_points(user.id, group_id, points)

    cursor.execute("""
        INSERT INTO responses (post_id, user_id, selected_option, correct)
        VALUES (?, ?, ?, ?)
    """, (post_id, user.id, option_index, correct))
    conn.commit()

    await callback.answer("Recorded")

# -------------------------
# ADMIN SCOREBOARD
# -------------------------
@dp.message(Command("scoreboard"))
async def scoreboard(message: Message):
    if not is_admin(message.from_user.id):
        return

    group_id = message.chat.id

    cursor.execute("""
        SELECT name, username, monthly_points
        FROM users
        WHERE group_id=?
        ORDER BY monthly_points DESC
    """, (group_id,))
    rows = cursor.fetchall()

    if not rows:
        await message.reply("No data.")
        return

    text = "Current Month Scoreboard\n\n"
    for i, row in enumerate(rows, 1):
        name, username, points = row
        text += f"{i}. {name} - {points}\n"

    await message.reply(text)

# -------------------------
# START
# -------------------------
async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
