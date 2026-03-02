import os
import asyncio
import sqlite3
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# -------------------------
# ENVIRONMENT VARIABLES
# -------------------------
TOKEN = os.getenv("TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
GROUP_ID = int(os.getenv("GROUP_ID"))

if not TOKEN or not ADMIN_ID or not GROUP_ID:
    raise ValueError("Missing environment variables.")

bot = Bot(token=TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# -------------------------
# DATABASE SETUP
# -------------------------
conn = sqlite3.connect("rice.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    name TEXT,
    username TEXT,
    points INTEGER DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS scored_posts (
    user_id INTEGER,
    message_id INTEGER,
    PRIMARY KEY (user_id, message_id)
)
""")

conn.commit()

# -------------------------
# HELPERS
# -------------------------
def is_admin(user_id):
    return user_id == ADMIN_ID

def add_user_if_not_exists(user):
    cursor.execute("SELECT user_id FROM users WHERE user_id=?", (user.id,))
    if cursor.fetchone() is None:
        cursor.execute(
            "INSERT INTO users (user_id, name, username, points) VALUES (?, ?, ?, 0)",
            (user.id, user.full_name, user.username)
        )
        conn.commit()

def add_point(user, message_id):
    cursor.execute(
        "SELECT 1 FROM scored_posts WHERE user_id=? AND message_id=?",
        (user.id, message_id)
    )

    if cursor.fetchone() is None:
        cursor.execute(
            "INSERT INTO scored_posts (user_id, message_id) VALUES (?, ?)",
            (user.id, message_id)
        )
        cursor.execute(
            "UPDATE users SET points = points + 1 WHERE user_id=?",
            (user.id,)
        )
        conn.commit()

# -------------------------
# WEEKLY LEADERBOARD JOB
# -------------------------
async def weekly_leaderboard():
    cursor.execute(
        "SELECT name, username, points FROM users ORDER BY points DESC LIMIT 10"
    )
    rows = cursor.fetchall()

    if not rows:
        return

    text = "Weekly RICE Leaderboard\n\n"

    for i, row in enumerate(rows, start=1):
        name, username, points = row
        if username:
            text += f"{i}. {name} (@{username}) - {points}\n"
        else:
            text += f"{i}. {name} - {points}\n"

    await bot.send_message(GROUP_ID, text)

    # Reset scores
    cursor.execute("UPDATE users SET points = 0")
    cursor.execute("DELETE FROM scored_posts")
    conn.commit()

# -------------------------
# DECISION POST (ABCD)
# -------------------------
@dp.message(Command("decision"))
async def decision_post(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return

    question = parts[1]

    builder = InlineKeyboardBuilder()
    builder.button(text="A", callback_data="decision_A")
    builder.button(text="B", callback_data="decision_B")
    builder.button(text="C", callback_data="decision_C")
    builder.button(text="D", callback_data="decision_D")
    builder.adjust(4)

    await message.answer(question, reply_markup=builder.as_markup())

    try:
        await message.delete()
    except:
        pass

# -------------------------
# OPINION POST (XYZ)
# -------------------------
@dp.message(Command("opinion"))
async def opinion_post(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return

    question = parts[1]

    builder = InlineKeyboardBuilder()
    builder.button(text="X", callback_data="opinion_X")
    builder.button(text="Y", callback_data="opinion_Y")
    builder.button(text="Z", callback_data="opinion_Z")
    builder.adjust(3)

    await message.answer(question, reply_markup=builder.as_markup())

    try:
        await message.delete()
    except:
        pass

# -------------------------
# HANDLE CTA CLICKS
# -------------------------
@dp.callback_query(F.data.startswith(("decision_", "opinion_")))
async def handle_click(callback: CallbackQuery):
    user = callback.from_user
    message_id = callback.message.message_id

    add_user_if_not_exists(user)
    add_point(user, message_id)

    await callback.answer("Recorded")

# -------------------------
# ADMIN LEADERBOARD COMMAND
# -------------------------
@dp.message(Command("leaderboard"))
async def leaderboard(message: Message):
    if not is_admin(message.from_user.id):
        return

    cursor.execute(
        "SELECT name, username, points FROM users ORDER BY points DESC LIMIT 10"
    )
    rows = cursor.fetchall()

    if not rows:
        await message.reply("No activity yet.")
        return

    text = "Weekly RICE Leaderboard\n\n"

    for i, row in enumerate(rows, start=1):
        name, username, points = row
        if username:
            text += f"{i}. {name} (@{username}) - {points}\n"
        else:
            text += f"{i}. {name} - {points}\n"

    await message.reply(text)

# -------------------------
# START BOT
# -------------------------
async def main():
    scheduler.add_job(
        weekly_leaderboard,
        trigger="cron",
        day_of_week="sun",
        hour=21,
        minute=0
    )
    scheduler.start()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
