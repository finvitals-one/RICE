import os
import asyncio
import sqlite3
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder


TOKEN = os.getenv("TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

bot = Bot(token=TOKEN)
dp = Dispatcher()

conn = sqlite3.connect("rice.db")
cursor = conn.cursor()


# ---------------- DATABASE ----------------

cursor.execute("""
CREATE TABLE IF NOT EXISTS users(
user_id INTEGER PRIMARY KEY,
name TEXT,
username TEXT,
points INTEGER DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS posts(
post_id INTEGER PRIMARY KEY AUTOINCREMENT,
post_code TEXT,
telegram_message_id INTEGER,
type TEXT,
question TEXT,
options TEXT,
correct_option INTEGER,
created_at TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS responses(
post_id INTEGER,
user_id INTEGER,
selected_option INTEGER,
correct INTEGER,
PRIMARY KEY(post_id,user_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS settings(
key TEXT PRIMARY KEY,
value TEXT
)
""")

conn.commit()


# ---------------- HELPERS ----------------

def generate_post_code(post_type):
    date_code = datetime.now().strftime("%d%m%y")
    return f"{post_type}_{date_code}"


def ensure_user(user):
    cursor.execute("""
    INSERT OR IGNORE INTO users(user_id,name,username,points)
    VALUES(?,?,?,0)
    """,(user.id,user.full_name,user.username))
    conn.commit()


def add_points(user_id,points):
    cursor.execute("""
    UPDATE users SET points = points + ?
    WHERE user_id=?
    """,(points,user_id))
    conn.commit()


def build_keyboard(post_id):

    cursor.execute("""
    SELECT options FROM posts WHERE post_id=?
    """,(post_id,))
    row = cursor.fetchone()

    options = row[0].split("|")

    builder = InlineKeyboardBuilder()

    for i,opt in enumerate(options,1):

        cursor.execute("""
        SELECT COUNT(*) FROM responses
        WHERE post_id=? AND selected_option=?
        """,(post_id,i))

        count = cursor.fetchone()[0]

        builder.button(
            text=f"{opt} ({count})",
            callback_data=f"{post_id}:{i}"
        )

    builder.adjust(1)

    return builder.as_markup()


# ---------------- COMMANDS ----------------


@dp.message(Command("setmembers"))
async def set_members(message: Message):

    if message.from_user.id != ADMIN_ID:
        return

    try:
        count = int(message.text.split()[1])
    except:
        return

    cursor.execute("""
    INSERT OR REPLACE INTO settings(key,value)
    VALUES("members",?)
    """,(count,))
    conn.commit()

    await message.delete()

    await bot.send_message(ADMIN_ID,f"Members set to {count}")


# ---------------- CREATE POSTS ----------------

async def create_post(message:Message,post_type):

    if message.from_user.id != ADMIN_ID:
        return

    parts = message.text.split("|")

    header = parts[0].split(maxsplit=1)

    if len(header) < 2:
        return

    question = header[1].strip()

    correct_option=None

    if post_type=="quiz":
        correct_option=int(parts[-1])
        options=[p.strip() for p in parts[1:-1]]
    else:
        options=[p.strip() for p in parts[1:]]

    options_string="|".join(options)

    post_code = generate_post_code(post_type)

    text=f"{post_code}\n\n{question}"

    builder = InlineKeyboardBuilder()

    for i,opt in enumerate(options,1):
        builder.button(text=f"{opt} (0)",callback_data=f"temp:{i}")

    builder.adjust(1)

    sent = await message.answer(text,reply_markup=builder.as_markup())

    cursor.execute("""
    INSERT INTO posts(post_code,telegram_message_id,type,question,options,correct_option,created_at)
    VALUES(?,?,?,?,?,?,?)
    """,(post_code,sent.message_id,post_type,question,options_string,correct_option,datetime.now()))

    conn.commit()

    post_id = cursor.lastrowid

    await bot.edit_message_reply_markup(
        chat_id=sent.chat.id,
        message_id=sent.message_id,
        reply_markup=build_keyboard(post_id)
    )

    await message.delete()


@dp.message(Command("quiz"))
async def quiz(message:Message):
    await create_post(message,"quiz")


@dp.message(Command("poll"))
async def poll(message:Message):
    await create_post(message,"poll")


@dp.message(Command("cta"))
async def cta(message:Message):
    await create_post(message,"cta")


# ---------------- LINK ----------------

@dp.message(Command("link"))
async def link(message:Message):

    if message.from_user.id != ADMIN_ID:
        return

    parts = message.text.split("|")

    text = parts[0].split(maxsplit=1)[1].strip()

    url = parts[1].strip()

    builder = InlineKeyboardBuilder()
    builder.button(text="Open Link",url=url)

    await message.answer(text,reply_markup=builder.as_markup())

    await message.delete()


# ---------------- BUTTON CLICK ----------------

@dp.callback_query(F.data.contains(":"))
async def handle_click(callback:CallbackQuery):

    data = callback.data.split(":")
    post_id = int(data[0])
    option_index = int(data[1])

    user = callback.from_user

    cursor.execute("""
    SELECT 1 FROM responses
    WHERE post_id=? AND user_id=?
    """,(post_id,user.id))

    if cursor.fetchone():
        await callback.answer("Already responded")
        return

    ensure_user(user)

    cursor.execute("""
    SELECT type,correct_option,options
    FROM posts
    WHERE post_id=?
    """,(post_id,))

    post_type,correct_option,options_string = cursor.fetchone()

    options = options_string.split("|")

    correct = 0
    points = 1

    popup="Recorded"

    if post_type=="quiz":

        if option_index==correct_option:

            correct=1
            points += 2
            popup="Correct answer"

        else:

            popup=f"Wrong. Correct: {options[correct_option-1]}"

    add_points(user.id,points)

    cursor.execute("""
    INSERT INTO responses(post_id,user_id,selected_option,correct)
    VALUES(?,?,?,?)
    """,(post_id,user.id,option_index,correct))

    conn.commit()

    await bot.edit_message_reply_markup(
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id,
        reply_markup=build_keyboard(post_id)
    )

    await callback.answer(popup,show_alert=True)


# ---------------- SCOREBOARD ----------------

@dp.message(Command("scoreboard"))
async def scoreboard(message:Message):

    if message.from_user.id != ADMIN_ID:
        return

    cursor.execute("""
    SELECT name,points
    FROM users
    ORDER BY points DESC
    """)

    rows = cursor.fetchall()

    text="Scoreboard\n\n"

    for i,row in enumerate(rows,1):
        text+=f"{i}. {row[0]} - {row[1]}\n"

    await message.delete()

    await bot.send_message(ADMIN_ID,text)


# ---------------- RESET ----------------

@dp.message(Command("resetscores"))
async def reset_scores(message:Message):

    if message.from_user.id != ADMIN_ID:
        return

    cursor.execute("UPDATE users SET points=0")
    conn.commit()

    await message.delete()

    await bot.send_message(ADMIN_ID,"Scores reset")


# ---------------- REPORT ----------------

@dp.message(Command("report"))
async def report(message:Message):

    if message.from_user.id != ADMIN_ID:
        return

    cursor.execute("SELECT value FROM settings WHERE key='members'")
    row = cursor.fetchone()

    members = int(row[0]) if row else 0

    cursor.execute("SELECT COUNT(DISTINCT user_id) FROM responses")
    participants = cursor.fetchone()[0]

    percent = 0

    if members>0:
        percent = round((participants/members)*100,1)

    text=f"""
Engagement Report

Members: {members}
Participants: {participants}
Participation: {percent}%
"""

    await message.delete()

    await bot.send_message(ADMIN_ID,text)


# ---------------- START ----------------

async def main():

    await bot.delete_webhook(drop_pending_updates=True)

    await dp.start_polling(bot)


if __name__=="__main__":
    asyncio.run(main())
