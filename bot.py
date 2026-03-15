import os
import csv
import asyncio
import sqlite3
import requests

from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder


TOKEN = os.getenv("TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
GROUP_ID = int(os.getenv("GROUP_ID"))
SHEET_URL = os.getenv("SHEET_URL")

bot = Bot(token=TOKEN)
dp = Dispatcher()


# ---------------- DATABASE ----------------

conn = sqlite3.connect("rice.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users(
user_id INTEGER,
group_id INTEGER,
name TEXT,
points INTEGER DEFAULT 0,
PRIMARY KEY(user_id,group_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS posts(
post_id INTEGER PRIMARY KEY AUTOINCREMENT,
message_id INTEGER,
correct_option INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS responses(
post_id INTEGER,
user_id INTEGER,
PRIMARY KEY(post_id,user_id)
)
""")

# NEW table for scheduler state
cursor.execute("""
CREATE TABLE IF NOT EXISTS scheduled_posts(
row_key TEXT PRIMARY KEY
)
""")

conn.commit()


# ---------------- SHEET ----------------

def fetch_sheet():

    r = requests.get(SHEET_URL)
    r.raise_for_status()

    lines = r.content.decode("utf-8").splitlines()

    reader = csv.DictReader(lines)

    rows = []

    for row in reader:

        clean = {}

        for k,v in row.items():
            if k:
                clean[k.strip().lower()] = v.strip() if v else ""

        rows.append(clean)

    return rows


def parse_datetime(date_str,time_str):

    formats = [

        ("%d/%m/%Y","%H:%M"),
        ("%d/%m/%Y","%H:%M:%S"),
        ("%d-%m-%Y","%H:%M"),
        ("%d-%m-%Y","%H:%M:%S")

    ]

    for df,tf in formats:

        try:
            return datetime.strptime(
                f"{date_str} {time_str}",
                f"{df} {tf}"
            )

        except:
            pass

    return None


# create unique key for each sheet row
def row_key(row):

    return f"{row['date']}_{row['time']}_{row['type']}_{row['question']}"


# ---------------- CREATE POST ----------------

async def create_post(row):

    question = row["question"]

    options = [o.strip() for o in row["options"].split("|") if o.strip()]

    correct = row.get("correct","")

    ptype = row["type"].lower()

    builder = InlineKeyboardBuilder()

    for i,opt in enumerate(options,1):

        builder.button(
            text=opt,
            callback_data=f"{ptype}:{i}"
        )

    builder.adjust(1)

    sent = await bot.send_message(
        GROUP_ID,
        question,
        reply_markup=builder.as_markup()
    )

    correct_option = None

    if ptype == "quiz":

        try:
            correct_option = int(correct)
        except:
            correct_option = None

    cursor.execute("""
    INSERT INTO posts(message_id,correct_option)
    VALUES(?,?)
    """,(sent.message_id,correct_option))

    conn.commit()


# ---------------- SCHEDULER ----------------

async def scheduler():

    while True:

        try:
            rows = fetch_sheet()
        except:
            await asyncio.sleep(120)
            continue

        now = datetime.now(timezone.utc) + timedelta(hours=5,minutes=30)

        for row in rows:

            key = row_key(row)

            cursor.execute(
                "SELECT 1 FROM scheduled_posts WHERE row_key=?",
                (key,)
            )

            if cursor.fetchone():
                continue


            date_val = row.get("date")
            time_val = row.get("time")

            if not date_val or not time_val:
                continue


            scheduled = parse_datetime(date_val,time_val)

            if not scheduled:
                continue


            scheduled = scheduled.replace(
                tzinfo=timezone.utc
            ) + timedelta(hours=5,minutes=30)


            if now >= scheduled:

                try:

                    await create_post(row)

                    cursor.execute(
                        "INSERT INTO scheduled_posts(row_key) VALUES(?)",
                        (key,)
                    )

                    conn.commit()

                except:
                    pass


        await asyncio.sleep(60)


# ---------------- RESPONSE HANDLING ----------------

@dp.callback_query(F.data.contains(":"))
async def handle_response(callback: CallbackQuery):

    user = callback.from_user
    message_id = callback.message.message_id

    cursor.execute("""
    SELECT post_id,correct_option
    FROM posts
    WHERE message_id=?
    """,(message_id,))

    post = cursor.fetchone()

    if not post:
        return

    post_id,correct_option = post

    cursor.execute("""
    SELECT 1 FROM responses
    WHERE post_id=? AND user_id=?
    """,(post_id,user.id))

    if cursor.fetchone():

        await callback.answer("Already responded")
        return


    ptype,option = callback.data.split(":")
    option = int(option)

    points = 1

    if ptype == "quiz" and option == correct_option:
        points += 2


    cursor.execute("""
    INSERT OR IGNORE INTO users(user_id,group_id,name)
    VALUES(?,?,?)
    """,(user.id,GROUP_ID,user.full_name))

    cursor.execute("""
    UPDATE users
    SET points = points + ?
    WHERE user_id=? AND group_id=?
    """,(points,user.id,GROUP_ID))

    cursor.execute("""
    INSERT INTO responses(post_id,user_id)
    VALUES(?,?)
    """,(post_id,user.id))

    conn.commit()

    await callback.answer("Recorded")


# ---------------- RESET SCORES ----------------

@dp.message(Command("resetscores"))
async def reset_scores(message:Message):

    if message.from_user.id != ADMIN_ID:
        return

    cursor.execute("UPDATE users SET points=0")
    cursor.execute("DELETE FROM responses")

    conn.commit()

    await message.reply("Scores reset")


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

    text = "🏆 RICE Board\n\n"

    for i,row in enumerate(rows,1):

        text += f"{i}. {row[0]} — {row[1]}\n"

    await message.reply(text)


# ---------------- START ----------------

async def main():

    await bot.delete_webhook(drop_pending_updates=True)

    asyncio.create_task(scheduler())

    await dp.start_polling(bot)


if __name__ == "__main__":

    asyncio.run(main())
