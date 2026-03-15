import os
import csv
import asyncio
import sqlite3
import requests

from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder


print("RICE BOT LOADING")


TOKEN = os.getenv("TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
OWNER_ID = int(os.getenv("OWNER_ID"))
GROUP_ID = int(os.getenv("GROUP_ID"))
SHEET_URL = os.getenv("SHEET_URL")

bot = Bot(token=TOKEN)
dp = Dispatcher()


# ---------------- DATABASE ----------------

conn = sqlite3.connect("rice.db")
cursor = conn.cursor()

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
PRIMARY KEY(post_id, user_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS settings(
key TEXT PRIMARY KEY,
value TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS scheduled_posts(
row_key TEXT PRIMARY KEY
)
""")

conn.commit()


# ---------------- HELPERS ----------------

def ensure_user(user):
    cursor.execute("""
    INSERT OR IGNORE INTO users(user_id, name, username, points)
    VALUES(?, ?, ?, 0)
    """, (user.id, user.full_name, user.username))
    conn.commit()


def add_points(user_id, points):
    cursor.execute("""
    UPDATE users SET points = points + ?
    WHERE user_id = ?
    """, (points, user_id))
    conn.commit()


def build_keyboard(post_id):

    cursor.execute("SELECT options FROM posts WHERE post_id=?", (post_id,))
    row = cursor.fetchone()

    if not row:
        return None

    options = row[0].split("|")

    builder = InlineKeyboardBuilder()

    for i, opt in enumerate(options, 1):

        cursor.execute("""
        SELECT COUNT(*) FROM responses
        WHERE post_id=? AND selected_option=?
        """, (post_id, i))

        count = cursor.fetchone()[0]

        builder.button(
            text=f"{opt} ({count})",
            callback_data=f"{post_id}:{i}"
        )

    builder.adjust(1)

    return builder.as_markup()


# ---------------- SHEET ----------------

def fetch_sheet():

    r = requests.get(SHEET_URL)
    r.raise_for_status()

    lines = r.content.decode("utf-8").splitlines()
    reader = csv.DictReader(lines)

    rows = []

    for row in reader:

        clean = {}

        for k, v in row.items():
            if k:
                clean[k.strip().lower()] = v.strip() if v else ""

        rows.append(clean)

    return rows


def parse_datetime(date_str, time_str):

    formats = [
        ("%d/%m/%Y", "%H:%M"),
        ("%d/%m/%Y", "%H:%M:%S"),
        ("%d-%m-%Y", "%H:%M"),
        ("%d-%m-%Y", "%H:%M:%S")
    ]

    for df, tf in formats:

        try:
            return datetime.strptime(
                f"{date_str} {time_str}",
                f"{df} {tf}"
            )
        except:
            pass

    return None


def row_key(row):
    # Unique key from date + time + type — no sheet structure change needed
    return f"{row['date']}_{row['time']}_{row['type']}"


def generate_post_code(row):
    ptype = row["type"].upper()
    date_part = row["date"].replace("/", "").replace("-", "")
    time_part = row["time"].replace(":", "")
    return f"{ptype}_{date_part}_{time_part}"


# ---------------- POST CREATION (from sheet row) ----------------

async def create_post(row):

    ptype = row["type"].lower()
    question = row["question"]
    options = [o.strip() for o in row["options"].split("|") if o.strip()]
    correct = row.get("correct", "").strip()
    post_code = generate_post_code(row)

    correct_option = None

    if ptype == "quiz":
        try:
            correct_option = int(correct)
        except:
            correct_option = None

    options_string = "|".join(options)

    text = f"{post_code}\n\n{question}"

    # Initial keyboard with 0 counts (temp callback to avoid issues)
    builder = InlineKeyboardBuilder()

    for i, opt in enumerate(options, 1):
        builder.button(text=f"{opt} (0)", callback_data="temp:0")

    builder.adjust(1)

    sent = await bot.send_message(
        GROUP_ID,
        text,
        reply_markup=builder.as_markup()
    )

    cursor.execute("""
    INSERT INTO posts(post_code, telegram_message_id, type, question, options, correct_option, created_at)
    VALUES(?, ?, ?, ?, ?, ?, ?)
    """, (post_code, sent.message_id, ptype, question, options_string, correct_option, datetime.now().isoformat()))

    conn.commit()

    post_id = cursor.lastrowid

    # Update keyboard with real post_id in callback_data
    await bot.edit_message_reply_markup(
        chat_id=GROUP_ID,
        message_id=sent.message_id,
        reply_markup=build_keyboard(post_id)
    )

    print(f"Post created: {post_code} | {question}")


# ---------------- SCHEDULER ----------------

async def scheduler():

    print("Scheduler started")

    while True:

        try:
            rows = fetch_sheet()
            print(f"Rows fetched: {len(rows)}")

        except Exception as e:
            print(f"Sheet error: {e}")
            await asyncio.sleep(120)
            continue

        now = datetime.now()
        print(f"Current time: {now}")

        for row in rows:

            date_val = row.get("date")
            time_val = row.get("time")

            if not date_val or not time_val:
                continue

            scheduled = parse_datetime(date_val, time_val)

            if not scheduled:
                continue

            if now >= scheduled:

                key = row_key(row)

                cursor.execute(
                    "SELECT 1 FROM scheduled_posts WHERE row_key=?",
                    (key,)
                )

                if cursor.fetchone():
                    continue

                try:
                    await create_post(row)

                    cursor.execute(
                        "INSERT INTO scheduled_posts(row_key) VALUES(?)",
                        (key,)
                    )

                    conn.commit()
                    print(f"Scheduled post executed: {key}")

                except Exception as e:
                    print(f"Post error: {e}")

        await asyncio.sleep(300)


# ---------------- BUTTON CLICK ----------------

@dp.callback_query(F.data.contains(":"))
async def handle_click(callback: CallbackQuery):

    try:
        post_id, option_index = map(int, callback.data.split(":"))
    except:
        await callback.answer()
        return

    user = callback.from_user

    cursor.execute("""
    SELECT 1 FROM responses
    WHERE post_id=? AND user_id=?
    """, (post_id, user.id))

    if cursor.fetchone():
        await callback.answer("Already responded", show_alert=True)
        return

    cursor.execute("""
    SELECT type, correct_option, options
    FROM posts
    WHERE post_id=?
    """, (post_id,))

    post = cursor.fetchone()

    if not post:
        await callback.answer()
        return

    post_type, correct_option, options_string = post
    options = options_string.split("|")

    ensure_user(user)

    correct = 0
    points = 1
    popup = "Recorded"

    if post_type == "quiz":

        if option_index == correct_option:
            correct = 1
            points += 2
            popup = "Correct ✅"
        else:
            popup = f"Wrong ❌  Correct: {options[correct_option - 1]}"

    add_points(user.id, points)

    cursor.execute("""
    INSERT INTO responses(post_id, user_id, selected_option, correct)
    VALUES(?, ?, ?, ?)
    """, (post_id, user.id, option_index, correct))

    conn.commit()

    await bot.edit_message_reply_markup(
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id,
        reply_markup=build_keyboard(post_id)
    )

    await callback.answer(popup, show_alert=True)


# ---------------- /start ----------------

@dp.message(Command("start"))
async def start(message: Message):

    if message.chat.type != "private":
        return

    # DEBUG — remove after confirming ADMIN_ID is correct
    await message.answer(f"Your ID: {message.from_user.id}\nADMIN_ID set: {ADMIN_ID}")

    if message.from_user.id != ADMIN_ID:
        return

    await message.answer(
"""RICE Bot Active ✅

Commands:
/setmembers [count] — Set total group members
/scoreboard — View leaderboard
/report — View engagement report
/resetscores — Reset all scores and responses"""
    )


# ---------------- /setmembers ----------------

@dp.message(Command("setmembers"))
async def set_members(message: Message):

    if message.chat.type != "private":
        return

    if message.from_user.id != ADMIN_ID:
        return

    try:
        count = int(message.text.split()[1])
    except:
        await message.answer("Usage: /setmembers 150")
        return

    cursor.execute("""
    INSERT OR REPLACE INTO settings(key, value)
    VALUES('members', ?)
    """, (str(count),))

    conn.commit()

    await message.answer(f"✅ Members set to {count}")


# ---------------- /scoreboard ----------------

@dp.message(Command("scoreboard"))
async def scoreboard(message: Message):

    if message.chat.type != "private":
        return

    if message.from_user.id != ADMIN_ID:
        return

    cursor.execute("""
    SELECT name, points
    FROM users
    ORDER BY points DESC
    LIMIT 20
    """)

    rows = cursor.fetchall()

    if not rows:
        await message.answer("No scores yet.")
        return

    text = "🏆 RICE Leaderboard\n\n"

    for i, row in enumerate(rows, 1):
        text += f"{i}. {row[0]} — {row[1]}\n"

    await message.answer(text)


# ---------------- /report ----------------

@dp.message(Command("report"))
async def report(message: Message):

    if message.chat.type != "private":
        return

    if message.from_user.id != ADMIN_ID:
        return

    cursor.execute("SELECT value FROM settings WHERE key='members'")
    row = cursor.fetchone()
    members = int(row[0]) if row else 0

    cursor.execute("""
    SELECT post_id, post_code, type, options, correct_option
    FROM posts
    ORDER BY created_at DESC
    """)

    posts = cursor.fetchall()

    if not posts:
        await message.answer("No posts yet.")
        return

    report_text = "📊 Engagement Report\n\n"

    for post_id, post_code, ptype, options_string, correct_option in posts:

        options = options_string.split("|")

        cursor.execute("""
        SELECT COUNT(*) FROM responses WHERE post_id=?
        """, (post_id,))

        participants = cursor.fetchone()[0]

        rate = round((participants / members) * 100, 1) if members else 0

        report_text += f"📌 {post_code}\n"
        report_text += f"Participants: {participants}"
        report_text += f" ({rate}%)\n" if members else "\n"

        cursor.execute("""
        SELECT selected_option, COUNT(*)
        FROM responses
        WHERE post_id=?
        GROUP BY selected_option
        """, (post_id,))

        votes = dict(cursor.fetchall())

        for i, opt in enumerate(options, 1):
            report_text += f"  {opt} — {votes.get(i, 0)}\n"

        if ptype == "quiz":

            cursor.execute("""
            SELECT COUNT(*) FROM responses
            WHERE post_id=? AND correct=1
            """, (post_id,))

            correct_count = cursor.fetchone()[0]
            report_text += f"  ✅ Correct: {correct_count}\n"

        report_text += "\n"

    await message.answer(report_text)


# ---------------- /resetscores ----------------

@dp.message(Command("resetscores"))
async def reset_scores(message: Message):

    if message.chat.type != "private":
        return

    if message.from_user.id != ADMIN_ID:
        return

    # Reset points
    cursor.execute("UPDATE users SET points=0")

    # Clear responses — so users can participate again after reset
    cursor.execute("DELETE FROM responses")

    conn.commit()

    await message.answer("✅ RICE scores and responses reset.")


# ---------------- MAIN ----------------

async def main():

    print("RICE bot starting")

    await bot.delete_webhook(drop_pending_updates=True)

    asyncio.create_task(scheduler())

    await dp.start_polling(bot)


if __name__ == "__main__":

    asyncio.run(main())
