# ============================================
# Бот "Онлайн черга в душ"
# Python 3.8.10
# python-telegram-bot==13.15
# ============================================
#
# Інструкція:
#  - Вставте токен бота у змінну TOKEN: "Вставте токен тут"
#  - Вставте ID адміна у змінну ADMIN_ID: "ід адміна"
#  - Збережіть як bot.py і запустіть: python bot.py
#
# Примітки:
#  - Код включає автоматичне визначення часової різниці між
#    datetime.now() та datetime.utcnow() (AUTO_TZ_OFFSET_HOURS).
#    Це допомагає коректно визначати робочі години незалежно від
#    локального налаштування сервера.
#  - VIP-логіка ("Стати передостаннім") і логіка "Завершити душ"
#    виправлені відповідно до ваших вимог.
#  - Якщо щось треба підлаштувати — скажіть конкретно, і я внесу
#    корективи.
#
# ============================================

import os
import sqlite3
import math
import traceback
import shutil
import json
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Any

# telegram imports (v13.15)
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    Bot,
)
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackQueryHandler,
    ConversationHandler,
    CallbackContext,
)

# ============================================
# Налаштування — ЗАМІНІТЬ ТУТ
# ============================================
TOKEN = "8270520671:AAEsObGdNzItcwA5QwKTbD4Tgj0ioOJMORE"     # 🔹 ВСТАВТЕ ТОКЕН СВОГО БОТА СЮДИ
ADMIN_ID = "5796029813"           # 🔹 ВСТАВТЕ ID АДМІНА СЮДИ
DB_PATH = "shower_queue.db"
LOG_FILE = "bot_events.log"
BACKUP_DIR = "backups"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# ============================================
# Conversation states
# ============================================
(
    REGISTER_NAME,
    REGISTER_GENDER,
    GROUP_SIZE_SELECT,
    ADMIN_WAIT_DELETE,
    ADMIN_WAIT_VIP,
    ADMIN_WAIT_MAKE_SETTINGS,
    ADMIN_WAIT_INPUT,
) = range(7)

# ============================================
# Логування і допоміжні функції
# ============================================
def log_event(msg: str):
    """Логування в файл + консоль з відміткою часу."""
    try:
        line = f"[{datetime.now().strftime(DATETIME_FORMAT)}] {msg}"
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        print(line)
    except Exception:
        print("Помилка логування:", traceback.format_exc())


def ensure_dirs():
    if not os.path.isdir(BACKUP_DIR):
        try:
            os.makedirs(BACKUP_DIR, exist_ok=True)
        except Exception as e:
            log_event(f"Не вдалось створити папку бекапів: {e}")


ensure_dirs()


def parse_admin_id(admin_raw) -> Optional[int]:
    if isinstance(admin_raw, int):
        return admin_raw
    if isinstance(admin_raw, str):
        try:
            admin_clean = admin_raw.strip()
            if admin_clean.isdigit():
                return int(admin_clean)
        except Exception:
            return None
    return None


ADMIN_ID_INT = parse_admin_id(ADMIN_ID)

# ============================================
# Автовизначення TZ offset (вирішує проблему з серверним часом)
# ============================================
def detect_tz_offset_hours() -> int:
    """
    Обчислює зсув у годинах: datetime.now() - datetime.utcnow().
    Округлює до найближчого цілого (зазвичай 2 або 3 для Kyiv),
    або повертає 0 у разі невдачі.
    """
    try:
        now_local_system = datetime.now()
        now_utc = datetime.utcnow()
        delta = now_local_system - now_utc
        hours = int(round(delta.total_seconds() / 3600.0))
        return hours
    except Exception:
        return 0


AUTO_TZ_OFFSET_HOURS = detect_tz_offset_hours()
log_event(f"AUTO_TZ_OFFSET_HOURS detected: {AUTO_TZ_OFFSET_HOURS}")


def now_local() -> datetime:
    """
    Повертає локальний час для логіки бота (UTC + AUTO_TZ_OFFSET_HOURS).
    Якщо сервер вже має точно налаштований локальний час (Kyiv),
    AUTO_TZ_OFFSET_HOURS може бути 3 (або 2 враховуючи DST).
    """
    return datetime.utcnow() + timedelta(hours=AUTO_TZ_OFFSET_HOURS)


# ============================================
# Ініціалізація бази даних
# ============================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE,
            name TEXT,
            gender TEXT,
            vip INTEGER DEFAULT 0,
            registered INTEGER DEFAULT 0,
            created_at TEXT,
            last_seen TEXT
        )
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            group_size INTEGER DEFAULT 1,
            in_shower INTEGER DEFAULT 0,
            start_time TEXT,
            inserted_at TEXT
        )
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS status (
            id INTEGER PRIMARY KEY,
            cabins INTEGER DEFAULT 3,
            water TEXT DEFAULT 'Невідомо',
            last_update TEXT
        )
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """
    )
    c.execute("SELECT id FROM status WHERE id=1")
    if not c.fetchone():
        now = now_local().strftime(DATETIME_FORMAT)
        c.execute("INSERT INTO status (id, cabins, water, last_update) VALUES (1, 3, 'Невідомо', ?)", (now,))
    c.execute("SELECT value FROM settings WHERE key='working_hours'")
    if not c.fetchone():
        working = {
            # 0=Mon ... 6=Sun
            "0": [[10, 14], [17, 21]],
            "1": [[10, 14], [17, 21]],
            "2": [[10, 14], [17, 21]],
            "3": [[10, 14], [17, 21]],
            "4": [[10, 14], [17, 21]],
            "5": [[10, 14], [17, 21]],
        }
        c.execute("INSERT INTO settings (key, value) VALUES (?,?)", ("working_hours", json.dumps(working)))
    c.execute("SELECT value FROM settings WHERE key='avg_times'")
    if not c.fetchone():
        avg_times = {"male_min": 10, "male_max": 15, "female_min": 15, "female_max": 20}
        c.execute("INSERT INTO settings (key, value) VALUES (?,?)", ("avg_times", json.dumps(avg_times)))
    conn.commit()
    conn.close()
    log_event("Ініціалізація бази даних завершена.")


init_db()

# ============================================
# DB helpers
# ============================================
def db_connect():
    return sqlite3.connect(DB_PATH)


def get_user_row(user_id: int) -> Optional[Tuple]:
    conn = db_connect()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row


def register_user_if_not_exists(user_id: int):
    row = get_user_row(user_id)
    if not row:
        conn = db_connect()
        c = conn.cursor()
        now = now_local().strftime(DATETIME_FORMAT)
        c.execute("INSERT OR IGNORE INTO users (user_id, registered, created_at, last_seen) VALUES (?,0,?,?)", (user_id, now, now))
        conn.commit()
        conn.close()
        log_event(f"Додано тимчасовий запис користувача: {user_id}")


def update_user_last_seen(user_id: int):
    conn = db_connect()
    c = conn.cursor()
    now = now_local().strftime(DATETIME_FORMAT)
    c.execute("UPDATE users SET last_seen=? WHERE user_id=?", (now, user_id))
    conn.commit()
    conn.close()


def set_user_registered(user_id: int, name: str, gender: str, vip: int = 0):
    conn = db_connect()
    c = conn.cursor()
    now = now_local().strftime(DATETIME_FORMAT)
    c.execute("UPDATE users SET name=?, gender=?, vip=?, registered=1, last_seen=? WHERE user_id=?", (name, gender, vip, now, user_id))
    conn.commit()
    conn.close()
    log_event(f"Користувач {user_id} зареєстрований як {name}, {gender}, VIP={vip}")


def add_to_queue(user_id: int, group_size: int = 1):
    conn = db_connect()
    c = conn.cursor()
    now = now_local().strftime(DATETIME_FORMAT)
    c.execute("INSERT INTO queue (user_id, group_size, in_shower, start_time, inserted_at) VALUES (?,?,?,?,?)", (user_id, group_size, 0, None, now))
    conn.commit()
    conn.close()
    log_event(f"Користувач {user_id} доданий в чергу група={group_size}")


def remove_queue_entry_by_user(user_id: int):
    conn = db_connect()
    c = conn.cursor()
    c.execute("DELETE FROM queue WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()
    log_event(f"Користувач {user_id} видалений з черги (якщо був).")


def get_full_queue() -> List[Tuple]:
    conn = db_connect()
    c = conn.cursor()
    c.execute("SELECT id, user_id, group_size, in_shower, start_time, inserted_at FROM queue ORDER BY id ASC")
    rows = c.fetchall()
    conn.close()
    return rows


def get_status_row():
    conn = db_connect()
    c = conn.cursor()
    c.execute("SELECT id, cabins, water, last_update FROM status WHERE id=1")
    row = c.fetchone()
    conn.close()
    return row


def update_status_row(cabins: Optional[int] = None, water: Optional[str] = None):
    conn = db_connect()
    c = conn.cursor()
    now = now_local().strftime(DATETIME_FORMAT)
    if cabins is not None:
        c.execute("UPDATE status SET cabins=?, last_update=? WHERE id=1", (cabins, now))
    if water is not None:
        c.execute("UPDATE status SET water=?, last_update=? WHERE id=1", (water, now))
    if cabins is None and water is None:
        c.execute("UPDATE status SET last_update=? WHERE id=1", (now,))
    conn.commit()
    conn.close()
    log_event(f"Оновлено статус душу cabins={cabins} water={water}")


def set_first_in_shower_by_queue_id(queue_id: int, send_notification: bool = False):
    """
    Позначає запис як в душі.
    Якщо send_notification=True -> надсилає DM з кнопкою 'Завершити душ' (використовувати для job/авто).
    Якщо False -> не надсилає DM (використовувати коли користувач вже бачить повідомлення і ми edit-уємо).
    """
    conn = db_connect()
    c = conn.cursor()
    now = now_local().strftime(DATETIME_FORMAT)
    c.execute("UPDATE queue SET in_shower=1, start_time=? WHERE id=?", (now, queue_id))
    conn.commit()
    c.execute("SELECT user_id FROM queue WHERE id=?", (queue_id,))
    row = c.fetchone()
    conn.close()
    if row:
        uid = row[0]
        log_event(f"Позначено queue.id={queue_id} (user={uid}) як в душі.")
        if send_notification:
            try:
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Завершити душ", callback_data="finish_shower")],
                                           [InlineKeyboardButton("❌ Відмінити чергу", callback_data="cancel_queue")]])
                Bot(TOKEN).send_message(uid, "🚿 Ви зараз у душі. Коли закінчите — натисніть 'Завершити душ' у цьому повідомленні.", reply_markup=kb)
                log_event(f"Надіслано DM про початок душу користувачу {uid}")
            except Exception as e:
                log_event(f"Не вдалось надіслати DM про початок душу для {uid}: {e}")
    else:
        log_event(f"set_first_in_shower_by_queue_id: не знайдено запис queue_id={queue_id}")


def clear_in_shower_for_user(user_id: int):
    conn = db_connect()
    c = conn.cursor()
    c.execute("DELETE FROM queue WHERE user_id=? AND in_shower=1", (user_id,))
    conn.commit()
    conn.close()
    log_event(f"Видалено запис в душі для користувача {user_id}.")


def shift_queue_if_needed(send_notification: bool = False):
    """
    Якщо нема нікого в душі, бере перший запис та позначає його в душі.
    Якщо send_notification=True -> надсилає DM, інакше лише помічає (для випадків, коли ми edit-уємо повідомлення).
    Повертає user_id того, хто пішов в душ (або None).
    """
    q = get_full_queue()
    if not q:
        return None
    in_shower = [r for r in q if r[3] == 1]
    if in_shower:
        return in_shower[0][1]
    first = q[0]
    set_first_in_shower_by_queue_id(first[0], send_notification)
    return first[1]


# ============================================
# Settings helpers
# ============================================
def get_setting(key: str) -> Optional[Any]:
    conn = db_connect()
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return row[0]


def set_setting(key: str, value: Any):
    conn = db_connect()
    c = conn.cursor()
    if isinstance(value, (dict, list)):
        val = json.dumps(value)
    else:
        val = str(value)
    c.execute("REPLACE INTO settings (key, value) VALUES (?,?)", (key, val))
    conn.commit()
    conn.close()
    log_event(f"Налаштування встановлено: {key} = {val}")


def get_setting_safe(key: str):
    try:
        return get_setting(key)
    except Exception as e:
        log_event(f"get_setting_safe error: {e}")
        return None


# ============================================
# Time & wait calculations
# ============================================
def avg_minutes_for_gender(gender: str) -> int:
    avg = get_setting("avg_times")
    if not avg:
        male = (10 + 15) / 2
        female = (15 + 20) / 2
    else:
        male = (avg.get("male_min", 10) + avg.get("male_max", 15)) / 2
        female = (avg.get("female_min", 15) + avg.get("female_max", 20)) / 2
    if gender and gender.lower().startswith("х"):
        return int(round(male))
    else:
        return int(round(female))


def calculate_wait_time_for_user(user_id: int) -> Tuple[int, int]:
    queue = get_full_queue()
    status = get_status_row()
    cabins = status[1] if status else 1
    pos = None
    for idx, row in enumerate(queue):
        if row[1] == user_id:
            pos = idx + 1
            break
    if pos is None:
        return 0, 0
    if pos == 1 and queue[0][3] == 1:
        return 0, 1
    total_minutes = 0.0
    conn = db_connect()
    c = conn.cursor()
    for row in queue[: pos - 1]:
        uid = row[1]
        group = row[2] or 1
        c.execute("SELECT gender FROM users WHERE user_id=?", (uid,))
        g = c.fetchone()
        gender = g[0] if g else "Хлопець"
        avg = avg_minutes_for_gender(gender)
        blocks = math.ceil(group / cabins)
        total_minutes += avg * blocks
    conn.close()
    return math.ceil(total_minutes), pos


def format_minutes_as_text(minutes: int) -> str:
    if minutes < 60:
        return f"{minutes} хв"
    h = minutes // 60
    m = minutes % 60
    if m == 0:
        return f"{h} год"
    return f"{h} год {m} хв"


# ============================================
# Keyboards
# ============================================
def main_menu_markup(vip: bool = False, admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("🚿 Стати в чергу", callback_data="join_queue")],
        [InlineKeyboardButton("📊 Статус душу", callback_data="status")],
        [InlineKeyboardButton("📋 Переглянути чергу", callback_data="view_queue")],
    ]
    if vip:
        buttons.append([InlineKeyboardButton("⭐ Стати передостаннім", callback_data="vip_join")])
    if admin:
        buttons.append([InlineKeyboardButton("⚙️ Адмін меню", callback_data="admin_menu")])
    return InlineKeyboardMarkup(buttons)


def minimal_back_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_menu")]])


def in_shower_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Завершити душ", callback_data="finish_shower")],
        [InlineKeyboardButton("❌ Відмінити чергу", callback_data="cancel_queue")],
    ])


# ============================================
# Working hours check
# ============================================
def is_working_hours(dt: Optional[datetime] = None) -> bool:
    if dt is None:
        now = now_local()
    else:
        now = dt
    weekday = now.weekday()
    working = get_setting_safe("working_hours")
    if not working:
        working = {
            "0": [[10, 14], [17, 21]],
            "1": [[10, 14], [17, 21]],
            "2": [[10, 14], [17, 21]],
            "3": [[10, 14], [17, 21]],
            "4": [[10, 14], [17, 21]],
            "5": [[10, 14], [17, 21]],
        }
    periods = working.get(str(weekday), [])
    current_minutes = now.hour * 60 + now.minute
    for p in periods:
        try:
            start, end = int(p[0]), int(p[1])
            start_min = start * 60
            end_min = end * 60
            if start_min <= current_minutes < end_min:
                return True
        except Exception:
            continue
    return False


def working_hours_text() -> str:
    working = get_setting_safe("working_hours")
    if not working:
        working = {
            "0": [[10, 14], [17, 21]],
            "1": [[10, 14], [17, 21]],
            "2": [[10, 14], [17, 21]],
            "3": [[10, 14], [17, 21]],
            "4": [[10, 14], [17, 21]],
            "5": [[10, 14], [17, 21]],
        }
    days = ["Понеділок","Вівторок","Середа","Четвер","П'ятниця","Субота","Неділя"]
    parts = []
    for d in range(7):
        periods = working.get(str(d), [])
        if not periods:
            parts.append(f"{days[d]}: закрито")
            continue
        segs = []
        for p in periods:
            start, end = int(p[0]), int(p[1])
            if start == 0 and end == 24:
                segs.append("цілодобово")
            else:
                segs.append(f"{start:02d}:00–{end:02d}:00")
        parts.append(f"{days[d]}: {', '.join(segs)}")
    return "\n".join(parts)


def send_not_working_callback(query):
    try:
        text = "⚠️ Бот зараз не працює поза робочим часом.\n\nРобочі години:\n" + working_hours_text()
        try:
            query.edit_message_text(text)
        except Exception:
            safe_answer(query, show_alert=True, text="Бот зараз не працює. Перевірте робочі години.")
    except Exception as e:
        log_event(f"send_not_working_callback error: {e}")


# ============================================
# Safe answer wrapper
# ============================================
def safe_answer(query, **kwargs):
    try:
        query.answer(**kwargs)
    except Exception as e:
        # ігноруємо помилки типу "Query is too old..."
        log_event(f"safe_answer ignored: {e}")


# ============================================
# send in-shower DM (auto)
# ============================================
def send_in_shower_notification(user_id: int):
    kb = in_shower_keyboard()
    text = "🚿 Ви зараз у душі. Коли закінчите — натисніть 'Завершити душ' у цьому повідомленні."
    try:
        Bot(TOKEN).send_message(user_id, text, reply_markup=kb)
    except Exception as e:
        log_event(f"Не вдалось надіслати DM про початок душу для {user_id}: {e}")


# ============================================
# Start / Help
# ============================================
def start(update: Update, context: CallbackContext):
    user = update.effective_user
    user_id = user.id
    register_user_if_not_exists(user_id)
    update_user_last_seen(user_id)
    row = get_user_row(user_id)
    need_register = not row or row[5] == 0 or not row[2] or not row[3]
    if need_register:
        update.message.reply_text("👋 Привіт! Будь ласка, введіть своє ім'я для реєстрації:")
        return REGISTER_NAME
    else:
        vip_flag = bool(row[4])
        is_admin = (ADMIN_ID_INT is not None and user_id == ADMIN_ID_INT)
        update.message.reply_text(f"Вітаю, {row[2]}!", reply_markup=main_menu_markup(vip=vip_flag, admin=is_admin))
        return ConversationHandler.END


def help_command(update: Update, context: CallbackContext):
    update.message.reply_text("Цей бот керує онлайн-чергою до душу. Натисніть /start щоб почати.")


# ============================================
# Registration handlers
# ============================================
def register_name_handler(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    user_id = update.effective_user.id
    context.user_data["reg_name"] = text[:64]
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Хлопець", callback_data="gender_m")],
        [InlineKeyboardButton("Дівчина", callback_data="gender_f")],
    ])
    update.message.reply_text("Оберіть вашу стать:", reply_markup=keyboard)
    return REGISTER_GENDER


def register_gender_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    user_id = query.from_user.id
    data = query.data
    name = context.user_data.get("reg_name", "Користувач")
    gender = "Хлопець" if data == "gender_m" else "Дівчина"
    set_user_registered(user_id, name, gender, vip=0)
    vip_flag = False
    is_admin = (ADMIN_ID_INT is not None and user_id == ADMIN_ID_INT)
    try:
        query.edit_message_text(text=f"✅ Реєстрацію завершено! Привіт, {name}.", reply_markup=main_menu_markup(vip=vip_flag, admin=is_admin))
    except Exception:
        try:
            Bot(TOKEN).send_message(user_id, f"✅ Реєстрацію завершено! Привіт, {name}.")
        except Exception:
            pass
    return ConversationHandler.END


# ============================================
# Join queue handlers
# ============================================
def join_queue_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    user_id = query.from_user.id
    if not is_working_hours() and not (ADMIN_ID_INT is not None and user_id == ADMIN_ID_INT):
        return send_not_working_callback(query)
    user_row = get_user_row(user_id)
    if not user_row or user_row[5] == 0:
        try:
            query.edit_message_text("⚠️ Ви не зареєстровані. Введіть /start щоб зареєструватись.")
        except Exception:
            pass
        return
    q = get_full_queue()
    if any(r[1] == user_id for r in q):
        vip_flag = bool(user_row[4])
        is_admin = (ADMIN_ID_INT is not None and user_id == ADMIN_ID_INT)
        try:
            query.edit_message_text("⚠️ Ви вже у черзі!", reply_markup=main_menu_markup(vip=vip_flag, admin=is_admin))
        except Exception:
            pass
        return
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Один", callback_data="solo")],
        [InlineKeyboardButton("Групою", callback_data="group")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_menu")],
    ])
    try:
        query.edit_message_text("🚿 Ви йдете в душ: один чи групою?", reply_markup=keyboard)
    except Exception:
        pass


def back_to_menu_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    user_id = query.from_user.id
    user_row = get_user_row(user_id)
    vip_flag = bool(user_row[4]) if user_row else False
    is_admin = (ADMIN_ID_INT is not None and user_id == ADMIN_ID_INT)
    try:
        query.edit_message_text("Меню:", reply_markup=main_menu_markup(vip=vip_flag, admin=is_admin))
    except Exception:
        pass


def solo_selected_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    user_id = query.from_user.id
    if not is_working_hours() and not (ADMIN_ID_INT is not None and user_id == ADMIN_ID_INT):
        return send_not_working_callback(query)
    add_to_queue(user_id, group_size=1)
    handle_queue_addition_notifications(query, user_id, 1)


def group_selected_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    user_id = query.from_user.id
    if not is_working_hours() and not (ADMIN_ID_INT is not None and user_id == ADMIN_ID_INT):
        return send_not_working_callback(query)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("2", callback_data="group_2"), InlineKeyboardButton("3", callback_data="group_3")],
        [InlineKeyboardButton("4", callback_data="group_4"), InlineKeyboardButton("5+", callback_data="group_5")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="join_queue")],
    ])
    try:
        query.edit_message_text("👥 Вкажіть кількість осіб у групі:", reply_markup=keyboard)
    except Exception:
        pass


def handle_group_size_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    user_id = query.from_user.id
    if not is_working_hours() and not (ADMIN_ID_INT is not None and user_id == ADMIN_ID_INT):
        return send_not_working_callback(query)
    data = query.data
    size = 5 if data == "group_5" else int(data.split("_")[1])
    add_to_queue(user_id, group_size=size)
    handle_queue_addition_notifications(query, user_id, size)


def handle_queue_addition_notifications(query, user_id: int, group_size: int):
    """
    Викликається після додавання користувача в чергу.
    Якщо користувач перший -> ставимо в душі та надсилаємо кнопку "Завершити душ".
    Інакше — показуємо позицію і орієнтовний час.
    Також викликаємо notify_next_user(), та інші оновлення.
    """
    try:
        safe_answer(query)
    except Exception:
        pass
    q = get_full_queue()
    pos = None
    for idx, r in enumerate(q):
        if r[1] == user_id:
            pos = idx + 1
            break
    if pos is None:
        try:
            query.edit_message_text("Помилка: не вдалось знайти вас у черзі.")
        except Exception:
            pass
        return
    in_shower_list = [r for r in q if r[3] == 1]
    # Якщо перший і ніхто не в душі -> помітити як в душі та edit повідомлення (без DM)
    if pos == 1 and not in_shower_list:
        queue_id = None
        for r in q:
            if r[1] == user_id:
                queue_id = r[0]
                break
        if queue_id is not None:
            set_first_in_shower_by_queue_id(queue_id, send_notification=False)
            kb = in_shower_keyboard()
            try:
                query.edit_message_text(
                    "🚿 Ви перший і зараз у душі.\nКоли закінчите — натисніть кнопку в цьому повідомленні.",
                    reply_markup=kb,
                )
            except Exception:
                pass
            update_status_row()
            notify_next_user()
            return
    wait_min, position = calculate_wait_time_for_user(user_id)
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Відмінити чергу", callback_data="cancel_queue")]])
    try:
        query.edit_message_text(
            f"🧼 Ви додані в чергу!\n\nВаш номер у черзі: {position}\nОрієнтовний час очікування: ~{format_minutes_as_text(wait_min)}",
            reply_markup=keyboard,
        )
    except Exception:
        pass
    notify_next_user()


# ============================================
# notify_next_user
# ============================================
def notify_next_user():
    q = get_full_queue()
    if not q:
        return
    in_shower_idxs = [i for i, r in enumerate(q) if r[3] == 1]
    if in_shower_idxs:
        first_idx = in_shower_idxs[0]
        next_idx = first_idx + 1
        if next_idx < len(q):
            next_user_id = q[next_idx][1]
            try:
                Bot(TOKEN).send_message(next_user_id, "🔔 Ви наступний! Підготуйте речі та спустіться 🚿")
                log_event(f"Повідомлено наступного користувача {next_user_id}")
            except Exception as e:
                log_event(f"Не вдалось надіслати повідомлення наступному {next_user_id}: {e}")
    else:
        if len(q) >= 1:
            first_user = q[0][1]
            try:
                Bot(TOKEN).send_message(first_user, "🔔 Ви перший у черзі — скоро почнеться ваша черга. Підготуйтеся.")
                log_event(f"Повідомлено першого у черзі {first_user}")
            except Exception as e:
                log_event(f"Не вдалось повідомити першого {first_user}: {e}")


# ============================================
# Finish shower + feedback
# ============================================
def finish_shower_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    user_id = query.from_user.id
    q = get_full_queue()
    in_shower = [r for r in q if r[1] == user_id and r[3] == 1]
    if not in_shower:
        try:
            query.edit_message_text("❌ Ви не позначені як 'в душі'. Якщо це помилка, зверніться до адміна.")
        except Exception:
            pass
        return
    # Видаляємо поточного з черги
    remove_queue_entry_by_user(user_id)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Тепла", callback_data="water_warm"),
         InlineKeyboardButton("Ледь тепла", callback_data="water_lukewarm"),
         InlineKeyboardButton("Холодна", callback_data="water_cold")],
    ])
    try:
        query.edit_message_text("🧾 Як вам була вода?", reply_markup=keyboard)
    except Exception:
        pass


def water_feedback_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    data = query.data
    mapping = {
        "water_warm": "Тепла",
        "water_lukewarm": "Ледь тепла",
        "water_cold": "Холодна",
    }
    water_state = mapping.get(data, "Невідомо")
    update_status_row(water=water_state)
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(str(i), callback_data=f"cabins_{i}") for i in range(1, 6)]])
    try:
        query.edit_message_text("🚿 Скільки кабінок працювало?", reply_markup=keyboard)
    except Exception:
        pass


def cabins_feedback_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    data = query.data
    cabins = int(data.split("_")[1])
    update_status_row(cabins=cabins)
    # Після опитування — зсуваємо чергу і надсилаємо DM тому, хто тепер у душі
    next_user = shift_queue_and_notify(send_notification=True)
    try:
        query.edit_message_text("✅ Дякуємо за відгук! Статус душу оновлено.", reply_markup=minimal_back_markup())
    except Exception:
        pass


def shift_queue_and_notify(send_notification: bool = True):
    """
    Після завершення душу — зсуваємо чергу.
    Якщо send_notification=True -> надсилаємо DM користувачу, який став в душі.
    Повертає user_id наступного або None.
    """
    q = get_full_queue()
    if not q:
        log_event("Черга порожня після завершення душу.")
        return None
    in_shower = [r for r in q if r[3] == 1]
    if in_shower:
        return in_shower[0][1]
    first = q[0]
    set_first_in_shower_by_queue_id(first[0], send_notification=send_notification)
    notify_next_user()
    return first[1]


# ============================================
# Cancel queue handler
# ============================================
def cancel_queue_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    user_id = query.from_user.id
    was_in_shower = False
    q = get_full_queue()
    for r in q:
        if r[1] == user_id and r[3] == 1:
            was_in_shower = True
            break
    remove_queue_entry_by_user(user_id)
    try:
        query.edit_message_text("❌ Ви вийшли з черги.", reply_markup=main_menu_markup(vip=False, admin=(ADMIN_ID_INT == user_id)))
    except Exception:
        pass
    if was_in_shower:
        shift_queue_and_notify(send_notification=True)


# ============================================
# Status & View queue
# ============================================
def status_info_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    status = get_status_row()
    q = get_full_queue()
    conn = db_connect()
    c = conn.cursor()
    in_shower = [r for r in q if r[3] == 1]
    now_in_shower = "—"
    time_in_shower = "—"
    if in_shower:
        uid = in_shower[0][1]
        c.execute("SELECT name FROM users WHERE user_id=?", (uid,))
        r = c.fetchone()
        name = r[0] if r else f"UID {uid}"
        gsize = in_shower[0][2]
        group_str = f"(група-{gsize})" if gsize and gsize > 1 else "(один)"
        now_in_shower = f"{name} {group_str}"
        if in_shower[0][4]:
            try:
                start_time = datetime.strptime(in_shower[0][4], DATETIME_FORMAT)
                diff = now_local() - start_time
                minutes = diff.seconds // 60
                time_in_shower = format_minutes_as_text(minutes)
            except Exception:
                time_in_shower = "—"
    next_user = "—"
    if len(q) > 1:
        next_uid = q[1][1]
        c.execute("SELECT name FROM users WHERE user_id=?", (next_uid,))
        r = c.fetchone()
        next_user = r[0] if r else f"UID {next_uid}"
    conn.close()
    msg = (
        f"🚿 *Статус душу:*\n\n"
        f"Кабінок: {status[1]}\n"
        f"Вода: {status[2]}\n"
        f"Зараз у душі: {now_in_shower}\n"
        f"Час у душі: {time_in_shower}\n"
        f"Наступний: {next_user}\n"
        f"Останнє оновлення: {status[3]}"
    )
    try:
        query.edit_message_text(msg, parse_mode="Markdown", reply_markup=minimal_back_markup())
    except Exception:
        pass


def view_queue_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    q = get_full_queue()
    if not q:
        try:
            query.edit_message_text("🕓 Черга порожня.", reply_markup=minimal_back_markup())
        except Exception:
            pass
        return
    conn = db_connect()
    c = conn.cursor()
    lines = []
    for i, r in enumerate(q):
        uid = r[1]
        c.execute("SELECT name FROM users WHERE user_id=?", (uid,))
        row = c.fetchone()
        name = row[0] if row else f"UID {uid}"
        group_str = "(один)" if r[2] == 1 else f"(група-{r[2]})"
        status_str = "(миється)" if r[3] == 1 else ""
        lines.append(f"{i+1}. {name} {group_str} {status_str}")
    conn.close()
    msg = "📋 *Черга:*\n\n" + "\n".join(lines)
    try:
        query.edit_message_text(msg, parse_mode="Markdown", reply_markup=minimal_back_markup())
    except Exception:
        pass


# ============================================
# VIP: Стати передостаннім
# ============================================
def vip_join_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    user_id = query.from_user.id
    if not is_working_hours() and not (ADMIN_ID_INT is not None and user_id == ADMIN_ID_INT):
        return send_not_working_callback(query)
    user_row = get_user_row(user_id)
    if not user_row or not user_row[4]:
        try:
            query.edit_message_text("🚫 Ця функція доступна лише для VIP користувачів.")
        except Exception:
            pass
        return
    q = get_full_queue()
    if any(r[1] == user_id for r in q):
        try:
            query.edit_message_text("⚠️ Ви вже у черзі.")
        except Exception:
            pass
        return
    vip_positions = []
    for i, r in enumerate(q):
        uid = r[1]
        ur = get_user_row(uid)
        if ur and ur[4] == 1:
            vip_positions.append(i)
    insert_index = len(q)
    if vip_positions:
        insert_index = vip_positions[-1] + 1
    # Якщо черга пуста -> поводимося як перший (помітимо як в душі та edit повідомлення)
    if len(q) == 0:
        add_to_queue(user_id, group_size=1)
        conn = db_connect()
        c = conn.cursor()
        c.execute("SELECT id FROM queue WHERE user_id=? ORDER BY id DESC LIMIT 1", (user_id,))
        row = c.fetchone()
        conn.close()
        if row:
            queue_id = row[0]
            set_first_in_shower_by_queue_id(queue_id, send_notification=False)
            kb = in_shower_keyboard()
            try:
                query.edit_message_text("🚿 Ви зараз у душі!\nКоли закінчите — натисніть кнопку нижче.", reply_markup=kb)
            except Exception:
                pass
            update_status_row()
            notify_next_user()
            return
        else:
            try:
                query.edit_message_text("⚠️ Помилка при додаванні в чергу. Спробуйте ще раз.")
            except Exception:
                pass
            return
    # Додаємо і пересортуємо
    add_to_queue(user_id, group_size=1)
    reorder_queue_to_insert_at(user_id, insert_index)
    q_after = get_full_queue()
    pos = None
    queue_row_id = None
    for idx, r in enumerate(q_after):
        if r[1] == user_id:
            pos = idx + 1
            queue_row_id = r[0]
            break
    in_shower_list = [r for r in q_after if r[3] == 1]
    if pos == 1 and not in_shower_list:
        if queue_row_id is not None:
            set_first_in_shower_by_queue_id(queue_row_id, send_notification=False)
            kb = in_shower_keyboard()
            try:
                query.edit_message_text("🚿 Ви зараз у душі!\nКоли закінчите — натисніть кнопку нижче.", reply_markup=kb)
            except Exception:
                pass
            update_status_row()
            notify_next_user()
            return
    wait_min, position = calculate_wait_time_for_user(user_id)
    try:
        query.edit_message_text(
            f"⭐ Ви стали в чергу як VIP!\nВаш номер: {position}\nОрієнтовний час очікування: ~{format_minutes_as_text(wait_min)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Відмінити чергу", callback_data="cancel_queue")]])
        )
    except Exception:
        pass


def reorder_queue_to_insert_at(user_id: int, insert_index: int):
    conn = db_connect()
    c = conn.cursor()
    c.execute("SELECT user_id, group_size, in_shower, start_time, inserted_at FROM queue ORDER BY id ASC")
    rows_raw = c.fetchall()
    if not any(r[0] == user_id for r in rows_raw):
        conn.close()
        return
    moving_row = None
    remaining = []
    for r in rows_raw:
        if r[0] == user_id:
            moving_row = r
        else:
            remaining.append(list(r))
    if moving_row is None:
        conn.close()
        return
    if insert_index < 0:
        insert_index = 0
    if insert_index > len(remaining):
        insert_index = len(remaining)
    remaining.insert(insert_index, list(moving_row))
    c.execute("DELETE FROM queue")
    for r in remaining:
        uid, group_size, in_shower, start_time, inserted_at = r
        c.execute("INSERT INTO queue (user_id, group_size, in_shower, start_time, inserted_at) VALUES (?,?,?,?,?)", (uid, group_size, in_shower, start_time, inserted_at))
    conn.commit()
    conn.close()
    log_event(f"Черга пересортована: user {user_id} вставлений на позицію {insert_index + 1}")


# ============================================
# Admin menu and actions
# ============================================
def admin_menu_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    user_id = query.from_user.id
    if ADMIN_ID_INT is None or user_id != ADMIN_ID_INT:
        try:
            query.edit_message_text("🚫 У вас немає прав доступу.")
        except Exception:
            pass
        return
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Список користувачів", callback_data="admin_users")],
        [InlineKeyboardButton("❌ Видалити користувача", callback_data="admin_delete")],
        [InlineKeyboardButton("⭐ Зробити VIP", callback_data="admin_vip")],
        [InlineKeyboardButton("⚙️ Налаштування", callback_data="admin_settings")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_menu")],
    ])
    try:
        query.edit_message_text("⚙️ Адмін меню:", reply_markup=keyboard)
    except Exception:
        pass


def admin_list_users_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    conn = db_connect()
    c = conn.cursor()
    c.execute("SELECT name, gender, vip, user_id FROM users WHERE registered=1 ORDER BY vip DESC, name ASC")
    rows = c.fetchall()
    conn.close()
    if not rows:
        try:
            query.edit_message_text("❌ Немає зареєстрованих користувачів.", reply_markup=minimal_back_markup())
        except Exception:
            pass
        return
    msg = "👥 *Список користувачів:*\n\n"
    for i, r in enumerate(rows, 1):
        vip = "так" if r[2] == 1 else "ні"
        msg += f"{i}. {r[0]} (стать: {r[1]}) VIP:{vip} ID:{r[3]}\n"
    try:
        query.edit_message_text(msg, parse_mode="Markdown", reply_markup=minimal_back_markup())
    except Exception:
        pass


def admin_delete_user_start_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    query.edit_message_text("✏️ Введіть ім’я або ID користувача, якого потрібно видалити:")
    return ADMIN_WAIT_DELETE


def admin_delete_user_finish_handler(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    user_id = None
    conn = db_connect()
    c = conn.cursor()
    if text.isdigit():
        c.execute("SELECT user_id FROM users WHERE user_id=?", (int(text),))
        r = c.fetchone()
        if r:
            user_id = r[0]
    else:
        c.execute("SELECT user_id FROM users WHERE name LIKE ?", (text,))
        r = c.fetchone()
        if r:
            user_id = r[0]
    if not user_id:
        update.message.reply_text("❌ Користувача не знайдено.")
        conn.close()
        return ConversationHandler.END
    c.execute("DELETE FROM users WHERE user_id=?", (user_id,))
    c.execute("DELETE FROM queue WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()
    update.message.reply_text("✅ Користувача видалено з бази.")
    shift_queue_and_notify(send_notification=True)
    return ConversationHandler.END


def admin_make_vip_start_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    query.edit_message_text("⭐ Введіть ім’я або ID користувача, щоб зробити його VIP:")
    return ADMIN_WAIT_VIP


def admin_make_vip_finish_handler(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    user_id = None
    conn = db_connect()
    c = conn.cursor()
    if text.isdigit():
        c.execute("SELECT user_id FROM users WHERE user_id=?", (int(text),))
        r = c.fetchone()
        if r:
            user_id = r[0]
    else:
        c.execute("SELECT user_id FROM users WHERE name LIKE ?", (text,))
        r = c.fetchone()
        if r:
            user_id = r[0]
    if not user_id:
        update.message.reply_text("❌ Користувача не знайдено.")
        conn.close()
        return ConversationHandler.END
    c.execute("UPDATE users SET vip=1 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()
    update.message.reply_text("✅ Користувачу присвоєно статус VIP.")
    return ConversationHandler.END


# ============================================
# Admin settings
# ============================================
def admin_settings_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    user_id = query.from_user.id
    if ADMIN_ID_INT is None or user_id != ADMIN_ID_INT:
        try:
            query.edit_message_text("🚫 У вас немає прав доступу.")
        except Exception:
            pass
        return
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏱ Середній час (хлопці/дівчата)", callback_data="admin_avg_times")],
        [InlineKeyboardButton("🕒 Робочі години", callback_data="admin_working_hours")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="admin_menu")],
    ])
    try:
        query.edit_message_text("⚙️ Налаштування адміна:", reply_markup=keyboard)
    except Exception:
        pass


def admin_avg_times_start(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    avg = get_setting("avg_times")
    if not avg:
        avg = {"male_min": 10, "male_max": 15, "female_min": 15, "female_max": 20}
    msg = (
        "Поточні середні часи (хвилин):\n"
        f"Хлопці: {avg.get('male_min')} - {avg.get('male_max')}\n"
        f"Дівчата: {avg.get('female_min')} - {avg.get('female_max')}\n\n"
        "Введіть нові значення у форматі: male_min,male_max,female_min,female_max\n"
        "Наприклад: 10,15,15,20"
    )
    try:
        query.edit_message_text(msg)
    except Exception:
        pass
    return ADMIN_WAIT_MAKE_SETTINGS


def admin_avg_times_finish(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    parts = [p.strip() for p in text.split(",")]
    if len(parts) != 4 or not all(p.isdigit() for p in parts):
        update.message.reply_text("Невірний формат. Спробуйте ще раз.")
        return ConversationHandler.END
    male_min, male_max, female_min, female_max = map(int, parts)
    set_setting("avg_times", {"male_min": male_min, "male_max": male_max, "female_min": female_min, "female_max": female_max})
    update.message.reply_text("✅ Значення середніх часів оновлено.")
    return ConversationHandler.END


def admin_working_hours_start(update: Update, context: CallbackContext):
    query = update.callback_query
    safe_answer(query)
    working = get_setting("working_hours")
    if not working:
        working = {}
    msg = "Поточні робочі години (JSON формат):\n" + json.dumps(working, indent=2, ensure_ascii=False) + "\n\n" \
          "Відправте нові робочі години в JSON форматі, де ключі — дні тижня (0=Понеділок ... 6=Неділя), значення — список періодів [start_hour,end_hour].\n" \
          "Наприклад:\n" \
          '{"0": [[10,14],[17,21]], "6": [[0,24]]}\n' \
          "Будьте уважні з форматом!"
    try:
        query.edit_message_text(msg)
    except Exception:
        pass
    return ADMIN_WAIT_MAKE_SETTINGS


def admin_working_hours_finish(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    try:
        parsed = json.loads(text)
        for k, v in parsed.items():
            if not k.isdigit():
                raise ValueError("ключі повинні бути числами")
            if not isinstance(v, list):
                raise ValueError("значення повинні бути списками")
        set_setting("working_hours", parsed)
        update.message.reply_text("✅ Робочі години оновлено.")
    except Exception as e:
        update.message.reply_text("❌ Помилка в форматі JSON: " + str(e))
    return ConversationHandler.END


# ============================================
# Backup / DB check
# ============================================
def backup_database():
    try:
        ensure_dirs()
        timestamp = now_local().strftime("%Y%m%d_%H%M%S")
        backup_name = os.path.join(BACKUP_DIR, f"backup_{timestamp}.db")
        shutil.copy(DB_PATH, backup_name)
        log_event(f"📦 Резервна копія БД створена: {backup_name}")
        return backup_name
    except Exception as e:
        log_event(f"Не вдалося створити резервну копію: {e}")
        return None


def check_database_integrity():
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users")
        c.execute("SELECT COUNT(*) FROM queue")
        c.execute("SELECT COUNT(*) FROM status")
        conn.close()
        log_event("✅ Перевірка бази даних пройшла.")
    except Exception as e:
        log_event(f"❌ Помилка бази даних: {e}")


# ============================================
# job_update_status
# ============================================
def job_update_status(context: CallbackContext):
    try:
        log_event("Запуск job_update_status")
        # Якщо поза робочим часом — нічого не робимо
        if not is_working_hours():
            log_event("Поза робочим часом — job_update_status нічого не робить.")
            return
        current_in_shower = [r for r in get_full_queue() if r[3] == 1]
        if not current_in_shower:
            first_id = shift_queue_if_needed(send_notification=True)
            if first_id:
                log_event(f"Авто-запуск: поставлено в душ user={first_id}")
        q = get_full_queue()
        for r in q:
            uid = r[1]
            wait_min, pos = calculate_wait_time_for_user(uid)
            try:
                if pos > 1 and wait_min <= 5:
                    context.bot.send_message(uid, f"🔔 Нагадування: ваш орієнтовний час очікування ~{format_minutes_as_text(wait_min)}. Будьте готові.")
            except Exception:
                pass
    except Exception as e:
        log_event(f"Помилка в job_update_status: {traceback.format_exc()}")


# ============================================
# Button router
# ============================================
def button_router(update: Update, context: CallbackContext):
    query = update.callback_query
    data = query.data
    try:
        if data == "join_queue":
            return join_queue_handler(update, context)
        elif data == "solo":
            return solo_selected_handler(update, context)
        elif data == "group":
            return group_selected_handler(update, context)
        elif data.startswith("group_"):
            return handle_group_size_handler(update, context)
        elif data == "finish_shower":
            return finish_shower_handler(update, context)
        elif data in ("water_warm", "water_lukewarm", "water_cold"):
            return water_feedback_handler(update, context)
        elif data.startswith("cabins_"):
            return cabins_feedback_handler(update, context)
        elif data == "cancel_queue":
            return cancel_queue_handler(update, context)
        elif data == "status":
            return status_info_handler(update, context)
        elif data == "view_queue":
            return view_queue_handler(update, context)
        elif data == "back_to_menu":
            return back_to_menu_handler(update, context)
        elif data == "admin_menu":
            return admin_menu_handler(update, context)
        elif data == "admin_users":
            return admin_list_users_handler(update, context)
        elif data == "admin_delete":
            return admin_delete_user_start_handler(update, context)
        elif data == "admin_vip":
            return admin_make_vip_start_handler(update, context)
        elif data == "vip_join":
            return vip_join_handler(update, context)
        elif data == "admin_settings":
            return admin_settings_handler(update, context)
        elif data == "admin_avg_times":
            return admin_avg_times_start(update, context)
        elif data == "admin_working_hours":
            return admin_working_hours_start(update, context)
        else:
            safe_answer(query, show_alert=True, text="Невідома команда")
    except Exception as e:
        log_event(f"⚠️ Помилка button_router: {traceback.format_exc()}")
        try:
            safe_answer(query, show_alert=True, text="Помилка. Перевірте лог.")
        except Exception:
            pass


# ============================================
# Text handlers and admin commands
# ============================================
def unknown_text_handler(update: Update, context: CallbackContext):
    update.message.reply_text("❓ Невідома команда. Скористайтесь меню або натисніть /start.")


def cmd_backup(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if ADMIN_ID_INT is None or user_id != ADMIN_ID_INT:
        update.message.reply_text("🚫 У вас немає прав доступу.")
        return
    backup = backup_database()
    if backup:
        update.message.reply_text(f"✅ Резервну копію створено: {backup}")
    else:
        update.message.reply_text("❌ Не вдалося створити резервну копію. Перевірте логи.")


def cmd_stats(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if ADMIN_ID_INT is None or user_id != ADMIN_ID_INT:
        update.message.reply_text("🚫 У вас немає прав доступу.")
        return
    conn = db_connect()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM users")
    total_users = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM queue")
    total_in_queue = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM users WHERE vip=1")
    vip_users = c.fetchone()[0]
    conn.close()
    status = get_status_row()
    cabins = status[1] if status else "—"
    water = status[2] if status else "—"
    stats = (
        f"📊 Статистика бота\n\n"
        f"👥 Зареєстровано користувачів: {total_users}\n"
        f"🏆 VIP користувачів: {vip_users}\n"
        f"🚿 У черзі зараз: {total_in_queue}\n"
        f"🧰 Робочих кабінок: {cabins}\n"
        f"💧 Вода: {water}"
    )
    update.message.reply_text(stats)


# ============================================
# Diagnostic command: nowinfo
# ============================================
def nowinfo_cmd(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    local_system = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    auto_offset = AUTO_TZ_OFFSET_HOURS
    nowloc = now_local().strftime("%Y-%m-%d %H:%M:%S")
    working = is_working_hours()
    text = (
        f"UTC (datetime.utcnow()): {utc}\n"
        f"System local (datetime.now()): {local_system}\n"
        f"AUTO_TZ_OFFSET_HOURS: {auto_offset}\n"
        f"now_local() (UTC + offset): {nowloc}\n"
        f"is_working_hours(): {working}\n\n"
        "Якщо now_local() не відповідає вашому київському часу — налаштуйте серверний час або повідомте мені."
    )
    update.message.reply_text(text)


# ============================================
# Main
# ============================================
def main():
    print("🚀 Запуск бота 'Онлайн черга в душ'...")
    log_event("Старт бота")
    init_db()
    check_database_integrity()
    try:
        log_event(f"Локальний час для перевірки (now_local): {now_local().strftime(DATETIME_FORMAT)}, is_working={is_working_hours()}")
    except Exception:
        pass

    if TOKEN == "Вставте токен тут":
        print("⚠️ УВАГА: Ви не вставили токен бота! Відредагуйте змінну TOKEN у файлі bot.py.")
        log_event("Токен не вставлено. Завершення.")
        return
    try:
        updater = Updater(TOKEN, use_context=True)
    except Exception as e:
        log_event(f"Не вдалось підключитись до Telegram: {e}")
        raise
    dp = updater.dispatcher

    register_conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            REGISTER_NAME: [MessageHandler(Filters.text & ~Filters.command, register_name_handler)],
            REGISTER_GENDER: [CallbackQueryHandler(register_gender_handler, pattern="^gender_")],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
        allow_reentry=True,
    )

    admin_delete_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_delete_user_start_handler, pattern="^admin_delete$")],
        states={ADMIN_WAIT_DELETE: [MessageHandler(Filters.text & ~Filters.command, admin_delete_user_finish_handler)]},
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
    )

    admin_vip_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_make_vip_start_handler, pattern="^admin_vip$")],
        states={ADMIN_WAIT_VIP: [MessageHandler(Filters.text & ~Filters.command, admin_make_vip_finish_handler)]},
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
    )

    admin_settings_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(admin_avg_times_start, pattern="^admin_avg_times$"),
            CallbackQueryHandler(admin_working_hours_start, pattern="^admin_working_hours$"),
        ],
        states={ADMIN_WAIT_MAKE_SETTINGS: [MessageHandler(Filters.text & ~Filters.command, admin_avg_times_finish)]},
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
    )

    dp.add_handler(register_conv)
    dp.add_handler(admin_delete_conv)
    dp.add_handler(admin_vip_conv)
    dp.add_handler(CallbackQueryHandler(button_router))
    dp.add_handler(CommandHandler("help", help_command))
    dp.add_handler(CommandHandler("backup", cmd_backup))
    dp.add_handler(CommandHandler("stats", cmd_stats))
    dp.add_handler(CommandHandler("nowinfo", nowinfo_cmd))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, unknown_text_handler))
    dp.add_handler(MessageHandler(Filters.command, unknown_text_handler))

    job_queue = updater.job_queue
    job_queue.run_repeating(job_update_status, interval=30, first=10)

    updater.start_polling()
    log_event("✅ Бот запущено та очікує повідомлень...")
    print("✅ Бот запущено. Натисніть Ctrl+C щоб зупинити.")
    updater.idle()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log_event("🛑 Бот зупинено вручну (KeyboardInterrupt).")
        print("🛑 Бот зупинено.")
    except Exception as e:
        log_event(f"❌ Критична помилка: {traceback.format_exc()}")
        print(f"❌ Критична помилка: {e}")

# ============================================
# Кінець файлу
# ============================================
