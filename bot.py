# -*- coding: utf-8 -*-
"""
TEST CENTER BOT — Aiogram v3 (Single-file, FULL)
Features:
- Super admin + permissioned admins
- Groups: members, settings (tg chat id, kick limits), group tests inline
- Tests: create, assign (public or multi-group), pause/resume/finish, global rating (text+pdf)
- Results: user submit (no SMS), group manual results, import results (from DB) + optional notify
- Attendance: daily mark (X only), archive, send DM to absent users, attendance PDF with group name
- Tasks: create with deadline + points + optional media, publish alerts, students submit any media, one submission, admin grades, auto-kick for missed tasks
- Global broadcast supports media
"""

import asyncio
import json
import logging
import os
import time
import shutil
import zipfile
import random
import re
import sqlite3
import string
import html
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List, Tuple


def row_get(row, key, default=None):
    """Safe getter for sqlite3.Row / dict / objects."""
    try:
        if row is None:
            return default
        if isinstance(row, dict):
            return row.get(key, default)
        # sqlite3.Row supports mapping interface
        if hasattr(row, "keys"):
            return row[key] if key in row.keys() else default
        # fallback: attribute access
        return getattr(row, key, default)
    except Exception:
        return default


from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# ---- PDF (fpdf) ----
from fpdf import FPDF
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

UZ_TZ = ZoneInfo("Asia/Tashkent")

def to_uz_time_str(dt_value) -> str:
    """Convert DB datetime (str/datetime) to Uzbekistan time string YYYY-MM-DD HH:MM."""
    if dt_value is None:
        return ""
    if isinstance(dt_value, datetime):
        dt = dt_value
    else:
        s = str(dt_value).strip()
        if not s:
            return ""
        s2 = s.replace(" ", "T")
        try:
            dt = datetime.fromisoformat(s2)
        except Exception:
            try:
                dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                return s
    if dt.tzinfo is None:
        # store naive timestamps as UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(UZ_TZ).strftime("%Y-%m-%d %H:%M")


# =========================
# CONFIG (yours; can be fake)
# =========================
API_TOKEN = os.getenv("BOT_TOKEN", "")  # set in hosting env
# fallback for local dev (optional): set BOT_TOKEN in env
SUPER_ADMIN_ID = 7880323063
DB_NAME = os.getenv("DB_PATH", "test_educenter.db")
# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)

# =========================
# ROUTER / DISPATCHER
# =========================
router = Router()
dp = Dispatcher(storage=MemoryStorage())

# =========================
# Helpers
# =========================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")

def escape_html(s: object) -> str:
    """Escape text for HTML parse mode."""
    return html.escape(str(s), quote=False)

def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def parse_dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M")


def pdf_safe(s: str) -> str:
    """Make text safe for FPDF (latin-1)."""
    if s is None:
        return ""
    s = str(s)
    repl = {
        "’": "'", "‘": "'", "ʼ": "'", "ʻ": "'",
        "“": '"', "”": '"',
        "–": "-", "—": "-",
        "•": "-",
        "о‘": "o'", "g‘": "g'", "O‘": "O'", "G‘": "G'",
        "ў": "u'", "Ў": "U'",
    }
    for a, b in repl.items():
        s = s.replace(a, b)
    return s.encode("latin-1", "replace").decode("latin-1")

def gen_test_id_5() -> str:
    return str(random.randint(10000, 99999))

def gen_group_code() -> str:
    # 4 digits + 2 letters from A..H (user requested)
    nums = f"{random.randint(1000, 9999)}"
    letters = random.choice("ABCDEFGH") + random.choice("ABCDEFGH")
    return nums + letters

def safe_int(x, default=None):
    try:
        return int(x)
    except:
        return default

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_attendance_schema(conn: sqlite3.Connection) -> None:
    """Ensure attendance tables exist (safe to call often). Helps after DB restore/migrations."""
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS attendance_archive(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER,
        user_id INTEGER,
        att_date TEXT,
        status TEXT,
        note TEXT,
        created_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS attendance_days(
        group_id INTEGER,
        att_date TEXT,
        saved_at TEXT,
        saved_by INTEGER,
        PRIMARY KEY(group_id, att_date)
    )""")
    conn.commit()



def log_admin(admin_id: int, action: str, payload: dict | None = None) -> None:
    """Best-effort admin audit logger. Never raises."""
    try:
        with db() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS admin_logs (
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       ts TEXT,
                       admin_id INTEGER,
                       action TEXT,
                       payload TEXT
                   )"""
            )
            conn.execute(
                "INSERT INTO admin_logs (ts, admin_id, action, payload) VALUES (?,?,?,?)",
                (
                    datetime.utcnow().isoformat(timespec="seconds"),
                    int(admin_id),
                    str(action),
                    json.dumps(payload or {}, ensure_ascii=False),
                ),
            )
            conn.commit()
    except Exception:
        # Do not break main flow on logging failures
        return

def safe_pdf_text(s: str) -> str:
    """
    fpdf default fonts can break on non-latin-1 chars.
    We normalize & replace problematic chars to avoid latin-1 errors.
    """
    if s is None:
        return ""
    s = str(s)
    # Replace Uzbek apostrophes variants and quotes
    s = s.replace("ʻ", "'").replace("’", "'").replace("`", "'")
    s = s.replace("“", '"').replace("”", '"')
    # Remove any remaining non-latin-1
    s2 = []
    for ch in s:
        if ord(ch) <= 255:
            s2.append(ch)
        else:
            s2.append("?")
    return "".join(s2)

async def safe_edit(call: CallbackQuery, text: str, kb: InlineKeyboardMarkup):
    """
    Avoid TelegramBadRequest: message is not modified
    """
    try:
        if call.message and (call.message.text == text) and (call.message.reply_markup == kb):
            await call.answer()
            return
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_text(text, reply_markup=kb)
        except Exception:
            try:
                await call.message.answer(text, reply_markup=kb)
            except:
                pass

def kb_home_user() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")]
    ])

def kb_home_admin(uid: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_std_nav(is_admin: bool) -> InlineKeyboardMarkup:
    """Standard navigation: Back + Menu on one line."""
    if is_admin:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Ortga", callback_data="a:back"),
             InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")]
        ])
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data="u:back"),
         InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")]
    ])


def kb_back_home(back_cb: str = "a:home") -> InlineKeyboardMarkup:
    """Inline navigation: Back + Menu on one line (admin callbacks)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=back_cb),
         InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])

# =========================
# DB INIT / MIGRATIONS
# =========================

def migrate_task_submissions_columns(conn: sqlite3.Connection) -> None:
    """Backwards-compatible migration for legacy task_submissions schema."""
    c = conn.cursor()
    # If table doesn't exist yet, skip (it will be created in init_db).
    cols = [r[1] for r in c.execute("PRAGMA table_info(task_submissions)").fetchall()]
    if not cols:
        return
    if "content_type" not in cols:
        c.execute("ALTER TABLE task_submissions ADD COLUMN content_type TEXT")
    if "file_id" not in cols:
        c.execute("ALTER TABLE task_submissions ADD COLUMN file_id TEXT")
    if "text" not in cols:
        c.execute("ALTER TABLE task_submissions ADD COLUMN text TEXT")
    if "graded_at" not in cols:
        c.execute("ALTER TABLE task_submissions ADD COLUMN graded_at TEXT")
    if "graded_by" not in cols:
        c.execute("ALTER TABLE task_submissions ADD COLUMN graded_by INTEGER")




def ensure_attendance_schema(conn) -> None:
    """Ensure attendance tables exist for older DBs or after restore."""
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS attendance_archive(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER,
        user_id INTEGER,
        att_date TEXT,
        status TEXT,
        note TEXT,
        created_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS attendance_days(
        group_id INTEGER,
        att_date TEXT,
        saved_at TEXT,
        saved_by INTEGER,
        PRIMARY KEY(group_id, att_date)
    )""")
    conn.commit()

def init_db() -> None:
    conn = db()
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            full_name TEXT,
            created_at TEXT
        )""")

    c.execute("""CREATE TABLE IF NOT EXISTS admins(
            user_id INTEGER PRIMARY KEY,
            role TEXT,
            added_at TEXT
        )""")

    c.execute("""CREATE TABLE IF NOT EXISTS admin_permissions(
            admin_id INTEGER,
            perm TEXT,
            enabled INTEGER DEFAULT 0,
            UNIQUE(admin_id, perm)
        )""")

    c.execute("""CREATE TABLE IF NOT EXISTS groups(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            invite_code TEXT UNIQUE,
            tg_chat_id INTEGER,
            att_absent_limit INTEGER DEFAULT 5,
            task_miss_limit INTEGER DEFAULT 5
        )""")

    c.execute("""CREATE TABLE IF NOT EXISTS members(
            group_id INTEGER,
            user_id INTEGER,
            UNIQUE(group_id, user_id)
        )""")

    c.execute("""CREATE TABLE IF NOT EXISTS tests(
            test_id TEXT PRIMARY KEY,
            keys TEXT,
            status TEXT,
            deadline TEXT,
            created_at TEXT,
            is_public INTEGER DEFAULT 0
        )""")

    c.execute("""CREATE TABLE IF NOT EXISTS test_groups(
            test_id TEXT,
            group_id INTEGER,
            UNIQUE(test_id, group_id)
        )""")

    c.execute("""CREATE TABLE IF NOT EXISTS results(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            test_id TEXT,
            score INTEGER,
            total INTEGER,
            percent REAL,
            date TEXT,
            full_name TEXT
        )""")

    c.execute("""CREATE TABLE IF NOT EXISTS submissions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            test_id TEXT,
            answers TEXT,
            submitted_at TEXT,
            UNIQUE(user_id, test_id)
        )""")

    # Attendance: store only absent explicitly; missing row => present
    c.execute("""CREATE TABLE IF NOT EXISTS attendance(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER,
            user_id INTEGER,
            att_date TEXT,
            status TEXT,
            UNIQUE(group_id, user_id, att_date)
        )""")

    # Attendance days archive (a day is considered "saved/finalized" when admin presses Save/Report/DM)
    c.execute("""CREATE TABLE IF NOT EXISTS attendance_days(
        group_id INTEGER,
        att_date TEXT,
        saved_at TEXT,
        saved_by INTEGER,
        PRIMARY KEY(group_id, att_date)
    )""")
    # Counters
    c.execute("""CREATE TABLE IF NOT EXISTS counters(
            group_id INTEGER,
            user_id INTEGER,
            absent_count INTEGER DEFAULT 0,
            missed_task_count INTEGER DEFAULT 0,
            UNIQUE(group_id, user_id)
        )""")

    # Tasks
    c.execute("""CREATE TABLE IF NOT EXISTS tasks(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER,
            title TEXT,
            description TEXT,
            points INTEGER,
            due_at TEXT,
            created_at TEXT,
            status TEXT DEFAULT 'draft'  -- draft/published/closed
        )""")

    c.execute("""CREATE TABLE IF NOT EXISTS task_media(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            file_type TEXT,
            file_id TEXT
        )""")

    c.execute("""CREATE TABLE IF NOT EXISTS task_submissions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            user_id INTEGER,
            full_name TEXT,
            submitted_at TEXT,
            msg_json TEXT,
            score INTEGER,
            feedback TEXT,
            content_type TEXT,
            file_id TEXT,
            text TEXT,
            graded_at TEXT,
            graded_by INTEGER,
            UNIQUE(task_id, user_id)
        )""")

    # Apply migrations for legacy DBs
    migrate_task_submissions_columns(conn)

    # Ensure super admin
    c.execute(
        "INSERT OR IGNORE INTO admins(user_id, role, added_at) VALUES (?,?,?)",
        (SUPER_ADMIN_ID, "super", now_str()),
    )

    conn.commit()
    conn.close()


init_db()

# =========================
# PERMISSIONS
# =========================
PERMS = [
    ("groups", "Guruhlar"),
    ("tests", "Testlar"),
    ("broadcast", "Xabar"),
    ("attendance", "Davomat"),
    ("results", "Natijalar"),
    ("tasks", "Vazifalar"),
    ("admins", "Adminlar"),
]

def is_super(uid: int) -> bool:
    return uid == SUPER_ADMIN_ID

def is_admin(uid: int) -> bool:
    conn = db()
    r = conn.execute("SELECT role FROM admins WHERE user_id=?", (uid,)).fetchone()
    conn.close()
    return r is not None

def has_perm(uid: int, perm: str) -> bool:
    if is_super(uid):
        return True
    conn = db()
    r = conn.execute("SELECT enabled FROM admin_permissions WHERE admin_id=? AND perm=?", (uid, perm)).fetchone()
    conn.close()
    return bool(r and int(r["enabled"]) == 1)

async def guard(call: CallbackQuery, perm: Optional[str] = None) -> bool:
    uid = call.from_user.id
    if not is_admin(uid):
        await call.answer("Ruxsat yo‘q.", show_alert=True)
        return False
    if perm and not has_perm(uid, perm):
        await call.answer("Bu funksiya siz uchun yopiq.", show_alert=True)
        return False
    return True


# backward-compat wrapper (some handlers call guard_call)
async def guard_call(call: CallbackQuery, perm: Optional[str] = None) -> bool:
    return await guard(call, perm)


# message-compat wrapper (some handlers call guard_msg)
async def guard_msg(message, perm: Optional[str] = None) -> bool:
    """Permission guard for message-based admin actions."""
    uid = getattr(getattr(message, "from_user", None), "id", 0) or 0
    if not is_admin(uid):
        try:
            await message.reply("Ruxsat yo‘q.")
        except Exception:
            pass
        return False
    if perm and not has_perm(uid, perm):
        try:
            await message.reply("Bu funksiya siz uchun yopiq.")
        except Exception:
            pass
        return False
    return True



def get_all_admin_ids() -> List[int]:
    """Return all admin user IDs including SUPER_ADMIN_ID."""
    ids: List[int] = []
    try:
        with db() as conn:
            rows = conn.execute("SELECT user_id FROM admins").fetchall()
            ids = [int(r["user_id"]) for r in rows if r and r["user_id"] is not None]
    except Exception:
        ids = []
    if SUPER_ADMIN_ID not in ids:
        ids.insert(0, int(SUPER_ADMIN_ID))
    # de-dup while preserving order
    seen = set()
    out: List[int] = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


def make_db_snapshot_zip() -> Tuple[str, str]:
    """Create a consistent sqlite snapshot and return (zip_path, caption). Raises on failure."""
    db_path = os.path.abspath(DB_NAME)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB topilmadi: {db_path}")

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    snap_path = f"/tmp/backup_{ts}_{os.path.basename(db_path)}"
    zip_path = f"/tmp/backup_{ts}_{os.path.basename(db_path)}.zip"

    # create snapshot
    src = sqlite3.connect(db_path)
    try:
        dst = sqlite3.connect(snap_path)
        try:
            src.backup(dst)
            dst.commit()
        finally:
            dst.close()
    finally:
        src.close()

    # zip it (often smaller + safer)
    import zipfile
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(snap_path, arcname=os.path.basename(db_path))

    try:
        os.remove(snap_path)
    except Exception:
        pass

    size_mb = os.path.getsize(zip_path) / (1024 * 1024)
    caption = (
        f"✅ DB backup: <b>{escape_html(os.path.basename(db_path))}</b>\n"
        f"📦 ZIP hajm: <b>{size_mb:.1f} MB</b>\n"
        f"🕒 {escape_html(now_str())}"
    )
    return zip_path, caption


async def send_db_backup_to_admins(bot: Bot, reason: str = "scheduled"):
    """Send DB backup to all admins (DM). Never raises."""
    try:
        zip_path, caption = make_db_snapshot_zip()
    except Exception as e:
        # if snapshot failed, notify super admin only
        try:
            await bot.send_message(int(SUPER_ADMIN_ID), f"❌ DB backup xatolik ({escape_html(reason)}): <code>{escape_html(e)}</code>")
        except Exception:
            pass
        return

    # Telegram bot file size limits exist; try anyway, but warn if huge.
    try:
        size_mb = os.path.getsize(zip_path) / (1024 * 1024)
        if size_mb > 45:
            warn = f"⚠️ Backup fayl juda katta: <b>{size_mb:.1f} MB</b>. Telegram limitiga urilishi mumkin."
        else:
            warn = ""
    except Exception:
        warn = ""

    ids = get_all_admin_ids()
    for uid in ids:
        try:
            await bot.send_document(
                chat_id=int(uid),
                document=FSInputFile(zip_path, filename=os.path.basename(zip_path)),
                caption=(caption + (("\n\n" + warn) if warn else ""))
            )
        except Exception:
            # ignore per-admin failures (blocked bot, etc.)
            pass

    try:
        os.remove(zip_path)
    except Exception:
        pass


def seconds_until_next_backup(hour: int = 6, minute: int = 0, tz_name: str = "Asia/Samarkand") -> int:
    """Seconds until next scheduled time in given timezone."""
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return max(1, int((target - now).total_seconds()))


# =========================
# ADMIN: DB BACKUP (download SQLite file)
# =========================
@router.message(Command("backup_db"))
async def cmd_backup_db(message: Message):
    """Send current SQLite DB backup (zip) to admin as a document."""
    if not await guard_msg(message, "admins"):
        return
    try:
        zip_path, caption = make_db_snapshot_zip()
    except Exception as e:
        await message.reply(f"❌ Backup qilishda xatolik: <code>{escape_html(e)}</code>")
        return

    # Prefer sending to admin private chat (safer), fallback to current chat
    target_chat_id = message.from_user.id
    try:
        await message.bot.send_document(
            chat_id=target_chat_id,
            document=FSInputFile(zip_path, filename=os.path.basename(zip_path)),
            caption=caption
        )
        if message.chat.id != target_chat_id:
            await message.reply("✅ Backup shaxsiy chatga yuborildi (DM).")
    except Exception:
        try:
            await message.answer_document(
                document=FSInputFile(zip_path, filename=os.path.basename(zip_path)),
                caption=caption
            )
        except Exception as e:
            await message.reply(f"❌ Fayl yuborilmadi: <code>{escape_html(e)}</code>")
    finally:
        try:
            if os.path.exists(zip_path):
                os.remove(zip_path)
        except Exception:
            pass


# =========================
# ADMIN: DB RESTORE (upload SQLite file)
# =========================
def _is_sqlite_file(p: str) -> bool:
    try:
        with open(p, "rb") as f:
            head = f.read(16)
        return head.startswith(b"SQLite format 3\x00")
    except Exception:
        return False


def _restore_db_from_path(src_path: str) -> str:
    """Restore DB from .db or .zip containing a .db. Returns restored db filename."""
    db_path = os.path.abspath(DB_NAME)

    tmp_db = None
    cleanup = []

    # If zip: extract first *.db
    if src_path.lower().endswith(".zip"):
        with zipfile.ZipFile(src_path, "r") as z:
            cand = [n for n in z.namelist() if n.lower().endswith(".db")]
            if not cand:
                raise ValueError("ZIP ichida .db topilmadi.")
            name = cand[0]
            tmp_db = f"/tmp/restore_{int(time.time())}_{os.path.basename(name)}"
            z.extract(name, "/tmp")
            extracted = os.path.join("/tmp", name)
            # zip may contain dirs
            if os.path.isdir(extracted):
                raise ValueError("ZIP format noto‘g‘ri.")
            os.replace(extracted, tmp_db)
            cleanup.append(tmp_db)
    else:
        tmp_db = src_path

    if not _is_sqlite_file(tmp_db):
        raise ValueError("Bu fayl SQLite DB emas (header mos emas).")

    # Replace atomically
    new_path = db_path + ".new"
    shutil.copyfile(tmp_db, new_path)
    os.replace(new_path, db_path)

    for p in cleanup:
        try:
            os.remove(p)
        except Exception:
            pass
    return os.path.basename(db_path)


# --- Restore FSM state (must be defined before handlers) ---
class RestoreState(StatesGroup):
    waiting_file = State()


@router.message(Command("restore_db"))
async def cmd_restore_db(message: Message, state: FSMContext):
    """Ask admin to upload a .zip/.db to restore."""
    if not await guard_msg(message, "admins"):
        return
    await state.set_state(RestoreState.waiting_file)
    await message.reply(
        "♻️ <b>DB Restore</b>\n"
        "Menga <b>.zip</b> (ichida .db) yoki to‘g‘ridan-to‘g‘ri <b>.db</b> fayl yuboring.\n"
        "⚠️ Bu amaliyot mavjud bazani <b>butunlay almashtiradi</b>.\n"
        "Bekor qilish: /cancel"
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    cur = await state.get_state()
    if cur == RestoreState.waiting_file.state:
        await state.clear()
        await message.reply("✅ Bekor qilindi.")
        return


@router.message(RestoreState.waiting_file, F.document)
async def restore_db_document(message: Message, state: FSMContext):
    if not await guard_msg(message, "admins"):
        return

    # Safety: backup current DB before restore
    try:
        pre_zip, pre_cap = make_db_snapshot_zip()
        try:
            await message.bot.send_document(
                chat_id=message.from_user.id,
                document=FSInputFile(pre_zip, filename=os.path.basename(pre_zip)),
                caption=(pre_cap + "\n\n⚠️ Restore’dan oldingi avtomatik backup (before_restore).")
            )
        finally:
            try:
                if os.path.exists(pre_zip):
                    os.remove(pre_zip)
            except Exception:
                pass
    except Exception:
        # If backup fails, continue but warn
        try:
            await message.reply("⚠️ Restore’dan oldin backup olishda xatolik bo‘ldi, lekin davom etaman.")
        except Exception:
            pass

    doc = message.document
    fname = (doc.file_name or "").lower()
    if not (fname.endswith(".db") or fname.endswith(".zip")):
        await message.reply("❌ Faqat .db yoki .zip yuboring.")
        return

    tmp_path = f"/tmp/upload_{int(time.time())}_{doc.file_unique_id}_{os.path.basename(doc.file_name or 'db.zip')}"
    try:
        # aiogram v3 download helper
        await message.bot.download(doc, destination=tmp_path)
    except Exception as e:
        await message.reply(f"❌ Faylni yuklab bo‘lmadi: <code>{escape_html(e)}</code>")
        return

    try:
        restored = _restore_db_from_path(tmp_path)
    except Exception as e:
        await message.reply(f"❌ Restore xatolik: <code>{escape_html(e)}</code>")
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

    await state.clear()
    await message.reply(
        "✅ <b>DB tiklandi.</b>\n"
        f"Fayl: <code>{escape_html(restored)}</code>\n"
        "🔄 Endi hostingda <b>Restart</b> qiling (yoki servisni qayta ishga tushiring), shunda bot yangi DB bilan ishlaydi."
    )

def ensure_user(uid: int, name: str):
    conn = db()
    row = conn.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone()
    if not row:
        conn.execute("INSERT INTO users(user_id, full_name, created_at) VALUES (?,?,?)",
                     (uid, name, now_str()))
    else:
        # keep name if exists
        pass
    conn.commit()
    conn.close()

def get_user_name(uid: int) -> str:
    conn = db()
    r = conn.execute("SELECT full_name FROM users WHERE user_id=?", (uid,)).fetchone()
    conn.close()
    return r["full_name"] if r and r["full_name"] else str(uid)

def ensure_deadline(test_id: str) -> Tuple[Optional[str], Optional[str]]:
    conn = db()
    r = conn.execute("SELECT status, deadline FROM tests WHERE test_id=?", (test_id,)).fetchone()
    if not r:
        conn.close()
        return None, None
    status, deadline = r["status"], r["deadline"]
    try:
        if deadline and status != "finished":
            if datetime.now() >= parse_dt(deadline):
                conn.execute("UPDATE tests SET status='finished' WHERE test_id=?", (test_id,))
                conn.commit()
                status = "finished"
    except:
        pass
    conn.close()
    return status, deadline

# =========================
# STATES
# =========================
class UState(StatesGroup):
    reg_name = State()
    join_code = State()
    solve_tid = State()
    solve_answers = State()
    task_submit = State()


class AState(StatesGroup):
    waiting_file = State()

    # global broadcast (any content: text or media)
    broadcast_any = State()

    # group create
    g_name = State()

    # test create
    t_keys = State()
    t_minutes = State()
    t_assign = State()

    # manual results
    m_tid = State()
    m_total = State()
    m_scores = State()

    # import results
    imp_tid = State()

    # group settings
    gs_chatid = State()
    gs_att_limit = State()
    gs_task_limit = State()

    # tasks (draft builder)
    task_title = State()
    task_desc_media = State()
    task_points = State()
    task_due = State()

    # grading
    grade_score = State()
    grade_feedback = State()

# =========================
# KEYBOARDS (User/Admin Home)
# =========================
def kb_user_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Guruhga qo‘shilish", callback_data="u:join")],
        [InlineKeyboardButton(text="📚 Guruhlarim", callback_data="u:mygroups")],
        [InlineKeyboardButton(text="📝 Test topshirish", callback_data="u:solve")],
        [InlineKeyboardButton(text="📄 Natijalarim", callback_data="u:myresults")],
    ])

def kb_admin_home(uid: int) -> InlineKeyboardMarkup:
    rows = []
    if has_perm(uid, "groups") or is_super(uid):
        rows.append([InlineKeyboardButton(text="👥 Guruhlar", callback_data="a:groups")])
    if has_perm(uid, "tests") or is_super(uid):
        rows.append([InlineKeyboardButton(text="🧪 Testlar", callback_data="a:tests")])
    if has_perm(uid, "broadcast") or is_super(uid):
        rows.append([InlineKeyboardButton(text="📢 Global xabar", callback_data="a:broadcast")])
    if is_super(uid):
        rows.append([InlineKeyboardButton(text="👮 Adminlar", callback_data="a:admins")])
    rows.append([InlineKeyboardButton(text="👤 User rejimi", callback_data="a:as_user")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# =========================
# PDF GENERATORS
# =========================
def pdf_rating(filename: str, title: str, rows: List[Tuple[str, int, int, float, str]]):
    """
    rows: (full_name, score, total, percent, date)
    """
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=12)

    # Header
    pdf.set_fill_color(33, 150, 243)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Arial", "B", 14)
    pdf.cell(190, 12, txt=pdf_safe(title), ln=True, align="C", fill=True)

    pdf.ln(3)
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Arial", "B", 10)
    pdf.set_fill_color(230, 230, 230)

    pdf.cell(12, 8, pdf_safe("No"), 1, 0, "C", True)
    pdf.cell(78, 8, pdf_safe("Ism"), 1, 0, "C", True)
    pdf.cell(26, 8, pdf_safe("Ball"), 1, 0, "C", True)
    pdf.cell(22, 8, pdf_safe("Foiz"), 1, 0, "C", True)
    pdf.cell(52, 8, pdf_safe("Sana"), 1, 1, "C", True)

    pdf.set_font("Arial", "", 10)

    for i, (name, score, total, percent, date_s) in enumerate(rows, 1):
        p = float(percent)

        # Color bands (you requested: 85+ green, 65+ yellow, else red)
        if p >= 85:
            pdf.set_fill_color(90, 220, 120)
        elif p >= 65:
            pdf.set_fill_color(255, 215, 80)
        else:
            pdf.set_fill_color(255, 110, 110)

        pdf.cell(12, 8, str(i), 1, 0, "C", True)
        pdf.cell(78, 8, pdf_safe(name)[:44], 1, 0, "L", True)
        pdf.cell(26, 8, pdf_safe(f"{score}/{total}"), 1, 0, "C", True)
        pdf.cell(22, 8, pdf_safe(f"{p:.1f}%"), 1, 0, "C", True)
        pdf.cell(52, 8, pdf_safe(date_s), 1, 1, "C", True)

    pdf.output(filename)


def pdf_attendance(filename: str, group_name: str, date_s: str, rows: List[Tuple[str, str]]):
    """
    rows = [(name, status)] status: present/absent
    """
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=12)

    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, safe_pdf_text(f"Davomat — {group_name}"), ln=1, align="C")
    pdf.set_font("Arial", "", 11)
    pdf.cell(0, 8, safe_pdf_text(f"Sana: {date_s} | Jami: {len(rows)}"), ln=1, align="C")
    pdf.ln(2)

    # Header
    pdf.set_font("Arial", "B", 10)
    pdf.set_fill_color(230, 230, 230)
    pdf.cell(10, 8, "#", 1, 0, "C", True)
    pdf.cell(140, 8, "Ism", 1, 0, "L", True)
    pdf.cell(40, 8, "Holat", 1, 1, "C", True)

    pdf.set_font("Arial", "", 10)
    for i, (name, st) in enumerate(rows, 1):
        if st == "absent":
            pdf.set_fill_color(255, 210, 210)
            label = "Qatnashmadi"
        else:
            pdf.set_fill_color(200, 255, 200)
            label = "Qatnashdi"
        pdf.cell(10, 8, str(i), 1, 0, "C", True)
        pdf.cell(140, 8, safe_pdf_text(name)[:80], 1, 0, "L", True)
        pdf.cell(40, 8, safe_pdf_text(label), 1, 1, "C", True)

    pdf.output(filename)
# =========================
# START / USER REGISTER
# =========================
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id

    # ensure user row exists
    conn = db()
    u = conn.execute("SELECT full_name FROM users WHERE user_id=?", (uid,)).fetchone()
    conn.close()

    if not u:
        # ask name (first time)
        await message.answer(
            "👋 Salom! Ism va familiyangizni kiriting (masalan: Ali Valiyev):"
        )
        await state.set_state(UState.reg_name)
        return

    # admin or user panel
    if is_admin(uid):
        await message.answer("⚙️ <b>Admin panel</b>", reply_markup=kb_admin_home(uid))
    else:
        await message.answer(f"👋 Salom, <b>{safe_pdf_text(u['full_name'])}</b>!", reply_markup=kb_user_home())

@router.message(UState.reg_name)
async def reg_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if len(name) < 3:
        await message.answer("Iltimos, ism-familiyani to‘liq yozing:")
        return
    ensure_user(message.from_user.id, name)
    await state.clear()
    await message.answer("✅ Saqlandi! Asosiy menyu:", reply_markup=kb_user_home())

# =========================
# USER HOME NAV
# =========================
@router.callback_query(F.data == "u:home")
async def u_home(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(call, "🏠 <b>Menyu</b>", kb_user_home())

@router.callback_query(F.data == "a:home")
async def a_home(call: CallbackQuery, state: FSMContext):
    await state.clear()
    uid = call.from_user.id
    if not is_admin(uid):
        await call.answer("Ruxsat yo‘q.", show_alert=True)
        return
    await safe_edit(call, "🏠 <b>Admin panel</b>", kb_admin_home(uid))

@router.callback_query(F.data == "a:as_user")
async def a_as_user(call: CallbackQuery, state: FSMContext):
    await state.clear()
    uid = call.from_user.id
    if not is_admin(uid):
        await call.answer("Ruxsat yo‘q.", show_alert=True)
        return
    kb = kb_user_home()
    kb.inline_keyboard.append([InlineKeyboardButton(text="🔙 Admin panel", callback_data="a:home")])
    await safe_edit(call, "👤 User rejimi", kb)

# =========================
# USER: join group
# =========================
@router.callback_query(F.data == "u:join")
async def u_join(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(call, "🔑 Guruh kodini kiriting (masalan: 1234AB):", kb_home_user())
    await state.set_state(UState.join_code)

@router.message(UState.join_code)
async def u_join_code(message: Message, state: FSMContext):
    code = (message.text or "").upper().strip()
    if not re.fullmatch(r"\d{4}[A-H]{2}", code):
        await message.answer("❌ Kod formati xato. Masalan: 1234AB")
        return
    uid = message.from_user.id
    ensure_user(uid, message.from_user.full_name or "No Name")

    conn = db()
    g = conn.execute("SELECT id, name FROM groups WHERE invite_code=?", (code,)).fetchone()
    if not g:
        conn.close()
        await message.answer("❌ Guruh topilmadi. Kodni tekshiring.")
        return
    exists = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (g["id"], uid)).fetchone()
    if not exists:
        conn.execute("INSERT INTO members(group_id, user_id) VALUES (?,?)", (g["id"], uid))
        conn.execute("INSERT OR IGNORE INTO counters(group_id, user_id, absent_count, missed_task_count) VALUES (?,?,0,0)",
                     (g["id"], uid))
        conn.commit()
    conn.close()

    await state.clear()
    await message.answer(f"✅ <b>{safe_pdf_text(g['name'])}</b> guruhiga qo‘shildingiz.", reply_markup=kb_user_home())

def user_groups(uid: int) -> List[Tuple[int, str]]:
    conn = db()
    rows = conn.execute("""
        SELECT g.id, g.name
        FROM members m JOIN groups g ON g.id=m.group_id
        WHERE m.user_id=?
        ORDER BY g.name
    """, (uid,)).fetchall()
    conn.close()
    return [(int(r["id"]), r["name"]) for r in rows]

# =========================
# USER: My groups & tests (INLINE)
# =========================
@router.callback_query(F.data == "u:mygroups")
async def u_mygroups(call: CallbackQuery):
    uid = call.from_user.id
    groups = user_groups(uid)
    if not groups:
        await safe_edit(call, "Siz hech qaysi guruhda emassiz.", kb_user_home())
        return

    kb_rows = []
    for gid, name in groups:
        kb_rows.append([InlineKeyboardButton(text=f"📌 {name}", callback_data=f"u:g:{gid}")])
    kb_rows.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")])
    await safe_edit(call, "📚 <b>Guruhlarim</b>\nGuruhni tanlang:", InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("u:g:"))
async def u_group_view(call: CallbackQuery):
    uid = call.from_user.id
    gid = int(call.data.split(":")[2])

    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not mem or not g:
        await call.answer("Bu guruh sizniki emas.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧪 Guruh testlari", callback_data=f"u:gt:{gid}")],
        [InlineKeyboardButton(text="📌 Vazifalar", callback_data=f"u:tasks:{gid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data="u:mygroups")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")],
    ])
    await safe_edit(call, f"📌 <b>{safe_pdf_text(g['name'])}</b>\nQuyidan bo‘lim tanlang:", kb)

# =========================
# USER: group tests list
# =========================
def tests_for_user_in_group(uid: int, gid: int) -> List[sqlite3.Row]:
    conn = db()
    # allowed: public OR assigned to this group
    rows = conn.execute("""
        SELECT t.test_id, t.status, t.deadline, COALESCE(t.is_public,0) AS is_public
        FROM tests t
        LEFT JOIN test_groups tg ON tg.test_id=t.test_id
        WHERE (COALESCE(t.is_public,0)=1) OR (tg.group_id=?)
        GROUP BY t.test_id
        ORDER BY t.created_at DESC
    """, (gid,)).fetchall()
    conn.close()
    return rows

@router.callback_query(F.data.startswith("u:gt:"))
async def u_group_tests(call: CallbackQuery):
    uid = call.from_user.id
    gid = int(call.data.split(":")[2])

    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not mem or not g:
        await call.answer("Bu guruh sizniki emas.", show_alert=True)
        return

    rows = tests_for_user_in_group(uid, gid)
    if not rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"u:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")]])
        await safe_edit(call, "Bu guruhda hozircha test yo‘q.", kb)
        return

    kb_rows = []
    for r in rows[:30]:
        status, dl = ensure_deadline(r["test_id"])
        icon = "🟢" if status == "active" else "⏸" if status == "paused" else "🏁"
        kb_rows.append([InlineKeyboardButton(
            text=f"{icon} {r['test_id']} ({status})",
            callback_data=f"u:solve_tid:{r['test_id']}"
        )])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"u:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")])
    await safe_edit(call, f"🧪 <b>{safe_pdf_text(g['name'])}</b> — Testlar:", InlineKeyboardMarkup(inline_keyboard=kb_rows))

# =========================
# USER: Solve test (by id from list or manual)
# =========================
@router.callback_query(F.data == "u:solve")
async def u_solve(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(call, "📝 Test ID kiriting (masalan: 12345):", kb_home_user())
    await state.set_state(UState.solve_tid)

@router.callback_query(F.data.startswith("u:solve_tid:"))
async def u_solve_from_button(call: CallbackQuery, state: FSMContext):
    await state.clear()
    tid = call.data.split(":")[2]
    await state.update_data(tid=tid)
    await safe_edit(call, f"📝 Test <code>{tid}</code>\nJavoblarni yuboring (A/B/C/D). Masalan: ABCDAB...", kb_home_user())
    await state.set_state(UState.solve_answers)

@router.message(UState.solve_tid)
async def u_solve_tid_msg(message: Message, state: FSMContext):
    tid = (message.text or "").strip()
    status, deadline = ensure_deadline(tid)
    if status is None:
        await message.answer("❌ Test topilmadi.")
        return
    if status == "paused":
        await message.answer("⏸ Test vaqtincha to‘xtatilgan.")
        await state.clear()
        return
    if status == "finished":
        await message.answer("🏁 Test yakunlangan (deadline o‘tgan yoki yakunlangan).")
        await state.clear()
        return

    # allow if public OR assigned to any of user's groups
    uid = message.from_user.id
    conn = db()
    pub = conn.execute("SELECT COALESCE(is_public,0) AS p FROM tests WHERE test_id=?", (tid,)).fetchone()
    if pub and int(pub["p"]) == 1:
        allowed = True
    else:
        gids = conn.execute("SELECT group_id FROM members WHERE user_id=?", (uid,)).fetchall()
        if not gids:
            allowed = False
        else:
            myg = [int(x["group_id"]) for x in gids]
            tg = conn.execute("SELECT group_id FROM test_groups WHERE test_id=?", (tid,)).fetchall()
            allowed_set = {int(x["group_id"]) for x in tg}
            allowed = any(g in allowed_set for g in myg)
    # anti-cheat
    already = conn.execute("SELECT 1 FROM submissions WHERE user_id=? AND test_id=?", (uid, tid)).fetchone()
    keys = conn.execute("SELECT keys FROM tests WHERE test_id=?", (tid,)).fetchone()
    conn.close()

    if not allowed:
        await message.answer("❌ Bu test sizga biriktirilmagan (public emas va guruhingizda yo‘q).")
        await state.clear()
        return
    if already:
        await message.answer("⚠️ Siz bu testni 1 marta topshirib bo‘lgansiz.")
        await state.clear()
        return
    if not keys:
        await message.answer("❌ Test topilmadi.")
        await state.clear()
        return

    await state.update_data(tid=tid, keys=keys["keys"])
    await message.answer(f"✅ Test topildi. Savollar: {len(keys['keys'])} ta.\nJavoblarni yuboring (A/B/C/D).")
    await state.set_state(UState.solve_answers)

@router.message(UState.solve_answers)
async def u_solve_answers(message: Message, state: FSMContext):
    data = await state.get_data()
    tid = data.get("tid")
    keys = data.get("keys", "")

    status, _ = ensure_deadline(tid)
    if status != "active":
        await message.answer("⛔️ Test tugagan yoki pauzada.")
        await state.clear()
        return

    ans = (message.text or "").upper().strip().replace(" ", "")
    if (not ans) or any(ch not in "ABCD" for ch in ans):
        await message.answer("⚠️ Faqat A/B/C/D bo‘lsin.")
        return
    if len(ans) != len(keys):
        await message.answer(f"⚠️ Javoblar soni {len(keys)} ta bo‘lishi kerak.")
        return

    uid = message.from_user.id
    ensure_user(uid, message.from_user.full_name or "No Name")
    full_name = get_user_name(uid)

    conn = db()
    # anti-cheat
    already = conn.execute("SELECT 1 FROM submissions WHERE user_id=? AND test_id=?", (uid, tid)).fetchone()
    if already:
        conn.close()
        await message.answer("⚠️ Siz bu testni topshirib bo‘lgansiz.")
        await state.clear()
        return

    score = sum(1 for a, k in zip(ans, keys) if a == k)
    total = len(keys)
    pct = (score / total) * 100 if total else 0.0

    conn.execute("""INSERT INTO submissions(user_id, test_id, answers, submitted_at)
                    VALUES (?,?,?,?)""", (uid, tid, ans, now_str()))
    conn.execute("""INSERT INTO results(user_id, test_id, score, total, percent, date, full_name)
                    VALUES (?,?,?,?,?,?,?)""", (uid, tid, score, total, pct, now_str(), full_name))
    conn.commit()
    conn.close()

    await state.clear()
    await message.answer(
        f"✅ <b>Natija</b>\nTest: <code>{tid}</code>\nBall: <b>{score}/{total}</b>\nFoiz: <b>{pct:.1f}%</b>",
        reply_markup=kb_user_home()
    )

# =========================
# USER: my results
# =========================
@router.callback_query(F.data == "u:myresults")
async def u_myresults(call: CallbackQuery):
    uid = call.from_user.id
    conn = db()
    rows = conn.execute("""SELECT test_id, score, total, percent, date
                           FROM results WHERE user_id=?
                           ORDER BY id DESC LIMIT 15""", (uid,)).fetchall()
    conn.close()
    if not rows:
        await safe_edit(call, "Sizda hali natija yo‘q.", kb_user_home())
        return

    text = "📄 <b>Natijalarim</b>\n\n"
    for i, r in enumerate(rows, 1):
        text += f"{i}) <code>{r['test_id']}</code> — <b>{r['score']}/{r['total']}</b> ({r['percent']:.1f}%) | {r['date']}\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")]
    ])
    await safe_edit(call, text, kb)

# =========================
# ADMIN: GROUPS LIST / CREATE / VIEW
# =========================
@router.callback_query(F.data == "a:groups")
async def a_groups(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    conn = db()
    groups = conn.execute("SELECT id, name, invite_code FROM groups ORDER BY id DESC").fetchall()
    conn.close()

    kb_rows = []
    for g in groups:
        kb_rows.append([InlineKeyboardButton(text=f"📁 {g['name']}", callback_data=f"a:g:{g['id']}")])
    kb_rows.append([InlineKeyboardButton(text="➕ Guruh yaratish", callback_data="a:g_add")])
    kb_rows.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, "👥 <b>Guruhlar</b>", InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data == "a:g_add")
async def a_g_add(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "groups"):
        return
    await state.clear()
    await safe_edit(call, "🆕 Guruh nomini kiriting:", kb_home_admin(call.from_user.id))
    await state.set_state(AState.g_name)

@router.message(AState.g_name)
async def a_g_add_save(message: Message, state: FSMContext):
    uid = message.from_user.id
    if not is_admin(uid) or not has_perm(uid, "groups"):
        await state.clear()
        return
    name = (message.text or "").strip()
    if len(name) < 2:
        await message.answer("Guruh nomi qisqa. Qayta kiriting:")
        return

    conn = db()
    code = None
    for _ in range(200):
        cand = gen_group_code()
        ex = conn.execute("SELECT 1 FROM groups WHERE invite_code=?", (cand,)).fetchone()
        if not ex:
            code = cand
            break
    if not code:
        conn.close()
        await message.answer("Kod yaratib bo‘lmadi.")
        await state.clear()
        return

    conn.execute("INSERT INTO groups(name, invite_code) VALUES (?,?)", (name, code))
    conn.commit()
    conn.close()

    await state.clear()
    await message.answer(f"✅ Guruh yaratildi: <b>{safe_pdf_text(name)}</b>\nKod: <code>{code}</code>",
                         reply_markup=kb_admin_home(uid))

@router.callback_query(F.data.startswith("a:g:"))
async def a_group_view(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    g = conn.execute("SELECT * FROM groups WHERE id=?", (gid,)).fetchone()
    cnt = conn.execute("SELECT COUNT(*) AS c FROM members WHERE group_id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👨‍🎓 O‘quvchilar", callback_data=f"a:g_students:{gid}")],
        [InlineKeyboardButton(text="🧪 Guruh testlari", callback_data=f"a:g_tests:{gid}")],
        [InlineKeyboardButton(text="📥 Natija (manual/import)", callback_data=f"a:g_results:{gid}")],
        [InlineKeyboardButton(text="🗓 Davomat", callback_data=f"a:g_att:{gid}")],
        [InlineKeyboardButton(text="📌 Vazifalar", callback_data=f"a:g_tasks:{gid}")],
        [InlineKeyboardButton(text="⚙️ Sozlamalar", callback_data=f"a:g_set:{gid}")],
        [InlineKeyboardButton(text="🔁 Kod yangilash", callback_data=f"a:g_regen:{gid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data="a:groups")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])

    text = (f"📁 <b>{safe_pdf_text(g['name'])}</b>\n"
            f"🔑 Kod: <code>{g['invite_code']}</code>\n"
            f"👨‍🎓 O‘quvchilar: <b>{int(cnt['c'])}</b>\n"
            f"📌 tg_chat_id: <code>{g['tg_chat_id'] if g['tg_chat_id'] else 'yo‘q'}</code>\n"
            f"🚪 Absent kick limit: <b>{g['att_absent_limit']}</b>\n"
            f"🚪 Task miss kick limit: <b>{g['task_miss_limit']}</b>\n")
    await safe_edit(call, text, kb)

@router.callback_query(F.data.startswith("a:g_regen:"))
async def a_group_regen(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    code = None
    for _ in range(200):
        cand = gen_group_code()
        ex = conn.execute("SELECT 1 FROM groups WHERE invite_code=?", (cand,)).fetchone()
        if not ex:
            code = cand
            break
    if not code:
        conn.close()
        await call.answer("Kod yaratib bo‘lmadi.", show_alert=True)
        return
    conn.execute("UPDATE groups SET invite_code=? WHERE id=?", (code, gid))
    conn.commit()
    conn.close()
    await call.answer("✅ Kod yangilandi", show_alert=True)
    # refresh view
    await a_group_view(call)
# =========================
# ADMIN: Group Students (list + remove)
# =========================
@router.callback_query(F.data.startswith("a:g_students:"))
async def a_g_students(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    g = conn.execute("SELECT name, tg_chat_id FROM groups WHERE id=?", (gid,)).fetchone()
    students = conn.execute("""
        SELECT u.user_id, u.full_name
        FROM members m JOIN users u ON u.user_id=m.user_id
        WHERE m.group_id=?
        ORDER BY u.full_name
    """, (gid,)).fetchall()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    text = f"👨‍🎓 <b>{safe_pdf_text(g['name'])}</b> — O‘quvchilar\n\n"
    kb_rows = []
    for i, s in enumerate(students, 1):
        text += f"{i}. {safe_pdf_text(s['full_name'])}\n"
        kb_rows.append([InlineKeyboardButton(text=f"❌ {s['full_name'][:18]}", callback_data=f"a:g_kick:{gid}:{s['user_id']}")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, text, InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("a:g_kick:"))
async def a_g_kick(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    _, _, gid, uid = call.data.split(":")
    gid = int(gid); uid = int(uid)

    conn = db()
    g = conn.execute("SELECT tg_chat_id FROM groups WHERE id=?", (gid,)).fetchone()
    conn.execute("DELETE FROM members WHERE group_id=? AND user_id=?", (gid, uid))
    conn.commit()
    conn.close()

    # kick from telegram group if chat_id set
    if g and g["tg_chat_id"]:
        try:
            await call.bot.ban_chat_member(chat_id=int(g["tg_chat_id"]), user_id=uid)
            await call.bot.unban_chat_member(chat_id=int(g["tg_chat_id"]), user_id=uid)
        except:
            pass

    await call.answer("Chiqarildi", show_alert=True)
    await a_g_students(call)

# =========================
# ADMIN: Group Settings
# =========================
@router.callback_query(F.data.startswith("a:g_set:"))
async def a_g_set(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    g = conn.execute("SELECT * FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 tg_chat_id sozlash", callback_data=f"a:gs_chat:{gid}")],
        [InlineKeyboardButton(text="🚪 Absent kick limit", callback_data=f"a:gs_att:{gid}")],
        [InlineKeyboardButton(text="🚪 Task miss kick limit", callback_data=f"a:gs_task:{gid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    text = (f"⚙️ <b>Sozlamalar</b>\nGuruh: <b>{safe_pdf_text(g['name'])}</b>\n\n"
            f"tg_chat_id: <code>{g['tg_chat_id'] if g['tg_chat_id'] else 'yo‘q'}</code>\n"
            f"Absent kick limit: <b>{g['att_absent_limit']}</b>\n"
            f"Task miss kick limit: <b>{g['task_miss_limit']}</b>\n\n"
            f"tg_chat_id — Telegram guruh ID (minus bilan), masalan: -1001234567890\n"
            f"Botni o‘sha TG guruhda admin qiling.")
    await safe_edit(call, text, kb)

@router.callback_query(F.data.startswith("a:gs_chat:"))
async def a_gs_chat(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid)
    await safe_edit(call, "💬 tg_chat_id kiriting (masalan: -1001234567890). Bekor qilish: /cancel", kb_home_admin(call.from_user.id))
    await state.set_state(AState.gs_chatid)

@router.message(AState.gs_chatid)
async def a_gs_chat_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "groups"):
        await state.clear()
        return
    data = await state.get_data()
    gid = int(data["gid"])
    v = (message.text or "").strip()
    chat_id = safe_int(v)
    if chat_id is None:
        await message.answer("❌ Raqam bo‘lishi kerak. Masalan: -1001234567890")
        return
    conn = db()
    conn.execute("UPDATE groups SET tg_chat_id=? WHERE id=?", (chat_id, gid))
    conn.commit()
    conn.close()
    await state.clear()
    await message.answer("✅ Saqlandi", reply_markup=kb_admin_home(message.from_user.id))

@router.callback_query(F.data.startswith("a:gs_att:"))
async def a_gs_att(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid)
    await safe_edit(call, "🚪 Absent kick limit kiriting (masalan: 5):", kb_home_admin(call.from_user.id))
    await state.set_state(AState.gs_att_limit)

@router.message(AState.gs_att_limit)
async def a_gs_att_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "groups"):
        await state.clear()
        return
    data = await state.get_data()
    gid = int(data["gid"])
    lim = safe_int((message.text or "").strip())
    if lim is None or lim < 1:
        await message.answer("❌ 1 dan katta raqam kiriting.")
        return
    conn = db()
    conn.execute("UPDATE groups SET att_absent_limit=? WHERE id=?", (lim, gid))
    conn.commit()
    conn.close()
    await state.clear()
    await message.answer("✅ Saqlandi", reply_markup=kb_admin_home(message.from_user.id))

@router.callback_query(F.data.startswith("a:gs_task:"))
async def a_gs_task(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid)
    await safe_edit(call, "🚪 Task miss kick limit kiriting (masalan: 5):", kb_home_admin(call.from_user.id))
    await state.set_state(AState.gs_task_limit)

@router.message(AState.gs_task_limit)
async def a_gs_task_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "groups"):
        await state.clear()
        return
    data = await state.get_data()
    gid = int(data["gid"])
    lim = safe_int((message.text or "").strip())
    if lim is None or lim < 1:
        await message.answer("❌ 1 dan katta raqam kiriting.")
        return
    conn = db()
    conn.execute("UPDATE groups SET task_miss_limit=? WHERE id=?", (lim, gid))
    conn.commit()
    conn.close()
    await state.clear()
    await message.answer("✅ Saqlandi", reply_markup=kb_admin_home(message.from_user.id))

# =========================
# ATTENDANCE (Group-only) + Archive + Send DM
# =========================
def attendance_map(gid: int, date_s: str) -> dict:
    conn = db()
    rows = conn.execute("SELECT user_id, status FROM attendance WHERE group_id=? AND att_date=?",
                        (gid, date_s)).fetchall()
    conn.close()
    return {int(r["user_id"]): r["status"] for r in rows}

def group_students(gid: int) -> List[Tuple[int, str]]:
    conn = db()
    rows = conn.execute("""
        SELECT u.user_id, u.full_name
        FROM members m JOIN users u ON u.user_id=m.user_id
        WHERE m.group_id=?
        ORDER BY u.full_name
    """, (gid,)).fetchall()
    conn.close()
    return [(int(r["user_id"]), r["full_name"]) for r in rows]

@router.callback_query(F.data.startswith("a:g_att:"))
async def a_g_att_menu(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    parts = call.data.split(":")
    gid = int(parts[2])
    d = parts[3] if len(parts) > 3 else today_str()
    await _render_attendance_screen(call, gid, d)


async def _render_attendance_screen(call: CallbackQuery, gid: int, d: str):
    """Render attendance UI for group/date. Do NOT mutate call.data (CallbackQuery is frozen in aiogram v3)."""
    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    studs = group_students(gid)
    amap = attendance_map(gid, d)

    # UI: Only mark absent with ❌; default present
    kb_rows = []
    for uid, name in studs:
        st = amap.get(uid, "present")
        icon = "❌" if st == "absent" else "✅"
        kb_rows.append([InlineKeyboardButton(
            text=f"{icon} {name[:22]}",
            callback_data=f"a:att_t:{gid}:{uid}:{d}"
        )])

    kb_rows.append([InlineKeyboardButton(text="✅ Saqlash", callback_data=f"a:att_save:{gid}:{d}")])
    kb_rows.append([InlineKeyboardButton(text="📨 Yo‘qlarga DM yuborish", callback_data=f"a:att_send:{gid}:{d}")])
    kb_rows.append([InlineKeyboardButton(text="📄 Hisobot (text)", callback_data=f"a:att_rep:{gid}:{d}")])
    kb_rows.append([InlineKeyboardButton(text="📥 Hisobot (PDF)", callback_data=f"a:att_pdf:{gid}:{d}")])
    kb_rows.append([InlineKeyboardButton(text="🗂 Arxiv", callback_data=f"a:att_arc:{gid}")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, f"🗓 <b>Davomat</b>\nGuruh: <b>{safe_pdf_text(g['name'])}</b>\nSana: <code>{d}</code>\n\n"
                          f"Faqat qatnashmaganlarni ❌ qilib belgilang.", InlineKeyboardMarkup(inline_keyboard=kb_rows))


@router.callback_query(F.data.startswith("a:att:"))
async def a_att_open(call: CallbackQuery):
    # Open attendance for selected archived date
    if not await guard(call, "attendance"):
        return
    parts = call.data.split(":")
    if len(parts) < 4:
        await call.answer("Xatolik.", show_alert=True)
        return
    gid = int(parts[2])
    d = parts[3]
    # Open the same attendance screen for archived date
    await _render_attendance_screen(call, gid, d)

@router.callback_query(F.data.startswith("a:att_t:"))
async def a_att_toggle(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    _, _, gid, uid, d = call.data.split(":")
    gid = int(gid); uid = int(uid)

    conn = db()
    cur = conn.execute("""
        SELECT status FROM attendance WHERE group_id=? AND user_id=? AND att_date=?
    """, (gid, uid, d)).fetchone()

    if not cur:
        # mark absent
        conn.execute("""INSERT OR REPLACE INTO attendance(group_id, user_id, att_date, status)
                        VALUES (?,?,?,'absent')""", (gid, uid, d))
    else:
        # if absent -> remove row (back to present)
        if cur["status"] == "absent":
            conn.execute("DELETE FROM attendance WHERE group_id=? AND user_id=? AND att_date=?", (gid, uid, d))
        else:
            conn.execute("UPDATE attendance SET status='absent' WHERE group_id=? AND user_id=? AND att_date=?", (gid, uid, d))
    conn.commit()
    conn.close()

    await a_g_att_menu(call)

@router.callback_query(F.data.startswith("a:att_rep:"))
async def a_att_report_text(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    _, _, gid, d = call.data.split(":")
    gid = int(gid)

    # save day to archive / apply kick limits (only once per date)
    await finalize_attendance_day(call.bot, gid, d, saved_by=call.from_user.id, send_dm=False)


    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    studs = group_students(gid)
    amap = attendance_map(gid, d)
    absent = [(uid, nm) for uid, nm in studs if amap.get(uid, "present") == "absent"]
    present = len(studs) - len(absent)

    text = (f"📄 <b>Davomat hisoboti</b>\n"
            f"Guruh: <b>{safe_pdf_text(g['name'])}</b>\n"
            f"Sana: <code>{d}</code>\n\n"
            f"Jami: <b>{len(studs)}</b>\n"
            f"✅ Qatnashdi: <b>{present}</b>\n"
            f"❌ Qatnashmadi: <b>{len(absent)}</b>\n\n")

    if absent:
        text += "❌ <b>QATNASHMAGANLAR:</b>\n"
        for i, (_uid, nm) in enumerate(absent, 1):
            text += f"{i}. {safe_pdf_text(nm)}\n"
    else:
        text += "✅ Bugun hamma qatnashgan."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 PDF", callback_data=f"a:att_pdf:{gid}:{d}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g_att:{gid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    await safe_edit(call, text, kb)

@router.callback_query(F.data.startswith("a:att_pdf:"))
async def a_att_pdf(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    _, _, gid, d = call.data.split(":")
    gid = int(gid)

    # save day to archive / apply kick limits (only once per date)
    await finalize_attendance_day(call.bot, gid, d, saved_by=call.from_user.id, send_dm=False)


    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    studs = group_students(gid)
    amap = attendance_map(gid, d)
    rows = [(nm, "absent" if amap.get(uid, "present") == "absent" else "present") for uid, nm in studs]

    fname = f"attendance_G{gid}_{d}.pdf"
    pdf_attendance(fname, g["name"], d, rows)
    try:
        await call.message.answer_document(FSInputFile(fname))
    finally:
        try:
            os.remove(fname)
        except:
            pass



# ---------------- Attendance finalize / archive / auto-kick ----------------
async def finalize_attendance_day(bot: Bot, gid: int, att_date: str, saved_by: int, *, send_dm: bool = False) -> dict:
    """Finalize attendance day: record day into attendance_days (once), increment absent counters once, and auto-kick if limit reached.
    If send_dm=True, DM absent users with their current counter (does not re-increment if already finalized)."""
    conn = db()
    g = conn.execute("SELECT id, name, tg_chat_id, att_absent_limit FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        return {"ok": False, "error": "group_not_found"}

    studs = group_students(gid)
    amap = attendance_map(gid, att_date)
    absent = [(uid, nm) for uid, nm in studs if amap.get(uid, "present") == "absent"]

    conn = db()
    cur = conn.execute(
        "INSERT OR IGNORE INTO attendance_days(group_id, att_date, saved_at, saved_by) VALUES (?,?,?,?)",
        (gid, att_date, now_str(), int(saved_by)),
    )
    conn.commit()
    inserted = (cur.rowcount == 1)
    conn.close()

    sent = 0
    kicked = 0

    limit = int(g["att_absent_limit"] or 0)
    if limit <= 0:
        limit = 999999

    for uid, nm in absent:
        conn = db()
        conn.execute("INSERT OR IGNORE INTO counters(group_id, user_id, absent_count, missed_task_count) VALUES (?,?,0,0)", (gid, uid))
        if inserted:
            conn.execute("UPDATE counters SET absent_count = absent_count + 1 WHERE group_id=? AND user_id=?", (gid, uid))
        row = conn.execute("SELECT absent_count FROM counters WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
        conn.commit()
        conn.close()

        cnt_abs = int(row["absent_count"]) if row else 0

        if send_dm:
            try:
                await bot.send_message(
                    uid,
                    f"🗓 <b>Davomat ogohlantirish</b>\n"
                    f"Guruh: <b>{safe_pdf_text(g['name'])}</b>\n"
                    f"Sana: <code>{att_date}</code>\n\n"
                    f"Siz bugun darsga qatnashmadingiz ❌\n"
                    f"Sababsiz qoldirish: <b>{cnt_abs}/{limit}</b>",
                )
                sent += 1
            except Exception:
                pass

        if inserted and cnt_abs >= limit:
            conn = db()
            conn.execute("DELETE FROM members WHERE group_id=? AND user_id=?", (gid, uid))
            conn.commit()
            conn.close()

            if g["tg_chat_id"]:
                try:
                    await bot.ban_chat_member(chat_id=int(g["tg_chat_id"]), user_id=uid)
                    await bot.unban_chat_member(chat_id=int(g["tg_chat_id"]), user_id=uid)
                except Exception:
                    pass
            try:
                await bot.send_message(uid, f"⛔️ Siz <b>{safe_pdf_text(g['name'])}</b> guruhidan chiqarildingiz (davomat limitiga yetdi).")
            except Exception:
                pass
            kicked += 1

    return {"ok": True, "inserted": inserted, "absent": len(absent), "sent": sent, "kicked": kicked}

@router.callback_query(F.data.startswith("a:att_send:"))
async def a_att_send(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    _, _, gid, d = call.data.split(":")
    gid = int(gid)

    res = await finalize_attendance_day(call.bot, gid, d, saved_by=call.from_user.id, send_dm=True)
    if not res.get("ok"):
        await call.answer("Xatolik.", show_alert=True)
        return

    msg = f"✅ Yuborildi: {res['sent']} ta\n📌 Yo‘qlar: {res['absent']} ta"
    msg += "\n🗂 Arxivga saqlandi." if res.get("inserted") else "\nℹ️ Bu sana avval saqlangan."
    if res.get("kicked"):
        msg += f"\n⛔️ Kick: {res['kicked']}"
    await call.answer(msg, show_alert=True)

@router.callback_query(F.data.startswith("a:att_save:"))
async def a_att_save(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    _, _, gid, d = call.data.split(":")
    gid = int(gid)

    res = await finalize_attendance_day(call.bot, gid, d, saved_by=call.from_user.id, send_dm=False)
    if not res.get("ok"):
        await call.answer("Xatolik.", show_alert=True)
        return

    if res.get("inserted"):
        await call.answer("✅ Davomat saqlandi va arxivga qo‘shildi.", show_alert=True)
    else:
        await call.answer("ℹ️ Bu sana avval saqlangan.", show_alert=True)

@router.callback_query(F.data.startswith("a:att_arc:"))
async def a_att_archive(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    ensure_attendance_schema(conn)
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    
    try:
        dates = conn.execute("SELECT att_date FROM attendance_days WHERE group_id=? ORDER BY att_date DESC LIMIT 60", (gid,)).fetchall()
    except Exception as e:
        if "attendance_days" in str(e):
            ensure_attendance_schema(conn)
            dates = conn.execute("SELECT att_date FROM attendance_days WHERE group_id=? ORDER BY att_date DESC LIMIT 60", (gid,)).fetchall()
        else:
            raise

    conn.close()

    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    rows = []
    for r in dates:
        d = r["att_date"]
        rows.append([InlineKeyboardButton(text=f"🗓 {d}", callback_data=f"a:att:{gid}:{d}")])

    if not rows:
        rows.append([InlineKeyboardButton(text="(Arxiv bo‘sh)", callback_data="noop")])

    rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g_att:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call,
                    f"🗂 <b>Davomat arxivi</b>\nGuruh: <b>{safe_pdf_text(g['name'])}</b>\n\nSaqlangan sanalar:",
                    InlineKeyboardMarkup(inline_keyboard=rows))

@router.callback_query(F.data == "a:tests")
async def a_tests(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    conn = db()
    rows = conn.execute("SELECT test_id, status, deadline FROM tests ORDER BY created_at DESC LIMIT 30").fetchall()
    conn.close()

    kb_rows = []
    for r in rows:
        st, dl = ensure_deadline(r["test_id"])
        icon = "🟢" if st == "active" else "⏸" if st == "paused" else "🏁"
        kb_rows.append([InlineKeyboardButton(text=f"{icon} {r['test_id']} ({st})", callback_data=f"a:t:{r['test_id']}")])
    kb_rows.append([InlineKeyboardButton(text="➕ Test yaratish", callback_data="a:t_add")])
    kb_rows.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])
    await safe_edit(call, "🧪 <b>Testlar</b>", InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data == "a:t_add")
async def a_t_add(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tests"):
        return
    await state.clear()
    await safe_edit(call, "🧩 Javoblar kalitini yuboring (faqat A/B/C/D), masalan: ABCDABCD", kb_home_admin(call.from_user.id))
    await state.set_state(AState.t_keys)

@router.message(AState.t_keys)
async def a_t_keys(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tests"):
        await state.clear()
        return
    keys = (message.text or "").upper().strip().replace(" ", "")
    if not keys or any(ch not in "ABCD" for ch in keys):
        await message.answer("❌ Faqat A/B/C/D bo‘lsin. Qayta yuboring:")
        return
    await state.update_data(keys=keys)
    await message.answer("⏳ Test davomiyligi (minut) ni kiriting:")
    await state.set_state(AState.t_minutes)

async def kb_assign_builder(test_id: str, selected: set, is_public: int) -> InlineKeyboardMarkup:
    conn = db()
    groups = conn.execute("SELECT id, name FROM groups ORDER BY id DESC").fetchall()
    conn.close()

    rows = []
    pub_icon = "🌐✅" if is_public else "🌐❌"
    rows.append([InlineKeyboardButton(text=f"{pub_icon} Public", callback_data=f"a:t_pub:{test_id}")])
    for g in groups:
        gid = int(g["id"])
        mark = "✅" if gid in selected else "➖"
        rows.append([InlineKeyboardButton(text=f"{mark} {g['name'][:18]}", callback_data=f"a:t_g:{test_id}:{gid}")])
    rows.append([InlineKeyboardButton(text="💾 Saqlash", callback_data=f"a:t_save:{test_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data="a:tests")])
    rows.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@router.message(AState.t_minutes)
async def a_t_minutes(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tests"):
        await state.clear()
        return
    mins = safe_int((message.text or "").strip())
    if mins is None or mins < 1:
        await message.answer("❌ Minut raqam bo‘lsin. Qayta kiriting:")
        return

    data = await state.get_data()
    keys = data["keys"]
    tid = gen_test_id_5()
    deadline = (datetime.now() + timedelta(minutes=mins)).strftime("%Y-%m-%d %H:%M")

    conn = db()
    conn.execute("""INSERT INTO tests(test_id, keys, status, deadline, created_at, is_public)
                    VALUES (?,?,?,?,?,0)""", (tid, keys, "active", deadline, now_str()))
    conn.commit()
    conn.close()

    await state.update_data(tid=tid, selected=set(), is_public=0)
    kb = await kb_assign_builder(tid, set(), 0)
    await message.answer(
        f"✅ Test yaratildi: <b>{tid}</b>\nSavollar: <b>{len(keys)}</b>\nDeadline: <code>{deadline}</code>\n\n"
        f"Endi testni Public yoki guruh(lar)ga biriktiring:",
        reply_markup=kb
    )
    await state.set_state(AState.t_assign)

@router.callback_query(AState.t_assign, F.data.startswith("a:t_pub:"))
async def a_t_pub(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    st, _ = ensure_deadline(tid)
    if st == "finished":
        await call.answer("Yakunlangan testni o‘zgartirib bo‘lmaydi.", show_alert=True)
        return
    data = await state.get_data()
    is_public = 0 if int(data.get("is_public", 0)) == 1 else 1
    await state.update_data(is_public=is_public)
    kb = await kb_assign_builder(tid, set(data.get("selected", set())), is_public)
    await safe_edit(call, call.message.text, kb)

@router.callback_query(AState.t_assign, F.data.startswith("a:t_g:"))
async def a_t_toggle_group(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tests"):
        return
    _, _, tid, gid = call.data.split(":")
    gid = int(gid)
    data = await state.get_data()
    selected = set(data.get("selected", set()))
    is_public = int(data.get("is_public", 0))
    if gid in selected:
        selected.remove(gid)
    else:
        selected.add(gid)
    await state.update_data(selected=selected)
    kb = await kb_assign_builder(tid, selected, is_public)
    await safe_edit(call, call.message.text, kb)

@router.callback_query(AState.t_assign, F.data.startswith("a:t_save:"))
async def a_t_assign_save(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    data = await state.get_data()
    selected = set(data.get("selected", set()))
    is_public = int(data.get("is_public", 0))

    conn = db()
    conn.execute("UPDATE tests SET is_public=? WHERE test_id=?", (is_public, tid))
    conn.execute("DELETE FROM test_groups WHERE test_id=?", (tid,))
    for gid in selected:
        conn.execute("INSERT OR IGNORE INTO test_groups(test_id, group_id) VALUES (?,?)", (tid, gid))
    conn.commit()
    conn.close()

    await state.clear()
    await safe_edit(call, f"✅ Test <b>{tid}</b> saqlandi.\nPublic: <b>{'ON' if is_public else 'OFF'}</b>\nGuruhlar: <b>{', '.join(map(str, selected)) if selected else 'yo‘q'}</b>",
                    kb_admin_home(call.from_user.id))

# =========================
# ADMIN: Group Tests list (inside group)
# =========================
@router.callback_query(F.data.startswith("a:g_tests:"))
async def a_g_tests(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    tests = conn.execute("""
        SELECT t.test_id, t.status, t.deadline, COALESCE(t.is_public,0) as is_public
        FROM tests t
        LEFT JOIN test_groups tg ON tg.test_id=t.test_id
        WHERE tg.group_id=? OR COALESCE(t.is_public,0)=1
        GROUP BY t.test_id
        ORDER BY t.created_at DESC
        LIMIT 30
    """, (gid,)).fetchall()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    kb_rows = []
    for t in tests:
        st, _ = ensure_deadline(t["test_id"])
        icon = "🟢" if st == "active" else "⏸" if st == "paused" else "🏁"
        kb_rows.append([InlineKeyboardButton(text=f"{icon} {t['test_id']}", callback_data=f"a:t:{t['test_id']}")])
    kb_rows.append([InlineKeyboardButton(text="➕ Test yaratish", callback_data="a:t_add")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, f"🧪 <b>{safe_pdf_text(g['name'])}</b> — Testlar", InlineKeyboardMarkup(inline_keyboard=kb_rows))

# =========================
# ADMIN: Test options + rating (text+pdf)
# =========================
@router.callback_query(F.data.startswith("a:t:"))
async def a_t_opt(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    st, dl = ensure_deadline(tid)
    if st is None:
        await call.answer("Test topilmadi.", show_alert=True)
        return

    conn = db()
    row = conn.execute("SELECT COALESCE(is_public,0) as p FROM tests WHERE test_id=?", (tid,)).fetchone()
    groups = conn.execute("SELECT group_id FROM test_groups WHERE test_id=? ORDER BY group_id", (tid,)).fetchall()
    conn.close()

    is_public = int(row["p"]) if row else 0
    grp_list = ", ".join(str(int(g["group_id"])) for g in groups) if groups else "yo‘q"

    kb_rows = []
    if st == "active":
        kb_rows.append([InlineKeyboardButton(text="⏸ Pauza", callback_data=f"a:t_pause:{tid}")])
    if st == "paused":
        kb_rows.append([InlineKeyboardButton(text="▶️ Davom", callback_data=f"a:t_resume:{tid}")])
    if st != "finished":
        kb_rows.append([InlineKeyboardButton(text="🏁 Yakunlash", callback_data=f"a:t_finish:{tid}")])
    kb_rows.append([InlineKeyboardButton(text="🏆 Reyting (text)", callback_data=f"a:t_rate:{tid}")])
    kb_rows.append([InlineKeyboardButton(text="📥 Reyting (PDF)", callback_data=f"a:t_pdf:{tid}")])
    if st != "finished":
        kb_rows.append([InlineKeyboardButton(text="🔁 Biriktirish", callback_data=f"a:t_reassign:{tid}")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data="a:tests"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    text = (f"⚙️ <b>Test</b>: <code>{tid}</code>\n"
            f"Holat: <b>{st}</b>\n"
            f"Deadline: <code>{dl}</code>\n"
            f"Public: <b>{'ON' if is_public else 'OFF'}</b>\n"
            f"Guruhlar: <code>{grp_list}</code>\n\n"
            f"📌 PDF faqat test yakunlanganda ma’qul (ammo bu yerda har doim ochiladi).")
    await safe_edit(call, text, InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("a:t_pause:"))
async def a_t_pause(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    conn = db()
    conn.execute("UPDATE tests SET status='paused' WHERE test_id=?", (tid,))
    conn.commit(); conn.close()
    await call.answer("Pauza", show_alert=True)
    await a_t_opt(call)

@router.callback_query(F.data.startswith("a:t_resume:"))
async def a_t_resume(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    st, _ = ensure_deadline(tid)
    if st == "finished":
        await call.answer("Yakunlangan testni davom ettirib bo‘lmaydi.", show_alert=True)
        return
    conn = db()
    conn.execute("UPDATE tests SET status='active' WHERE test_id=?", (tid,))
    conn.commit(); conn.close()
    await call.answer("Davom", show_alert=True)
    await a_t_opt(call)

@router.callback_query(F.data.startswith("a:t_finish:"))
async def a_t_finish(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    conn = db()
    conn.execute("UPDATE tests SET status='finished' WHERE test_id=?", (tid,))
    conn.commit(); conn.close()
    await call.answer("Yakunlandi", show_alert=True)
    await a_t_opt(call)

@router.callback_query(F.data.startswith("a:t_rate:"))
async def a_t_rate(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    st, dl = ensure_deadline(tid)

    conn = db()
    rows = conn.execute("""SELECT full_name, score, total, percent, date
                           FROM results WHERE test_id=?
                           ORDER BY percent DESC, score DESC""", (tid,)).fetchall()
    conn.close()
    if not rows:
        await call.answer("Natija yo‘q.", show_alert=True)
        return

    text = f"🏆 <b>Reyting</b> — <code>{tid}</code>\nHolat: <b>{st}</b> | ⏰ <code>{dl}</code>\n\n"
    for i, r in enumerate(rows, 1):
        text += f"{i}. {safe_pdf_text(r['full_name'])} — <b>{r['percent']:.1f}%</b> | {to_uz_time_str(r['date'])}\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 PDF", callback_data=f"a:t_pdf:{tid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:t:{tid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    await safe_edit(call, text, kb)

@router.callback_query(F.data.startswith("a:t_reassign:"))
async def a_t_reassign(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    st, _ = ensure_deadline(tid)
    if st == "finished":
        await call.answer("Yakunlangan testni biriktirib bo‘lmaydi.", show_alert=True)
        return

    conn = db()
    grp = conn.execute("SELECT group_id FROM test_groups WHERE test_id=?", (tid,)).fetchall()
    pub = conn.execute("SELECT COALESCE(is_public,0) as p FROM tests WHERE test_id=?", (tid,)).fetchone()
    conn.close()
    selected = {int(x["group_id"]) for x in grp}
    is_public = int(pub["p"]) if pub else 0

    await state.clear()
    await state.update_data(tid=tid, selected=selected, is_public=is_public)
    kb = await kb_assign_builder(tid, selected, is_public)
    await safe_edit(call, "🔁 Biriktirishni yangilang:", kb)
    await state.set_state(AState.t_assign)

# =========================
# GROUP RESULTS: manual + import (inside group)
# =========================
@router.callback_query(F.data.startswith("a:g_results:"))
async def a_g_results(call: CallbackQuery):
    if not await guard(call, "results"):
        return
    gid = int(call.data.split(":")[2])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Manual natija", callback_data=f"a:m_start:{gid}")],
        [InlineKeyboardButton(text="📥 Import natija", callback_data=f"a:imp_start:{gid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    await safe_edit(call, "📥 <b>Natijalar</b>\nManual yoki Import tanlang:", kb)

@router.callback_query(F.data.startswith("a:m_start:"))
async def a_m_start(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "results"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid)
    await safe_edit(call, "📝 Manual: Test ID kiriting (masalan: 12345):", kb_home_admin(call.from_user.id))
    await state.set_state(AState.m_tid)

@router.message(AState.m_tid)
async def a_m_tid(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "results"):
        await state.clear()
        return
    tid = (message.text or "").strip()
    await state.update_data(tid=tid)
    await message.answer("Jami savollar soni (total) ni kiriting:")
    await state.set_state(AState.m_total)

@router.message(AState.m_total)
async def a_m_total(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "results"):
        await state.clear()
        return
    total = safe_int((message.text or "").strip())
    if total is None or total < 1:
        await message.answer("❌ Total raqam bo‘lsin.")
        return
    data = await state.get_data()
    gid = int(data["gid"])
    students = group_students(gid)
    if not students:
        await message.answer("Guruhda o‘quvchi yo‘q.")
        await state.clear()
        return
    await state.update_data(total=total, students=students)
    preview = "\n".join([f"{i+1}. {nm}" for i, (_uid, nm) in enumerate(students)])
    await message.answer(
        f"✅ Endi ballarni ketma-ket yuboring.\n"
        f"O‘quvchilar: <b>{len(students)}</b>\n\n{safe_pdf_text(preview)}\n\n"
        f"Format: 10 9 8 ... (bo‘shliq bilan).",
    )
    await state.set_state(AState.m_scores)

@router.message(AState.m_scores)
async def a_m_scores(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "results"):
        await state.clear()
        return
    data = await state.get_data()
    gid = int(data["gid"])
    tid = data["tid"]
    total = int(data["total"])
    students = data["students"]

    parts = re.split(r"[,\s]+", (message.text or "").strip())
    scores = [int(p) for p in parts if p.isdigit()]
    if len(scores) != len(students):
        await message.answer(f"❌ Ballar soni mos emas. Kerak: {len(students)}, Siz: {len(scores)}")
        return

    conn = db()
    dt = now_str()
    for idx, (uid, nm) in enumerate(students):
        sc = scores[idx]
        pct = (sc / total) * 100 if total else 0.0
        conn.execute("""INSERT INTO results(user_id, test_id, score, total, percent, date, full_name)
                        VALUES (?,?,?,?,?,?,?)""", (uid, tid, sc, total, pct, dt, nm))
    conn.commit()
    conn.close()

    await state.clear()
    await message.answer(f"✅ Manual natijalar saqlandi.\nTest: <code>{tid}</code>\nGuruh: <code>{gid}</code>", reply_markup=kb_admin_home(message.from_user.id))

@router.callback_query(F.data.startswith("a:imp_start:"))
async def a_imp_start(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "results"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid)
    await safe_edit(call, "📥 Import: Test ID kiriting (natijalar DBda bo‘lishi kerak):", kb_home_admin(call.from_user.id))
    await state.set_state(AState.imp_tid)

@router.message(AState.imp_tid)
async def a_imp_tid(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "results"):
        await state.clear()
        return
    tid = (message.text or "").strip()
    data = await state.get_data()
    gid = int(data["gid"])

    # Import = show rating for that group & test (no duplication logic here)
    conn = db()
    ids = conn.execute("SELECT user_id FROM members WHERE group_id=?", (gid,)).fetchall()
    user_ids = [int(x["user_id"]) for x in ids]
    if not user_ids:
        conn.close()
        await message.answer("Guruh bo‘sh.")
        await state.clear()
        return

    q = ",".join(["?"] * len(user_ids))
    rows = conn.execute(f"""
        SELECT full_name, percent, date
        FROM results
        WHERE test_id=? AND user_id IN ({q})
        ORDER BY percent DESC
    """, (tid, *user_ids)).fetchall()
    conn.close()

    if not rows:
        await message.answer("Bu guruhda bu test bo‘yicha natija topilmadi.")
        await state.clear()
        return

    text = f"✅ Import topildi.\nTest: <code>{tid}</code>\nGuruh: <code>{gid}</code>\nNatija: <b>{len(rows)}</b> ta\n\n"
    for i, r in enumerate(rows[:15], 1):
        text += f"{i}. {safe_pdf_text(r['full_name'])} — {r['percent']:.1f}%\n"
    if len(rows) > 15:
        text += f"... yana {len(rows)-15} ta"

    await state.clear()
    await message.answer(text, reply_markup=kb_admin_home(message.from_user.id))

# =========================
# TASKS (inside group) — create draft, allow description+media in same message, publish alerts
# =========================
@router.callback_query(F.data.startswith("a:g_tasks:"))
async def a_g_tasks(call: CallbackQuery):
    if not await guard(call, "tasks"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    tasks = conn.execute("""SELECT id, title, due_at, status FROM tasks
                            WHERE group_id=? ORDER BY id DESC LIMIT 20""", (gid,)).fetchall()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    kb_rows = [[InlineKeyboardButton(text="➕ Vazifa yaratish", callback_data=f"a:task_new:{gid}")]]
    for t in tasks:
        st = t["status"]
        icon = "🟡" if st == "draft" else "🟢" if st == "published" else "🏁"
        kb_rows.append([InlineKeyboardButton(text=f"{icon} {t['title'][:18]}", callback_data=f"a:task_v:{gid}:{t['id']}")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, f"📌 <b>{safe_pdf_text(g['name'])}</b> — Vazifalar", InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("a:task_new:"))
async def a_task_new(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tasks"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid, media=[])
    await safe_edit(call, "🆕 Vazifa nomini kiriting:", kb_home_admin(call.from_user.id))
    await state.set_state(AState.task_title)

@router.message(AState.task_title)
async def a_task_title(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tasks"):
        await state.clear()
        return
    title = (message.text or "").strip()
    if len(title) < 2:
        await message.answer("Nom juda qisqa. Qayta kiriting:")
        return
    await state.update_data(title=title)
    await message.answer("📝 Endi <b>description</b> yuboring.\n"
                         "Bu joyga matn ham, photo/video/audio/document ham yuborsangiz bo‘ladi.\n"
                         "Agar yana media qo‘shmoqchi bo‘lsangiz, ketma-ket yuboring.\n"
                         "Tugatish uchun: /done")
    await state.set_state(AState.task_desc_media)

@router.message(AState.task_desc_media)
async def a_task_desc_media(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tasks"):
        await state.clear()
        return

    if (message.text or "").strip().lower() == "/done":
        await message.answer("💯 Vazifa ballini kiriting (masalan: 10):")
        await state.set_state(AState.task_points)
        return

    data = await state.get_data()
    desc = data.get("desc", "")
    media = data.get("media", [])

    # collect text
    if message.text:
        desc = (desc + "\n" + message.text.strip()).strip()

    # collect media (file_id)
    def add_media(ftype: str, fid: str):
        media.append({"type": ftype, "file_id": fid})

    if message.photo:
        add_media("photo", message.photo[-1].file_id)
    elif message.video:
        add_media("video", message.video.file_id)
    elif message.document:
        add_media("document", message.document.file_id)
    elif message.audio:
        add_media("audio", message.audio.file_id)
    elif message.voice:
        add_media("voice", message.voice.file_id)

    await state.update_data(desc=desc, media=media)
    await message.answer("✅ Qabul qilindi. Yana qo‘shing yoki /done bosing.")

@router.message(AState.task_points)
async def a_task_points(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tasks"):
        await state.clear()
        return
    points = safe_int((message.text or "").strip())
    if points is None or points < 1:
        await message.answer("❌ 1 dan katta raqam kiriting.")
        return
    await state.update_data(points=points)
    await message.answer("⏰ Deadline kiriting (YYYY-MM-DD HH:MM), masalan: 2026-02-20 18:00")
    await state.set_state(AState.task_due)

@router.message(AState.task_due)
async def a_task_due(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tasks"):
        await state.clear()
        return
    due_s = (message.text or "").strip()
    try:
        parse_dt(due_s)
    except:
        await message.answer("❌ Format xato. Masalan: 2026-02-20 18:00")
        return

    data = await state.get_data()
    gid = int(data["gid"])
    title = data["title"]
    desc = data.get("desc", "")
    points = int(data["points"])
    media = data.get("media", [])

    conn = db()
    cur = conn.execute("""INSERT INTO tasks(group_id, title, description, points, due_at, created_at, status)
                          VALUES (?,?,?,?,?,?, 'draft')""",
                       (gid, title, desc, points, due_s, now_str()))
    task_id = cur.lastrowid
    for m in media:
        conn.execute("""INSERT INTO task_media(task_id, file_type, file_id) VALUES (?,?,?)""",
                     (task_id, m["type"], m["file_id"]))
    conn.commit()
    conn.close()

    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📣 Publish", callback_data=f"a:task_pub:{gid}:{task_id}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g_tasks:{gid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    await message.answer(
        f"✅ Vazifa draft saqlandi.\n"
        f"Vazifa: <b>{safe_pdf_text(title)}</b>\n"
        f"Ball: <b>{points}</b>\n"
        f"Deadline: <code>{due_s}</code>\n\n"
        f"Endi publish qiling:",
        reply_markup=kb
    )

@router.callback_query(F.data.startswith("a:task_v:"))
async def a_task_view(call: CallbackQuery):
    if not await guard(call, "tasks"):
        return
    _, _, gid, tid = call.data.split(":")
    gid = int(gid); tid = int(tid)

    conn = db()
    t = conn.execute("SELECT * FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    conn.close()
    if not t:
        await call.answer("Vazifa topilmadi.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📣 Publish", callback_data=f"a:task_pub:{gid}:{tid}")],
        [InlineKeyboardButton(text="📥 Submissions", callback_data=f"a:task_subs:{gid}:{tid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g_tasks:{gid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    text = (f"📌 <b>{safe_pdf_text(t['title'])}</b>\n"
            f"Status: <b>{t['status']}</b>\n"
            f"Ball: <b>{t['points']}</b>\n"
            f"Deadline: <code>{t['due_at']}</code>\n\n"
            f"{safe_pdf_text(t['description'] or '')[:1500]}")
    await safe_edit(call, text, kb)

@router.callback_query(F.data.startswith("a:task_subs:"))
async def a_task_subs(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    _, _, gid_s, tid_s = call.data.split(":", 3)
    gid = int(gid_s); tid = int(tid_s)

    conn = db()
    # task title
    t = conn.execute("SELECT id, title, points FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    if not t:
        conn.close()
        await call.answer("Vazifa topilmadi.", show_alert=True)
        return

    subs = conn.execute("""SELECT ts.id AS id, ts.user_id, u.full_name, ts.submitted_at,
                                    COALESCE(ts.score, -1) AS score
                             FROM task_submissions ts
                             JOIN users u ON u.user_id=ts.user_id
                             WHERE ts.task_id=?
                             ORDER BY ts.submitted_at DESC""", (tid,)).fetchall()
    conn.close()

    rows = []
    for s in subs:
        score = int(s["score"])
        score_txt = "⏳ Baholanmagan" if score < 0 else f"⭐ {score}/{int(t['points'])}"
        rows.append([InlineKeyboardButton(text=f"👤 {s['full_name']} • {score_txt}", callback_data=f"a:task_sub_v:{s['id']}")])

    if not rows:
        rows.append([InlineKeyboardButton(text="(Topshiriqlar yo‘q)", callback_data="noop")])

    rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:task_v:{gid}:{tid}")])
    rows.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, f"📨 <b>Topshiriqlar</b>\nVazifa: <b>{safe_pdf_text(t['title'])}</b>", InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data.startswith("a:task_view:"))
async def a_task_view_redirect(call: CallbackQuery):
    """Back-button helper: open group menu from task context."""
    if not await guard_call(call, "tasks"):
        return
    try:
        gid = int(call.data.split(":")[-1])
    except Exception:
        await call.answer("Noto‘g‘ri so‘rov.", show_alert=True)
        return
    # redirect to group panel if exists, else home
    await call.answer()
    # Prefer existing group view callback
    try:
        # emulate click to existing handler by editing message with button to group
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 Guruhga qaytish", callback_data=f"a:g:{gid}")],
            [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")]
        ])
        await safe_edit(call, "⬅️ Qayerga qaytasiz?", kb)
    except Exception:
        await safe_edit(call, "🏠 Menyu", kb_home_admin(call.from_user.id))

@router.callback_query(F.data.startswith("a:task_grade:"))
async def a_task_grade_start(call: CallbackQuery, state: FSMContext):
    if not await guard_call(call, "tasks"):
        return
    # Accept: a:task_grade:<sub_id>  (preferred)
    parts = call.data.split(":")
    try:
        sub_id = int(parts[-1])
    except:
        await call.answer("Noto‘g‘ri so‘rov.", show_alert=True)
        return

    conn = db()
    sub = conn.execute("""
        SELECT ts.id,
               t.group_id AS group_id,
               ts.task_id,
               ts.user_id,
               u.full_name,
               COALESCE(ts.score, -1) AS score,
               COALESCE(t.points, 0)  AS max_score
        FROM task_submissions ts
        LEFT JOIN tasks t ON t.id=ts.task_id
        LEFT JOIN users u ON u.user_id=ts.user_id
        WHERE ts.id=?
    """, (sub_id,)).fetchone()
    conn.close()
    if not sub:
        await call.answer("Topshiriq topilmadi.", show_alert=True)
        return

    await state.update_data(grade_sub_id=sub_id)

    # max points from tasks.points
    max_points = int(sub["max_score"] or 0)
    ggid = int(sub["group_id"] or 0)
    ttid = int(sub["task_id"] or 0)
    student_name = sub["full_name"] or str(sub["user_id"])

    await safe_edit(
        call,
        "📝 <b>Baholash</b>\n"
        f"👤 {escape_html(student_name)}\n"
        f"⭐ Maks: {max_points}\n\n"
        "Ball kiriting (0..maks):",
        kb_back_home(f"a:task_subs:{ggid}:{ttid}")
    )
    await state.set_state(AState.grade_score)


@router.message(AState.grade_score)
async def a_task_grade_save(message: Message, state: FSMContext):
    if not await guard_msg(message, "tasks"):
        await state.clear()
        return

    data = await state.get_data()
    sub_id = data.get("grade_sub_id")
    if not sub_id:
        await message.answer("Holat topilmadi. Qayta urinib ko‘ring.")
        await state.clear()
        return

    try:
        score = int((message.text or "").strip())
    except:
        await message.answer("Ball raqam bo‘lishi kerak. Masalan: 7")
        return

    conn = db()
    sub = conn.execute("""
        SELECT ts.id, ts.task_id, ts.user_id, COALESCE(u.full_name,''), COALESCE(t.points,0)
        FROM task_submissions ts
        LEFT JOIN tasks t ON t.id=ts.task_id
        LEFT JOIN users u ON u.user_id=ts.user_id
        WHERE ts.id=?
    """, (sub_id,)).fetchone()
    if not sub:
        conn.close()
        await message.answer("Topshiriq topilmadi.")
        await state.clear()
        return

    task_id = int(sub[1]); user_id = int(sub[2]); full_name = sub[3] or str(user_id)
    max_score = int(sub[4] or 0)
    if max_score < 0:
        max_score = 0
    if score < 0 or (max_score > 0 and score > max_score):
        await message.answer(f"Ball 0..{max_score} oralig‘ida bo‘lsin.")
        conn.close()
        return

    conn.execute("UPDATE task_submissions SET score=?, graded_at=?, graded_by=? WHERE id=?",
                 (score, now_str(), message.from_user.id, sub_id))
    conn.commit()
    conn.close()

    # Notify student (Telegram)
    try:
        await bot.send_message(
            user_id,
            f"✅ <b>Topshiriq baholandi</b>\n"
            f"🧑‍🎓 {full_name}\n"
            f"⭐ Ball: <b>{score}</b>/{max_score}\n"
            f"📌 Topshiriq ID: <code>{task_id}</code>"
        )
    except:
        pass

    log_admin(message.from_user.id, "task_grade", {"sub_id": sub_id, "task_id": task_id, "user_id": user_id, "score": score})

    await message.answer("✅ Baholandi.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁️ Ko‘rish", callback_data=f"a:task_sub_v:{sub_id}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")]
    ]))
    await state.clear()



@router.message(AState.grade_feedback)
async def a_task_grade_finish(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    gid = int(data["gid"]); tid = int(data["tid"]); uid = int(data["uid"])
    score = int(data["score"])
    fb = message.text.strip()
    if fb == "-":
        fb = ""

    conn = db()
    t = conn.execute("SELECT title, points FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    u = conn.execute("SELECT full_name FROM users WHERE user_id=?", (uid,)).fetchone()
    if not t or not u:
        conn.close()
        await state.clear()
        await message.answer("Topilmadi.", reply_markup=kb_home_admin())
        return

    conn.execute("""UPDATE task_submissions
                    SET score=?, feedback=?, graded_by=?, graded_at=?
                    WHERE task_id=? AND user_id=?""", (score, fb, message.from_user.id, now_str(), tid, uid))
    conn.commit()
    conn.close()

    # Notify student (Telegram message)
    try:
        msg = (f"✅ <b>Vazifa baholandi</b>\n"
               f"📌 {safe_pdf_text(t['title'])}\n"
               f"⭐ Ball: <b>{score}/{int(t['points'])}</b>")
        if fb:
            msg += f"\n💬 Izoh: {safe_pdf_text(fb)}"
        await bot.send_message(uid, msg)
    except Exception:
        pass

    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:task_sub_v:{sub_id}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")]])
    await message.answer("✅ Saqlandi va o‘quvchiga yuborildi.", reply_markup=kb)


@router.callback_query(F.data.startswith("a:task_pub:"))
async def a_task_publish(call: CallbackQuery):
    if not await guard(call, "tasks"):
        return
    _, _, gid, tid = call.data.split(":")
    gid = int(gid); tid = int(tid)

    conn = db()
    t = conn.execute("SELECT * FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    if not t:
        conn.close()
        await call.answer("Vazifa topilmadi.", show_alert=True)
        return
    conn.execute("UPDATE tasks SET status='published' WHERE id=?", (tid,))
    # alert members
    members = conn.execute("SELECT user_id FROM members WHERE group_id=?", (gid,)).fetchall()
    conn.commit()
    conn.close()

    sent = 0
    for r in members:
        uid = int(r["user_id"])
        try:
            await call.bot.send_message(
                uid,
                f"📢 <b>Yangi vazifa!</b>\n"
                f"Guruh: <b>{safe_pdf_text(get_group_name(gid))}</b>\n"
                f"Vazifa: <b>{safe_pdf_text(t['title'])}</b>\n"
                f"Ball: <b>{t['points']}</b>\n"
                f"Deadline: <code>{t['due_at']}</code>\n\n"
                f"Vazifani topshirish uchun: Guruhlarim → Guruh → Vazifalar"
            )
            sent += 1
        except:
            pass

    await call.answer(f"Publish ✅ (alert: {sent})", show_alert=True)
    await a_task_view(call)

def get_group_name(gid: int) -> str:
    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    return g["name"] if g else str(gid)

# USER: tasks list + submit
@router.callback_query(F.data.startswith("u:tasks:"))
async def u_tasks(call: CallbackQuery):
    uid = call.from_user.id
    gid = int(call.data.split(":")[2])

    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    tasks = conn.execute("""SELECT id, title, due_at, points
                            FROM tasks WHERE group_id=? AND status='published'
                            ORDER BY id DESC LIMIT 20""", (gid,)).fetchall()
    conn.close()
    if not mem or not g:
        await call.answer("Bu guruh sizniki emas.", show_alert=True)
        return

    kb_rows = []
    for t in tasks:
        kb_rows.append([InlineKeyboardButton(
            text=f"📝 {t['title'][:18]}",
            callback_data=f"u:task_v:{gid}:{t['id']}"
        )])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"u:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")])

    await safe_edit(call, f"📌 <b>{safe_pdf_text(g['name'])}</b> — Vazifalar", InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("u:task_v:"))
async def u_task_view(call: CallbackQuery):
    uid = call.from_user.id
    _, _, gid, tid = call.data.split(":")
    gid = int(gid); tid = int(tid)

    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    t = conn.execute("SELECT * FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    sub = conn.execute("SELECT score, submitted_at FROM task_submissions WHERE task_id=? AND user_id=?", (tid, uid)).fetchone()
    conn.close()

    if not mem or not t:
        await call.answer("Topilmadi.", show_alert=True)
        return

    btns = []
    if sub:
        score = sub["score"]
        score_txt = f"✅ Yuborilgan | Ball: {score if score is not None else 'tekshirilmagan'}"
        btns.append([InlineKeyboardButton(text=score_txt, callback_data="noop")])
    else:
        btns.append([InlineKeyboardButton(text="📤 Vazifani yuborish", callback_data=f"u:task_send:{gid}:{tid}")])

    btns.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"u:tasks:{gid}")])
    btns.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")])

    text = (f"📌 <b>{safe_pdf_text(t['title'])}</b>\n"
            f"Ball: <b>{t['points']}</b>\n"
            f"Deadline: <code>{t['due_at']}</code>\n\n"
            f"{safe_pdf_text(t['description'] or '')[:1500]}\n\n"
            f"📎 Topshirish: istalgan format (text/photo/video/audio/document/voice).")
    await safe_edit(call, text, InlineKeyboardMarkup(inline_keyboard=btns))

@router.callback_query(F.data.startswith("u:task_send:"))
async def u_task_send(call: CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    _, _, gid, tid = call.data.split(":")
    gid = int(gid); tid = int(tid)

    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    sub = conn.execute("SELECT 1 FROM task_submissions WHERE task_id=? AND user_id=?", (tid, uid)).fetchone()
    t = conn.execute("SELECT due_at FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    conn.close()

    if not mem:
        await call.answer("Bu guruh sizniki emas.", show_alert=True)
        return
    if sub:
        await call.answer("Siz allaqachon yuborgansiz.", show_alert=True)
        return
    if not t:
        await call.answer("Vazifa topilmadi.", show_alert=True)
        return

    # deadline check
    try:
        if datetime.now() > parse_dt(t["due_at"]):
            await call.answer("Deadline o‘tgan. Topshirib bo‘lmaydi.", show_alert=True)
            return
    except:
        pass

    await state.clear()
    await state.update_data(task_gid=gid, task_id=tid)
    await safe_edit(call, "📤 Vazifani yuboring (istalgan format). Bekor: /cancel", kb_home_user())
    # reuse UState.solve_answers? create simple state:
    await state.set_state(UState.task_submit)  # reuse state for any content

@router.message(UState.task_submit)
async def u_task_receive_any(message: Message, state: FSMContext):
    data = await state.get_data()
    if "task_id" not in data:
        return  # this handler is also used by test submit in other flow; guarded there
    gid = int(data["task_gid"])
    tid = int(data["task_id"])
    uid = message.from_user.id

    # verify membership + not already
    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    sub = conn.execute("SELECT 1 FROM task_submissions WHERE task_id=? AND user_id=?", (tid, uid)).fetchone()
    t = conn.execute("SELECT due_at, title FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    conn.close()
    if not mem:
        await message.answer("Bu guruh sizniki emas.")
        await state.clear()
        return
    if sub:
        await message.answer("Siz allaqachon yuborgansiz.")
        await state.clear()
        return
    if not t:
        await message.answer("Vazifa topilmadi.")
        await state.clear()
        return
    try:
        if datetime.now() > parse_dt(t["due_at"]):
            await message.answer("Deadline o‘tgan.")
            await state.clear()
            return
    except:
        pass

    ensure_user(uid, message.from_user.full_name or "No Name")
    full_name = get_user_name(uid)

    # store full message json (for admin view)
    msg_json = message.model_dump_json(exclude_none=True)

    conn = db()
    cur = conn.execute(
        """INSERT INTO task_submissions(task_id, user_id, full_name, submitted_at, msg_json)
           VALUES (?,?,?,?,?)""",
        (tid, uid, full_name, now_str(), msg_json),
    )
    sub_id = int(cur.lastrowid or 0)
    conn.commit()

    # Notify admins to grade (tasks perm OR super)
    try:
        trow = conn.execute("SELECT group_id, title FROM tasks WHERE id=?", (tid,)).fetchone()
        gid = int(trow["group_id"]) if trow else 0
        ttitle = trow["title"] if trow else f"#{tid}"
        admin_rows = conn.execute(
            """SELECT a.user_id
                 FROM admins a
                 LEFT JOIN admin_permissions p
                   ON p.admin_id=a.user_id AND p.perm='tasks'
                 WHERE a.role='super' OR COALESCE(p.enabled,0)=1"""
        ).fetchall()
        admin_ids = [int(r["user_id"]) for r in admin_rows] if admin_rows else []
        if SUPER_ADMIN_ID not in admin_ids:
            admin_ids.append(SUPER_ADMIN_ID)

        alert_txt = (
            "🆕 <b>Yangi vazifa yuborildi</b>\n"
            f"👤 O‘quvchi: <b>{escape_html(full_name)}</b>\n"
            f"📌 Vazifa: <b>{escape_html(ttitle)}</b>\n"
            f"🆔 Sub ID: <code>{sub_id}</code>\n"
            "Baholang 👇"
        )
        alert_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👁️ Ko‘rish / Baholash", callback_data=f"a:task_sub_v:{sub_id}")],
        ])

        for aid in admin_ids:
            try:
                await bot.send_message(aid, alert_txt, reply_markup=alert_kb)
            except Exception:
                pass

        # Also notify the group's Telegram chat if linked (optional)
        try:
            ginfo = conn.execute("SELECT tg_chat_id FROM groups WHERE id=?", (gid,)).fetchone()
            tg_chat_id = int(ginfo["tg_chat_id"]) if ginfo and ginfo["tg_chat_id"] else 0
        except Exception:
            tg_chat_id = 0

        if tg_chat_id:
            try:
                await bot.send_message(tg_chat_id, alert_txt, reply_markup=alert_kb)
            except Exception:
                pass
    except Exception:
                pass
    except Exception:
        pass

    conn.close()

    await state.clear()
    await message.answer("✅ Vazifa qabul qilindi. Tekshiruvdan so‘ng ball qo‘yiladi.", reply_markup=kb_user_home())

# =========================
# BACKGROUND: enforce kick limits for missed tasks
# =========================
async def enforce_kick_limits(bot: Bot):
    """
    If task published and due passed, and user didn't submit => missed_task_count++
    If missed_task_count >= limit => remove + kick from tg group
    """
    conn = db()
    # published tasks past due
    tasks = conn.execute("""
        SELECT id, group_id, due_at
        FROM tasks
        WHERE status='published'
    """).fetchall()

    for t in tasks:
        try:
            due = parse_dt(t["due_at"])
        except:
            continue
        if datetime.now() <= due:
            continue

        gid = int(t["group_id"])
        task_id = int(t["id"])

        # get members
        members = conn.execute("SELECT user_id FROM members WHERE group_id=?", (gid,)).fetchall()
        limit_row = conn.execute("SELECT tg_chat_id, task_miss_limit FROM groups WHERE id=?", (gid,)).fetchone()
        tg_chat_id = int(limit_row["tg_chat_id"]) if limit_row and limit_row["tg_chat_id"] else None
        lim = int(limit_row["task_miss_limit"]) if limit_row else 5

        for m in members:
            uid = int(m["user_id"])
            sub = conn.execute("SELECT 1 FROM task_submissions WHERE task_id=? AND user_id=?", (task_id, uid)).fetchone()
            if sub:
                continue

            # increment missed_task_count once per task per user: we can mark via a pseudo row in submissions? simplest: use attendance table? We'll use a special log table quickly:
            conn.execute("""CREATE TABLE IF NOT EXISTS task_miss_log(
                task_id INTEGER, group_id INTEGER, user_id INTEGER,
                UNIQUE(task_id, group_id, user_id)
            )""")
            already = conn.execute("SELECT 1 FROM task_miss_log WHERE task_id=? AND group_id=? AND user_id=?",
                                   (task_id, gid, uid)).fetchone()
            if already:
                continue

            conn.execute("INSERT OR IGNORE INTO task_miss_log(task_id, group_id, user_id) VALUES (?,?,?)",
                         (task_id, gid, uid))
            conn.execute("INSERT OR IGNORE INTO counters(group_id, user_id, absent_count, missed_task_count) VALUES (?,?,0,0)",
                         (gid, uid))
            conn.execute("UPDATE counters SET missed_task_count = missed_task_count + 1 WHERE group_id=? AND user_id=?",
                         (gid, uid))
            row = conn.execute("SELECT missed_task_count FROM counters WHERE group_id=? AND user_id=?",
                               (gid, uid)).fetchone()
            cnt = int(row["missed_task_count"]) if row else 0

            # alert DM
            try:
                await bot.send_message(uid, f"⚠️ Vazifa deadline o‘tdi va siz topshirmadingiz.\n"
                                            f"Jarima: <b>{cnt}/{lim}</b>\n"
                                            f"Agar limitdan oshsa guruhdan chiqarilasiz.")
            except:
                pass

            # kick if exceeded
            if cnt >= lim:
                conn.execute("DELETE FROM members WHERE group_id=? AND user_id=?", (gid, uid))
                if tg_chat_id:
                    try:
                        await bot.ban_chat_member(chat_id=tg_chat_id, user_id=uid)
                        await bot.unban_chat_member(chat_id=tg_chat_id, user_id=uid)
                    except:
                        pass
                try:
                    await bot.send_message(uid, "⛔️ Vazifalarni bajarmagani uchun guruhdan chiqarildingiz.")
                except:
                    pass

    conn.commit()
    conn.close()

# =========================
# GLOBAL BROADCAST (text + media)
# =========================
@router.callback_query(F.data == "a:broadcast")
async def a_broadcast(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "broadcast"):
        return
    await state.clear()
    await safe_edit(call, "📢 Barcha userlarga yuboriladigan xabarni yuboring (text yoki media). Bekor: /cancel", kb_home_admin(call.from_user.id))
    await state.set_state(AState.broadcast_any)

@router.message(AState.broadcast_any)
async def a_broadcast_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "broadcast"):
        await state.clear()
        return
    conn = db()
    users = conn.execute("SELECT user_id FROM users").fetchall()
    conn.close()

    sent = 0
    for r in users:
        uid = int(r["user_id"])
        try:
            # copy message (works for text & most media)
            await message.copy_to(chat_id=uid)
            sent += 1
        except:
            pass

    await state.clear()
    await message.answer(f"✅ Yuborildi: {sent} ta", reply_markup=kb_admin_home(message.from_user.id))

# =========================
# ADMIN: ADMINS (super only) minimal
# =========================
@router.callback_query(F.data == "a:admins")
async def a_admins(call: CallbackQuery):
    if not is_super(call.from_user.id):
        await call.answer("Faqat super admin.", show_alert=True)
        return
    conn = db()
    admins = conn.execute("SELECT user_id, role FROM admins ORDER BY role DESC").fetchall()
    conn.close()
    text = "👮 <b>Adminlar</b>\n\n" + "\n".join([f"• <code>{a['user_id']}</code> — {a['role']}" for a in admins])
    await safe_edit(call, text, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")]]))

# =========================
# CANCEL command
# =========================
@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if is_admin(uid):
        await message.answer("Bekor qilindi.", reply_markup=kb_admin_home(uid))
    else:
        await message.answer("Bekor qilindi.", reply_markup=kb_user_home())

# =========================
# STARTUP TASKS
# =========================
async def on_startup(bot: Bot):
    # periodic enforcement (kick limits, missed tasks, etc.)
    async def loop_kick():
        while True:
            try:
                await enforce_kick_limits(bot)
            except Exception:
                pass
            await asyncio.sleep(300)

    # daily DB backup to admins at 06:00 Asia/Samarkand
    async def loop_daily_backup():
        while True:
            try:
                wait_s = seconds_until_local_time("Asia/Samarkand", 6, 0)
                await asyncio.sleep(wait_s)
                await send_db_backup_to_admins(bot, reason="daily 06:00")
            except Exception:
                # if something fails, don't crash the bot
                await asyncio.sleep(300)

    asyncio.create_task(loop_kick())
    asyncio.create_task(loop_daily_backup())


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id

    # ensure user row exists
    conn = db()
    u = conn.execute("SELECT full_name FROM users WHERE user_id=?", (uid,)).fetchone()
    conn.close()

    if not u:
        # ask name (first time)
        await message.answer(
            "👋 Salom! Ism va familiyangizni kiriting (masalan: Ali Valiyev):"
        )
        await state.set_state(UState.reg_name)
        return

    # admin or user panel
    if is_admin(uid):
        await message.answer("⚙️ <b>Admin panel</b>", reply_markup=kb_admin_home(uid))
    else:
        await message.answer(f"👋 Salom, <b>{safe_pdf_text(u['full_name'])}</b>!", reply_markup=kb_user_home())

@router.message(UState.reg_name)
async def reg_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if len(name) < 3:
        await message.answer("Iltimos, ism-familiyani to‘liq yozing:")
        return
    ensure_user(message.from_user.id, name)
    await state.clear()
    await message.answer("✅ Saqlandi! Asosiy menyu:", reply_markup=kb_user_home())

# =========================
# USER HOME NAV
# =========================
@router.callback_query(F.data == "u:home")
async def u_home(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(call, "🏠 <b>Menyu</b>", kb_user_home())

@router.callback_query(F.data == "a:home")
async def a_home(call: CallbackQuery, state: FSMContext):
    await state.clear()
    uid = call.from_user.id
    if not is_admin(uid):
        await call.answer("Ruxsat yo‘q.", show_alert=True)
        return
    await safe_edit(call, "🏠 <b>Admin panel</b>", kb_admin_home(uid))

@router.callback_query(F.data == "a:as_user")
async def a_as_user(call: CallbackQuery, state: FSMContext):
    await state.clear()
    uid = call.from_user.id
    if not is_admin(uid):
        await call.answer("Ruxsat yo‘q.", show_alert=True)
        return
    kb = kb_user_home()
    kb.inline_keyboard.append([InlineKeyboardButton(text="🔙 Admin panel", callback_data="a:home")])
    await safe_edit(call, "👤 User rejimi", kb)

# =========================
# USER: join group
# =========================
@router.callback_query(F.data == "u:join")
async def u_join(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(call, "🔑 Guruh kodini kiriting (masalan: 1234AB):", kb_home_user())
    await state.set_state(UState.join_code)

@router.message(UState.join_code)
async def u_join_code(message: Message, state: FSMContext):
    code = (message.text or "").upper().strip()
    if not re.fullmatch(r"\d{4}[A-H]{2}", code):
        await message.answer("❌ Kod formati xato. Masalan: 1234AB")
        return
    uid = message.from_user.id
    ensure_user(uid, message.from_user.full_name or "No Name")

    conn = db()
    g = conn.execute("SELECT id, name FROM groups WHERE invite_code=?", (code,)).fetchone()
    if not g:
        conn.close()
        await message.answer("❌ Guruh topilmadi. Kodni tekshiring.")
        return
    exists = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (g["id"], uid)).fetchone()
    if not exists:
        conn.execute("INSERT INTO members(group_id, user_id) VALUES (?,?)", (g["id"], uid))
        conn.execute("INSERT OR IGNORE INTO counters(group_id, user_id, absent_count, missed_task_count) VALUES (?,?,0,0)",
                     (g["id"], uid))
        conn.commit()
    conn.close()

    await state.clear()
    await message.answer(f"✅ <b>{safe_pdf_text(g['name'])}</b> guruhiga qo‘shildingiz.", reply_markup=kb_user_home())

def user_groups(uid: int) -> List[Tuple[int, str]]:
    conn = db()
    rows = conn.execute("""
        SELECT g.id, g.name
        FROM members m JOIN groups g ON g.id=m.group_id
        WHERE m.user_id=?
        ORDER BY g.name
    """, (uid,)).fetchall()
    conn.close()
    return [(int(r["id"]), r["name"]) for r in rows]

# =========================
# USER: My groups & tests (INLINE)
# =========================
@router.callback_query(F.data == "u:mygroups")
async def u_mygroups(call: CallbackQuery):
    uid = call.from_user.id
    groups = user_groups(uid)
    if not groups:
        await safe_edit(call, "Siz hech qaysi guruhda emassiz.", kb_user_home())
        return

    kb_rows = []
    for gid, name in groups:
        kb_rows.append([InlineKeyboardButton(text=f"📌 {name}", callback_data=f"u:g:{gid}")])
    kb_rows.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")])
    await safe_edit(call, "📚 <b>Guruhlarim</b>\nGuruhni tanlang:", InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("u:g:"))
async def u_group_view(call: CallbackQuery):
    uid = call.from_user.id
    gid = int(call.data.split(":")[2])

    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not mem or not g:
        await call.answer("Bu guruh sizniki emas.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧪 Guruh testlari", callback_data=f"u:gt:{gid}")],
        [InlineKeyboardButton(text="📌 Vazifalar", callback_data=f"u:tasks:{gid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data="u:mygroups")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")],
    ])
    await safe_edit(call, f"📌 <b>{safe_pdf_text(g['name'])}</b>\nQuyidan bo‘lim tanlang:", kb)

# =========================
# USER: group tests list
# =========================
def tests_for_user_in_group(uid: int, gid: int) -> List[sqlite3.Row]:
    conn = db()
    # allowed: public OR assigned to this group
    rows = conn.execute("""
        SELECT t.test_id, t.status, t.deadline, COALESCE(t.is_public,0) AS is_public
        FROM tests t
        LEFT JOIN test_groups tg ON tg.test_id=t.test_id
        WHERE (COALESCE(t.is_public,0)=1) OR (tg.group_id=?)
        GROUP BY t.test_id
        ORDER BY t.created_at DESC
    """, (gid,)).fetchall()
    conn.close()
    return rows

@router.callback_query(F.data.startswith("u:gt:"))
async def u_group_tests(call: CallbackQuery):
    uid = call.from_user.id
    gid = int(call.data.split(":")[2])

    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not mem or not g:
        await call.answer("Bu guruh sizniki emas.", show_alert=True)
        return

    rows = tests_for_user_in_group(uid, gid)
    if not rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"u:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")]])
        await safe_edit(call, "Bu guruhda hozircha test yo‘q.", kb)
        return

    kb_rows = []
    for r in rows[:30]:
        status, dl = ensure_deadline(r["test_id"])
        icon = "🟢" if status == "active" else "⏸" if status == "paused" else "🏁"
        kb_rows.append([InlineKeyboardButton(
            text=f"{icon} {r['test_id']} ({status})",
            callback_data=f"u:solve_tid:{r['test_id']}"
        )])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"u:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")])
    await safe_edit(call, f"🧪 <b>{safe_pdf_text(g['name'])}</b> — Testlar:", InlineKeyboardMarkup(inline_keyboard=kb_rows))

# =========================
# USER: Solve test (by id from list or manual)
# =========================
@router.callback_query(F.data == "u:solve")
async def u_solve(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(call, "📝 Test ID kiriting (masalan: 12345):", kb_home_user())
    await state.set_state(UState.solve_tid)

@router.callback_query(F.data.startswith("u:solve_tid:"))
async def u_solve_from_button(call: CallbackQuery, state: FSMContext):
    await state.clear()
    tid = call.data.split(":")[2]
    await state.update_data(tid=tid)
    await safe_edit(call, f"📝 Test <code>{tid}</code>\nJavoblarni yuboring (A/B/C/D). Masalan: ABCDAB...", kb_home_user())
    await state.set_state(UState.solve_answers)

@router.message(UState.solve_tid)
async def u_solve_tid_msg(message: Message, state: FSMContext):
    tid = (message.text or "").strip()
    status, deadline = ensure_deadline(tid)
    if status is None:
        await message.answer("❌ Test topilmadi.")
        return
    if status == "paused":
        await message.answer("⏸ Test vaqtincha to‘xtatilgan.")
        await state.clear()
        return
    if status == "finished":
        await message.answer("🏁 Test yakunlangan (deadline o‘tgan yoki yakunlangan).")
        await state.clear()
        return

    # allow if public OR assigned to any of user's groups
    uid = message.from_user.id
    conn = db()
    pub = conn.execute("SELECT COALESCE(is_public,0) AS p FROM tests WHERE test_id=?", (tid,)).fetchone()
    if pub and int(pub["p"]) == 1:
        allowed = True
    else:
        gids = conn.execute("SELECT group_id FROM members WHERE user_id=?", (uid,)).fetchall()
        if not gids:
            allowed = False
        else:
            myg = [int(x["group_id"]) for x in gids]
            tg = conn.execute("SELECT group_id FROM test_groups WHERE test_id=?", (tid,)).fetchall()
            allowed_set = {int(x["group_id"]) for x in tg}
            allowed = any(g in allowed_set for g in myg)
    # anti-cheat
    already = conn.execute("SELECT 1 FROM submissions WHERE user_id=? AND test_id=?", (uid, tid)).fetchone()
    keys = conn.execute("SELECT keys FROM tests WHERE test_id=?", (tid,)).fetchone()
    conn.close()

    if not allowed:
        await message.answer("❌ Bu test sizga biriktirilmagan (public emas va guruhingizda yo‘q).")
        await state.clear()
        return
    if already:
        await message.answer("⚠️ Siz bu testni 1 marta topshirib bo‘lgansiz.")
        await state.clear()
        return
    if not keys:
        await message.answer("❌ Test topilmadi.")
        await state.clear()
        return

    await state.update_data(tid=tid, keys=keys["keys"])
    await message.answer(f"✅ Test topildi. Savollar: {len(keys['keys'])} ta.\nJavoblarni yuboring (A/B/C/D).")
    await state.set_state(UState.solve_answers)

@router.message(UState.solve_answers)
async def u_solve_answers(message: Message, state: FSMContext):
    data = await state.get_data()
    tid = data.get("tid")
    keys = data.get("keys", "")

    status, _ = ensure_deadline(tid)
    if status != "active":
        await message.answer("⛔️ Test tugagan yoki pauzada.")
        await state.clear()
        return

    ans = (message.text or "").upper().strip().replace(" ", "")
    if (not ans) or any(ch not in "ABCD" for ch in ans):
        await message.answer("⚠️ Faqat A/B/C/D bo‘lsin.")
        return
    if len(ans) != len(keys):
        await message.answer(f"⚠️ Javoblar soni {len(keys)} ta bo‘lishi kerak.")
        return

    uid = message.from_user.id
    ensure_user(uid, message.from_user.full_name or "No Name")
    full_name = get_user_name(uid)

    conn = db()
    # anti-cheat
    already = conn.execute("SELECT 1 FROM submissions WHERE user_id=? AND test_id=?", (uid, tid)).fetchone()
    if already:
        conn.close()
        await message.answer("⚠️ Siz bu testni topshirib bo‘lgansiz.")
        await state.clear()
        return

    score = sum(1 for a, k in zip(ans, keys) if a == k)
    total = len(keys)
    pct = (score / total) * 100 if total else 0.0

    conn.execute("""INSERT INTO submissions(user_id, test_id, answers, submitted_at)
                    VALUES (?,?,?,?)""", (uid, tid, ans, now_str()))
    conn.execute("""INSERT INTO results(user_id, test_id, score, total, percent, date, full_name)
                    VALUES (?,?,?,?,?,?,?)""", (uid, tid, score, total, pct, now_str(), full_name))
    conn.commit()
    conn.close()

    await state.clear()
    await message.answer(
        f"✅ <b>Natija</b>\nTest: <code>{tid}</code>\nBall: <b>{score}/{total}</b>\nFoiz: <b>{pct:.1f}%</b>",
        reply_markup=kb_user_home()
    )

# =========================
# USER: my results
# =========================
@router.callback_query(F.data == "u:myresults")
async def u_myresults(call: CallbackQuery):
    uid = call.from_user.id
    conn = db()
    rows = conn.execute("""SELECT test_id, score, total, percent, date
                           FROM results WHERE user_id=?
                           ORDER BY id DESC LIMIT 15""", (uid,)).fetchall()
    conn.close()
    if not rows:
        await safe_edit(call, "Sizda hali natija yo‘q.", kb_user_home())
        return

    text = "📄 <b>Natijalarim</b>\n\n"
    for i, r in enumerate(rows, 1):
        text += f"{i}) <code>{r['test_id']}</code> — <b>{r['score']}/{r['total']}</b> ({r['percent']:.1f}%) | {r['date']}\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")]
    ])
    await safe_edit(call, text, kb)

# =========================
# ADMIN: GROUPS LIST / CREATE / VIEW
# =========================
@router.callback_query(F.data == "a:groups")
async def a_groups(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    conn = db()
    groups = conn.execute("SELECT id, name, invite_code FROM groups ORDER BY id DESC").fetchall()
    conn.close()

    kb_rows = []
    for g in groups:
        kb_rows.append([InlineKeyboardButton(text=f"📁 {g['name']}", callback_data=f"a:g:{g['id']}")])
    kb_rows.append([InlineKeyboardButton(text="➕ Guruh yaratish", callback_data="a:g_add")])
    kb_rows.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, "👥 <b>Guruhlar</b>", InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data == "a:g_add")
async def a_g_add(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "groups"):
        return
    await state.clear()
    await safe_edit(call, "🆕 Guruh nomini kiriting:", kb_home_admin(call.from_user.id))
    await state.set_state(AState.g_name)

@router.message(AState.g_name)
async def a_g_add_save(message: Message, state: FSMContext):
    uid = message.from_user.id
    if not is_admin(uid) or not has_perm(uid, "groups"):
        await state.clear()
        return
    name = (message.text or "").strip()
    if len(name) < 2:
        await message.answer("Guruh nomi qisqa. Qayta kiriting:")
        return

    conn = db()
    code = None
    for _ in range(200):
        cand = gen_group_code()
        ex = conn.execute("SELECT 1 FROM groups WHERE invite_code=?", (cand,)).fetchone()
        if not ex:
            code = cand
            break
    if not code:
        conn.close()
        await message.answer("Kod yaratib bo‘lmadi.")
        await state.clear()
        return

    conn.execute("INSERT INTO groups(name, invite_code) VALUES (?,?)", (name, code))
    conn.commit()
    conn.close()

    await state.clear()
    await message.answer(f"✅ Guruh yaratildi: <b>{safe_pdf_text(name)}</b>\nKod: <code>{code}</code>",
                         reply_markup=kb_admin_home(uid))

@router.callback_query(F.data.startswith("a:g:"))
async def a_group_view(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    g = conn.execute("SELECT * FROM groups WHERE id=?", (gid,)).fetchone()
    cnt = conn.execute("SELECT COUNT(*) AS c FROM members WHERE group_id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👨‍🎓 O‘quvchilar", callback_data=f"a:g_students:{gid}")],
        [InlineKeyboardButton(text="🧪 Guruh testlari", callback_data=f"a:g_tests:{gid}")],
        [InlineKeyboardButton(text="📥 Natija (manual/import)", callback_data=f"a:g_results:{gid}")],
        [InlineKeyboardButton(text="🗓 Davomat", callback_data=f"a:g_att:{gid}")],
        [InlineKeyboardButton(text="📌 Vazifalar", callback_data=f"a:g_tasks:{gid}")],
        [InlineKeyboardButton(text="⚙️ Sozlamalar", callback_data=f"a:g_set:{gid}")],
        [InlineKeyboardButton(text="🔁 Kod yangilash", callback_data=f"a:g_regen:{gid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data="a:groups")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])

    text = (f"📁 <b>{safe_pdf_text(g['name'])}</b>\n"
            f"🔑 Kod: <code>{g['invite_code']}</code>\n"
            f"👨‍🎓 O‘quvchilar: <b>{int(cnt['c'])}</b>\n"
            f"📌 tg_chat_id: <code>{g['tg_chat_id'] if g['tg_chat_id'] else 'yo‘q'}</code>\n"
            f"🚪 Absent kick limit: <b>{g['att_absent_limit']}</b>\n"
            f"🚪 Task miss kick limit: <b>{g['task_miss_limit']}</b>\n")
    await safe_edit(call, text, kb)

@router.callback_query(F.data.startswith("a:g_regen:"))
async def a_group_regen(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    code = None
    for _ in range(200):
        cand = gen_group_code()
        ex = conn.execute("SELECT 1 FROM groups WHERE invite_code=?", (cand,)).fetchone()
        if not ex:
            code = cand
            break
    if not code:
        conn.close()
        await call.answer("Kod yaratib bo‘lmadi.", show_alert=True)
        return
    conn.execute("UPDATE groups SET invite_code=? WHERE id=?", (code, gid))
    conn.commit()
    conn.close()
    await call.answer("✅ Kod yangilandi", show_alert=True)
    # refresh view
    await a_group_view(call)
# =========================
# ADMIN: Group Students (list + remove)
# =========================
@router.callback_query(F.data.startswith("a:g_students:"))
async def a_g_students(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    g = conn.execute("SELECT name, tg_chat_id FROM groups WHERE id=?", (gid,)).fetchone()
    students = conn.execute("""
        SELECT u.user_id, u.full_name
        FROM members m JOIN users u ON u.user_id=m.user_id
        WHERE m.group_id=?
        ORDER BY u.full_name
    """, (gid,)).fetchall()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    text = f"👨‍🎓 <b>{safe_pdf_text(g['name'])}</b> — O‘quvchilar\n\n"
    kb_rows = []
    for i, s in enumerate(students, 1):
        text += f"{i}. {safe_pdf_text(s['full_name'])}\n"
        kb_rows.append([InlineKeyboardButton(text=f"❌ {s['full_name'][:18]}", callback_data=f"a:g_kick:{gid}:{s['user_id']}")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, text, InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("a:g_kick:"))
async def a_g_kick(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    _, _, gid, uid = call.data.split(":")
    gid = int(gid); uid = int(uid)

    conn = db()
    g = conn.execute("SELECT tg_chat_id FROM groups WHERE id=?", (gid,)).fetchone()
    conn.execute("DELETE FROM members WHERE group_id=? AND user_id=?", (gid, uid))
    conn.commit()
    conn.close()

    # kick from telegram group if chat_id set
    if g and g["tg_chat_id"]:
        try:
            await call.bot.ban_chat_member(chat_id=int(g["tg_chat_id"]), user_id=uid)
            await call.bot.unban_chat_member(chat_id=int(g["tg_chat_id"]), user_id=uid)
        except:
            pass

    await call.answer("Chiqarildi", show_alert=True)
    await a_g_students(call)

# =========================
# ADMIN: Group Settings
# =========================
@router.callback_query(F.data.startswith("a:g_set:"))
async def a_g_set(call: CallbackQuery):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    g = conn.execute("SELECT * FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 tg_chat_id sozlash", callback_data=f"a:gs_chat:{gid}")],
        [InlineKeyboardButton(text="🚪 Absent kick limit", callback_data=f"a:gs_att:{gid}")],
        [InlineKeyboardButton(text="🚪 Task miss kick limit", callback_data=f"a:gs_task:{gid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    text = (f"⚙️ <b>Sozlamalar</b>\nGuruh: <b>{safe_pdf_text(g['name'])}</b>\n\n"
            f"tg_chat_id: <code>{g['tg_chat_id'] if g['tg_chat_id'] else 'yo‘q'}</code>\n"
            f"Absent kick limit: <b>{g['att_absent_limit']}</b>\n"
            f"Task miss kick limit: <b>{g['task_miss_limit']}</b>\n\n"
            f"tg_chat_id — Telegram guruh ID (minus bilan), masalan: -1001234567890\n"
            f"Botni o‘sha TG guruhda admin qiling.")
    await safe_edit(call, text, kb)

@router.callback_query(F.data.startswith("a:gs_chat:"))
async def a_gs_chat(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid)
    await safe_edit(call, "💬 tg_chat_id kiriting (masalan: -1001234567890). Bekor qilish: /cancel", kb_home_admin(call.from_user.id))
    await state.set_state(AState.gs_chatid)

@router.message(AState.gs_chatid)
async def a_gs_chat_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "groups"):
        await state.clear()
        return
    data = await state.get_data()
    gid = int(data["gid"])
    v = (message.text or "").strip()
    chat_id = safe_int(v)
    if chat_id is None:
        await message.answer("❌ Raqam bo‘lishi kerak. Masalan: -1001234567890")
        return
    conn = db()
    conn.execute("UPDATE groups SET tg_chat_id=? WHERE id=?", (chat_id, gid))
    conn.commit()
    conn.close()
    await state.clear()
    await message.answer("✅ Saqlandi", reply_markup=kb_admin_home(message.from_user.id))

@router.callback_query(F.data.startswith("a:gs_att:"))
async def a_gs_att(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid)
    await safe_edit(call, "🚪 Absent kick limit kiriting (masalan: 5):", kb_home_admin(call.from_user.id))
    await state.set_state(AState.gs_att_limit)

@router.message(AState.gs_att_limit)
async def a_gs_att_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "groups"):
        await state.clear()
        return
    data = await state.get_data()
    gid = int(data["gid"])
    lim = safe_int((message.text or "").strip())
    if lim is None or lim < 1:
        await message.answer("❌ 1 dan katta raqam kiriting.")
        return
    conn = db()
    conn.execute("UPDATE groups SET att_absent_limit=? WHERE id=?", (lim, gid))
    conn.commit()
    conn.close()
    await state.clear()
    await message.answer("✅ Saqlandi", reply_markup=kb_admin_home(message.from_user.id))

@router.callback_query(F.data.startswith("a:gs_task:"))
async def a_gs_task(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "groups"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid)
    await safe_edit(call, "🚪 Task miss kick limit kiriting (masalan: 5):", kb_home_admin(call.from_user.id))
    await state.set_state(AState.gs_task_limit)

@router.message(AState.gs_task_limit)
async def a_gs_task_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "groups"):
        await state.clear()
        return
    data = await state.get_data()
    gid = int(data["gid"])
    lim = safe_int((message.text or "").strip())
    if lim is None or lim < 1:
        await message.answer("❌ 1 dan katta raqam kiriting.")
        return
    conn = db()
    conn.execute("UPDATE groups SET task_miss_limit=? WHERE id=?", (lim, gid))
    conn.commit()
    conn.close()
    await state.clear()
    await message.answer("✅ Saqlandi", reply_markup=kb_admin_home(message.from_user.id))

# =========================
# ATTENDANCE (Group-only) + Archive + Send DM
# =========================
def attendance_map(gid: int, date_s: str) -> dict:
    conn = db()
    rows = conn.execute("SELECT user_id, status FROM attendance WHERE group_id=? AND att_date=?",
                        (gid, date_s)).fetchall()
    conn.close()
    return {int(r["user_id"]): r["status"] for r in rows}

def group_students(gid: int) -> List[Tuple[int, str]]:
    conn = db()
    rows = conn.execute("""
        SELECT u.user_id, u.full_name
        FROM members m JOIN users u ON u.user_id=m.user_id
        WHERE m.group_id=?
        ORDER BY u.full_name
    """, (gid,)).fetchall()
    conn.close()
    return [(int(r["user_id"]), r["full_name"]) for r in rows]

@router.callback_query(F.data.startswith("a:g_att:"))
async def a_g_att_menu(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    gid = int(call.data.split(":")[2])
    d = today_str()

    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    studs = group_students(gid)
    amap = attendance_map(gid, d)

    # UI: Only mark absent with ❌; default present
    kb_rows = []
    for uid, name in studs:
        st = amap.get(uid, "present")
        icon = "❌" if st == "absent" else "✅"
        kb_rows.append([InlineKeyboardButton(
            text=f"{icon} {name[:22]}",
            callback_data=f"a:att_t:{gid}:{uid}:{d}"
        )])

    kb_rows.append([InlineKeyboardButton(text="📨 Yo‘qlarga DM yuborish", callback_data=f"a:att_send:{gid}:{d}")])
    kb_rows.append([InlineKeyboardButton(text="📄 Hisobot (text)", callback_data=f"a:att_rep:{gid}:{d}")])
    kb_rows.append([InlineKeyboardButton(text="📥 Hisobot (PDF)", callback_data=f"a:att_pdf:{gid}:{d}")])
    kb_rows.append([InlineKeyboardButton(text="🗂 Arxiv", callback_data=f"a:att_arc:{gid}")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, f"🗓 <b>Davomat</b>\nGuruh: <b>{safe_pdf_text(g['name'])}</b>\nSana: <code>{d}</code>\n\n"
                          f"Faqat qatnashmaganlarni ❌ qilib belgilang.", InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("a:att_t:"))
async def a_att_toggle(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    _, _, gid, uid, d = call.data.split(":")
    gid = int(gid); uid = int(uid)

    conn = db()
    cur = conn.execute("""
        SELECT status FROM attendance WHERE group_id=? AND user_id=? AND att_date=?
    """, (gid, uid, d)).fetchone()

    if not cur:
        # mark absent
        conn.execute("""INSERT OR REPLACE INTO attendance(group_id, user_id, att_date, status)
                        VALUES (?,?,?,'absent')""", (gid, uid, d))
    else:
        # if absent -> remove row (back to present)
        if cur["status"] == "absent":
            conn.execute("DELETE FROM attendance WHERE group_id=? AND user_id=? AND att_date=?", (gid, uid, d))
        else:
            conn.execute("UPDATE attendance SET status='absent' WHERE group_id=? AND user_id=? AND att_date=?", (gid, uid, d))
    conn.commit()
    conn.close()

    await a_g_att_menu(call)

@router.callback_query(F.data.startswith("a:att_rep:"))
async def a_att_report_text(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    _, _, gid, d = call.data.split(":")
    gid = int(gid)

    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    studs = group_students(gid)
    amap = attendance_map(gid, d)
    absent = [(uid, nm) for uid, nm in studs if amap.get(uid, "present") == "absent"]
    present = len(studs) - len(absent)

    text = (f"📄 <b>Davomat hisoboti</b>\n"
            f"Guruh: <b>{safe_pdf_text(g['name'])}</b>\n"
            f"Sana: <code>{d}</code>\n\n"
            f"Jami: <b>{len(studs)}</b>\n"
            f"✅ Qatnashdi: <b>{present}</b>\n"
            f"❌ Qatnashmadi: <b>{len(absent)}</b>\n\n")

    if absent:
        text += "❌ <b>QATNASHMAGANLAR:</b>\n"
        for i, (_uid, nm) in enumerate(absent, 1):
            text += f"{i}. {safe_pdf_text(nm)}\n"
    else:
        text += "✅ Bugun hamma qatnashgan."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 PDF", callback_data=f"a:att_pdf:{gid}:{d}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g_att:{gid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    await safe_edit(call, text, kb)

@router.callback_query(F.data.startswith("a:att_pdf:"))
async def a_att_pdf(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    _, _, gid, d = call.data.split(":")
    gid = int(gid)

    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    studs = group_students(gid)
    amap = attendance_map(gid, d)
    rows = [(nm, "absent" if amap.get(uid, "present") == "absent" else "present") for uid, nm in studs]

    fname = f"attendance_G{gid}_{d}.pdf"
    pdf_attendance(fname, g["name"], d, rows)
    try:
        await call.message.answer_document(FSInputFile(fname))
    finally:
        try:
            os.remove(fname)
        except:
            pass

@router.callback_query(F.data.startswith("a:att_send:"))
async def a_att_send(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    _, _, gid, d = call.data.split(":")
    gid = int(gid)

    conn = db()
    g = conn.execute("SELECT name, tg_chat_id, att_absent_limit FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    studs = group_students(gid)
    amap = attendance_map(gid, d)
    absent = [(uid, nm) for uid, nm in studs if amap.get(uid, "present") == "absent"]

    sent = 0
    for uid, nm in absent:
        # increment absent counter
        conn = db()
        conn.execute("INSERT OR IGNORE INTO counters(group_id, user_id, absent_count, missed_task_count) VALUES (?,?,0,0)",
                     (gid, uid))
        conn.execute("UPDATE counters SET absent_count = absent_count + 1 WHERE group_id=? AND user_id=?", (gid, uid))
        row = conn.execute("SELECT absent_count FROM counters WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
        conn.commit()
        conn.close()

        cnt_abs = int(row["absent_count"]) if row else 0
        limit = int(g["att_absent_limit"])

        # DM user
        try:
            await call.bot.send_message(
                uid,
                f"🗓 <b>Davomat ogohlantirish</b>\n"
                f"Guruh: <b>{safe_pdf_text(g['name'])}</b>\n"
                f"Sana: <code>{d}</code>\n\n"
                f"Siz bugun darsga qatnashmadingiz ❌\n"
                f"Sababsiz qoldirish: <b>{cnt_abs}/{limit}</b>"
            )
            sent += 1
        except:
            pass

        # auto-kick if exceeded
        if cnt_abs >= limit:
            # remove from DB
            conn = db()
            conn.execute("DELETE FROM members WHERE group_id=? AND user_id=?", (gid, uid))
            conn.commit()
            conn.close()

            # kick from telegram group if possible
            if g["tg_chat_id"]:
                try:
                    await call.bot.ban_chat_member(chat_id=int(g["tg_chat_id"]), user_id=uid)
                    await call.bot.unban_chat_member(chat_id=int(g["tg_chat_id"]), user_id=uid)
                except:
                    pass
            try:
                await call.bot.send_message(uid, f"⛔️ Siz <b>{safe_pdf_text(g['name'])}</b> guruhidan chiqarildingiz (davomat limiti oshdi).")
            except:
                pass

    await call.answer(f"Yuborildi: {sent} ta", show_alert=True)
    await a_g_att_menu(call)

@router.callback_query(F.data.startswith("a:att_arc:"))
async def a_att_archive(call: CallbackQuery):
    if not await guard(call, "attendance"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    dates = conn.execute("""
        SELECT DISTINCT att_date FROM attendance WHERE group_id=? ORDER BY att_date DESC LIMIT 30
    """, (gid,)).fetchall()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    kb_rows = []
    for r in dates:
        d = r["att_date"]
        kb_rows.append([InlineKeyboardButton(text=f"📅 {d}", callback_data=f"a:att_rep:{gid}:{d}")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g_att:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, f"🗂 <b>Davomat arxivi</b>\nGuruh: <b>{safe_pdf_text(g['name'])}</b>", InlineKeyboardMarkup(inline_keyboard=kb_rows))

# =========================
# ADMIN: TESTS (create + assign)
# =========================
@router.callback_query(F.data == "a:tests")
async def a_tests(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    conn = db()
    rows = conn.execute("SELECT test_id, status, deadline FROM tests ORDER BY created_at DESC LIMIT 30").fetchall()
    conn.close()

    kb_rows = []
    for r in rows:
        st, dl = ensure_deadline(r["test_id"])
        icon = "🟢" if st == "active" else "⏸" if st == "paused" else "🏁"
        kb_rows.append([InlineKeyboardButton(text=f"{icon} {r['test_id']} ({st})", callback_data=f"a:t:{r['test_id']}")])
    kb_rows.append([InlineKeyboardButton(text="➕ Test yaratish", callback_data="a:t_add")])
    kb_rows.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])
    await safe_edit(call, "🧪 <b>Testlar</b>", InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data == "a:t_add")
async def a_t_add(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tests"):
        return
    await state.clear()
    await safe_edit(call, "🧩 Javoblar kalitini yuboring (faqat A/B/C/D), masalan: ABCDABCD", kb_home_admin(call.from_user.id))
    await state.set_state(AState.t_keys)

@router.message(AState.t_keys)
async def a_t_keys(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tests"):
        await state.clear()
        return
    keys = (message.text or "").upper().strip().replace(" ", "")
    if not keys or any(ch not in "ABCD" for ch in keys):
        await message.answer("❌ Faqat A/B/C/D bo‘lsin. Qayta yuboring:")
        return
    await state.update_data(keys=keys)
    await message.answer("⏳ Test davomiyligi (minut) ni kiriting:")
    await state.set_state(AState.t_minutes)

async def kb_assign_builder(test_id: str, selected: set, is_public: int) -> InlineKeyboardMarkup:
    conn = db()
    groups = conn.execute("SELECT id, name FROM groups ORDER BY id DESC").fetchall()
    conn.close()

    rows = []
    pub_icon = "🌐✅" if is_public else "🌐❌"
    rows.append([InlineKeyboardButton(text=f"{pub_icon} Public", callback_data=f"a:t_pub:{test_id}")])
    for g in groups:
        gid = int(g["id"])
        mark = "✅" if gid in selected else "➖"
        rows.append([InlineKeyboardButton(text=f"{mark} {g['name'][:18]}", callback_data=f"a:t_g:{test_id}:{gid}")])
    rows.append([InlineKeyboardButton(text="💾 Saqlash", callback_data=f"a:t_save:{test_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data="a:tests")])
    rows.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@router.message(AState.t_minutes)
async def a_t_minutes(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tests"):
        await state.clear()
        return
    mins = safe_int((message.text or "").strip())
    if mins is None or mins < 1:
        await message.answer("❌ Minut raqam bo‘lsin. Qayta kiriting:")
        return

    data = await state.get_data()
    keys = data["keys"]
    tid = gen_test_id_5()
    deadline = (datetime.now() + timedelta(minutes=mins)).strftime("%Y-%m-%d %H:%M")

    conn = db()
    conn.execute("""INSERT INTO tests(test_id, keys, status, deadline, created_at, is_public)
                    VALUES (?,?,?,?,?,0)""", (tid, keys, "active", deadline, now_str()))
    conn.commit()
    conn.close()

    await state.update_data(tid=tid, selected=set(), is_public=0)
    kb = await kb_assign_builder(tid, set(), 0)
    await message.answer(
        f"✅ Test yaratildi: <b>{tid}</b>\nSavollar: <b>{len(keys)}</b>\nDeadline: <code>{deadline}</code>\n\n"
        f"Endi testni Public yoki guruh(lar)ga biriktiring:",
        reply_markup=kb
    )
    await state.set_state(AState.t_assign)

@router.callback_query(AState.t_assign, F.data.startswith("a:t_pub:"))
async def a_t_pub(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    st, _ = ensure_deadline(tid)
    if st == "finished":
        await call.answer("Yakunlangan testni o‘zgartirib bo‘lmaydi.", show_alert=True)
        return
    data = await state.get_data()
    is_public = 0 if int(data.get("is_public", 0)) == 1 else 1
    await state.update_data(is_public=is_public)
    kb = await kb_assign_builder(tid, set(data.get("selected", set())), is_public)
    await safe_edit(call, call.message.text, kb)

@router.callback_query(AState.t_assign, F.data.startswith("a:t_g:"))
async def a_t_toggle_group(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tests"):
        return
    _, _, tid, gid = call.data.split(":")
    gid = int(gid)
    data = await state.get_data()
    selected = set(data.get("selected", set()))
    is_public = int(data.get("is_public", 0))
    if gid in selected:
        selected.remove(gid)
    else:
        selected.add(gid)
    await state.update_data(selected=selected)
    kb = await kb_assign_builder(tid, selected, is_public)
    await safe_edit(call, call.message.text, kb)

@router.callback_query(AState.t_assign, F.data.startswith("a:t_save:"))
async def a_t_assign_save(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    data = await state.get_data()
    selected = set(data.get("selected", set()))
    is_public = int(data.get("is_public", 0))

    conn = db()
    conn.execute("UPDATE tests SET is_public=? WHERE test_id=?", (is_public, tid))
    conn.execute("DELETE FROM test_groups WHERE test_id=?", (tid,))
    for gid in selected:
        conn.execute("INSERT OR IGNORE INTO test_groups(test_id, group_id) VALUES (?,?)", (tid, gid))
    conn.commit()
    conn.close()

    await state.clear()
    await safe_edit(call, f"✅ Test <b>{tid}</b> saqlandi.\nPublic: <b>{'ON' if is_public else 'OFF'}</b>\nGuruhlar: <b>{', '.join(map(str, selected)) if selected else 'yo‘q'}</b>",
                    kb_admin_home(call.from_user.id))

# =========================
# ADMIN: Group Tests list (inside group)
# =========================
@router.callback_query(F.data.startswith("a:g_tests:"))
async def a_g_tests(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    tests = conn.execute("""
        SELECT t.test_id, t.status, t.deadline, COALESCE(t.is_public,0) as is_public
        FROM tests t
        LEFT JOIN test_groups tg ON tg.test_id=t.test_id
        WHERE tg.group_id=? OR COALESCE(t.is_public,0)=1
        GROUP BY t.test_id
        ORDER BY t.created_at DESC
        LIMIT 30
    """, (gid,)).fetchall()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    kb_rows = []
    for t in tests:
        st, _ = ensure_deadline(t["test_id"])
        icon = "🟢" if st == "active" else "⏸" if st == "paused" else "🏁"
        kb_rows.append([InlineKeyboardButton(text=f"{icon} {t['test_id']}", callback_data=f"a:t:{t['test_id']}")])
    kb_rows.append([InlineKeyboardButton(text="➕ Test yaratish", callback_data="a:t_add")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, f"🧪 <b>{safe_pdf_text(g['name'])}</b> — Testlar", InlineKeyboardMarkup(inline_keyboard=kb_rows))

# =========================
# ADMIN: Test options + rating (text+pdf)
# =========================
@router.callback_query(F.data.startswith("a:t:"))
async def a_t_opt(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    st, dl = ensure_deadline(tid)
    if st is None:
        await call.answer("Test topilmadi.", show_alert=True)
        return

    conn = db()
    row = conn.execute("SELECT COALESCE(is_public,0) as p FROM tests WHERE test_id=?", (tid,)).fetchone()
    groups = conn.execute("SELECT group_id FROM test_groups WHERE test_id=? ORDER BY group_id", (tid,)).fetchall()
    conn.close()

    is_public = int(row["p"]) if row else 0
    grp_list = ", ".join(str(int(g["group_id"])) for g in groups) if groups else "yo‘q"

    kb_rows = []
    if st == "active":
        kb_rows.append([InlineKeyboardButton(text="⏸ Pauza", callback_data=f"a:t_pause:{tid}")])
    if st == "paused":
        kb_rows.append([InlineKeyboardButton(text="▶️ Davom", callback_data=f"a:t_resume:{tid}")])
    if st != "finished":
        kb_rows.append([InlineKeyboardButton(text="🏁 Yakunlash", callback_data=f"a:t_finish:{tid}")])
    kb_rows.append([InlineKeyboardButton(text="🏆 Reyting (text)", callback_data=f"a:t_rate:{tid}")])
    kb_rows.append([InlineKeyboardButton(text="📥 Reyting (PDF)", callback_data=f"a:t_pdf:{tid}")])
    if st != "finished":
        kb_rows.append([InlineKeyboardButton(text="🔁 Biriktirish", callback_data=f"a:t_reassign:{tid}")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data="a:tests"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    text = (f"⚙️ <b>Test</b>: <code>{tid}</code>\n"
            f"Holat: <b>{st}</b>\n"
            f"Deadline: <code>{dl}</code>\n"
            f"Public: <b>{'ON' if is_public else 'OFF'}</b>\n"
            f"Guruhlar: <code>{grp_list}</code>\n\n"
            f"📌 PDF faqat test yakunlanganda ma’qul (ammo bu yerda har doim ochiladi).")
    await safe_edit(call, text, InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("a:t_pause:"))
async def a_t_pause(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    conn = db()
    conn.execute("UPDATE tests SET status='paused' WHERE test_id=?", (tid,))
    conn.commit(); conn.close()
    await call.answer("Pauza", show_alert=True)
    await a_t_opt(call)

@router.callback_query(F.data.startswith("a:t_resume:"))
async def a_t_resume(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    st, _ = ensure_deadline(tid)
    if st == "finished":
        await call.answer("Yakunlangan testni davom ettirib bo‘lmaydi.", show_alert=True)
        return
    conn = db()
    conn.execute("UPDATE tests SET status='active' WHERE test_id=?", (tid,))
    conn.commit(); conn.close()
    await call.answer("Davom", show_alert=True)
    await a_t_opt(call)

@router.callback_query(F.data.startswith("a:t_finish:"))
async def a_t_finish(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    conn = db()
    conn.execute("UPDATE tests SET status='finished' WHERE test_id=?", (tid,))
    conn.commit(); conn.close()
    await call.answer("Yakunlandi", show_alert=True)
    await a_t_opt(call)

@router.callback_query(F.data.startswith("a:t_rate:"))
async def a_t_rate(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    st, dl = ensure_deadline(tid)

    conn = db()
    rows = conn.execute("""SELECT full_name, score, total, percent, date
                           FROM results WHERE test_id=?
                           ORDER BY percent DESC, score DESC""", (tid,)).fetchall()
    conn.close()
    if not rows:
        await call.answer("Natija yo‘q.", show_alert=True)
        return

    text = f"🏆 <b>Reyting</b> — <code>{tid}</code>\nHolat: <b>{st}</b> | ⏰ <code>{dl}</code>\n\n"
    for i, r in enumerate(rows, 1):
        text += f"{i}. {safe_pdf_text(r['full_name'])} — <b>{r['percent']:.1f}%</b> | {to_uz_time_str(r['date'])}\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 PDF", callback_data=f"a:t_pdf:{tid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:t:{tid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    await safe_edit(call, text, kb)

@router.callback_query(F.data.startswith("a:t_pdf:"))
async def a_t_pdf(call: CallbackQuery):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]

    conn = db()
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(results)").fetchall()]
        has_score = "score" in cols
        has_total = "total" in cols
        has_date = "date" in cols

        select_cols = ["full_name", "percent"]
        if has_score:
            select_cols.append("score")
        if has_total:
            select_cols.append("total")
        if has_date:
            select_cols.append("date")

        q = f"SELECT {', '.join(select_cols)} FROM results WHERE test_id=? ORDER BY percent DESC"
        rows = conn.execute(q, (tid,)).fetchall()
    finally:
        conn.close()

    if not rows:
        await call.answer("Natija yo‘q.", show_alert=True)
        return

    fname = f"rating_{tid}.pdf"
    pdf_rows: List[Tuple[str, int, int, float, str]] = []
    for r in rows:
        name = r["full_name"]
        percent = float(r["percent"] or 0)
        score = int(r["score"] or 0) if has_score else 0
        total = int(r["total"] or 0) if has_total else 0
        date_raw = r["date"] if has_date and r["date"] else ""
        date_s = to_uz_time_str(date_raw) if date_raw else ""
        pdf_rows.append((name, score, total, percent, date_s))

    pdf_rating(fname, f"Reyting — Test {tid}", pdf_rows)
    try:
        await call.message.answer_document(FSInputFile(fname))
    finally:
        try:
            os.remove(fname)
        except Exception:
            pass

@router.callback_query(F.data.startswith("a:t_reassign:"))
async def a_t_reassign(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tests"):
        return
    tid = call.data.split(":")[2]
    st, _ = ensure_deadline(tid)
    if st == "finished":
        await call.answer("Yakunlangan testni biriktirib bo‘lmaydi.", show_alert=True)
        return

    conn = db()
    grp = conn.execute("SELECT group_id FROM test_groups WHERE test_id=?", (tid,)).fetchall()
    pub = conn.execute("SELECT COALESCE(is_public,0) as p FROM tests WHERE test_id=?", (tid,)).fetchone()
    conn.close()
    selected = {int(x["group_id"]) for x in grp}
    is_public = int(pub["p"]) if pub else 0

    await state.clear()
    await state.update_data(tid=tid, selected=selected, is_public=is_public)
    kb = await kb_assign_builder(tid, selected, is_public)
    await safe_edit(call, "🔁 Biriktirishni yangilang:", kb)
    await state.set_state(AState.t_assign)

# =========================
# GROUP RESULTS: manual + import (inside group)
# =========================
@router.callback_query(F.data.startswith("a:g_results:"))
async def a_g_results(call: CallbackQuery):
    if not await guard(call, "results"):
        return
    gid = int(call.data.split(":")[2])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Manual natija", callback_data=f"a:m_start:{gid}")],
        [InlineKeyboardButton(text="📥 Import natija", callback_data=f"a:imp_start:{gid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    await safe_edit(call, "📥 <b>Natijalar</b>\nManual yoki Import tanlang:", kb)

@router.callback_query(F.data.startswith("a:m_start:"))
async def a_m_start(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "results"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid)
    await safe_edit(call, "📝 Manual: Test ID kiriting (masalan: 12345):", kb_home_admin(call.from_user.id))
    await state.set_state(AState.m_tid)

@router.message(AState.m_tid)
async def a_m_tid(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "results"):
        await state.clear()
        return
    tid = (message.text or "").strip()
    await state.update_data(tid=tid)
    await message.answer("Jami savollar soni (total) ni kiriting:")
    await state.set_state(AState.m_total)

@router.message(AState.m_total)
async def a_m_total(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "results"):
        await state.clear()
        return
    total = safe_int((message.text or "").strip())
    if total is None or total < 1:
        await message.answer("❌ Total raqam bo‘lsin.")
        return
    data = await state.get_data()
    gid = int(data["gid"])
    students = group_students(gid)
    if not students:
        await message.answer("Guruhda o‘quvchi yo‘q.")
        await state.clear()
        return
    await state.update_data(total=total, students=students)
    preview = "\n".join([f"{i+1}. {nm}" for i, (_uid, nm) in enumerate(students)])
    await message.answer(
        f"✅ Endi ballarni ketma-ket yuboring.\n"
        f"O‘quvchilar: <b>{len(students)}</b>\n\n{safe_pdf_text(preview)}\n\n"
        f"Format: 10 9 8 ... (bo‘shliq bilan).",
    )
    await state.set_state(AState.m_scores)

@router.message(AState.m_scores)
async def a_m_scores(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "results"):
        await state.clear()
        return
    data = await state.get_data()
    gid = int(data["gid"])
    tid = data["tid"]
    total = int(data["total"])
    students = data["students"]

    parts = re.split(r"[,\s]+", (message.text or "").strip())
    scores = [int(p) for p in parts if p.isdigit()]
    if len(scores) != len(students):
        await message.answer(f"❌ Ballar soni mos emas. Kerak: {len(students)}, Siz: {len(scores)}")
        return

    conn = db()
    dt = now_str()
    for idx, (uid, nm) in enumerate(students):
        sc = scores[idx]
        pct = (sc / total) * 100 if total else 0.0
        conn.execute("""INSERT INTO results(user_id, test_id, score, total, percent, date, full_name)
                        VALUES (?,?,?,?,?,?,?)""", (uid, tid, sc, total, pct, dt, nm))
    conn.commit()
    conn.close()

    await state.clear()
    await message.answer(f"✅ Manual natijalar saqlandi.\nTest: <code>{tid}</code>\nGuruh: <code>{gid}</code>", reply_markup=kb_admin_home(message.from_user.id))

@router.callback_query(F.data.startswith("a:imp_start:"))
async def a_imp_start(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "results"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid)
    await safe_edit(call, "📥 Import: Test ID kiriting (natijalar DBda bo‘lishi kerak):", kb_home_admin(call.from_user.id))
    await state.set_state(AState.imp_tid)

@router.message(AState.imp_tid)
async def a_imp_tid(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "results"):
        await state.clear()
        return
    tid = (message.text or "").strip()
    data = await state.get_data()
    gid = int(data["gid"])

    # Import = show rating for that group & test (no duplication logic here)
    conn = db()
    ids = conn.execute("SELECT user_id FROM members WHERE group_id=?", (gid,)).fetchall()
    user_ids = [int(x["user_id"]) for x in ids]
    if not user_ids:
        conn.close()
        await message.answer("Guruh bo‘sh.")
        await state.clear()
        return

    q = ",".join(["?"] * len(user_ids))
    rows = conn.execute(f"""
        SELECT full_name, percent, date
        FROM results
        WHERE test_id=? AND user_id IN ({q})
        ORDER BY percent DESC
    """, (tid, *user_ids)).fetchall()
    conn.close()

    if not rows:
        await message.answer("Bu guruhda bu test bo‘yicha natija topilmadi.")
        await state.clear()
        return

    text = f"✅ Import topildi.\nTest: <code>{tid}</code>\nGuruh: <code>{gid}</code>\nNatija: <b>{len(rows)}</b> ta\n\n"
    for i, r in enumerate(rows[:15], 1):
        text += f"{i}. {safe_pdf_text(r['full_name'])} — {r['percent']:.1f}%\n"
    if len(rows) > 15:
        text += f"... yana {len(rows)-15} ta"

    await state.clear()
    await message.answer(text, reply_markup=kb_admin_home(message.from_user.id))

# =========================
# TASKS (inside group) — create draft, allow description+media in same message, publish alerts
# =========================
@router.callback_query(F.data.startswith("a:g_tasks:"))
async def a_g_tasks(call: CallbackQuery):
    if not await guard(call, "tasks"):
        return
    gid = int(call.data.split(":")[2])
    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    tasks = conn.execute("""SELECT id, title, due_at, status FROM tasks
                            WHERE group_id=? ORDER BY id DESC LIMIT 20""", (gid,)).fetchall()
    conn.close()
    if not g:
        await call.answer("Guruh topilmadi.", show_alert=True)
        return

    kb_rows = [[InlineKeyboardButton(text="➕ Vazifa yaratish", callback_data=f"a:task_new:{gid}")]]
    for t in tasks:
        st = t["status"]
        icon = "🟡" if st == "draft" else "🟢" if st == "published" else "🏁"
        kb_rows.append([InlineKeyboardButton(text=f"{icon} {t['title'][:18]}", callback_data=f"a:task_v:{gid}:{t['id']}")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, f"📌 <b>{safe_pdf_text(g['name'])}</b> — Vazifalar", InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("a:task_new:"))
async def a_task_new(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "tasks"):
        return
    gid = int(call.data.split(":")[2])
    await state.clear()
    await state.update_data(gid=gid, media=[])
    await safe_edit(call, "🆕 Vazifa nomini kiriting:", kb_home_admin(call.from_user.id))
    await state.set_state(AState.task_title)

@router.message(AState.task_title)
async def a_task_title(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tasks"):
        await state.clear()
        return
    title = (message.text or "").strip()
    if len(title) < 2:
        await message.answer("Nom juda qisqa. Qayta kiriting:")
        return
    await state.update_data(title=title)
    await message.answer("📝 Endi <b>description</b> yuboring.\n"
                         "Bu joyga matn ham, photo/video/audio/document ham yuborsangiz bo‘ladi.\n"
                         "Agar yana media qo‘shmoqchi bo‘lsangiz, ketma-ket yuboring.\n"
                         "Tugatish uchun: /done")
    await state.set_state(AState.task_desc_media)

@router.message(AState.task_desc_media)
async def a_task_desc_media(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tasks"):
        await state.clear()
        return

    if (message.text or "").strip().lower() == "/done":
        await message.answer("💯 Vazifa ballini kiriting (masalan: 10):")
        await state.set_state(AState.task_points)
        return

    data = await state.get_data()
    desc = data.get("desc", "")
    media = data.get("media", [])

    # collect text
    if message.text:
        desc = (desc + "\n" + message.text.strip()).strip()

    # collect media (file_id)
    def add_media(ftype: str, fid: str):
        media.append({"type": ftype, "file_id": fid})

    if message.photo:
        add_media("photo", message.photo[-1].file_id)
    elif message.video:
        add_media("video", message.video.file_id)
    elif message.document:
        add_media("document", message.document.file_id)
    elif message.audio:
        add_media("audio", message.audio.file_id)
    elif message.voice:
        add_media("voice", message.voice.file_id)

    await state.update_data(desc=desc, media=media)
    await message.answer("✅ Qabul qilindi. Yana qo‘shing yoki /done bosing.")

@router.message(AState.task_points)
async def a_task_points(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tasks"):
        await state.clear()
        return
    points = safe_int((message.text or "").strip())
    if points is None or points < 1:
        await message.answer("❌ 1 dan katta raqam kiriting.")
        return
    await state.update_data(points=points)
    await message.answer("⏰ Deadline kiriting (YYYY-MM-DD HH:MM), masalan: 2026-02-20 18:00")
    await state.set_state(AState.task_due)

@router.message(AState.task_due)
async def a_task_due(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "tasks"):
        await state.clear()
        return
    due_s = (message.text or "").strip()
    try:
        parse_dt(due_s)
    except:
        await message.answer("❌ Format xato. Masalan: 2026-02-20 18:00")
        return

    data = await state.get_data()
    gid = int(data["gid"])
    title = data["title"]
    desc = data.get("desc", "")
    points = int(data["points"])
    media = data.get("media", [])

    conn = db()
    cur = conn.execute("""INSERT INTO tasks(group_id, title, description, points, due_at, created_at, status)
                          VALUES (?,?,?,?,?,?, 'draft')""",
                       (gid, title, desc, points, due_s, now_str()))
    task_id = cur.lastrowid
    for m in media:
        conn.execute("""INSERT INTO task_media(task_id, file_type, file_id) VALUES (?,?,?)""",
                     (task_id, m["type"], m["file_id"]))
    conn.commit()
    conn.close()

    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📣 Publish", callback_data=f"a:task_pub:{gid}:{task_id}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g_tasks:{gid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    await message.answer(
        f"✅ Vazifa draft saqlandi.\n"
        f"Vazifa: <b>{safe_pdf_text(title)}</b>\n"
        f"Ball: <b>{points}</b>\n"
        f"Deadline: <code>{due_s}</code>\n\n"
        f"Endi publish qiling:",
        reply_markup=kb
    )

@router.callback_query(F.data.startswith("a:task_v:"))
async def a_task_view(call: CallbackQuery):
    if not await guard(call, "tasks"):
        return
    _, _, gid, tid = call.data.split(":")
    gid = int(gid); tid = int(tid)

    conn = db()
    t = conn.execute("SELECT * FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    conn.close()
    if not t:
        await call.answer("Vazifa topilmadi.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📣 Publish", callback_data=f"a:task_pub:{gid}:{tid}")],
        [InlineKeyboardButton(text="📥 Submissions", callback_data=f"a:task_subs:{gid}:{tid}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:g_tasks:{gid}")],
        [InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")],
    ])
    text = (f"📌 <b>{safe_pdf_text(t['title'])}</b>\n"
            f"Status: <b>{t['status']}</b>\n"
            f"Ball: <b>{t['points']}</b>\n"
            f"Deadline: <code>{t['due_at']}</code>\n\n"
            f"{safe_pdf_text(t['description'] or '')[:1500]}")
    await safe_edit(call, text, kb)

@router.callback_query(F.data.startswith("a:task_subs:"))
async def a_task_subs(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    _, _, gid_s, tid_s = call.data.split(":", 3)
    gid = int(gid_s); tid = int(tid_s)

    conn = db()
    # task title
    t = conn.execute("SELECT title, points FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    if not t:
        conn.close()
        await call.answer("Vazifa topilmadi.", show_alert=True)
        return

    subs = conn.execute("""SELECT ts.id AS id, ts.user_id, u.full_name, ts.submitted_at,
                                    COALESCE(ts.score, -1) AS score
                             FROM task_submissions ts
                             JOIN users u ON u.user_id=ts.user_id
                             WHERE ts.task_id=?
                             ORDER BY ts.submitted_at DESC""", (tid,)).fetchall()
    conn.close()

    rows = []
    for s in subs:
        score = int(s["score"])
        score_txt = "⏳ Baholanmagan" if score < 0 else f"⭐ {score}/{int(t['points'])}"
        rows.append([InlineKeyboardButton(text=f"👤 {s['full_name']} • {score_txt}", callback_data=f"a:task_sub_v:{s['id']}")])

    if not rows:
        rows.append([InlineKeyboardButton(text="(Topshiriqlar yo‘q)", callback_data="noop")])

    rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:task_v:{gid}:{tid}")])
    rows.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")])

    await safe_edit(call, f"📨 <b>Topshiriqlar</b>\nVazifa: <b>{safe_pdf_text(t['title'])}</b>", InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data.startswith("a:task_sub_v:"))
async def a_task_sub_view(call: CallbackQuery):
    if not await guard_call(call, "tasks"):
        return

    try:
        parts = call.data.split(":")
        sub_id = int(parts[-1])
    except Exception:
        await call.answer("Noto‘g‘ri so‘rov.", show_alert=True)
        return

    conn = db()
    row = conn.execute(
        "SELECT id, task_id, user_id, msg_json, submitted_at, score, feedback "
        "FROM task_submissions WHERE id=?",
        (sub_id,)
    ).fetchone()
    if not row:
        conn.close()
        await call.answer("Topilmadi.", show_alert=True)
        return

    sub = dict(row)
    trow = conn.execute("SELECT group_id, title FROM tasks WHERE id=?", (sub["task_id"],)).fetchone()
    conn.close()
    gid = int(trow["group_id"]) if trow else 0
    ttitle = trow["title"] if trow else f"#{sub['task_id']}"

    def _extract_from_msg_json(s: str):
        try:
            d = json.loads(s) if s else {}
        except Exception:
            d = {}
        txt = d.get("text") or ""
        cap = d.get("caption") or ""

        if d.get("photo"):
            ph = d["photo"][-1] if isinstance(d["photo"], list) else d["photo"]
            fid = (ph or {}).get("file_id")
            return ("photo", fid, cap or txt)

        if d.get("video"):
            fid = (d["video"] or {}).get("file_id")
            return ("video", fid, cap or txt)

        if d.get("document"):
            fid = (d["document"] or {}).get("file_id")
            return ("document", fid, cap or txt)

        if d.get("audio"):
            fid = (d["audio"] or {}).get("file_id")
            return ("audio", fid, cap or txt)

        if d.get("voice"):
            fid = (d["voice"] or {}).get("file_id")
            return ("voice", fid, cap or txt)

        return ("text", None, txt or cap)

    ctype, file_id, text = _extract_from_msg_json(sub.get("msg_json") or "")

    header = (
        f"📝 <b>Vazifa yuborilishi</b>\n"
        f"Vazifa: <b>{ttitle}</b>\n"
        f"Sub ID: <code>{sub['id']}</code>\n"
        f"User: <code>{sub['user_id']}</code>\n"
        f"Sana: <code>{sub.get('submitted_at','')}</code>\n"
    )
    if sub.get("score") is not None:
        header += f"✅ Baholangan: <b>{sub['score']}</b> ball\n"
    if sub.get("feedback"):
        header += f"💬 Izoh: {sub['feedback']}\n"

    # resend attachment/text to admin (separate message)
    try:
        if ctype == "photo" and file_id:
            await call.message.answer_photo(file_id, caption=(text or "")[:900])
        elif ctype == "video" and file_id:
            await call.message.answer_video(file_id, caption=(text or "")[:900])
        elif ctype == "document" and file_id:
            await call.message.answer_document(file_id, caption=(text or "")[:900])
        elif ctype == "audio" and file_id:
            await call.message.answer_audio(file_id, caption=(text or "")[:900])
        elif ctype == "voice" and file_id:
            await call.message.answer_voice(file_id, caption=(text or "")[:900])
        else:
            if text:
                await call.message.answer(f"🗒 Matn:\n{text}")
    except Exception:
        pass

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Baholash", callback_data=f"a:task_grade:{gid}:{sub['task_id']}:{sub['user_id']}:{sub_id}")],
        [InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:task_subs:{gid}:{sub['task_id']}"),
         InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")]
    ])
    await safe_edit(call, header, kb)
@router.callback_query(F.data.startswith("a:task_grade:"))
async def a_task_grade_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    parts = call.data.split(":")
    # formats: a:task_grade:gid:tid:uid:sub_id  OR a:task_grade:sub_id
    sub_id = None
    if len(parts) >= 6:
        gid = int(parts[2]); tid = int(parts[3]); uid = int(parts[4]); sub_id = int(parts[5])
    elif len(parts) >= 3:
        sub_id = int(parts[2])
        conn0 = db()
        r0 = conn0.execute("SELECT task_id, user_id FROM task_submissions WHERE id=?", (sub_id,)).fetchone()
        if not r0:
            conn0.close()
            await call.answer("Topilmadi.", show_alert=True)
            return
        tid = int(r0["task_id"]); uid = int(r0["user_id"])
        g0 = conn0.execute("SELECT group_id FROM tasks WHERE id=?", (tid,)).fetchone()
        gid = int(g0["group_id"]) if g0 else 0
        conn0.close()
    else:
        await call.answer("Noto‘g‘ri so‘rov.", show_alert=True)
        return

    conn = db()
    t = conn.execute("SELECT title, points FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    u = conn.execute("SELECT full_name FROM users WHERE user_id=?", (uid,)).fetchone()
    conn.close()
    if not t or not u:
        await call.answer("Topilmadi.", show_alert=True)
        return

    await state.update_data(gid=gid, tid=tid, uid=uid, max_points=int(t["points"]), grade_sub_id=sub_id)
    await state.set_state(AState.grade_score)
    await call.message.answer(
        f"⭐ <b>Baholash</b>\nVazifa: <b>{safe_pdf_text(t['title'])}</b>\nO‘quvchi: <b>{safe_pdf_text(u['full_name'])}</b>\n\n"
        f"0 dan {int(t['points'])} gacha ball kiriting:",
        reply_markup=kb_cancel_admin()
    )


@router.message(AState.grade_score)
async def a_task_grade_score(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    max_points = int(data.get("max_points", 0))
    try:
        score = int(message.text.strip())
    except Exception:
        await message.answer("Faqat raqam kiriting.", reply_markup=kb_cancel_admin())
        return
    if score < 0 or score > max_points:
        await message.answer(f"Ball 0..{max_points} oralig‘ida bo‘lsin.", reply_markup=kb_cancel_admin())
        return

    await state.update_data(score=score)
    await state.set_state(AState.grade_feedback)
    await message.answer("💬 Qisqa izoh (ixtiyoriy). Yo‘q bo‘lsa <code>-</code> yozing:", reply_markup=kb_cancel_admin())


@router.message(AState.grade_feedback)
async def a_task_grade_finish(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    gid = int(data["gid"]); tid = int(data["tid"]); uid = int(data["uid"])
    score = int(data["score"])
    fb = message.text.strip()
    if fb == "-":
        fb = ""

    conn = db()
    t = conn.execute("SELECT title, points FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    u = conn.execute("SELECT full_name FROM users WHERE user_id=?", (uid,)).fetchone()
    if not t or not u:
        conn.close()
        await state.clear()
        await message.answer("Topilmadi.", reply_markup=kb_home_admin())
        return

    conn.execute("""UPDATE task_submissions
                    SET score=?, feedback=?, graded_by=?, graded_at=?
                    WHERE task_id=? AND user_id=?""", (score, fb, message.from_user.id, now_str(), tid, uid))
    conn.commit()
    conn.close()

    # Notify student (Telegram message)
    try:
        msg = (f"✅ <b>Vazifa baholandi</b>\n"
               f"📌 {safe_pdf_text(t['title'])}\n"
               f"⭐ Ball: <b>{score}/{int(t['points'])}</b>")
        if fb:
            msg += f"\n💬 Izoh: {safe_pdf_text(fb)}"
        await bot.send_message(uid, msg)
    except Exception:
        pass

    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"a:task_sub_v:{sub_id}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")]])
    await message.answer("✅ Saqlandi va o‘quvchiga yuborildi.", reply_markup=kb)


@router.callback_query(F.data.startswith("a:task_pub:"))
async def a_task_publish(call: CallbackQuery):
    if not await guard(call, "tasks"):
        return
    _, _, gid, tid = call.data.split(":")
    gid = int(gid); tid = int(tid)

    conn = db()
    t = conn.execute("SELECT * FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    if not t:
        conn.close()
        await call.answer("Vazifa topilmadi.", show_alert=True)
        return
    conn.execute("UPDATE tasks SET status='published' WHERE id=?", (tid,))
    # alert members
    members = conn.execute("SELECT user_id FROM members WHERE group_id=?", (gid,)).fetchall()
    conn.commit()
    conn.close()

    sent = 0
    for r in members:
        uid = int(r["user_id"])
        try:
            await call.bot.send_message(
                uid,
                f"📢 <b>Yangi vazifa!</b>\n"
                f"Guruh: <b>{safe_pdf_text(get_group_name(gid))}</b>\n"
                f"Vazifa: <b>{safe_pdf_text(t['title'])}</b>\n"
                f"Ball: <b>{t['points']}</b>\n"
                f"Deadline: <code>{t['due_at']}</code>\n\n"
                f"Vazifani topshirish uchun: Guruhlarim → Guruh → Vazifalar"
            )
            sent += 1
        except:
            pass

    await call.answer(f"Publish ✅ (alert: {sent})", show_alert=True)
    await a_task_view(call)

def get_group_name(gid: int) -> str:
    conn = db()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    conn.close()
    return g["name"] if g else str(gid)

# USER: tasks list + submit
@router.callback_query(F.data.startswith("u:tasks:"))
async def u_tasks(call: CallbackQuery):
    uid = call.from_user.id
    gid = int(call.data.split(":")[2])

    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    g = conn.execute("SELECT name FROM groups WHERE id=?", (gid,)).fetchone()
    tasks = conn.execute("""SELECT id, title, due_at, points
                            FROM tasks WHERE group_id=? AND status='published'
                            ORDER BY id DESC LIMIT 20""", (gid,)).fetchall()
    conn.close()
    if not mem or not g:
        await call.answer("Bu guruh sizniki emas.", show_alert=True)
        return

    kb_rows = []
    for t in tasks:
        kb_rows.append([InlineKeyboardButton(
            text=f"📝 {t['title'][:18]}",
            callback_data=f"u:task_v:{gid}:{t['id']}"
        )])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"u:g:{gid}"), InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")])

    await safe_edit(call, f"📌 <b>{safe_pdf_text(g['name'])}</b> — Vazifalar", InlineKeyboardMarkup(inline_keyboard=kb_rows))

@router.callback_query(F.data.startswith("u:task_v:"))
async def u_task_view(call: CallbackQuery):
    uid = call.from_user.id
    _, _, gid, tid = call.data.split(":")
    gid = int(gid); tid = int(tid)

    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    t = conn.execute("SELECT * FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    sub = conn.execute("SELECT score, submitted_at FROM task_submissions WHERE task_id=? AND user_id=?", (tid, uid)).fetchone()
    conn.close()

    if not mem or not t:
        await call.answer("Topilmadi.", show_alert=True)
        return

    btns = []
    if sub:
        score = sub["score"]
        score_txt = f"✅ Yuborilgan | Ball: {score if score is not None else 'tekshirilmagan'}"
        btns.append([InlineKeyboardButton(text=score_txt, callback_data="noop")])
    else:
        btns.append([InlineKeyboardButton(text="📤 Vazifani yuborish", callback_data=f"u:task_send:{gid}:{tid}")])

    btns.append([InlineKeyboardButton(text="⬅️ Ortga", callback_data=f"u:tasks:{gid}")])
    btns.append([InlineKeyboardButton(text="🏠 Menyu", callback_data="u:home")])

    text = (f"📌 <b>{safe_pdf_text(t['title'])}</b>\n"
            f"Ball: <b>{t['points']}</b>\n"
            f"Deadline: <code>{t['due_at']}</code>\n\n"
            f"{safe_pdf_text(t['description'] or '')[:1500]}\n\n"
            f"📎 Topshirish: istalgan format (text/photo/video/audio/document/voice).")
    await safe_edit(call, text, InlineKeyboardMarkup(inline_keyboard=btns))

@router.callback_query(F.data.startswith("u:task_send:"))
async def u_task_send(call: CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    _, _, gid, tid = call.data.split(":")
    gid = int(gid); tid = int(tid)

    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    sub = conn.execute("SELECT 1 FROM task_submissions WHERE task_id=? AND user_id=?", (tid, uid)).fetchone()
    t = conn.execute("SELECT due_at FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    conn.close()

    if not mem:
        await call.answer("Bu guruh sizniki emas.", show_alert=True)
        return
    if sub:
        await call.answer("Siz allaqachon yuborgansiz.", show_alert=True)
        return
    if not t:
        await call.answer("Vazifa topilmadi.", show_alert=True)
        return

    # deadline check
    try:
        if datetime.now() > parse_dt(t["due_at"]):
            await call.answer("Deadline o‘tgan. Topshirib bo‘lmaydi.", show_alert=True)
            return
    except:
        pass

    await state.clear()
    await state.update_data(task_gid=gid, task_id=tid)
    await safe_edit(call, "📤 Vazifani yuboring (istalgan format). Bekor: /cancel", kb_home_user())
    # reuse UState.solve_answers? create simple state:
    await state.set_state(UState.task_submit)  # reuse state for any content

@router.message(UState.task_submit)
async def u_task_receive_any(message: Message, state: FSMContext):
    data = await state.get_data()
    if "task_id" not in data:
        return  # this handler is also used by test submit in other flow; guarded there
    gid = int(data["task_gid"])
    tid = int(data["task_id"])
    uid = message.from_user.id

    # verify membership + not already
    conn = db()
    mem = conn.execute("SELECT 1 FROM members WHERE group_id=? AND user_id=?", (gid, uid)).fetchone()
    sub = conn.execute("SELECT 1 FROM task_submissions WHERE task_id=? AND user_id=?", (tid, uid)).fetchone()
    t = conn.execute("SELECT due_at, title FROM tasks WHERE id=? AND group_id=?", (tid, gid)).fetchone()
    conn.close()
    if not mem:
        await message.answer("Bu guruh sizniki emas.")
        await state.clear()
        return
    if sub:
        await message.answer("Siz allaqachon yuborgansiz.")
        await state.clear()
        return
    if not t:
        await message.answer("Vazifa topilmadi.")
        await state.clear()
        return
    try:
        if datetime.now() > parse_dt(t["due_at"]):
            await message.answer("Deadline o‘tgan.")
            await state.clear()
            return
    except:
        pass

    ensure_user(uid, message.from_user.full_name or "No Name")
    full_name = get_user_name(uid)

    # store full message json (for admin view)
    msg_json = message.model_dump_json(exclude_none=True)

    conn = db()
    conn.execute("""INSERT INTO task_submissions(task_id, user_id, full_name, submitted_at, msg_json)
                    VALUES (?,?,?,?,?)""", (tid, uid, full_name, now_str(), msg_json))
    conn.commit()
    conn.close()

    await state.clear()
    await message.answer("✅ Vazifa qabul qilindi. Tekshiruvdan so‘ng ball qo‘yiladi.", reply_markup=kb_user_home())

# =========================
# BACKGROUND: enforce kick limits for missed tasks
# =========================
async def enforce_kick_limits(bot: Bot):
    """
    If task published and due passed, and user didn't submit => missed_task_count++
    If missed_task_count >= limit => remove + kick from tg group
    """
    conn = db()
    # published tasks past due
    tasks = conn.execute("""
        SELECT id, group_id, due_at
        FROM tasks
        WHERE status='published'
    """).fetchall()

    for t in tasks:
        try:
            due = parse_dt(t["due_at"])
        except:
            continue
        if datetime.now() <= due:
            continue

        gid = int(t["group_id"])
        task_id = int(t["id"])

        # get members
        members = conn.execute("SELECT user_id FROM members WHERE group_id=?", (gid,)).fetchall()
        limit_row = conn.execute("SELECT tg_chat_id, task_miss_limit FROM groups WHERE id=?", (gid,)).fetchone()
        tg_chat_id = int(limit_row["tg_chat_id"]) if limit_row and limit_row["tg_chat_id"] else None
        lim = int(limit_row["task_miss_limit"]) if limit_row else 5

        for m in members:
            uid = int(m["user_id"])
            sub = conn.execute("SELECT 1 FROM task_submissions WHERE task_id=? AND user_id=?", (task_id, uid)).fetchone()
            if sub:
                continue

            # increment missed_task_count once per task per user: we can mark via a pseudo row in submissions? simplest: use attendance table? We'll use a special log table quickly:
            conn.execute("""CREATE TABLE IF NOT EXISTS task_miss_log(
                task_id INTEGER, group_id INTEGER, user_id INTEGER,
                UNIQUE(task_id, group_id, user_id)
            )""")
            already = conn.execute("SELECT 1 FROM task_miss_log WHERE task_id=? AND group_id=? AND user_id=?",
                                   (task_id, gid, uid)).fetchone()
            if already:
                continue

            conn.execute("INSERT OR IGNORE INTO task_miss_log(task_id, group_id, user_id) VALUES (?,?,?)",
                         (task_id, gid, uid))
            conn.execute("INSERT OR IGNORE INTO counters(group_id, user_id, absent_count, missed_task_count) VALUES (?,?,0,0)",
                         (gid, uid))
            conn.execute("UPDATE counters SET missed_task_count = missed_task_count + 1 WHERE group_id=? AND user_id=?",
                         (gid, uid))
            row = conn.execute("SELECT missed_task_count FROM counters WHERE group_id=? AND user_id=?",
                               (gid, uid)).fetchone()
            cnt = int(row["missed_task_count"]) if row else 0

            # alert DM
            try:
                await bot.send_message(uid, f"⚠️ Vazifa deadline o‘tdi va siz topshirmadingiz.\n"
                                            f"Jarima: <b>{cnt}/{lim}</b>\n"
                                            f"Agar limitdan oshsa guruhdan chiqarilasiz.")
            except:
                pass

            # kick if exceeded
            if cnt >= lim:
                conn.execute("DELETE FROM members WHERE group_id=? AND user_id=?", (gid, uid))
                if tg_chat_id:
                    try:
                        await bot.ban_chat_member(chat_id=tg_chat_id, user_id=uid)
                        await bot.unban_chat_member(chat_id=tg_chat_id, user_id=uid)
                    except:
                        pass
                try:
                    await bot.send_message(uid, "⛔️ Vazifalarni bajarmagani uchun guruhdan chiqarildingiz.")
                except:
                    pass

    conn.commit()
    conn.close()

# =========================
# GLOBAL BROADCAST (text + media)
# =========================
@router.callback_query(F.data == "a:broadcast")
async def a_broadcast(call: CallbackQuery, state: FSMContext):
    if not await guard(call, "broadcast"):
        return
    await state.clear()
    await safe_edit(call, "📢 Barcha userlarga yuboriladigan xabarni yuboring (text yoki media). Bekor: /cancel", kb_home_admin(call.from_user.id))
    await state.set_state(AState.broadcast_any)

@router.message(AState.broadcast_any)
async def a_broadcast_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not has_perm(message.from_user.id, "broadcast"):
        await state.clear()
        return
    conn = db()
    users = conn.execute("SELECT user_id FROM users").fetchall()
    conn.close()

    sent = 0
    for r in users:
        uid = int(r["user_id"])
        try:
            # copy message (works for text & most media)
            await message.copy_to(chat_id=uid)
            sent += 1
        except:
            pass

    await state.clear()
    await message.answer(f"✅ Yuborildi: {sent} ta", reply_markup=kb_admin_home(message.from_user.id))

# =========================
# ADMIN: ADMINS (super only) minimal
# =========================
@router.callback_query(F.data == "a:admins")
async def a_admins(call: CallbackQuery):
    if not is_super(call.from_user.id):
        await call.answer("Faqat super admin.", show_alert=True)
        return
    conn = db()
    admins = conn.execute("SELECT user_id, role FROM admins ORDER BY role DESC").fetchall()
    conn.close()
    text = "👮 <b>Adminlar</b>\n\n" + "\n".join([f"• <code>{a['user_id']}</code> — {a['role']}" for a in admins])
    await safe_edit(call, text, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Menyu", callback_data="a:home")]]))

# =========================
# CANCEL command
# =========================
@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    if is_admin(uid):
        await message.answer("Bekor qilindi.", reply_markup=kb_admin_home(uid))
    else:
        await message.answer("Bekor qilindi.", reply_markup=kb_user_home())

# =========================
# STARTUP TASKS
# =========================
async def on_startup(bot: Bot):
    # periodic enforcement
    async def loop_kick():
        while True:
            try:
                await enforce_kick_limits(bot)
            except:
                pass
            await asyncio.sleep(300)
    asyncio.create_task(loop_kick())


# =========================
# MAIN (single entrypoint)
# =========================
async def start_health_server():
    """
    Koyeb Web Service uses TCP health checks on $PORT (often 8000).
    We open the port using only stdlib (no extra deps) so the instance stays healthy.
    """
    port = int(os.environ.get("PORT", "8000"))

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            # Don't block waiting for request bytes; health checks may only open TCP.
            try:
                await asyncio.wait_for(reader.read(16), timeout=0.2)
            except Exception:
                pass
        
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: 2\r\n"
                b"Connection: close\r\n"
                b"\r\n"
                b"ok"
            )
            writer.write(resp)
            await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
    server = await asyncio.start_server(handle, host="0.0.0.0", port=port)
    logging.info("Health server listening on 0.0.0.0:%s", port)
    return server


# =========================
# MAIN (single entrypoint)
# =========================
async def main():
    if not API_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set. Set environment variable BOT_TOKEN (or DB_PATH for DB).")

    # Start health server (for Koyeb web service) - does not affect bot logic.
    _health_server = await start_health_server()

    bot = Bot(
        token=API_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp.include_router(router)
    try:
        dp.startup.register(on_startup)
    except Exception:
        pass
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
