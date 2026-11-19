#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot: Mohit Tazkiah Library - Complete Arabic Version (Cloud-Ready V8 - Multimedia Support)
This version adds full support for photos and audio recordings linked to series messages.
"""

import json
import os
import logging
import asyncio
import http.server
import threading
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv

# تحميل متغيرات البيئة من ملف .env محلياً إن وجد
load_dotenv(override=False)

import firebase_admin
from firebase_admin import firestore, credentials

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# تهيئة Firebase Admin SDK مع دعم مفاتيح الخدمة لجميع منصات الاستضافة
def init_firebase():
    if firebase_admin._apps:
        return
    
    sa_env = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "").strip()
    gac_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    local_key_path = os.path.join(os.path.dirname(__file__), "service-account.json")
    local_firebase_key = os.path.join(os.path.dirname(__file__), "firebase-key.json")

    # 1. التحقق من متغير FIREBASE_SERVICE_ACCOUNT كنص JSON مباشر
    if sa_env:
        try:
            cred_dict = json.loads(sa_env)
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
            logger.info("Firebase Admin initialized via FIREBASE_SERVICE_ACCOUNT environment variable.")
            return
        except Exception as e:
            logger.error(f"Failed to initialize Firebase with FIREBASE_SERVICE_ACCOUNT: {e}")

    # 2. التحقق من مسار ملف GOOGLE_APPLICATION_CREDENTIALS
    if gac_path and os.path.exists(gac_path):
        try:
            cred = credentials.Certificate(gac_path)
            firebase_admin.initialize_app(cred)
            logger.info(f"Firebase Admin initialized via GOOGLE_APPLICATION_CREDENTIALS: {gac_path}")
            return
        except Exception as e:
            logger.error(f"Failed to initialize Firebase with GOOGLE_APPLICATION_CREDENTIALS: {e}")

    # 3. التحقق من وجود ملف مفتاح محلي باسم service-account.json أو firebase-key.json
    for key_file in (local_key_path, local_firebase_key):
        if os.path.exists(key_file):
            try:
                cred = credentials.Certificate(key_file)
                firebase_admin.initialize_app(cred)
                logger.info(f"Firebase Admin initialized via local key file: {key_file}")
                return
            except Exception as e:
                logger.error(f"Failed to initialize Firebase with local key {key_file}: {e}")

    # 4. محاولة التهيئة الافتراضية التلقائية (في حال وجود بيئة Google Cloud)
    try:
        firebase_admin.initialize_app()
        logger.info("Firebase Admin initialized with default credentials.")
    except Exception as e:
        logger.warning(f"Could not connect to Firebase: {e}. Bot will operate using local fallback files.")

init_firebase()

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, BotCommandScopeDefault
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, BasePersistence
)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.environ.get("ADMIN_ID", "0").strip()
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()
DATA_DIR = os.path.join(os.path.dirname(__file__), "bot_data")

# التأكد من وجود مجلد البيانات
os.makedirs(DATA_DIR, exist_ok=True)

_firestore_client = None

def get_db():
    global _firestore_client
    if _firestore_client is not None:
        return _firestore_client
    try:
        _firestore_client = firestore.client()
        return _firestore_client
    except Exception as e:
        logger.warning(f"Firestore client unavailable: {e}. Falling back to local data.")
        return None

INDEX = {}
SERIES_DATA = {}

def load_json(filename, default=None):
    file_path = os.path.join(DATA_DIR, filename)
    
    # Special handling for admins to try Firestore first
    if "admins" in filename:
        try:
            db = get_db()
            if db:
                admin_doc = db.collection("bot_config").document("admins").get()
                if admin_doc.exists:
                    logger.info("Admins loaded from Firestore.")
                    return admin_doc.to_dict()
        except Exception as e:
            logger.error(f"Error loading admins from Firestore: {e}")

    if not os.path.exists(file_path):
        return default if default is not None else ({} if "index" in filename or "admins" in filename else [])
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception as e:
            logger.error(f"Error reading {filename}: {e}")
            return default if default is not None else ({} if "index" in filename or "admins" in filename else [])

def load_library_from_firestore():
    """
    Initializes the Firestore client and loads all library data, handling sharded series.
    """
    global INDEX, SERIES_DATA
    logger.info("Connecting to Firestore and loading library data (with sharding support)...")
    try:
        db = get_db()
        if not db:
            raise ConnectionError("Firestore client is not available.")
        index_ref = db.collection("library_config").document("index").get()
        if not index_ref.exists:
            logger.critical("FATAL: Firestore 'index' document not found. Falling back to local files.")
            raise FileNotFoundError("Firestore index is missing.")

        INDEX = index_ref.to_dict()
        logger.info(f"Index loaded with {len(INDEX)} series. Now loading content.")

        for sid, s_info in INDEX.items():
            if s_info.get('sharded', False):
                # --- Handle Sharded Series ---
                num_parts = s_info.get('parts', 0)
                logger.info(f"Series '{sid}' is sharded into {num_parts} parts. Loading...")
                
                all_messages = []
                for i in range(num_parts):
                    part_id = f"{sid}_part_{i}"
                    part_ref = db.collection("library_series_content").document(part_id).get()
                    if part_ref.exists:
                        all_messages.extend(part_ref.to_dict().get("messages", []))
                        logger.info(f"  -> Loaded part {i} ('{part_id}')")
                    else:
                        logger.warning(f"  -> Missing part {i} ('{part_id}') for sharded series '{sid}'.")
                
                SERIES_DATA[sid] = all_messages
                logger.info(f"  -> Finished loading sharded series '{sid}' with {len(all_messages)} total messages.")
            else:
                # --- Handle Standard (Non-Sharded) Series ---
                series_ref = db.collection("library_series_content").document(sid).get()
                if series_ref.exists:
                    SERIES_DATA[sid] = series_ref.to_dict().get("messages", [])
                else:
                    logger.warning(f"Firestore document for non-sharded series '{sid}' not found. Assuming empty.")
                    SERIES_DATA[sid] = []

        logger.info(f"Library successfully loaded {len(INDEX)} series from Firestore.")

    except Exception as e:
        logger.critical(f"FATAL: Could not load library from Firestore: {e}", exc_info=True)
        # Fallback to local JSON files if Firestore fails
        logger.warning("Falling back to local JSON files due to Firestore error.")
        INDEX = load_json("index.json", default={})
        SERIES_DATA = {sid: load_json(f"{sid}.json", default=[]) for sid in INDEX}


# نظام المشرفين المتعددين
PRIMARY_ADMIN_ID = int(ADMIN_ID_RAW) if ADMIN_ID_RAW.isdigit() else 0
ADMINS_DATA = {"co_admins": []}

def get_all_admin_ids():
    ids = set()
    if PRIMARY_ADMIN_ID != 0:
        ids.add(PRIMARY_ADMIN_ID)
    for co in ADMINS_DATA.get("co_admins", []):
        if isinstance(co, dict) and "id" in co:
            ids.add(int(co["id"]))
        elif isinstance(co, int) or (isinstance(co, str) and str(co).isdigit()):
            ids.add(int(co))
    extra_env = os.environ.get("ADMIN_IDS", "").strip()
    if extra_env:
        for x in extra_env.split(","):
            x = x.strip()
            if x.isdigit():
                ids.add(int(x))
    return ids

def is_admin(user_id):
    if not user_id:
        return False
    return int(user_id) in get_all_admin_ids()

def save_admins():
    # Save locally
    with open(os.path.join(DATA_DIR, "admins.json"), "w", encoding="utf-8") as f:
        json.dump(ADMINS_DATA, f, ensure_ascii=False, indent=2)
    # Save to Firestore
    try:
        db = get_db()
        db.collection("bot_config").document("admins").set(ADMINS_DATA)
        logger.info("Admins successfully synced to Firestore.")
    except Exception as e:
        logger.error(f"Failed to sync admins to Firestore: {e}")

def save_library():
    """
    Saves the library to local backup and Firestore.
    NOTE: This will NOT save changes to sharded series content to prevent errors.
    """
    # 1. حفظ احتياطي محلي (اختياري)
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(os.path.join(DATA_DIR, "index.json"), "w", encoding="utf-8") as f:
        json.dump(INDEX, f, ensure_ascii=False, indent=2)
    for sid, messages in SERIES_DATA.items():
        with open(os.path.join(DATA_DIR, f"{sid}.json"), "w", encoding="utf-8") as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)
            
    # 2. الحفظ السحابي الدائم والآمن في Firestore
    try:
        db_client = firestore.client()
        # حفظ الفهرس الرئيسي دائماً
        db_client.collection("library_config").document("index").set(INDEX)
        
        # حفظ كل سلسلة ورسائلها كـ Document منفصل, مع تخطي السلاسل المقسمة
        for sid, messages in SERIES_DATA.items():
            s_info = INDEX.get(sid, {})
            if s_info.get('sharded', False):
                logger.warning(f"Skipping Firestore save for sharded series '{sid}'. Edits to sharded series must be done via re-upload.")
                continue # Do not attempt to save large sharded series from the bot

            db_client.collection("library_series_content").document(sid).set({"messages": messages})

        logger.info("Library non-sharded content successfully synced to Firestore Cloud.")
    except Exception as e:
        logger.error(f"Failed to sync library to Firestore: {e}")
        
    sync_database()

def sync_database():
    """Mirror Telegram-admin edits into the durable MySQL database when configured."""
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        return
    try:
        import pymysql
        parsed = urlparse(database_url)
        connection = pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=parsed.username,
            password=parsed.password,
            database=(parsed.path or "/").lstrip("/"),
            charset="utf8mb4",
            autocommit=False,
            connect_timeout=10,
        )
        try:
            with connection.cursor() as cursor:
                if INDEX:
                    placeholders = ",".join(["%s"] * len(INDEX))
                    cursor.execute(f"DELETE FROM library_series WHERE id NOT IN ({placeholders})", tuple(INDEX.keys()))
                else:
                    cursor.execute("DELETE FROM library_series")
                for order, (sid, info) in enumerate(INDEX.items(), 1):
                    cursor.execute(
                        "INSERT INTO library_series (id,title,topic,description,sortOrder) VALUES (%s,%s,%s,%s,%s) "
                        "ON DUPLICATE KEY UPDATE title=VALUES(title),topic=VALUES(topic),description=VALUES(description),sortOrder=VALUES(sortOrder)",
                        (sid, info.get("title", ""), info.get("topic", ""), info.get("description"), order),
                    )
                for sid, messages in SERIES_DATA.items():
                    cursor.execute("DELETE FROM library_messages WHERE seriesId=%s", (sid,))
                    for order, msg in enumerate(messages, 1):
                        cursor.execute(
                            "INSERT INTO library_messages (seriesId,sourceId,title,topic,content,sortOrder) VALUES (%s,%s,%s,%s,%s,%s)",
                            (sid, msg.get("id", order), msg.get("title", ""), msg.get("topic", ""), msg.get("text", ""), order),
                        )
            connection.commit()
        finally:
            connection.close()
    except Exception as error:
        logger.warning("Durable library sync skipped: %s", error)

# ─── دالة تحليل نطاق الأرقام للتحديد المتعدد ───
def parse_range_string(txt, max_val):
    result = set()
    parts = txt.replace("،", ",").split(",")
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            sub = part.split("-")
            if len(sub) == 2 and sub[0].strip().isdigit() and sub[1].strip().isdigit():
                start_n, end_n = int(sub[0].strip()), int(sub[1].strip())
                if start_n > end_n:
                    start_n, end_n = end_n, start_n
                for num in range(max(1, start_n), min(max_val, end_n) + 1):
                    result.add(num)
        elif part.isdigit():
            num = int(part)
            if 1 <= num <= max_val:
                result.add(num)
    return result

def get_bulk_selection(ctx, sid):
    if "bulk_sel" not in ctx.chat_data:
        ctx.chat_data["bulk_sel"] = {}
    if sid not in ctx.chat_data["bulk_sel"]:
        ctx.chat_data["bulk_sel"][sid] = set()
    return ctx.chat_data["bulk_sel"][sid]

# ─── HTML Escape ───
def esc(t):
    if not t:
        return ""
    return (str(t)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

# ─── User Data ───
def get_ud(ctx):
    if "ud" not in ctx.chat_data:
        ctx.chat_data["ud"] = {
            "bookmarks": {},
            "favorites": [],
            "progress": {},
            "settings": {"theme": "dark", "font": "medium", "notif": True},
            "stats": {"read": 0, "started": set(), "searches": 0, "last": None}
        }
    return ctx.chat_data["ud"]

# ─── Main Menu Keyboard ───
def main_kb():
    k = []
    row = []
    for i, (sid, info) in enumerate(INDEX.items(), 1):
        icon = info.get('icon', '📚')
        title = info.get('title', sid)
        display_title = title.split(' ', 1)[1] if ' ' in title else title
        row.append(InlineKeyboardButton(
            f"{icon} {display_title}",
            callback_data=f"s:{sid}"
        ))
        if i % 2 == 0:
            k.append(row)
            row = []
    if row:
        k.append(row)
    k.append([
        InlineKeyboardButton("🔖 الإشارات المرجعية", callback_data="m:bm"),
        InlineKeyboardButton("⭐ المفضلة", callback_data="m:fav")
    ])
    k.append([
        InlineKeyboardButton("📊 إحصائياتي", callback_data="m:st"),
        InlineKeyboardButton("🔍 البحث في المكتبة", callback_data="m:sr")
    ])
    k.append([
        InlineKeyboardButton("⚙️ الإعدادات", callback_data="m:set"),
        InlineKeyboardButton("ℹ️ عن المكتبة", callback_data="m:ab")
    ])
    return InlineKeyboardMarkup(k)

# ─── Reading Navigation Keyboard ───
def read_kb(sid, mid, total, bm=False, fav=False, part_idx=1, total_parts=1):
    keyboard = []
    
    if total_parts > 1:
        sub_nav = []
        if part_idx > 1:
            sub_nav.append(InlineKeyboardButton("◀️ الجزء السابق", callback_data=f"r:{sid}:{mid}:{part_idx-1}"))
        sub_nav.append(InlineKeyboardButton(f"جزء {part_idx} من {total_parts}", callback_data="noop"))
        if part_idx < total_parts:
            sub_nav.append(InlineKeyboardButton("الجزء التالي ▶️", callback_data=f"r:{sid}:{mid}:{part_idx+1}"))
        keyboard.append(sub_nav)

    nav = []
    if mid > 1:
        nav.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"r:{sid}:{mid-1}:1"))
    nav.append(InlineKeyboardButton(f"{mid} / {total}", callback_data="noop"))
    if mid < total:
        nav.append(InlineKeyboardButton("التالي ➡️", callback_data=f"r:{sid}:{mid+1}:1"))
    keyboard.append(nav)

    act = [
        InlineKeyboardButton("🔖 إزالة الإشارة" if bm else "🔖 إشارة مرجعية", callback_data=f"a:bm:{sid}:{mid}:{part_idx}"),
        InlineKeyboardButton("⭐ إزالة المفضلة" if fav else "⭐ أضف للمفضلة", callback_data=f"a:fav:{sid}:{mid}:{part_idx}")
    ]
    keyboard.append(act)

    ext = [
        InlineKeyboardButton("🔢 انتقال سريع", callback_data=f"a:jp:{sid}"),
        InlineKeyboardButton("🔍 بحث في السلسلة", callback_data=f"a:sr:{sid}"),
        InlineKeyboardButton("❓ استفسار للمؤلف", callback_data=f"a:in:{sid}:{mid}")
    ]
    keyboard.append(ext)

    back = [
        InlineKeyboardButton("📋 قائمة السلسلة", callback_data=f"s:{sid}"),
        InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")
    ]
    keyboard.append(back)

    return InlineKeyboardMarkup(keyboard)

# ─── Format Message Header ───
def fmt_msg(msg, title, total, part_idx=1, total_parts=1, text_part=""):
    mid = msg.get('id', 1)
    date_str = msg.get('date', '')
    length_str = msg.get('length', len(msg.get('text', '')))
    date_line = f"📅 {esc(date_str)} | " if date_str else ""
    
    part_header = f" (الجزء {part_idx}/{total_parts})" if total_parts > 1 else ""
    
    body = text_part if text_part else esc(msg.get('text', ''))
    
    # Add interactive texts for specific series/contexts
    if "nafs" in title.lower() or "nafs" in msg.get('category', ''):
        if "واحذر من الأعمال الصالحة بلا علم" not in body:
             body += "\n\n💡 واحذر من الأعمال الصالحة بلا علم ...... هل انت معي؟❓"

    return (
        f"<b>{esc(title)}</b>\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"📄 <b>الرسالة {mid}</b>{part_header} من أصل <b>{total}</b>\n"
        f"{date_line}📝 <b>{length_str}</b> حرف\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        f"{body}"
    )


# ─── /start ───
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    ud = get_ud(ctx)
    ud["stats"]["last"] = datetime.now().isoformat()
    total_all = sum(len(SERIES_DATA.get(sid, [])) for sid in SERIES_DATA)
    txt = (
        f"أهلاً بك <b>{esc(u.first_name)}</b> في 🤖 <b>مكتبة المحيط للتزكية</b>\n\n"
        f"📚 د. سامي المؤيد — {len(INDEX)} سلسلة — {total_all} رسالة\n"
        f"📅 2017 – 2026\n\n"
        f"✨ <b>المميزات:</b>\n"
        f"• 📖 قراءة منظمة لكل سلسلة\n"
        f"• 🔍 بحث ذكي في كل المكتبة\n"
        f"• 🔖 إشارات مرجعية + حفظ التقدم\n"
        f"• ⭐ مفضلة + ❓ استفسار للمؤلف والمشرفين\n\n"
        f"👇 اختر سلسلة للبدء:"
    )
    await update.message.reply_text(txt, parse_mode="HTML", reply_markup=main_kb())

# ─── /help ───
async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 <b>دليل أوامر واستخدام البوت</b>\n\n"
        "🏠 /start — فتح القائمة الرئيسية للسلاسل\n"
        "📌 /continue — مواصلة القراءة من آخر نقطة توقفت عندها\n"
        "📊 /stats — إحصائيات قراءتك وسلاسل المشاهدة\n"
        "🔍 /search — البحث المباشر في المكتبة\n"
        "🔖 /bookmarks — استعراض إشاراتك المرجعية\n"
        "⭐ /favorites — استعراض رسائلك المفضلة\n"
        "🆔 /whoami — معرفة معرّف حسابك الرقمي في تيليجرام\n"
        "🔐 /admin — لوحة إدارة محتوى المكتبة والمشرفين\n\n"
        "💡 <i>يمكنك أيضاً كتابة أي نص في المحادثة للبحث الفوري في جميع محتويات المكتبة.</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")]])
    )

# ─── /continue ───
async def cont_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ud = get_ud(ctx)
    prog = ud.get("progress", {})
    if not prog:
        await update.message.reply_text(
            "لم تبدأ قراءة أي سلسلة بعد. اضغط /start للبدء.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 رئيسية", callback_data="m:main")]])
        )
        return
    k = []
    for sid, mid in prog.items():
        if sid in INDEX:
            info = INDEX[sid]
            k.append([InlineKeyboardButton(
                f"{info.get('icon', '📚')} {info.get('title', sid)} (رسالة {mid})",
                callback_data=f"r:{sid}:{mid}"
            )])
    k.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")])
    await update.message.reply_text("📌 <b>مواصلة القراءة:</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(k))

# ─── /stats ───
async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ud = get_ud(ctx)
    st = ud["stats"]
    txt = (
        f"📊 <b>إحصائيات قراءتك</b>\n\n"
        f"📖 رسائل مقروءة: <b>{st['read']}</b>\n"
        f"📚 سلاسل بدأتها: <b>{len(st['started'])}</b>\n"
        f"🔍 عمليات بحث: <b>{st['searches']}</b>\n"
        f"🔖 إشارات مرجعية: <b>{sum(len(v) for v in ud['bookmarks'].values())}</b>\n"
        f"⭐ مفضلة: <b>{len(ud['favorites'])}</b>"
    )
    await update.message.reply_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(
        [[InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")]]
    ))

# ─── /search ───
async def search_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 <b>البحث في المكتبة</b>\nأرسل الكلمة أو العبارة التي تبحث عنها:", parse_mode="HTML")
    ctx.chat_data["mode"] = "search_global"

# ─── /bookmarks ───
async def bookmarks_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ud = get_ud(ctx)
    bm = ud.get("bookmarks", {})
    if not any(bm.values()):
        await update.message.reply_text(
            "🔖 <b>الإشارات المرجعية</b>\n\nلا توجد إشارات مرجعية محفوظة بعد.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 رئيسية", callback_data="m:main")]])
        )
        return
    k = []
    for sid, mids in bm.items():
        for mid in mids:
            if sid in INDEX:
                info = INDEX[sid]
                k.append([InlineKeyboardButton(
                    f"{info.get('icon', '📚')} {info.get('title', sid)} — رسالة {mid}",
                    callback_data=f"r:{sid}:{mid}"
                )])
    k.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")])
    await update.message.reply_text("🔖 <b>إشاراتك المرجعية:</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(k))

# ─── /favorites ───
async def favorites_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ud = get_ud(ctx)
    fav = ud.get("favorites", [])
    if not fav:
        await update.message.reply_text(
            "⭐ <b>المفضلة</b>\n\nلا توجد رسائل في المفضلة بعد.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 رئيسية", callback_data="m:main")]])
        )
        return
    k = []
    for sid, mid in fav:
        if sid in INDEX:
            info = INDEX[sid]
            k.append([InlineKeyboardButton(
                f"{info.get('icon', '📚')} {info.get('title', sid)} — رسالة {mid}",
                callback_data=f"r:{sid}:{mid}"
            )])
    k.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")])
    await update.message.reply_text("⭐ <b>رسائلك المفضلة:</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(k))

# ─── /whoami ───
async def whoami_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.message.reply_text("تعذر قراءة معرّف حساب Telegram.")
        return
    admin_status = " (👑 مسؤول في المكتبة)" if is_admin(user.id) else ""
    await update.message.reply_text(
        f"🆔 معرّف حسابك في تيليجرام:\n<code>{user.id}</code>{admin_status}",
        parse_mode="HTML"
    )

# ─── لوحة المشرفين (Admin Panel) ───
def admin_menu_kb(user_id=None):
    rows = [
        [InlineKeyboardButton("📚 إدارة السلاسل والمحتوى", callback_data="ad:series")],
        [InlineKeyboardButton("➕ إنشاء سلسلة جديدة", callback_data="ad:newseries")],
        [InlineKeyboardButton("📣 إرسال بث للجميع (Broadcast)", callback_data="ad:broadcast")],
        [InlineKeyboardButton("👥 إدارة المسؤولين والمشرفين", callback_data="ad:admins")],
        [InlineKeyboardButton("📊 إحصائيات عامة للمكتبة", callback_data="ad:stats")],
        [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")],
    ]
    return InlineKeyboardMarkup(rows)

async def admin_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not is_admin(user.id):
        logger.warning("/admin denied for user_id=%s", user.id if user else None)
        await update.message.reply_text("⛔ هذا الأمر مخصص لمديري ومشرفي المكتبة فقط.")
        return
    await update.message.reply_text(
        "🔐 <b>لوحة تحكم وإدارة مكتبة المحيط</b>\n\nاختر القسم المطلوب لإدارته فوراً:",
        parse_mode="HTML", reply_markup=admin_menu_kb(user.id)
    )

# ─── عرض واجهة التحديد المتعدد ───
async def render_bulk_view(q, ctx, sid, page):
    msgs = SERIES_DATA.get(sid, [])
    selected = get_bulk_selection(ctx, sid)
    per_page = 10
    total_pages = max(1, (len(msgs) + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    current_msgs = msgs[start_idx:end_idx]

    rows = []
    for msg in current_msgs:
        mid = msg['id']
        is_sel = mid in selected
        mark = "✅" if is_sel else "⬜"
        title_preview = msg.get('title') or (msg.get('text', '')[:25] + '...')
        rows.append([InlineKeyboardButton(f"{mark} {mid}. {title_preview}", callback_data=f"ad:btoggle:{sid}:{mid}:{page}")])

    # أزرار التنقل
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton("⬅️ السابقة", callback_data=f"ad:bulk:{sid}:{page-1}"))
    nav_row.append(InlineKeyboardButton(f"📄 صفحة {page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton("التالية ➡️", callback_data=f"ad:bulk:{sid}:{page+1}"))
    if nav_row:
        rows.append(nav_row)

    # أدوات التحديد السريع
    rows.append([
        InlineKeyboardButton("🔘 تحديد الصفحة الحالية", callback_data=f"ad:bsel_page:{sid}:{page}"),
        InlineKeyboardButton("🔢 تحديد بنطاق (مثال: 5-20)", callback_data=f"ad:bsel_range:{sid}:{page}")
    ])
    rows.append([
        InlineKeyboardButton("🔄 عكس التحديد", callback_data=f"ad:binvert:{sid}:{page}"),
        InlineKeyboardButton("❌ إلغاء تحديد الكل", callback_data=f"ad:bclear:{sid}:{page}")
    ])

    # شريط العمليات إذا كان هناك رسائل محددة
    sel_count = len(selected)
    if sel_count > 0:
        rows.append([
            InlineKeyboardButton(f"🗑 حذف المحدد ({sel_count} رسالة)", callback_data=f"ad:bdel_confirm:{sid}:{page}"),
            InlineKeyboardButton(f"🔄 نقل المحدد ({sel_count} رسالة)", callback_data=f"ad:bmove_choose:{sid}:{page}")
        ])

    rows.append([InlineKeyboardButton("⬅️ خروج من وضع التحديد", callback_data=f"ad:msgs:{sid}:{page}")])

    txt = (
        f"☑️ <b>وضع التحديد المتعدد — {esc(INDEX[sid].get('title', sid))}</b>\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
        f"📌 عدد الرسائل المحددة حالياً: <b>{sel_count}</b> من أصل <b>{len(msgs)}</b>\n"
        f"💡 <i>اضغط على أي رسالة لتحديدها/إلغائها، أو استخدم أزرار التحديد السريع أدناه:</i>"
    )
    await q.edit_message_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))

async def admin_callback(update, ctx, data):
    q = update.callback_query
    user = update.effective_user
    if not is_admin(user.id):
        await q.answer("⛔ غير مصرح لك بالوصول", show_alert=True)
        return True

    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "home"

    if action == "home":
        await q.edit_message_text("🔐 <b>لوحة إدارة مكتبة المحيط</b>\n\nاختر العملية المطلوبة:", parse_mode="HTML", reply_markup=admin_menu_kb(user.id))

    # ── إدارة السلاسل ──
    elif action == "series":
        rows = []
        for i, (sid, info) in enumerate(INDEX.items(), 1):
            rows.append([InlineKeyboardButton(f"{i}. {info.get('title', sid)} ({len(SERIES_DATA.get(sid, []))} رسالة)", callback_data=f"ad:sel:{sid}")])
        rows.append([InlineKeyboardButton("➕ إنشاء سلسلة جديدة", callback_data="ad:newseries")])
        rows.append([InlineKeyboardButton("⬅️ العودة للوحة الإدارة", callback_data="ad:home")])
        await q.edit_message_text("📚 <b>اختر السلسلة لإدارتها أو تعديل ترتيبها ومحتواها:</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))

    elif action == "newseries":
        ctx.chat_data["admin_state"] = {"kind": "new_series"}
        await q.edit_message_text(
            "➕ <b>إنشاء سلسلة جديدة</b>\n\n"
            "أرسل بيانات السلسلة بالترتيب التالي مفصولة بـ <code>|</code>:\n"
            "<b>المعرّف | الأيقونة | العنوان | الموضوع | الوصف</b>\n\n"
            "📌 <b>مثال:</b>\n"
            "<code>series2 | 🌸 | 🌸 سلسلة أمراض القلوب | أمراض القلوب وعلاجها | شرح مفصل لأمراض القلوب</code>",
            parse_mode="HTML"
        )

    elif action == "broadcast":
        ctx.chat_data["admin_state"] = {"kind": "broadcast"}
        await q.edit_message_text(
            "📣 <b>إرسال بث لجميع مستخدمي المكتبة</b>\n\n"
            "اكتب نص الرسالة التي تريد إرسالها للجميع الآن.\n"
            "📌 <i>سيصل البث لكل المستخدمين المسجلين في قاعدة البيانات.</i>",
            parse_mode="HTML"
        )

    elif action == "sel":
        sid = parts[2]
        if sid not in INDEX:
            await q.answer("السلسلة غير موجودة")
            return True
        info = INDEX[sid]
        msgs = SERIES_DATA.get(sid, [])
        total_chars = sum(m.get("length", len(m.get("text", ""))) for m in msgs)
        items = list(INDEX.keys())
        current_pos = items.index(sid) + 1 if sid in items else 1
        await q.edit_message_text(
            f"📚 <b>{esc(info.get('title', sid))}</b>\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
            f"🔢 الترتيب الحالي: <b>{current_pos}</b> من أصل <b>{len(INDEX)}</b>\n"
            f"📌 الموضوع: {esc(info.get('topic', 'غير محدد'))}\n"
            f"📝 عدد الرسائل: <b>{len(msgs)}</b> رسالة\n"
            f"📊 إجمالي الأحرف: <b>{total_chars}</b> حرف\n"
            f"📅 الفترة: {esc(info.get('period', 'غير محددة'))}\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📝 إدارة الرسائل والتعديل", callback_data=f"ad:msgs:{sid}")],
                [InlineKeyboardButton("☑️ التحديد المتعدد (نقل/حذف جماعي)", callback_data=f"ad:bulk:{sid}:1")],
                [InlineKeyboardButton("✏️ تعديل العنوان", callback_data=f"ad:editseries:{sid}:title"), InlineKeyboardButton("✏️ تعديل الموضوع", callback_data=f"ad:editseries:{sid}:topic")],
                [InlineKeyboardButton("✏️ تعديل الأيقونة", callback_data=f"ad:editseries:{sid}:icon"), InlineKeyboardButton("✏️ تعديل الوصف", callback_data=f"ad:editseries:{sid}:description")],
                [InlineKeyboardButton("🔢 نقل السلسلة لترتيب محدد", callback_data=f"ad:setpos_series:{sid}")],
                [InlineKeyboardButton("⬆️ رفع مركز", callback_data=f"ad:move:{sid}:up"), InlineKeyboardButton("⬇️ خفض مركز", callback_data=f"ad:move:{sid}:down")],
                [InlineKeyboardButton("🗑 حذف السلسلة بالكامل", callback_data=f"ad:delseriesconfirm:{sid}")],
                [InlineKeyboardButton("⬅️ قائمة السلاسل", callback_data="ad:series")]
            ])
        )

    elif action == "editseries":
        sid, field = parts[2], parts[3]
        ctx.chat_data["admin_state"] = {"kind": "edit_series", "sid": sid, "field": field}
        labels = {"title": "العنوان", "topic": "الموضوع", "icon": "الأيقونة (رمز تعبيري)", "description": "الوصف"}
        await q.edit_message_text(f"✏️ أرسل <b>{labels.get(field, field)}</b> الجديد للسلسلة:\n<b>{esc(INDEX[sid].get('title', sid))}</b>", parse_mode="HTML")

    elif action == "setpos_series":
        sid = parts[2]
        ctx.chat_data["admin_state"] = {"kind": "setpos_series", "sid": sid}
        await q.edit_message_text(
            f"🔢 <b>نقل السلسلة لموضع محدد</b>\n\n"
            f"السلسلة: <b>{esc(INDEX[sid].get('title', sid))}</b>\n"
            f"أرسل رقم الترتيب الجديد الذي تريده (من 1 إلى {len(INDEX)}):",
            parse_mode="HTML"
        )

    # ── إدارة الرسائل الفردية ──
    elif action == "msgs":
        sid = parts[2]
        msgs = SERIES_DATA.get(sid, [])
        page = int(parts[3]) if len(parts) > 3 else 1
        per_page = 10
        total_pages = max(1, (len(msgs) + per_page - 1) // per_page)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        current_msgs = msgs[start_idx:end_idx]

        rows = []
        for msg in current_msgs:
            title_preview = msg.get('title') or (msg.get('text', '')[:30] + '...')
            rows.append([InlineKeyboardButton(f"{msg['id']}. {title_preview}", callback_data=f"ad:msg:{sid}:{msg['id']}")])

        # أزرار التنقل بين الصفحات
        nav_row = []
        if page > 1:
            nav_row.append(InlineKeyboardButton("⬅️ السابقة", callback_data=f"ad:msgs:{sid}:{page-1}"))
        nav_row.append(InlineKeyboardButton(f"📄 صفحة {page}/{total_pages}", callback_data="noop"))
        if page < total_pages:
            nav_row.append(InlineKeyboardButton("التالية ➡️", callback_data=f"ad:msgs:{sid}:{page+1}"))
        if nav_row:
            rows.append(nav_row)

        rows.append([
            InlineKeyboardButton("☑️ التحديد المتعدد (نقل/حذف جماعي)", callback_data=f"ad:bulk:{sid}:{page}"),
            InlineKeyboardButton("➕ إضافة رسالة جديدة", callback_data=f"ad:newmsg:{sid}")
        ])
        rows.append([InlineKeyboardButton("⬅️ عودة للسلسلة", callback_data=f"ad:sel:{sid}")])

        await q.edit_message_text(
            f"📝 <b>إدارة رسائل: {esc(INDEX[sid].get('title', sid))}</b>\n"
            f"إجمالي الرسائل: <b>{len(msgs)}</b> رسالة — اضغط على رسالة لتعديلها أو نقلها، أو اختر التحديد المتعدد للعمليات الجماعية:",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows)
        )

    # ── التحديد المتعدد (Bulk Operations) ──
    elif action == "bulk":
        sid = parts[2]
        page = int(parts[3]) if len(parts) > 3 else 1
        await render_bulk_view(q, ctx, sid, page)

    elif action == "btoggle":
        sid, mid, page = parts[2], int(parts[3]), int(parts[4])
        selected = get_bulk_selection(ctx, sid)
        if mid in selected:
            selected.remove(mid)
        else:
            selected.add(mid)
        await render_bulk_view(q, ctx, sid, page)

    elif action == "bsel_page":
        sid, page = parts[2], int(parts[3])
        msgs = SERIES_DATA.get(sid, [])
        selected = get_bulk_selection(ctx, sid)
        per_page = 10
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        page_ids = [m['id'] for m in msgs[start_idx:end_idx]]
        
        if all(mid in selected for mid in page_ids):
            for mid in page_ids:
                selected.discard(mid)
        else:
            for mid in page_ids:
                selected.add(mid)
        await render_bulk_view(q, ctx, sid, page)

    elif action == "binvert":
        sid, page = parts[2], int(parts[3])
        msgs = SERIES_DATA.get(sid, [])
        selected = get_bulk_selection(ctx, sid)
        all_ids = set(m['id'] for m in msgs)
        ctx.chat_data["bulk_sel"][sid] = all_ids - selected
        await render_bulk_view(q, ctx, sid, page)

    elif action == "bclear":
        sid, page = parts[2], int(parts[3])
        if "bulk_sel" in ctx.chat_data and sid in ctx.chat_data["bulk_sel"]:
            ctx.chat_data["bulk_sel"][sid].clear()
        await q.answer("تم إلغاء التحديد")
        await render_bulk_view(q, ctx, sid, page)

    elif action == "bsel_range":
        sid, page = parts[2], int(parts[3])
        total = len(SERIES_DATA.get(sid, []))
        ctx.chat_data["admin_state"] = {"kind": "bulk_select_range", "sid": sid, "page": page, "total": total}
        await q.edit_message_text(
            f"🔢 <b>التحديد السريع بنطاق أرقام</b>\n\n"
            f"السلسلة تحتوي على <b>{total}</b> رسالة.\n"
            f"أرسل نطاق الأرقام المطلوب تحديدها بالشكل:\n"
            f"• <code>5-20</code> (لتحديد الرسائل من 5 إلى 20)\n"
            f"• <code>1, 5, 10-15</code> (لتحديد أرقام ونطاقات متعددة)\n\n"
            f"اكتب النطاق وأرسله الآن:",
            parse_mode="HTML"
        )

    elif action == "bdel_confirm":
        sid, page = parts[2], int(parts[3])
        selected = get_bulk_selection(ctx, sid)
        if not selected:
            await q.answer("لم تحدد أي رسائل بعد")
            return True
        await q.edit_message_text(
            f"⚠️ <b>تأكيد الحذف الجماعي</b>\n\n"
            f"هل تريد بالتأكيد حذف <b>{len(selected)}</b> رسالة محددة من سلسلة <b>{esc(INDEX[sid].get('title', sid))}</b> نهائياً؟\n"
            f"<i>سيتم إعادة ترقيم باقي الرسائل تلقائياً بعد الحذف.</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🗑 نعم، احذف {len(selected)} رسالة", callback_data=f"ad:bdel_do:{sid}:{page}")],
                [InlineKeyboardButton("❌ إلغاء", callback_data=f"ad:bulk:{sid}:{page}")]
            ])
        )

    elif action == "bdel_do":
        sid, page = parts[2], int(parts[3])
        selected = get_bulk_selection(ctx, sid)
        del_count = len(selected)
        if not selected:
            await q.answer("لا توجد رسائل محددة")
            return True
        
        SERIES_DATA[sid] = [m for m in SERIES_DATA.get(sid, []) if m['id'] not in selected]
        for i, m in enumerate(SERIES_DATA[sid], 1):
            m['id'] = i
        selected.clear()
        save_library()
        await q.answer(f"✅ تم حذف {del_count} رسالة بنجاح")
        await q.edit_message_text(
            f"✅ <b>تم حذف {del_count} رسالة بنجاح</b> وأعيد ترقيم باقي رسائل السلسلة تلقائياً.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ العودة للرسائل", callback_data=f"ad:msgs:{sid}:{page}")]])
        )

    elif action == "bmove_choose":
        sid, page = parts[2], int(parts[3])
        selected = get_bulk_selection(ctx, sid)
        if not selected:
            await q.answer("لم تحدد أي رسائل بعد")
            return True
        rows = []
        for other_sid, other_info in INDEX.items():
            if other_sid != sid:
                rows.append([InlineKeyboardButton(f"➡️ إلى: {other_info.get('title', other_sid)}", callback_data=f"ad:bmove_do:{sid}:{other_sid}:{page}")])
        rows.append([InlineKeyboardButton("❌ إلغاء", callback_data=f"ad:bulk:{sid}:{page}")])
        await q.edit_message_text(
            f"🔄 <b>النقل الجماعي لـ ({len(selected)}) رسالة محددة</b>\n\n"
            f"اختر السلسلة التي ترغب بنقل هذه الرسائل إليها:",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows)
        )

    elif action == "bmove_do":
        from_sid, to_sid, page = parts[2], parts[3], int(parts[4])
        selected = get_bulk_selection(ctx, from_sid)
        move_count = len(selected)
        if not selected:
            await q.answer("لا توجد رسائل محددة")
            return True
        
        msgs_from = SERIES_DATA.get(from_sid, [])
        to_move = [m for m in msgs_from if m['id'] in selected]
        remaining = [m for m in msgs_from if m['id'] not in selected]
        
        for i, m in enumerate(remaining, 1):
            m['id'] = i
        SERIES_DATA[from_sid] = remaining
        
        if to_sid not in SERIES_DATA:
            SERIES_DATA[to_sid] = []
        start_to_id = len(SERIES_DATA[to_sid]) + 1
        for i, m in enumerate(to_move, start_to_id):
            m['id'] = i
            SERIES_DATA[to_sid].append(m)
            
        selected.clear()
        save_library()
        await q.answer(f"✅ تم نقل {move_count} رسالة بنجاح")
        await q.edit_message_text(
            f"✅ <b>تم نقل {move_count} رسالة بنجاح</b> إلى سلسلة <b>{esc(INDEX[to_sid].get('title', to_sid))}</b>.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ العودة للرسائل", callback_data=f"ad:msgs:{from_sid}:{page}")]])
        )

    # ── إدارة رسالة فردية ──
    elif action == "msg":
        sid, mid = parts[2], int(parts[3])
        msgs = SERIES_DATA.get(sid, [])
        msg = next((m for m in msgs if m['id'] == mid), None)
        if not msg:
            await q.answer("الرسالة غير موجودة")
            return True
        
        media_info = ""
        if msg.get('photo_id'): media_info += "\n🖼️ تحتوي على صورة"
        if msg.get('audio_id'): media_info += "\n🔊 تحتوي على تسجيل صوتي"

        await q.edit_message_text(
            f"📝 <b>إدارة الرسالة رقم {mid} من {len(msgs)}</b>\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
            f"📌 العنوان: <b>{esc(msg.get('title', 'بدون عنوان'))}</b>\n"
            f"🏷️ الموضوع: <b>{esc(msg.get('topic', 'غير محدد'))}</b>\n"
            f"📅 التاريخ: <b>{esc(msg.get('date', 'غير محدد'))}</b>\n"
            f"📝 الحجم: <b>{msg.get('length', len(msg.get('text', '')))}</b> حرف{media_info}\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
            f"📖 <b>مقتطف من المحتوى:</b>\n{esc(msg.get('text', '')[:400])}...",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔢 نقل الترتيب لموضع محدد", callback_data=f"ad:setpos_msg:{sid}:{mid}")],
                [InlineKeyboardButton("✏️ تعديل العنوان", callback_data=f"ad:editmsg:{sid}:{mid}:title"), InlineKeyboardButton("✏️ تعديل الموضوع", callback_data=f"ad:editmsg:{sid}:{mid}:topic")],
                [InlineKeyboardButton("✏️ تعديل نص المحتوى", callback_data=f"ad:editmsg:{sid}:{mid}:text"), InlineKeyboardButton("✏️ تعديل التاريخ", callback_data=f"ad:editmsg:{sid}:{mid}:date")],
                [InlineKeyboardButton("🖼️/🔊 إضافة/تعديل وسائط", callback_data=f"ad:editmedia:{sid}:{mid}")],
                [InlineKeyboardButton("🔄 نقل إلى سلسلة أخرى", callback_data=f"ad:moveto_series:{sid}:{mid}")],
                [InlineKeyboardButton("⬆️ رفع خطوة", callback_data=f"ad:move_msg:{sid}:{mid}:up"), InlineKeyboardButton("⬇️ خفض خطوة", callback_data=f"ad:move_msg:{sid}:{mid}:down")],
                [InlineKeyboardButton("🗑 حذف الرسالة", callback_data=f"ad:delmsgconfirm:{sid}:{mid}")],
                [InlineKeyboardButton("⬅️ قائمة الرسائل", callback_data=f"ad:msgs:{sid}")]
            ])
        )

    elif action == "editmedia":
        sid, mid = parts[2], int(parts[3])
        ctx.chat_data["admin_state"] = {"kind": "edit_media", "sid": sid, "mid": mid}
        await q.edit_message_text(
            f"🖼️/🔊 <b>تحديث الوسائط للرسالة رقم {mid}</b>\n\n"
            f"أرسل الآن الصورة أو الملف الصوتي الذي تريد ربطه بهذه الرسالة.\n\n"
            f"💡 <i>ملاحظة: سيتم استبدال أي وسيط قديم من نفس النوع بالملف الجديد.</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء", callback_data=f"ad:msg:{sid}:{mid}")]])
        )

    elif action == "setpos_msg":
        sid, mid = parts[2], int(parts[3])
        total = len(SERIES_DATA.get(sid, []))
        ctx.chat_data["admin_state"] = {"kind": "setpos_msg", "sid": sid, "mid": mid, "total": total}
        await q.edit_message_text(
            f"🔢 <b>نقل الرسالة لموضع محدد مباشرة</b>\n\n"
            f"الرسالة الحالية رقم: <b>{mid}</b>\n"
            f"أرسل رقم الترتيب الجديد الذي تريده (مثلاً: <code>1</code> أو <code>5</code> من إجمالي {total}):",
            parse_mode="HTML"
        )

    elif action == "editmsg":
        sid, mid, field = parts[2], int(parts[3]), parts[4]
        ctx.chat_data["admin_state"] = {"kind": "edit_msg", "sid": sid, "mid": mid, "field": field}
        labels = {"title": "العنوان", "topic": "الموضوع", "text": "نص المحتوى الكامل", "date": "التاريخ (مثال: 2024-05-15)"}
        await q.edit_message_text(f"✏️ أرسل <b>{labels.get(field, field)}</b> الجديد للرسالة رقم {mid}:", parse_mode="HTML")

    elif action == "newmsg":
        sid = parts[2]
        ctx.chat_data["admin_state"] = {"kind": "new_msg", "sid": sid}
        await q.edit_message_text(
            "➕ <b>إضافة رسالة جديدة للسلسلة</b>\n\n"
            "أرسل الرسالة بالشكل التالي مفصولة بـ <code>|</code>:\n"
            "<b>العنوان | الموضوع | نص المحتوى</b>\n\n"
            "💡 أو أرسل نص المحتوى فقط مباشرة وسيقوم البوت بإنشائها تلقائياً.",
            parse_mode="HTML"
        )

    elif action == "moveto_series":
        sid, mid = parts[2], int(parts[3])
        rows = []
        for other_sid, other_info in INDEX.items():
            if other_sid != sid:
                rows.append([InlineKeyboardButton(f"➡️ إلى: {other_info.get('title', other_sid)}", callback_data=f"ad:domoveto:{sid}:{mid}:{other_sid}")])
        rows.append([InlineKeyboardButton("❌ إلغاء", callback_data=f"ad:msg:{sid}:{mid}")])
        await q.edit_message_text("🔄 <b>اختر السلسلة التي تريد نقل هذه الرسالة إليها:</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))

    elif action == "domoveto":
        from_sid, mid, to_sid = parts[2], int(parts[3]), parts[4]
        msgs_from = SERIES_DATA.get(from_sid, [])
        target_msg = next((m for m in msgs_from if m['id'] == mid), None)
        if target_msg:
            SERIES_DATA[from_sid] = [m for m in msgs_from if m['id'] != mid]
            for i, m in enumerate(SERIES_DATA[from_sid], 1):
                m['id'] = i
            if to_sid not in SERIES_DATA:
                SERIES_DATA[to_sid] = []
            target_msg['id'] = len(SERIES_DATA[to_sid]) + 1
            SERIES_DATA[to_sid].append(target_msg)
            save_library()
            await q.answer("✅ تم نقل الرسالة بنجاح!")
            await q.edit_message_text(f"✅ تم نقل الرسالة بنجاح إلى <b>{esc(INDEX[to_sid].get('title', to_sid))}</b>.", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ العودة للرسائل", callback_data=f"ad:msgs:{from_sid}")]]))

    elif action == "delseriesconfirm":
        sid = parts[2]
        await q.edit_message_text(f"⚠️ <b>تحذير:</b> هل تريد حذف السلسلة <b>{esc(INDEX.get(sid, {}).get('title', sid))}</b> وجميع رسائلها؟", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🗑 نعم، احذف نهائياً", callback_data=f"ad:delseries:{sid}"), InlineKeyboardButton("❌ إلغاء", callback_data=f"ad:sel:{sid}")]]))

    elif action == "delseries":
        sid = parts[2]
        INDEX.pop(sid, None)
        SERIES_DATA.pop(sid, None)
        save_library()
        await q.edit_message_text("✅ تم حذف السلسلة وكافة رسائلها بنجاح.", reply_markup=admin_menu_kb(user_id=user.id))

    elif action == "delmsgconfirm":
        sid, mid = parts[2], int(parts[3])
        await q.edit_message_text(f"⚠️ هل تريد بالتأكيد حذف الرسالة رقم <b>{mid}</b>؟", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🗑 نعم، احذف", callback_data=f"ad:delmsg:{sid}:{mid}"), InlineKeyboardButton("❌ إلغاء", callback_data=f"ad:msg:{sid}:{mid}")]]))

    elif action == "delmsg":
        sid, mid = parts[2], int(parts[3])
        SERIES_DATA[sid] = [m for m in SERIES_DATA.get(sid, []) if m['id'] != mid]
        for i, m in enumerate(SERIES_DATA[sid], 1):
            m['id'] = i
        save_library()
        await q.edit_message_text("✅ تم حذف الرسالة وأعيد ترتيب الأرقام تلقائياً.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ العودة للرسائل", callback_data=f"ad:msgs:{sid}")]]))

    elif action in {"move", "move_msg"}:
        sid = parts[2]
        if action == "move":
            direction = parts[3]
            items = list(INDEX.items())
            pos = [x[0] for x in items].index(sid)
            other = pos - 1 if direction == "up" else pos + 1
            if 0 <= other < len(items):
                items[pos], items[other] = items[other], items[pos]
                INDEX.clear()
                INDEX.update(items)
                save_library()
                await q.answer("✅ تم تحديث الترتيب")
            else:
                await q.answer("وصلت للحد الأقصى")
            await admin_callback(update, ctx, f"ad:sel:{sid}")
        else:
            mid = int(parts[3])
            direction = parts[4]
            msgs = SERIES_DATA.get(sid, [])
            pos = next(i for i, m in enumerate(msgs) if m['id'] == mid)
            other = pos - 1 if direction == "up" else pos + 1
            if 0 <= other < len(msgs):
                msgs[pos], msgs[other] = msgs[other], msgs[pos]
                for i, m in enumerate(msgs, 1):
                    m['id'] = i
                save_library()
                await q.answer("✅ تم تحديث الترتيب")
                await admin_callback(update, ctx, f"ad:msg:{sid}:{msgs[other]['id']}")
            else:
                await q.answer("وصلت للحد الأقصى")
                await admin_callback(update, ctx, f"ad:msg:{sid}:{mid}")

    # ── إدارة المشرفين ──
    elif action == "admins":
        co_admins = ADMINS_DATA.get("co_admins", [])
        txt = (
            "👥 <b>إدارة المسؤولين والمشرفين</b>\n"
            "<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
            f"👑 <b>المدير الرئيسي:</b> <code>{PRIMARY_ADMIN_ID}</code>\n\n"
            f"👥 <b>المشرفون الإضافيون ({len(co_admins)}):</b>\n"
        )
        if co_admins:
            for i, co in enumerate(co_admins, 1):
                c_id = co.get('id') if isinstance(co, dict) else co
                c_name = co.get('name', 'مشرف') if isinstance(co, dict) else 'مشرف'
                txt += f"{i}. <b>{esc(c_name)}</b> — <code>{c_id}</code>\n"
        else:
            txt += "<i>لا يوجد مشرفون إضافيون حالياً.</i>\n"
        txt += "<code>━━━━━━━━━━━━━━━━━━━━</code>"

        rows = [
            [InlineKeyboardButton("➕ إضافة مشرف جديد", callback_data="ad:add_admin")],
        ]
        if co_admins:
            rows.append([InlineKeyboardButton("🗑 إزالة مشرف", callback_data="ad:del_admin_menu")])
        rows.append([InlineKeyboardButton("⬅️ العودة للوحة الإدارة", callback_data="ad:home")])

        await q.edit_message_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))

    elif action == "add_admin":
        ctx.chat_data["admin_state"] = {"kind": "add_admin"}
        await q.edit_message_text(
            "➕ <b>إضافة مشرف جديد</b>\n\n"
            "أرسل معرّف تيليجرام الرقمي (Telegram ID) الخاص بالشخص مع اسمه الاختياري بالشكل:\n"
            "<b>المعرف | الاسم</b>\n\n"
            "📌 <b>مثال:</b> <code>123456789 | أ. محمد</code>\n"
            "(يمكن للشخص معرفة رقمه عبر البوت بالأمر /whoami)",
            parse_mode="HTML"
        )

    elif action == "del_admin_menu":
        co_admins = ADMINS_DATA.get("co_admins", [])
        rows = []
        for co in co_admins:
            c_id = co.get('id') if isinstance(co, dict) else co
            c_name = co.get('name', 'مشرف') if isinstance(co, dict) else 'مشرف'
            rows.append([InlineKeyboardButton(f"🗑 إزالة: {c_name} ({c_id})", callback_data=f"ad:del_admin_do:{c_id}")])
        rows.append([InlineKeyboardButton("⬅️ رجوع", callback_data="ad:admins")])
        await q.edit_message_text("🗑 <b>اختر المشرف الذي تريد إزالته:</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))

    elif action == "del_admin_do":
        target_id = parts[2]
        ADMINS_DATA["co_admins"] = [
            co for co in ADMINS_DATA.get("co_admins", [])
            if str(co.get('id') if isinstance(co, dict) else co) != str(target_id)
        ]
        save_admins()
        await q.answer("✅ تمت إزالة المشرف بنجاح")
        await admin_callback(update, ctx, "ad:admins")

    # ── إحصائيات عامة للمكتبة ──
    elif action == "stats":
        total_series = len(INDEX)
        total_msgs = sum(len(SERIES_DATA.get(sid, [])) for sid in SERIES_DATA)
        total_chars = sum(sum(m.get("length", len(m.get("text", ""))) for m in msgs) for msgs in SERIES_DATA.values())
        admins_count = len(get_all_admin_ids())
        await q.edit_message_text(
            f"📊 <b>إحصائيات مكتبة المحيط الشاملة</b>\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
            f"📚 إجمالي السلاسل: <b>{total_series}</b> سلسلة\n"
            f"📝 إجمالي الرسائل: <b>{total_msgs}</b> رسالة\n"
            f"📊 إجمالي الأحرف: <b>{total_chars:,}</b> حرف\n"
            f"👥 عدد المشرفين والمدراء: <b>{admins_count}</b>\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━</code>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="ad:home")]])
        )

    # ── الرد على استفسار القارئ ──
    elif action == "reply_inq":
        target_uid = int(parts[2])
        sid = parts[3]
        mid = int(parts[4])
        ctx.chat_data["admin_state"] = {
            "kind": "reply_inquiry",
            "target_uid": target_uid,
            "sid": sid,
            "mid": mid
        }
        info = INDEX.get(sid, {})
        await q.message.reply_text(
            f"💬 <b>كتابة الرد على القارئ:</b>\n\n"
            f"📚 السلسلة: <b>{esc(info.get('title', sid))}</b> — رسالة <b>{mid}</b>\n"
            f"👤 معرّف القارئ: <code>{target_uid}</code>\n\n"
            f"✍️ اكتب نص الإجابة والرد الآن ليتم إرساله مباشرة إلى القارئ:",
            parse_mode="HTML"
        )
    return True

# ─── دالة البث الجماعي ───
async def broadcast_handler(ctx: ContextTypes.DEFAULT_TYPE, message_text: str):
    """Sends a message to all users found in Firestore."""
    db = get_db()
    users_coll = db.collection(os.environ.get("FIRESTORE_USER_COLLECTION", "bot_users_data"))
    
    docs = await asyncio.to_thread(users_coll.stream)
    
    success_count = 0
    fail_count = 0
    
    formatted_msg = (
        f"📣 <b>إعلان هام من مكتبة المحيط</b>\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n\n"
        f"{message_text}\n\n"
        f"<code>━━━━━━━━━━━━━━━━━━━━</code>"
    )

    for doc in docs:
        try:
            user_id = int(doc.id)
            await ctx.bot.send_message(user_id, formatted_msg, parse_mode="HTML")
            success_count += 1
            await asyncio.sleep(0.05) # Rate limiting
        except Exception as e:
            fail_count += 1
            
    return success_count, fail_count

# ─── Callback Handler العام ───
async def cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d = q.data
    ud = get_ud(ctx)

    if d.startswith("ad:"):
        await admin_callback(update, ctx, d)
        return

    if d == "noop":
        await q.answer("📍")
        return

    if d == "m:main":
        await q.edit_message_text("👇 <b>القائمة الرئيسية</b> — اختر سلسلة:", parse_mode="HTML", reply_markup=main_kb())
        return

    if d == "m:ab":
        total_all = sum(len(SERIES_DATA.get(sid, [])) for sid in SERIES_DATA)
        await q.edit_message_text(
            f"ℹ️ <b>عن مكتبة المحيط للتزكية</b>\n\n"
            f"📚 قناة المحيط لتزكية النفس\n"
            f"✍️ د. سامي المؤيد\n"
            f"📅 الفترة: 2017 – 2026\n"
            f"📝 إجمالي الرسائل: {total_all} رسالة\n"
            f"📂 عدد السلاسل: {len(INDEX)}\n"
            f"🎯 تربية روحية — فهم سليم — عقلانية إسلامية",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 رجوع", callback_data="m:main")]])
        )
        return

    if d == "m:set":
        s = ud["settings"]
        await q.edit_message_text(
            f"⚙️ <b>الإعدادات</b>\n\n"
            f"🌙 الوضع: <b>{s['theme']}</b>\n"
            f"🔤 حجم الخط: <b>{s['font']}</b>\n"
            f"🔔 الإشعارات: <b>{'مفعلة' if s['notif'] else 'معطلة'}</b>\n\n"
            f"اختر ما تريد تغييره:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🌙/☀️ تبديل الوضع", callback_data="set:theme")],
                [InlineKeyboardButton("🔔 تبديل الإشعارات", callback_data="set:notif")],
                [InlineKeyboardButton("🏠 رجوع", callback_data="m:main")]
            ])
        )
        return

    if d.startswith("set:"):
        k = d.split(":")[1]
        if k == "theme":
            ud["settings"]["theme"] = "light" if ud["settings"]["theme"] == "dark" else "dark"
        elif k == "notif":
            ud["settings"]["notif"] = not ud["settings"]["notif"]
        await cb(update, ctx)
        return

    if d == "m:bm":
        bm = ud.get("bookmarks", {})
        if not any(bm.values()):
            await q.edit_message_text(
                "🔖 <b>الإشارات المرجعية</b>\n\nلا توجد إشارات مرجعية محفوظة بعد.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 رجوع", callback_data="m:main")]])
            )
            return
        k = []
        for sid, mids in bm.items():
            for mid in mids:
                if sid in INDEX:
                    info = INDEX[sid]
                    k.append([InlineKeyboardButton(
                        f"{info.get('icon', '📚')} {info.get('title', sid)} — رسالة {mid}",
                        callback_data=f"r:{sid}:{mid}"
                    )])
        k.append([InlineKeyboardButton("🏠 رجوع", callback_data="m:main")])
        await q.edit_message_text("🔖 <b>إشاراتك المرجعية:</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(k))
        return

    if d == "m:fav":
        fav = ud.get("favorites", [])
        if not fav:
            await q.edit_message_text(
                "⭐ <b>المفضلة</b>\n\nلا توجد رسائل مفضلة بعد.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 رجوع", callback_data="m:main")]])
            )
            return
        k = []
        for sid, mid in fav:
            if sid in INDEX:
                info = INDEX[sid]
                k.append([InlineKeyboardButton(
                    f"{info.get('icon', '📚')} {info.get('title', sid)} — رسالة {mid}",
                    callback_data=f"r:{sid}:{mid}"
                )])
        k.append([InlineKeyboardButton("🏠 رجوع", callback_data="m:main")])
        await q.edit_message_text("⭐ <b>رسائلك المفضلة:</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(k))
        return

    if d == "m:st":
        st = ud["stats"]
        await q.edit_message_text(
            f"📊 <b>إحصائياتك</b>\n\n"
            f"📖 مقروءة: <b>{st['read']}</b>\n"
            f"📚 سلاسل: <b>{len(st['started'])}</b>\n"
            f"🔍 بحث: <b>{st['searches']}</b>\n"
            f"🔖 إشارات: <b>{sum(len(v) for v in ud['bookmarks'].values())}</b>\n"
            f"⭐ مفضلة: <b>{len(ud['favorites'])}</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 رجوع", callback_data="m:main")]])
        )
        return

    if d == "m:sr":
        await q.edit_message_text("🔍 <b>البحث في المكتبة</b>\nأرسل الكلمة أو العبارة التي تبحث عنها:", parse_mode="HTML")
        ctx.chat_data["mode"] = "search_global"
        return

    if d.startswith("s:"):
        sid = d.split(":", 1)[1]
        if sid not in INDEX:
            await q.answer("السلسلة غير موجودة")
            return
        info = INDEX[sid]
        msgs = SERIES_DATA.get(sid, [])
        total = len(msgs)
        last = ud.get("progress", {}).get(sid, 0)
        keywords_str = ', '.join(info.get('keywords', []))
        txt = (
            f"{info.get('icon', '📚')} <b>{esc(info.get('title', sid))}</b>\n\n"
            f"{esc(info.get('description', ''))}\n\n"
            f"📨 إجمالي الرسائل: <b>{total}</b>\n"
            f"📅 الفترة: {esc(info.get('period', ''))}\n"
        )
        if last:
            txt += f"\n📌 آخر قراءة: رسالة <b>{last}</b>"
        k = []
        if total > 0:
            k.append([InlineKeyboardButton("▶️ ابدأ من الأولى", callback_data=f"r:{sid}:1")])
            if last:
                k[0].append(InlineKeyboardButton("📌 أكمل من آخر نقطة", callback_data=f"r:{sid}:{last}"))
            k.append([InlineKeyboardButton("🔍 بحث في السلسلة", callback_data=f"a:sr:{sid}")])
        k.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")])
        await q.edit_message_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(k))
        return

    if d.startswith("r:"): 
        parts_list = d.split(":")
        sid = parts_list[1]
        mid = int(parts_list[2])
        part_idx = int(parts_list[3]) if len(parts_list) > 3 else 1
        
        msgs = SERIES_DATA.get(sid, [])
        info = INDEX.get(sid, {})
        total = len(msgs)
        if total == 0:
            await q.answer("لا توجد رسائل في هذه السلسلة بعد")
            return
        if mid < 1: mid = 1
        if mid > total: mid = total
        
        ud["progress"][sid] = mid
        ud["stats"]["read"] += 1
        ud["stats"]["started"].add(sid)
        msg = msgs[mid - 1]
        
        text_parts = split_text_into_parts(msg.get('text', ''))
        total_parts = len(text_parts)
        if part_idx < 1: part_idx = 1
        if part_idx > total_parts: part_idx = total_parts
            
        text_part = text_parts[part_idx - 1] if text_parts else ""
        is_bm = mid in ud.get("bookmarks", {}).get(sid, [])
        is_fav = (sid, mid) in ud.get("favorites", [])
        
        caption = fmt_msg(msg, info.get("title", sid), total, part_idx, total_parts, text_part)
        kb = read_kb(sid, mid, total, is_bm, is_fav, part_idx, total_parts)

        # Handle Multimedia
        if msg.get('audio_id'):
            await q.delete_message()
            await ctx.bot.send_audio(
                chat_id=q.message.chat_id,
                audio=msg['audio_id'],
                caption=caption,
                parse_mode="HTML",
                reply_markup=kb
            )
        elif msg.get('photo_id'):
            await q.delete_message()
            await ctx.bot.send_photo(
                chat_id=q.message.chat_id,
                photo=msg['photo_id'],
                caption=caption,
                parse_mode="HTML",
                reply_markup=kb
            )
        else:
            try:
                await q.edit_message_text(caption, parse_mode="HTML", reply_markup=kb)
            except Exception: # Fallback if text only edit fails (e.g. previous was media)
                await q.delete_message()
                await ctx.bot.send_message(
                    chat_id=q.message.chat_id,
                    text=caption,
                    parse_mode="HTML",
                    reply_markup=kb
                )
        return

    if d.startswith("a:bm:"):
        parts_list = d.split(":")
        sid = parts_list[2]
        mid = int(parts_list[3])
        part_idx = int(parts_list[4]) if len(parts_list) > 4 else 1
        
        if sid not in ud["bookmarks"]:
            ud["bookmarks"][sid] = []
        if mid in ud["bookmarks"][sid]:
            ud["bookmarks"][sid].remove(mid)
            await q.answer("🔖 تمت إزالة الإشارة المرجعية")
        else:
            ud["bookmarks"][sid].append(mid)
            await q.answer("🔖 تمت إضافة الإشارة المرجعية")
            
        # Re-trigger view update (limited to caption if media)
        await cb(update, ctx) # Just call again to re-render
        return

    if d.startswith("a:fav:"):
        parts_list = d.split(":")
        sid = parts_list[2]
        mid = int(parts_list[3])
        part_idx = int(parts_list[4]) if len(parts_list) > 4 else 1
        
        key = (sid, mid)
        if key in ud["favorites"]:
            ud["favorites"].remove(key)
            await q.answer("⭐ تمت إزالة المفضلة")
        else:
            ud["favorites"].append(key)
            await q.answer("⭐ تمت إضافة المفضلة")
            
        await cb(update, ctx)
        return

    if d.startswith("a:jp:"):
        sid = d.split(":", 2)[2]
        total = len(SERIES_DATA.get(sid, []))
        # Use send_message instead of edit to ensure it works even if coming from media caption
        await ctx.bot.send_message(
            chat_id=q.message.chat_id,
            text=f"🔢 <b>الانتقال السريع</b>\nأرسل رقم الرسالة (1 – {total}):",
            parse_mode="HTML"
        )
        ctx.chat_data["mode"] = "jump"
        ctx.chat_data["jsid"] = sid
        ctx.chat_data["jtotal"] = total
        return

    if d.startswith("a:sr:"):
        sid = d.split(":", 2)[2]
        await ctx.bot.send_message(
            chat_id=q.message.chat_id,
            text="🔍 <b>البحث في السلسلة</b>\nأرسل الكلمة أو العبارة:",
            parse_mode="HTML"
        )
        ctx.chat_data["mode"] = "search_series"
        ctx.chat_data["ssid"] = sid
        return

    if d.startswith("a:in:"):
        _, _, sid, mid = d.split(":")
        mid = int(mid)
        info = INDEX.get(sid, {})
        await ctx.bot.send_message(
            chat_id=q.message.chat_id,
            text=f"❓ <b>إرسال استفسار للمؤلف والمشرفين</b>\n\n"
                 f"حول: <b>{esc(info.get('title', sid))}</b> — رسالة <b>{mid}</b>\n\n"
                 f"اكتب استفسارك وسؤلك الآن وسيتم تسليمه للمشرفين للإجابة عليك:",
            parse_mode="HTML"
        )
        ctx.chat_data["mode"] = "inquiry"
        ctx.chat_data["isid"] = sid
        ctx.chat_data["imid"] = mid
        return

# ─── معالج الرسائل النصية ───
async def text_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not update.message: return
    
    # Handle Photo/Audio attachments from admins
    admin_state = ctx.chat_data.get("admin_state")
    if admin_state and is_admin(user.id) and admin_state.get("kind") == "edit_media":
        sid = admin_state["sid"]
        mid = admin_state["mid"]
        msg = next((m for m in SERIES_DATA[sid] if m['id'] == mid), None)
        
        if not msg:
             await update.message.reply_text("⚠️ لم يتم العثور على الرسالة المستهدفة.")
             ctx.chat_data.pop("admin_state", None)
             return

        file_id = None
        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            msg['photo_id'] = file_id
            await update.message.reply_text(f"✅ تم ربط الصورة بالرسالة {mid} بنجاح.")
        elif update.message.audio:
            file_id = update.message.audio.file_id
            msg['audio_id'] = file_id
            await update.message.reply_text(f"✅ تم ربط الملف الصوتي بالرسالة {mid} بنجاح.")
        elif update.message.voice:
            file_id = update.message.voice.file_id
            msg['audio_id'] = file_id
            await update.message.reply_text(f"✅ تم ربط التسجيل الصوتي بالرسالة {mid} بنجاح.")
        else:
             await update.message.reply_text("⚠️ يرجى إرسال صورة أو ملف صوتي فقط.")
             return

        save_library()
        ctx.chat_data.pop("admin_state", None)
        return

    if not update.message.text: return
    txt = update.message.text.strip()
    ud = get_ud(ctx)

    # 1. إذا كان المشرف يقوم بعملية تحكم أو رد على استفسار
    if admin_state and user and is_admin(user.id):
        kind = admin_state.get("kind")
        try:
            # ── تنفيذ البث الجماعي ──
            if kind == "broadcast":
                await update.message.reply_text("⏳ جاري البدء في عملية البث لجميع المستخدمين...")
                success, fail = await broadcast_handler(ctx, txt)
                await update.message.reply_text(
                    f"✅ <b>اكتملت عملية البث</b>\n\n"
                    f"✔️ تم الإرسال بنجاح لـ <b>{success}</b> مستخدم.\n"
                    f"❌ فشل الإرسال لـ <b>{fail}</b> مستخدم (ربما قاموا بحظر البوت).",
                    parse_mode="HTML",
                    reply_markup=admin_menu_kb(user.id)
                )

            # ── الرد على استفسار القارئ ──
            elif kind == "reply_inquiry":
                target_uid = admin_state["target_uid"]
                sid = admin_state["sid"]
                mid = admin_state["mid"]
                info = INDEX.get(sid, {})
                
                reader_msg = (
                    f"📩 <b>وصلك رد من مشرف/مؤلف المكتبة على استفسارك</b>\n"
                    f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
                    f"📚 السلسلة: <b>{esc(info.get('title', sid))}</b> — رسالة <b>{mid}</b>\n\n"
                    f"💬 <b>نص الرد:</b>\n"
                    f"{esc(txt)}\n"
                    f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
                    f"<i>جزاكم الله خيراً لمتابعتكم لمكتبة المحيط للتزكية.</i>"
                )
                reader_kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📖 قراءة الرسالة في المكتبة", callback_data=f"r:{sid}:{mid}")],
                    [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")]
                ])
                try:
                    await ctx.bot.send_message(target_uid, reader_msg, parse_mode="HTML", reply_markup=reader_kb)
                    await update.message.reply_text("✅ <b>تم إرسال ردك بنجاح إلى القارئ!</b>", parse_mode="HTML")
                except Exception as e:
                    logger.error(f"Failed to send reply to user {target_uid}: {e}")
                    await update.message.reply_text(f"⚠️ تعذر تسليم الرد للقارئ (ربما قام بحظر البوت أو لم يبدأه): {e}")

            # ── التحديد بنطاق أرقام ──
            elif kind == "bulk_select_range":
                sid = admin_state["sid"]
                page = admin_state.get("page", 1)
                total = admin_state.get("total", len(SERIES_DATA.get(sid, [])))
                range_set = parse_range_string(txt, total)
                selected = get_bulk_selection(ctx, sid)
                selected.update(range_set)
                ctx.chat_data.pop("admin_state", None)
                await update.message.reply_text(
                    f"✅ تم إضافة <b>{len(range_set)}</b> رسالة إلى قائمة التحديد (المجموع المحدد الآن: {len(selected)} رسالة).",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ العودة للتحديد والعمليات", callback_data=f"ad:bulk:{sid}:{page}")]])
                )
                return

            # ── نقل الرسالة لموضع محدد ──
            elif kind == "setpos_msg":
                if not txt.isdigit():
                    raise ValueError("يجب إرسال رقم صحيح")
                target_pos = int(txt)
                sid = admin_state["sid"]
                mid = admin_state["mid"]
                msgs = SERIES_DATA.get(sid, [])
                total = len(msgs)
                if target_pos < 1 or target_pos > total:
                    await update.message.reply_text(f"⚠️ الترتيب يجب أن يكون بين 1 و {total}")
                    return
                current_idx = next(i for i, m in enumerate(msgs) if m['id'] == mid)
                msg_item = msgs.pop(current_idx)
                msgs.insert(target_pos - 1, msg_item)
                for i, m in enumerate(msgs, 1):
                    m['id'] = i
                save_library()
                reply = f"✅ تم نقل الرسالة بنجاح لتصبح في الترتيب رقم <b>{target_pos}</b>."
                await update.message.reply_text(reply, parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ العودة للرسائل", callback_data=f"ad:msgs:{sid}")]]))

            # ── نقل السلسلة لموضع محدد ──
            elif kind == "setpos_series":
                if not txt.isdigit():
                    raise ValueError("يجب إرسال رقم صحيح")
                target_pos = int(txt)
                sid = admin_state["sid"]
                items = list(INDEX.items())
                total = len(items)
                if target_pos < 1 or target_pos > total:
                    await update.message.reply_text(f"⚠️ الترتيب يجب أن يكون بين 1 و {total}")
                    return
                current_idx = [x[0] for x in items].index(sid)
                item = items.pop(current_idx)
                items.insert(target_pos - 1, item)
                INDEX.clear()
                INDEX.update(items)
                save_library()
                reply = f"✅ تم نقل السلسلة لتصبح في الترتيب رقم <b>{target_pos}</b>."
                await update.message.reply_text(reply, parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ قائمة السلاسل", callback_data="ad:series")]]))

            # ── إضافة مشرف جديد ──
            elif kind == "add_admin":
                parts = [p.strip() for p in txt.split("|", 1)]
                admin_id_str = parts[0]
                admin_name = parts[1] if len(parts) > 1 else "مشرف"
                if not admin_id_str.isdigit():
                    raise ValueError("معرف المشرف يجب أن يتكون من أرقام فقط")
                new_admin_id = int(admin_id_str)
                if "co_admins" not in ADMINS_DATA:
                    ADMINS_DATA["co_admins"] = []
                exists = any(
                    (co.get("id") == new_admin_id if isinstance(co, dict) else co == new_admin_id)
                    for co in ADMINS_DATA["co_admins"]
                )
                if exists or new_admin_id == PRIMARY_ADMIN_ID:
                    await update.message.reply_text("⚠️ هذا الحساب مسجل بالفعل كمسؤول.")
                    return
                ADMINS_DATA["co_admins"].append({"id": new_admin_id, "name": admin_name, "added_at": datetime.now().strftime("%Y-%m-%d")})
                save_admins()
                reply = f"✅ تم تعيين <b>{esc(admin_name)}</b> (<code>{new_admin_id}</code>) كمشرف في المكتبة بنجاح!"
                await update.message.reply_text(reply, parse_mode="HTML", reply_markup=admin_menu_kb(user.id))

            # ── تعديل السلسلة ──
            elif kind == "edit_series":
                INDEX[admin_state["sid"]][admin_state["field"]] = txt
                save_library()
                reply = "✅ تم تحديث بيانات السلسلة بنجاح."
                await update.message.reply_text(reply, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ العودة للسلسلة", callback_data=f"ad:sel:{admin_state['sid']}")]]))

            # ── تعديل الرسالة ──
            elif kind == "edit_msg":
                msg = next(m for m in SERIES_DATA[admin_state["sid"]] if m["id"] == admin_state["mid"])
                msg[admin_state["field"]] = txt
                if admin_state["field"] == "text":
                    msg["length"] = len(txt)
                save_library()
                reply = "✅ تم تحديث الرسالة بنجاح."
                await update.message.reply_text(reply, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ العودة للرسالة", callback_data=f"ad:msg:{admin_state['sid']}:{admin_state['mid']}")]]))

            # ── إنشاء سلسلة جديدة ──
            elif kind == "new_series":
                parts = [p.strip() for p in txt.split("|")]
                if len(parts) < 3:
                    raise ValueError("البيانات غير مكتملة")
                sid = parts[0]
                icon = parts[1] if len(parts) > 1 else "📚"
                title = parts[2] if len(parts) > 2 else sid
                topic = parts[3] if len(parts) > 3 else title
                desc = parts[4] if len(parts) > 4 else topic
                if sid in INDEX:
                    raise ValueError("المعرّف مستخدم بالفعل، اختر معرّفاً آخر")
                INDEX[sid] = {
                    "icon": icon,
                    "title": f"{icon} {title}" if not title.startswith(icon) else title,
                    "description": desc,
                    "topic": topic,
                    "period": datetime.now().strftime("%Y"),
                    "keywords": []
                }
                SERIES_DATA[sid] = []
                save_library()
                reply = f"✅ تم إنشاء السلسلة الجديدة <b>{esc(title)}</b> بنجاح!"
                await update.message.reply_text(reply, parse_mode="HTML", reply_markup=admin_menu_kb(user.id))

            # ── إنشاء رسالة جديدة ──
            elif kind == "new_msg":
                sid = admin_state["sid"]
                if "|" in txt:
                    parts = [p.strip() for p in txt.split("|", 2)]
                    title = parts[0]
                    topic = parts[1] if len(parts) > 1 else title
                    content = parts[2] if len(parts) > 2 else ""
                else:
                    content = txt
                    title = content[:30] + "..." if len(content) > 30 else content
                    topic = INDEX.get(sid, {}).get("topic", "")

                if sid not in SERIES_DATA:
                    SERIES_DATA[sid] = []
                new_id = len(SERIES_DATA[sid]) + 1
                SERIES_DATA[sid].append({
                    "id": new_id,
                    "title": title,
                    "topic": topic,
                    "text": content,
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "length": len(content)
                })
                save_library()
                reply = f"✅ أُضيفت الرسالة رقم <b>{new_id}</b> بنجاح إلى السلسلة!"
                await update.message.reply_text(reply, parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ قائمة الرسائل", callback_data=f"ad:msgs:{sid}")]]))

            else:
                raise ValueError("حالة إدارة غير معروفة")

            ctx.chat_data.pop("admin_state", None)
            return
        except ValueError as ve:
            await update.message.reply_text(f"⚠️ خطأ في الصيغة: {ve}\nيرجى إعادة المحاولة بالشكل المطلوب.")
            return

    # 2. معالجة أوضاع المستخدم العادي
    mode = ctx.chat_data.get("mode", "")

    # ── إرسال استفسار للمشرفين ──
    if mode == "inquiry":
        sid = ctx.chat_data.pop("isid", "")
        mid = ctx.chat_data.pop("imid", 1)
        ctx.chat_data.pop("mode", "")
        info = INDEX.get(sid, {})
        u = update.effective_user
        
        inquiry_text = (
            f"📩 <b>استفسار جديد من قارئ</b>\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━</code>\n"
            f"👤 <b>القارئ:</b> {esc(u.full_name)} (@{u.username or 'لا يوجد'})\n"
            f"🆔 <b>معرّف القارئ:</b> <code>{u.id}</code>\n"
            f"📚 <b>السلسلة:</b> {esc(info.get('title', sid))} — <b>رسالة {mid}</b>\n"
            f"📅 <b>التاريخ:</b> {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
            f"❓ <b>نص الاستفسار:</b>\n"
            f"{esc(txt)}\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━</code>"
        )
        admin_reply_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💬 الرد على هذا القارئ", callback_data=f"ad:reply_inq:{u.id}:{sid}:{mid}")]
        ])

        admin_ids = get_all_admin_ids()
        sent_count = 0
        for aid in admin_ids:
            try:
                await ctx.bot.send_message(aid, inquiry_text, parse_mode="HTML", reply_markup=admin_reply_kb)
                sent_count += 1
            except Exception as e:
                logger.error(f"Failed to notify admin {aid}: {e}")

        if sent_count > 0:
            await update.message.reply_text(
                "✅ <b>تم تسليم استفسارك بنجاح لمسؤولي المكتبة</b>\nسيصلك إشعار بالرد هنا فور الإجابة عليه إن شاء الله.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📖 العودة للرسالة", callback_data=f"r:{sid}:{mid}")],
                    [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")]
                ])
            )
        else:
            await update.message.reply_text("⚠️ تعذر إرسال الاستفسار، يرجى المحاولة لاحقاً.")
        return

    # ── الانتقال السريع ──
    if mode == "jump":
        sid = ctx.chat_data.pop("jsid", "")
        total = ctx.chat_data.pop("jtotal", 1)
        ctx.chat_data.pop("mode", "")
        if txt.isdigit():
            mid = int(txt)
            if 1 <= mid <= total:
                await update.message.reply_text(
                    "⏩ جاري الانتقال...",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📖 افتح الرسالة", callback_data=f"r:{sid}:{mid}")]])
                )
            else:
                await update.message.reply_text(f"⚠️ الرقم يجب أن يكون بين 1 و {total}")
        else:
            await update.message.reply_text("⚠️ أرسل رقماً صحيحاً.")
        return

    # ── البحث في سلسلة محددة ──
    if mode == "search_series":
        sid = ctx.chat_data.pop("ssid", "")
        ctx.chat_data.pop("mode", "")
        await do_search(update, ctx, sid, txt)
        return

    # ── البحث العام ──
    if mode == "search_global":
        ctx.chat_data.pop("mode", "")
        await do_search(update, ctx, None, txt)
        return

    # الوضع التلقائي: أي نص يرسله القارئ يعتبر بحثاً عاماً
    await do_search(update, ctx, None, txt)

# ─── دالة البحث ───
async def do_search(update, ctx, sid, query):
    ud = get_ud(ctx)
    ud["stats"]["searches"] += 1
    q = query.lower()
    results = []

    search_space = {sid: SERIES_DATA[sid]} if sid else SERIES_DATA

    for s, messages in search_space.items():
        for msg in messages:
            if q in msg.get("text", "").lower() or q in msg.get("title", "").lower():
                results.append((s, msg))
    
    title = f"سلسلة {INDEX.get(sid, {}).get('title', sid)}" if sid else "المكتبة بالكامل"

    if not results:
        await update.message.reply_text(
            f'🔍 لم يتم العثور على نتائج لـ «{esc(query)}» في {esc(title)}.',
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")]])
        )
        return

    txt = f"🔍 تم العثور على <b>{len(results)}</b> نتيجة في <b>{esc(title)}</b> لـ «{esc(query)}»:\n"
    k = []
    for s_res, msg_res in results[:20]:
        icon = INDEX.get(s_res, {}).get('icon', '📄')
        preview = esc(msg_res.get("text", "")[:35].replace("\n", " ") + "...")
        k.append([InlineKeyboardButton(
            f"{icon} رسالة {msg_res['id']}: {preview}",
            callback_data=f"r:{s_res}:{msg_res['id']}"
        )])
    k.append([InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="m:main")])
    await update.message.reply_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(k))

def split_text_into_parts(text, max_len=3500):
    """Splits the text into parts without breaking words or lines if possible."""
    if not isinstance(text, str) or len(text) <= max_len:
        return [text or ""]
    
    parts = []
    while text:
        if len(text) <= max_len:
            parts.append(text)
            break
        
        split_idx = text.rfind('\n', 0, max_len)
        if split_idx == -1 or split_idx < max_len * 0.7:
            split_idx = text.rfind(' ', 0, max_len)
        if split_idx == -1 or split_idx < max_len * 0.5:
            split_idx = max_len
            
        parts.append(text[:split_idx].strip())
        text = text[split_idx:].strip()
        
    return parts if parts else [""]

# ─── تسجيل الأوامر السريعة في تيليجرام ───
async def setup_bot_commands(application: Application):
    """تسجيل الأوامر التلقائية في زر القائمة (Menu) داخل تيليجرام"""
    commands = [
        BotCommand("start", "🏠 القائمة الرئيسية للمكتبة"),
        BotCommand("continue", "📌 متابعة القراءة من آخر نقطة"),
        BotCommand("stats", "📊 إحصائيات قراءتي"),
        BotCommand("search", "🔍 البحث في نصوص المكتبة"),
        BotCommand("bookmarks", "🔖 الإشارات المرجعية المحفوظة"),
        BotCommand("favorites", "⭐ الرسائل المفضلة"),
        BotCommand("whoami", "🆔 معرف حسابك في تيليجرام"),
        BotCommand("admin", "🔐 لوحة إدارة المكتبة (للمشرفين)"),
        BotCommand("help", "📖 دليل استخدام البوت"),
    ]
    try:
        await application.bot.set_my_commands(commands, scope=BotCommandScopeDefault())
        logger.info("Bot commands menu registered successfully.")
    except Exception as e:
        logger.warning(f"Could not register bot commands menu: {e}")

# ─── معالج الأخطاء العامة ───
async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """يلتقط أي خطأ غير متوقع ويسجله دون إيقاف البوت"""
    logger.error("خطأ غير متوقع أثناء معالجة التحديث:", exc_info=ctx.error)
    if update and isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ حدث خطأ غير متوقع. يرجى المحاولة مرة أخرى أو كتابة /start للعودة للقائمة الرئيسية."
            )
        except Exception:
            pass

class FirestorePersistence(BasePersistence):
    """Custom persistence class for python-telegram-bot using Cloud Firestore with local memory fallback."""
    def __init__(self, store_user_data=True, store_chat_data=True, store_bot_data=False):
        super().__init__()
        self.store_user_data = store_user_data
        self.store_chat_data = store_chat_data
        self.store_bot_data = store_bot_data
        self.db = get_db()
        self._memory_chat_data = {}

        if self.db:
            try:
                self.coll = self.db.collection(os.environ.get("FIRESTORE_USER_COLLECTION", "bot_users_data"))
            except Exception as e:
                logger.error(f"Failed to access Firestore collection: {e}")
                self.coll = None
        else:
            self.coll = None

    async def get_chat_data(self) -> dict:
        if not self.coll:
            return self._memory_chat_data
        chat_data = {}
        try:
            docs = await asyncio.to_thread(self.coll.stream)
            for doc in docs:
                chat_id = doc.id
                data = self._from_firestore_dict(doc.to_dict())
                chat_data[int(chat_id)] = data
            self._memory_chat_data = chat_data
            logging.info(f"Successfully loaded {len(chat_data)} users from Firestore.")
        except Exception as e:
            logging.error(f"Error loading data from Firestore: {e}", exc_info=True)
            return self._memory_chat_data
        return chat_data

    async def update_chat_data(self, chat_id: int, data: dict) -> None:
        self._memory_chat_data[chat_id] = data
        if not self.coll:
            return
        try:
            clean_data = self._to_firestore_dict(data)
            await asyncio.to_thread(self.coll.document(str(chat_id)).set, clean_data)
        except Exception as e:
            logger.error(f"Error saving data to Firestore for chat {chat_id}: {e}", exc_info=True)

    def _to_firestore_dict(self, d: object) -> object:
        if isinstance(d, dict):
            return {str(k): self._to_firestore_dict(v) for k, v in d.items()}
        if isinstance(d, (list, tuple)):
            return [self._to_firestore_dict(i) for i in d]
        if isinstance(d, set):
            return [self._to_firestore_dict(i) for i in d]
        return d

    def _from_firestore_dict(self, d: object) -> object:
        if isinstance(d, dict):
            # Special handling for user stats 'started' set
            if 'ud' in d and 'stats' in d['ud'] and 'started' in d['ud']['stats'] and isinstance(d['ud']['stats']['started'], list):
                 d['ud']['stats']['started'] = set(d['ud']['stats']['started'])
            return {self._convert_key(k): self._from_firestore_dict(v) for k, v in d.items()}
        if isinstance(d, list):
            return [self._from_firestore_dict(i) for i in d]
        return d
    
    def _convert_key(self, key: str):
        try:
            return int(key)
        except (ValueError, TypeError):
            return key

    async def get_user_data(self) -> dict: return {}
    async def update_user_data(self, user_id: int, data: dict) -> None: pass
    async def get_bot_data(self) -> dict: return {}
    async def update_bot_data(self, data: dict) -> None: pass
    async def flush(self) -> None: pass

    async def drop_chat_data(self, chat_id: int) -> None: pass
    async def drop_user_data(self, user_id: int) -> None: pass
    async def get_callback_data(self) -> tuple: return ()
    async def get_conversations(self, name: str) -> dict: return {}
    async def refresh_bot_data(self, bot_data: dict) -> None: pass
    async def refresh_chat_data(self, chat_id: int, chat_data: dict) -> None: pass
    async def refresh_user_data(self, user_id: int, user_data: dict) -> None: pass
    async def update_callback_data(self, callback_data: tuple) -> None: pass
    async def update_conversation(self, name: str, key: tuple, new_state: object) -> None: pass


def create_app():
    global INDEX, ADMINS_DATA
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_TOKEN_HERE":
        logger.error("BOT_TOKEN is not set!")
        raise ValueError("BOT_TOKEN is not set!")

    if not INDEX:
        load_library_from_firestore()

    if not ADMINS_DATA.get("co_admins"):
        loaded = load_json("admins.json", default=None)
        if loaded:
            ADMINS_DATA = loaded

    persistence = FirestorePersistence()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .persistence(persistence)
        .post_init(setup_bot_commands)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("continue", cont_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("search", search_cmd))
    app.add_handler(CommandHandler("bookmarks", bookmarks_cmd))
    app.add_handler(CommandHandler("favorites", favorites_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("whoami", whoami_cmd))

    app.add_handler(CallbackQueryHandler(cb))
    app.add_handler(MessageHandler(filters.TEXT | filters.PHOTO | filters.AUDIO | filters.VOICE, text_handler))
    app.add_error_handler(error_handler)

    return app

class HealthCheckHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK - Telegram Bot is running!")

    def log_message(self, format, *args):
        pass  # منع السجلات المتكررة للفحص الدوري

def start_health_check_server():
    """تشغيل خادم HTTP خفيف في الخلفية للمنصات السحابية كـ Render لتبقيه نشطاً"""
    port_str = os.environ.get("PORT")
    if not port_str:
        return
    try:
        port = int(port_str)
        server = http.server.HTTPServer(("0.0.0.0", port), HealthCheckHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        logger.info(f"Health check HTTP server started on 0.0.0.0:{port}")
    except Exception as e:
        logger.warning(f"Could not start health check HTTP server on port {port_str}: {e}")

if __name__ == "__main__":
    start_health_check_server()
    app = create_app()
    logger.info("Bot is starting polling mode...")
    app.run_polling(drop_pending_updates=True)