#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mexanick Market

"""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_healthcheck_server():
    server = HTTPServer(("0.0.0.0", 8000), HealthHandler)
    server.serve_forever()

threading.Thread(target=run_healthcheck_server, daemon=True).start()
import asyncio
from datetime import datetime
import pytz
import logging
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
import aiosqlite
import sqlite3

# ---------------- CONFIG ----------------
BOT_TOKEN = "8379265766:AAEz5DHkaF3o-edaSR2jJftRpBPADUmo6ds"           
CRYPTO_TOKEN = "369438:AAEKsbWPZPQ0V3YNV4O0GHcWTvSbzkEar43"       
ADMIN_ID = 1041720539                       
ADMIN_USERNAME = "@mexanickq"
DB_FILE = "mexanick_market.db"
MARKET_NAME = "💎Mexanick Market💎"
CRYPTO_PAY_URL = 'https://pay.crypt.bot/api'
CRYPTO_ASSETS = ['USDT','BTC','ETH','TON','TRX']
ASSET_MAP = {'USDT':'tether','BTC':'bitcoin','ETH':'ethereum','TON':'the-open-network','TRX':'tron'}
# ----------------------------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ---------------- States ----------------
class DepositState(StatesGroup):
    asset = State()
    amount = State()

class SellerCreate(StatesGroup):
    info = State()

class SellerEditInfo(StatesGroup):
    info = State()

class AddProduct(StatesGroup):
    photo = State()
    title = State()
    desc = State()
    price = State()
    quantity = State()
    content = State()

class EditProduct(StatesGroup):
    field = State()
    value = State()

class AdminNewCategory(StatesGroup):
    name = State()

class AdminEditCategory(StatesGroup):
    cat_id = State()
    name = State()

class AdminNewSub(StatesGroup):
    name = State()

class AdminEditSub(StatesGroup):
    sub_id = State()
    name = State()

class AdminSearchUser(StatesGroup):
    user_id = State()

class AdminBalanceChange(StatesGroup):
    amount = State()

class AdminProdSearch(StatesGroup):
    prod_id = State()

class AdminEditProduct(StatesGroup):
    name = State()
    desc = State()

class ReviewState(StatesGroup):
    rating = State()
    text = State()

class DisputeState(StatesGroup):
    description = State()

class AdminCloseDispute(StatesGroup):
    reason = State()

class SettingsState(StatesGroup):
    action = State()

# ---------------- Database helpers ----------------
async def init_db():
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            balance REAL DEFAULT 0.0,
            notify_enabled INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS invoices(
            invoice_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amount REAL,
            asset TEXT,
            status TEXT DEFAULT 'unpaid',
            hash TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS categories(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
        CREATE TABLE IF NOT EXISTS subcategories(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER,
            name TEXT
        );
        CREATE TABLE IF NOT EXISTS sellers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            info TEXT
        );
        CREATE TABLE IF NOT EXISTS products(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seller_id INTEGER,
            title TEXT,
            description TEXT,
            photo_file_id TEXT,
            category_id INTEGER,
            subcategory_id INTEGER,
            price REAL,
            quantity INTEGER DEFAULT 1,
            content_text TEXT,
            content_file_id TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS reviews(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            user_id INTEGER,
            username TEXT,
            rating INTEGER,
            text TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS disputes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER,
            user_id INTEGER,
            description TEXT,
            status TEXT DEFAULT 'open',
            created_at TEXT,
            close_reason TEXT
        );
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS orders(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            product_id INTEGER,
            seller_id INTEGER,
            price REAL,
            created_at TEXT
        );
        INSERT OR IGNORE INTO settings(key,value) VALUES ('maintenance','off');
        """)
        await conn.commit()

# ---------------- Utility ----------------
def now_iso():
    msk_tz = pytz.timezone('Europe/Moscow')
    return datetime.now(msk_tz).isoformat()

async def is_maintenance():
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT value FROM settings WHERE key='maintenance'") as cur:
            r = await cur.fetchone()
            return r and r['value'] == 'on'

async def maintenance_block(message: types.Message | CallbackQuery):
    if await is_maintenance():
        text = "🛠️ Сейчас идут технические работы. Попробуйте позже."
        if isinstance(message, CallbackQuery):
            await message.answer(text)
            await bot.send_message(message.message.chat.id, text)
        else:
            await message.answer(text)
        return True
    return False

async def ensure_user_record(user: types.User):
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute("INSERT OR IGNORE INTO users(user_id,username,balance,notify_enabled) VALUES (?,?,?,?)", 
                         (user.id, user.username, 0.0, 1))
        await conn.execute("UPDATE users SET username = ? WHERE user_id = ?", (user.username, user.id))
        await conn.commit()

async def is_notify_enabled(user_id: int) -> bool:
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT notify_enabled FROM users WHERE user_id=?", (user_id,)) as cur:
            r = await cur.fetchone()
            return r['notify_enabled'] == 1 if r else True

def format_money(amount):
    return f"{amount:.2f} RUB"

# ---------------- MARKUP HELPER ----------------
def simple_markup(buttons_list):
    """Создаёт InlineKeyboardMarkup для aiogram 3.20.0"""
    inline_keyboard = []
    for row in buttons_list:
        if isinstance(row, list):
            inline_keyboard.append(row)
        else:
            inline_keyboard.append([row])
    return InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

# ---------------- Inline markups ----------------
def main_menu_markup(user_id: int):
    buttons = [
        [
            InlineKeyboardButton(text="💰 Баланс", callback_data="menu_balance"),
            InlineKeyboardButton(text="💸 Пополнить", callback_data="menu_deposit")
        ],
        [
            InlineKeyboardButton(text="🛍 Товары", callback_data="menu_products"),
            InlineKeyboardButton(text="➕ Продать", callback_data="menu_sell")
        ],
        [
            InlineKeyboardButton(text="📋 Мои покупки", callback_data="menu_my_orders"),
            InlineKeyboardButton(text="📞 Поддержка", callback_data="menu_support")
        ],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu_settings")]
    ]
    if user_id == ADMIN_ID:
        buttons.append([InlineKeyboardButton(text="🔧 Админ панель", callback_data="menu_admin")])
    return simple_markup(buttons)

def cancel_markup(text="Отмена"):
    return simple_markup([InlineKeyboardButton(text="❌ " + text, callback_data="action_cancel")])

async def build_categories_markup(admin_view=False):
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id,name FROM categories ORDER BY name") as cur:
            cats = await cur.fetchall()
    
    buttons = []
    for c in cats:
        buttons.append([InlineKeyboardButton(text=f"📁 {c['name']}", callback_data=f"cat|{c['id']}")])
    
    if admin_view:
        buttons.append([InlineKeyboardButton(text="➕ Создать категорию", callback_data="admin_create_category")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_back_main")])
    
    return simple_markup(buttons)

async def build_admin_categories_markup():
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id,name FROM categories ORDER BY name") as cur:
            cats = await cur.fetchall()
    
    buttons = []
    for c in cats:
        buttons.append([
            InlineKeyboardButton(text=f"📁 {c['name']}", callback_data=f"admin_view_cat|{c['id']}"),
            InlineKeyboardButton(text="✏️", callback_data=f"admin_edit_cat|{c['id']}"),
            InlineKeyboardButton(text="🗑", callback_data=f"admin_delete_cat|{c['id']}")
        ])
    buttons.append([InlineKeyboardButton(text="➕ Создать категорию", callback_data="admin_create_category")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_admin")])
    
    return simple_markup(buttons)

async def build_admin_subcategories_markup(cat_id):
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id,name FROM subcategories WHERE category_id=? ORDER BY name", (cat_id,)) as cur:
            subs = await cur.fetchall()
    
    buttons = []
    for s in subs:
        buttons.append([
            InlineKeyboardButton(text=f"📂 {s['name']}", callback_data=f"list_products|sub|{s['id']}|1"),
            InlineKeyboardButton(text="✏️", callback_data=f"admin_edit_sub|{s['id']}"),
            InlineKeyboardButton(text="🗑", callback_data=f"admin_delete_sub|{s['id']}")
        ])
    buttons.append([InlineKeyboardButton(text="➕ Создать подкатегорию", callback_data=f"admin_create_sub|{cat_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_cats")])
    
    return simple_markup(buttons)

# ---------------- Crypto ----------------
def crypto_headers():
    return {'Crypto-Pay-API-Token': CRYPTO_TOKEN, 'Content-Type': 'application/json'}

def get_rate(asset):
    coin_id = ASSET_MAP.get(asset)
    if not coin_id:
        raise ValueError("Unknown asset")
    try:
        r = requests.get(f'https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=rub', timeout=8)
        r.raise_for_status()
        data = r.json()
        return float(data[coin_id]['rub'])
    except Exception:
        return 100.0

def create_invoice(asset, amount, description, user_id):
    payload = {'asset': asset, 'amount': str(amount), 'description': description}
    try:
        r = requests.post(f'{CRYPTO_PAY_URL}/createInvoice', json=payload, headers=crypto_headers(), timeout=10)
        r.raise_for_status()
        resp = r.json()
        if resp.get('ok'):
            inv = resp['result']
            asyncio.create_task(save_invoice_db(int(inv['invoice_id']), user_id, amount, asset, inv.get('hash')))
        return resp
    except Exception as e:
        logging.error("create_invoice error: %s", e)
        return {'ok': False, 'error': str(e)}

async def save_invoice_db(invoice_id, user_id, amount, asset, hash_val):
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute("INSERT OR IGNORE INTO invoices(invoice_id,user_id,amount,asset,hash,created_at) VALUES (?,?,?,?,?,?)",
                           (invoice_id, user_id, amount, asset, hash_val, now_iso()))
        await conn.commit()

def get_invoices(invoice_ids):
    try:
        payload = {'invoice_ids': ','.join(map(str, invoice_ids))}
        r = requests.get(f'{CRYPTO_PAY_URL}/getInvoices', params=payload, headers=crypto_headers(), timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error("get_invoices error: %s", e)
        return {'ok': False, 'error': str(e)}

async def background_payment_checker():
    while True:
        try:
            async with aiosqlite.connect(DB_FILE) as conn:
                conn.row_factory = aiosqlite.Row
                async with conn.execute("SELECT invoice_id FROM invoices WHERE status='unpaid'") as cur:
                    rows = await cur.fetchall()
            if rows:
                invoice_ids = [r['invoice_id'] for r in rows]
                resp = get_invoices(invoice_ids)
                if resp.get('ok'):
                    items = resp['result'].get('items', [])
                    for it in items:
                        if it.get('status') == 'paid':
                            invoice_id = int(it['invoice_id'])
                            async with aiosqlite.connect(DB_FILE) as conn:
                                conn.row_factory = aiosqlite.Row
                                async with conn.execute("SELECT user_id,amount,asset FROM invoices WHERE invoice_id=?", (invoice_id,)) as cur:
                                    row = await cur.fetchone()
                                if row:
                                    user_id, amount, asset = row['user_id'], row['amount'], row['asset']
                                    try:
                                        rate = get_rate(asset)
                                        rub_amount = float(amount) * float(rate)
                                    except Exception:
                                        rub_amount = 0.0
                                    await conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (rub_amount, user_id))
                                    await conn.execute("UPDATE invoices SET status='paid' WHERE invoice_id=?", (invoice_id,))
                                    await conn.commit()
                                    if await is_notify_enabled(user_id):
                                        try:
                                            await bot.send_message(user_id, f"🎉 Платёж подтверждён — баланс пополнен на {rub_amount:.2f} RUB")
                                        except Exception:
                                            pass
        except Exception as e:
            logging.error("Payment checker loop error: %s", e)
        await asyncio.sleep(20)

# ---------------- Handlers ----------------
@dp.message(CommandStart())
async def handler_start(message: Message):
    if await maintenance_block(message): return
    await ensure_user_record(message.from_user)
    await message.answer(f"👋 Добро пожаловать в *{MARKET_NAME}*!\nВыберите действие ниже:", 
                        parse_mode="Markdown", 
                        reply_markup=main_menu_markup(message.from_user.id))

# --- Balance & deposit ---
@dp.callback_query(lambda c: c.data == "menu_balance")
async def cb_balance(callback: CallbackQuery):
    if await maintenance_block(callback): return
    await ensure_user_record(callback.from_user)
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT balance FROM users WHERE user_id=?", (callback.from_user.id,)) as cur:
            bal = (await cur.fetchone())['balance']
    text = f"💰 Ваш баланс: *{format_money(bal)}*"
    markup = simple_markup([
        [InlineKeyboardButton(text="💸 Пополнить", callback_data="menu_deposit")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_back_main")]
    ])
    await callback.message.answer(text, parse_mode="Markdown", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "menu_deposit")
async def cb_deposit(callback: CallbackQuery, state: FSMContext):
    if await maintenance_block(callback): return
    markup = simple_markup([
        [InlineKeyboardButton(text="USDT", callback_data="deposit_asset|USDT"), 
         InlineKeyboardButton(text="BTC", callback_data="deposit_asset|BTC")],
        [InlineKeyboardButton(text="ETH", callback_data="deposit_asset|ETH"), 
         InlineKeyboardButton(text="TON", callback_data="deposit_asset|TON")],
        [InlineKeyboardButton(text="TRX", callback_data="deposit_asset|TRX")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_back_main")]
    ])
    await callback.message.answer("💸 Выберите валюту для пополнения (через CryptoBot):", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("deposit_asset|"))
async def cb_deposit_asset(callback: CallbackQuery, state: FSMContext):
    if await maintenance_block(callback): return
    asset = callback.data.split("|", 1)[1]
    await state.set_state(DepositState.amount)
    await state.set_data({"asset": asset})
    await callback.message.answer(f"💸 Введите сумму в RUB, которую хотите пополнить через *{asset}*:", 
                                parse_mode="Markdown", reply_markup=cancel_markup())
    await callback.answer()

@dp.message(DepositState.amount)
async def process_deposit_amount(message: Message, state: FSMContext):
    data = await state.get_data()
    asset = data.get("asset")
    if message.text.strip().lower() in ["отмена", "cancel", "❌"]:
        await message.answer("❌ Пополнение отменено.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    try:
        rub = float(message.text.strip().replace(',', '.'))
        if rub <= 0:
            raise ValueError("<=0")
    except Exception:
        await message.answer("❌ Неверная сумма. Введите число (например 1000.50).", reply_markup=cancel_markup())
        return
    try:
        rate = get_rate(asset)
        crypto_amount = rub / rate
    except Exception:
        await message.answer("❌ Не удалось получить курс. Попробуйте позже.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    resp = create_invoice(asset, crypto_amount, f"Пополнение {MARKET_NAME} для {message.from_user.id}", message.from_user.id)
    if resp.get('ok'):
        inv = resp['result']
        pay_url = f"https://t.me/CryptoBot/app?startapp=invoice-{inv.get('hash')}&mode=compact"
        markup = simple_markup([
            [InlineKeyboardButton(text="💳 Оплатить", url=pay_url)],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"invoice_cancel|{inv.get('invoice_id')}")]
        ])
        await message.answer(f"💳 Счет создан:\nСумма: *{rub:.2f} RUB* (~{crypto_amount:.6f} {asset})\nНажмите кнопку для оплаты (CryptoBot).", 
                           parse_mode="Markdown", reply_markup=markup)
    else:
        await message.answer(f"❌ Ошибка создания счета: {resp.get('error')}", reply_markup=main_menu_markup(message.from_user.id))
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("invoice_cancel|"))
async def cb_invoice_cancel(callback: CallbackQuery):
    inv_id = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT user_id,status FROM invoices WHERE invoice_id=?", (inv_id,)) as cur:
            r = await cur.fetchone()
        if not r:
            await callback.message.answer("Счет не найден.")
            await callback.answer()
            return
        if r['user_id'] != callback.from_user.id:
            await callback.message.answer("Это не ваш счет.")
            await callback.answer()
            return
        if r['status'] != 'unpaid':
            await callback.message.answer("Счет уже оплачен или отменён.")
            await callback.answer()
            return
        await conn.execute("DELETE FROM invoices WHERE invoice_id=?", (inv_id,))
        await conn.commit()
    await callback.message.answer("❌ Счет отменён.", reply_markup=main_menu_markup(callback.from_user.id))
    await callback.answer()

# ---------------- Categories & Products ----------------
@dp.callback_query(lambda c: c.data == "menu_products")
async def cb_products(callback: CallbackQuery):
    if await maintenance_block(callback): return
    await ensure_user_record(callback.from_user)
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT COUNT(*) as cnt FROM products") as cur:
            total = (await cur.fetchone())['cnt']
    text = f"🛍 Категории товаров (всего товаров: {total})"
    markup = await build_categories_markup(admin_view=(callback.from_user.id == ADMIN_ID))
    await callback.message.answer(text, reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("cat|"))
async def cb_category(callback: CallbackQuery):
    if await maintenance_block(callback): return
    cat_id = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT name FROM categories WHERE id=?", (cat_id,)) as cur:
            cat = await cur.fetchone()
        async with conn.execute("SELECT id,name FROM subcategories WHERE category_id=? ORDER BY name", (cat_id,)) as cur:
            subs = await cur.fetchall()
    if not cat:
        await callback.message.answer("Категория не найдена.")
        await callback.answer()
        return
    buttons = [[InlineKeyboardButton(text="📦 Все товары в категории", callback_data=f"list_products|cat|{cat_id}|1")]]
    for s in subs:
        buttons.append([InlineKeyboardButton(text=f"📂 {s['name']}", callback_data=f"list_products|sub|{s['id']}|1")])
    if callback.from_user.id == ADMIN_ID:
        buttons.append([InlineKeyboardButton(text="➕ Создать подкатегорию", callback_data=f"admin_create_sub|{cat_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_products")])
    markup = simple_markup(buttons)
    await callback.message.answer(f"📁 *{cat['name']}*\nВыберите подкатегорию или просмотреть все товары:", 
                                parse_mode="Markdown", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("list_products|"))
async def cb_list_products(callback: CallbackQuery):
    if await maintenance_block(callback): return
    parts = callback.data.split("|")
    mode = parts[1]
    ident = int(parts[2])
    page = int(parts[3])
    per_page = 10
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        if mode == "cat":
            async with conn.execute("SELECT COUNT(*) as cnt FROM products WHERE category_id=?", (ident,)) as cur:
                total = (await cur.fetchone())['cnt']
            async with conn.execute("SELECT id,title FROM products WHERE category_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?", 
                                    (ident, per_page, (page-1)*per_page)) as cur:
                prods = await cur.fetchall()
        else:
            async with conn.execute("SELECT COUNT(*) as cnt FROM products WHERE subcategory_id=?", (ident,)) as cur:
                total = (await cur.fetchone())['cnt']
            async with conn.execute("SELECT id,title FROM products WHERE subcategory_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?", 
                                    (ident, per_page, (page-1)*per_page)) as cur:
                prods = await cur.fetchall()
    if not prods:
        await callback.message.answer("В этой категории пока нет товаров.", reply_markup=main_menu_markup(callback.from_user.id))
        await callback.answer("Товары не найдены.")
        return
    buttons = []
    for p in prods:
        buttons.append([InlineKeyboardButton(text=f"🛒 {p['title']}", callback_data=f"view_product|{p['id']}")])
    total_pages = (total + per_page - 1) // per_page
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"list_products|{mode}|{ident}|{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"list_products|{mode}|{ident}|{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cat|{ident}" if mode == "cat" else f"list_products|cat|{ident}|1")])
    markup = simple_markup(buttons)
    await callback.message.answer(f"Товары (страница {page}/{total_pages}):", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("view_product|"))
async def cb_view_product(callback: CallbackQuery):
    if await maintenance_block(callback): return
    pid = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("""SELECT p.*, s.user_id as seller_user_id, s.username as seller_username
                                FROM products p LEFT JOIN sellers s ON p.seller_id = s.id WHERE p.id=?""", (pid,)) as cur:
            p = await cur.fetchone()
    if not p:
        await callback.message.answer("Товар не найден.")
        await callback.answer()
        return
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT AVG(rating) as avg, COUNT(*) as cnt FROM reviews WHERE product_id=?", (pid,)) as cur:
            stats = await cur.fetchone()
            avg = stats['avg'] if stats['avg'] else 0.0
            cnt = stats['cnt']
    msk_tz = pytz.timezone('Europe/Moscow')
    created_at_msk = datetime.fromisoformat(p['created_at']).astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
    text = f"🛒 *{p['title']}* (ID: {p['id']})\n\n{p['description']}\n\n💵 Цена: *{format_money(p['price'])}*\n📦 Количество: {p['quantity']}\n👤 Продавец: @{p['seller_username'] or '-'}\n⭐ Рейтинг товара: *{avg:.1f}* / 5.0 ({cnt} отзывов)\n📅 Создан: {created_at_msk}"
    markup = simple_markup([
        [
            InlineKeyboardButton(text="🛍 Купить", callback_data=f"buy|{pid}"),
            InlineKeyboardButton(text="📝 Отзыв", callback_data=f"review|{pid}")
        ],
        [InlineKeyboardButton(text="👤 Карточка продавца", callback_data=f"seller_card|{p['seller_user_id']}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_products")]
    ])
    if p['photo_file_id']:
        await bot.send_photo(callback.message.chat.id, p['photo_file_id'], caption=text, parse_mode="Markdown", reply_markup=markup)
    else:
        await callback.message.answer(text, parse_mode="Markdown", reply_markup=markup)
    await callback.answer()

# ---------------- Seller Card ----------------
@dp.callback_query(lambda c: c.data.startswith("seller_card|"))
async def cb_seller_card(callback: CallbackQuery):
    if await maintenance_block(callback): return
    seller_user_id = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id,username,info,user_id FROM sellers WHERE user_id=?", (seller_user_id,)) as cur:
            s = await cur.fetchone()
        if not s:
            await callback.message.answer("Продавец не найден.")
            await callback.answer()
            return
        async with conn.execute("SELECT COUNT(*) as cnt FROM products WHERE seller_id=?", (s['id'],)) as cur:
            total_products = (await cur.fetchone())['cnt']
        async with conn.execute("""SELECT AVG(r.rating) as avg, COUNT(r.id) as cnt FROM reviews r
                                JOIN products p ON r.product_id = p.id
                                WHERE p.seller_id = ?""", (s['id'],)) as cur:
            st = await cur.fetchone()
            avg = st['avg'] or 0.0
            cnt = st['cnt'] or 0
        async with conn.execute("""SELECT r.username, r.rating, r.text, r.created_at
                                FROM reviews r
                                JOIN products p ON r.product_id = p.id
                                WHERE p.seller_id = ?
                                ORDER BY r.created_at DESC LIMIT 5""", (s['id'],)) as cur:
            reviews = await cur.fetchall()
    msk_tz = pytz.timezone('Europe/Moscow')
    reviews_text = ""
    for r in reviews:
        uname = r['username'] or 'анон'
        created_at_msk = datetime.fromisoformat(r['created_at']).astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
        reviews_text += f"⭐ {r['rating']}/5 — @{uname} ({created_at_msk}): {r['text'] or 'Без текста'}\n"
    text = (f"👤 *Продавец:* @{s['username'] or 'анон'}\n"
            f"📌 О продавце: {s['info'] or '-'}\n\n"
            f"📦 Товаров: {total_products}\n⭐ Средняя оценка: *{avg:.1f}* / 5.0 ({cnt} отзывов)\n\n"
            f"📝 Последние отзывы:\n{reviews_text or 'Отзывов пока нет.'}")
    markup = simple_markup([
        [InlineKeyboardButton(text="📦 Посмотреть товары продавца", callback_data=f"list_seller_products|{s['id']}|1")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_products")]
    ])
    await callback.message.answer(text, parse_mode="Markdown", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("list_seller_products|"))
async def cb_list_seller_products(callback: CallbackQuery):
    parts = callback.data.split("|")
    sid = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 1
    per_page = 10
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT COUNT(*) as cnt FROM products WHERE seller_id=?", (sid,)) as cur:
            total = (await cur.fetchone())['cnt']
        async with conn.execute("SELECT id,title FROM products WHERE seller_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?", 
                                (sid, per_page, (page-1)*per_page)) as cur:
            prods = await cur.fetchall()
    if not prods:
        await callback.message.answer("У продавца пока нет товаров.", reply_markup=main_menu_markup(callback.from_user.id))
        await callback.answer("Нет товаров")
        return
    buttons = []
    for p in prods:
        buttons.append([InlineKeyboardButton(text=f"🛒 {p['title']}", callback_data=f"view_product|{p['id']}")])
    total_pages = (total + per_page - 1) // per_page
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"list_seller_products|{sid}|{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"list_seller_products|{sid}|{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_products")])
    markup = simple_markup(buttons)
    await callback.message.answer(f"Товары продавца (страница {page}/{total_pages}):", reply_markup=markup)
    await callback.answer()

# ---------------- Buy & Reviews & Disputes logic ----------------
@dp.callback_query(lambda c: c.data.startswith("buy|"))
async def cb_buy(callback: CallbackQuery):
    if await maintenance_block(callback): return
    pid = int(callback.data.split("|", 1)[1])
    await ensure_user_record(callback.from_user)
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id,title,price,quantity,content_text,content_file_id,seller_id FROM products WHERE id=?", (pid,)) as cur:
            p = await cur.fetchone()
        if not p:
            await callback.message.answer("Товар не найден.")
            await callback.answer()
            return
        if p['quantity'] <= 0:
            await callback.message.answer("Товар закончился.")
            await callback.answer()
            return
        async with conn.execute("SELECT balance FROM users WHERE user_id=?", (callback.from_user.id,)) as cur:
            bal = (await cur.fetchone())['balance']
    if bal < p['price']:
        await callback.message.answer(f"❌ На балансе {format_money(bal)}, цена: {format_money(p['price'])}. Пополните баланс.", 
                                    reply_markup=main_menu_markup(callback.from_user.id))
        await callback.answer("Недостаточно средств.")
        return
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (p['price'], callback.from_user.id))
        await conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = (SELECT user_id FROM sellers WHERE id=?)", 
                         (p['price'], p['seller_id']))
        await conn.execute("UPDATE products SET quantity = quantity - 1 WHERE id=?", (pid,))
        async with conn.execute("INSERT INTO orders(user_id, product_id, seller_id, price, created_at) VALUES (?, ?, ?, ?, ?) RETURNING id", 
                              (callback.from_user.id, pid, p['seller_id'], p['price'], now_iso())) as cur:
            row = await cur.fetchone()
            order_id = row['id'] if row else None
        await conn.commit()
    if not order_id:
        await callback.message.answer("Ошибка при создании заказа.")
        await callback.answer()
        return
    await callback.message.answer(f"✅ Вы купили *{p['title']}* за {format_money(p['price'])}.\nСпасибо за покупку!", 
                                parse_mode="Markdown")
    # Send content
    if p['content_text']:
        await callback.message.answer(f"Содержимое товара: {p['content_text']}")
    elif p['content_file_id']:
        try:
            await bot.send_document(callback.message.chat.id, p['content_file_id'], caption="Содержимое товара")
        except:
            await bot.send_photo(callback.message.chat.id, p['content_file_id'], caption="Содержимое товара")
    # Post-purchase options
    markup = simple_markup([
        [InlineKeyboardButton(text="📝 Оставить отзыв", callback_data=f"review|{pid}"),
         InlineKeyboardButton(text="⚖️ Открыть спор", callback_data=f"dispute|{order_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_back_main")]
    ])
    await callback.message.answer("Что дальше?", reply_markup=markup)
    # Notify seller
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT user_id FROM sellers WHERE id=?", (p['seller_id'],)) as cur:
            seller = await cur.fetchone()
    if seller and await is_notify_enabled(seller['user_id']):
        try:
            await bot.send_message(seller['user_id'], 
                                 f"🎉 Ваш товар *{p['title']}* куплен за {format_money(p['price'])}!\nБаланс пополнен.", 
                                 parse_mode="Markdown")
        except Exception:
            pass
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("review|"))
async def cb_review(callback: CallbackQuery, state: FSMContext):
    if await maintenance_block(callback): return
    pid = int(callback.data.split("|", 1)[1])
    user_id = callback.from_user.id
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT 1 FROM orders WHERE user_id=? AND product_id=? LIMIT 1", (user_id, pid)) as cur:
            purchased = await cur.fetchone() is not None
        async with conn.execute("SELECT 1 FROM reviews WHERE product_id=? AND user_id=? LIMIT 1", (pid, user_id)) as cur:
            already = await cur.fetchone() is not None
    if not purchased:
        await callback.message.answer("Отзыв можно оставить только после покупки этого товара!")
        await callback.answer()
        return
    if already:
        await callback.message.answer("Вы уже оставляли отзыв на этот товар.")
        await callback.answer()
        return
    markup = simple_markup([
        [InlineKeyboardButton(text="⭐⭐⭐⭐⭐ 5", callback_data=f"leave_rating|{pid}|5")],
        [InlineKeyboardButton(text="⭐⭐⭐⭐ 4", callback_data=f"leave_rating|{pid}|4")],
        [InlineKeyboardButton(text="⭐⭐⭐ 3", callback_data=f"leave_rating|{pid}|3")],
        [InlineKeyboardButton(text="⭐⭐ 2", callback_data=f"leave_rating|{pid}|2")],
        [InlineKeyboardButton(text="⭐ 1", callback_data=f"leave_rating|{pid}|1")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"view_product|{pid}")]
    ])
    await callback.message.answer("⭐ Выберите оценку (1–5):", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("leave_rating|"))
async def cb_leave_rating(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("|")
    pid = int(parts[1])
    rating = int(parts[2])
    await state.set_state(ReviewState.text)
    await state.set_data({"pid": pid, "rating": rating})
    await callback.message.answer("✍️ Оставьте текст отзыва (или напишите `-` чтобы пропустить):", reply_markup=cancel_markup("Пропустить"))
    await callback.answer()

@dp.message(ReviewState.text)
async def process_review_text(message: Message, state: FSMContext):
    data = await state.get_data()
    pid = data.get("pid")
    rating = data.get("rating")
    text = message.text.strip() if message.text and message.text.strip().lower() not in ["-", "отмена", "cancel", "❌"] else ""
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute("INSERT INTO reviews(product_id,user_id,username,rating,text,created_at) VALUES (?,?,?,?,?,?)",
                           (pid, message.from_user.id, message.from_user.username, rating, text, now_iso()))
        await conn.commit()
    await message.answer("✅ Спасибо за отзыв!", reply_markup=main_menu_markup(message.from_user.id))
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("dispute|"))
async def cb_dispute(callback: CallbackQuery, state: FSMContext):
    if await maintenance_block(callback): return
    order_id = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT 1 FROM disputes WHERE order_id=?", (order_id,)) as cur:
            exists = await cur.fetchone() is not None
    if exists:
        await callback.message.answer("Спор по этой покупке уже открыт.")
        await callback.answer()
        return
    await state.set_state(DisputeState.description)
    await state.set_data({"order_id": order_id})
    await callback.message.answer("⚖️ Опишите проблему со сделкой:", reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(DisputeState.description)
async def process_dispute_desc(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("❌ Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    desc = message.text.strip()
    data = await state.get_data()
    order_id = data.get("order_id")
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute("INSERT INTO disputes(order_id, user_id, description, created_at) VALUES (?, ?, ?, ?)",
                           (order_id, message.from_user.id, desc, now_iso()))
        await conn.commit()
    await message.answer("⚖️ Заявка на спор отправлена администраторам.", reply_markup=main_menu_markup(message.from_user.id))
    await state.clear()

# ---------------- Selling ----------------
@dp.callback_query(lambda c: c.data == "menu_sell")
async def cb_menu_sell(callback: CallbackQuery):
    if await maintenance_block(callback): return
    await ensure_user_record(callback.from_user)
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id FROM sellers WHERE user_id=?", (callback.from_user.id,)) as cur:
            s = await cur.fetchone()
    if not s:
        markup = simple_markup([
            [InlineKeyboardButton(text="📝 Создать профиль продавца", callback_data="seller_create")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_back_main")]
        ])
        await callback.message.answer("Вы ещё не продавец. Создайте профиль продавца, чтобы выставлять товары.", reply_markup=markup)
    else:
        markup = simple_markup([
            [InlineKeyboardButton(text="➕ Добавить товар", callback_data="add_product")],
            [InlineKeyboardButton(text="📦 Мои товары", callback_data=f"my_products|{callback.from_user.id}|1")],
            [InlineKeyboardButton(text="💸 Мои продажи", callback_data=f"my_sales|{s['id']}|1")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_back_main")]
        ])
        await callback.message.answer("🎪 Панель продавца", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "seller_create")
async def cb_seller_create(callback: CallbackQuery, state: FSMContext):
    if await maintenance_block(callback): return
    await state.set_state(SellerCreate.info)
    await callback.message.answer("📝 Введите краткую информацию о себе (описание продавца):", reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(SellerCreate.info)
async def process_seller_info(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("❌ Создание профиля отменено.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    info = message.text.strip()
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute("INSERT OR IGNORE INTO sellers(user_id,username,info) VALUES (?,?,?)", 
                         (message.from_user.id, message.from_user.username, info))
        await conn.execute("UPDATE sellers SET info=? WHERE user_id=?", (info, message.from_user.id))
        await conn.commit()
    await message.answer("✅ Профиль продавца создан/обновлён.", reply_markup=main_menu_markup(message.from_user.id))
    await state.clear()

@dp.callback_query(lambda c: c.data == "seller_edit_info")
async def cb_seller_edit_info(callback: CallbackQuery, state: FSMContext):
    if await maintenance_block(callback): return
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id FROM sellers WHERE user_id=?", (callback.from_user.id,)) as cur:
            s = await cur.fetchone()
        if not s:
            await callback.message.answer("Профиль продавца не найден.")
            await callback.answer()
            return
    await state.set_state(SellerEditInfo.info)
    await callback.message.answer("📝 Введите новое описание продавца:", reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(SellerEditInfo.info)
async def process_seller_edit_info(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("❌ Изменение профиля отменено.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    info = message.text.strip()
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute("UPDATE sellers SET info=? WHERE user_id=?", (info, message.from_user.id))
        await conn.commit()
    await message.answer("✅ Описание продавца обновлено.", reply_markup=main_menu_markup(message.from_user.id))
    await state.clear()

# Add product flow
@dp.callback_query(lambda c: c.data == "add_product")
async def cb_add_product(callback: CallbackQuery, state: FSMContext):
    if await maintenance_block(callback): return
    await ensure_user_record(callback.from_user)
    await state.set_state(AddProduct.photo)
    await callback.message.answer("📸 Пришлите фотографию товара (или URL изображения, опционально):", reply_markup=cancel_markup("Отменить"))
    await callback.answer()

@dp.message(AddProduct.photo)
async def handle_product_photo(message: Message, state: FSMContext):
    if message.text and message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("❌ Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.text and (message.text.startswith("http://") or message.text.startswith("https://")):
        file_id = message.text.strip()
    await state.update_data(photo=file_id)
    await state.set_state(AddProduct.title)
    await message.answer("✏️ Введите название товара:", reply_markup=cancel_markup("Отмена"))

@dp.message(AddProduct.title)
async def handle_product_title(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("❌ Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    await state.update_data(title=message.text.strip())
    await state.set_state(AddProduct.desc)
    await message.answer("📝 Введите краткое описание товара:", reply_markup=cancel_markup("Отмена"))

@dp.message(AddProduct.desc)
async def handle_product_desc(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("❌ Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    await state.update_data(desc=message.text.strip())
    await state.set_state(AddProduct.price)
    await message.answer("💵 Укажите цену в RUB (например 499.99):", reply_markup=cancel_markup("Отмена"))

@dp.message(AddProduct.price)
async def handle_product_price(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("❌ Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    try:
        price = float(message.text.strip().replace(',', '.'))
        if price <= 0: raise ValueError
    except Exception:
        await message.answer("❌ Неверная цена. Введите число.", reply_markup=cancel_markup())
        return
    await state.update_data(price=price)
    await state.set_state(AddProduct.quantity)
    await message.answer("📦 Укажите количество товара:", reply_markup=cancel_markup("Отмена"))

@dp.message(AddProduct.quantity)
async def handle_product_quantity(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("❌ Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    try:
        quantity = int(message.text.strip())
        if quantity <= 0: raise ValueError
    except Exception:
        await message.answer("❌ Неверное количество. Введите целое число >0.", reply_markup=cancel_markup())
        return
    await state.update_data(quantity=quantity)
    await state.set_state(AddProduct.content)
    await message.answer("📄 Отправьте содержимое товара (текст, фото или файл):", reply_markup=cancel_markup("Отмена"))

@dp.message(AddProduct.content)
async def handle_product_content(message: Message, state: FSMContext):
    if message.text and message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("❌ Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    content_text = None
    content_file_id = None
    if message.text:
        content_text = message.text.strip()
    elif message.photo:
        content_file_id = message.photo[-1].file_id
    elif message.document:
        content_file_id = message.document.file_id
    else:
        await message.answer("❌ Пожалуйста, отправьте текст, фото или файл.", reply_markup=cancel_markup())
        return
    await state.update_data(content_text=content_text, content_file_id=content_file_id)
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id,name FROM categories ORDER BY name") as cur:
            cats = await cur.fetchall()
    buttons = []
    for c in cats:
        buttons.append([InlineKeyboardButton(text=c['name'], callback_data=f"addprod_cat|{c['id']}")])
    if message.from_user.id == ADMIN_ID:
        buttons.append([InlineKeyboardButton(text="➕ Создать категорию", callback_data="admin_create_category")])
    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="action_cancel")])
    markup = simple_markup(buttons)
    await message.answer("📂 Выберите категорию для товара:", reply_markup=markup)

@dp.callback_query(lambda c: c.data.startswith("addprod_cat|"))
async def cb_addprod_cat(callback: CallbackQuery, state: FSMContext):
    cat_id = int(callback.data.split("|", 1)[1])
    await state.update_data(cat_id=cat_id)
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id,name FROM subcategories WHERE category_id=?", (cat_id,)) as cur:
            subs = await cur.fetchall()
    buttons = []
    for s in subs:
        buttons.append([InlineKeyboardButton(text=s['name'], callback_data=f"addprod_sub|{s['id']}")])
    if callback.from_user.id == ADMIN_ID:
        buttons.append([InlineKeyboardButton(text="➕ Создать подкатегорию", callback_data=f"admin_create_sub|{cat_id}")])
    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="action_cancel")])
    markup = simple_markup(buttons)
    await callback.message.answer("📂 Выберите подкатегорию:", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("addprod_sub|"))
async def cb_addprod_sub(callback: CallbackQuery, state: FSMContext):
    sub_id = int(callback.data.split("|", 1)[1])
    data = await state.get_data()
    photo = data.get("photo")
    title = data.get("title")
    desc = data.get("desc")
    price = data.get("price")
    quantity = data.get("quantity")
    content_text = data.get("content_text")
    content_file_id = data.get("content_file_id")
    cat_id = data.get("cat_id")
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id FROM sellers WHERE user_id=?", (callback.from_user.id,)) as cur:
            srow = await cur.fetchone()
        if not srow:
            await conn.execute("INSERT INTO sellers(user_id,username) VALUES (?,?)", (callback.from_user.id, callback.from_user.username))
            await conn.commit()
            async with conn.execute("SELECT last_insert_rowid() as id") as cur:
                seller_id = (await cur.fetchone())['id']
        else:
            seller_id = srow['id']
        await conn.execute("""INSERT INTO products(seller_id,title,description,photo_file_id,category_id,subcategory_id,price,quantity,content_text,content_file_id,created_at)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""", (seller_id, title, desc, photo, cat_id, sub_id, price, quantity, content_text, content_file_id, now_iso()))
        await conn.commit()
    await callback.message.answer("✅ Товар выставлен.", reply_markup=main_menu_markup(callback.from_user.id))
    await callback.answer("Товар добавлен!")
    await state.clear()

# ---------------- My products ----------------
@dp.callback_query(lambda c: c.data.startswith("my_products|"))
async def cb_my_products(callback: CallbackQuery):
    parts = callback.data.split("|")
    uid = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 1
    per_page = 10
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id FROM sellers WHERE user_id=?", (uid,)) as cur:
            s = await cur.fetchone()
        if not s:
            await callback.message.answer("Создайте профиль продавца, чтобы выставлять товары.", reply_markup=main_menu_markup(callback.from_user.id))
            await callback.answer("У вас нет профиля продавца.")
            return
        seller_id = s['id']
        async with conn.execute("SELECT COUNT(*) as cnt FROM products WHERE seller_id=?", (seller_id,)) as cur:
            total = (await cur.fetchone())['cnt']
        async with conn.execute("SELECT id,title FROM products WHERE seller_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?", 
                                (seller_id, per_page, (page-1)*per_page)) as cur:
            prods = await cur.fetchall()
    if not prods:
        await callback.message.answer("У вас пока нет товаров.", reply_markup=main_menu_markup(callback.from_user.id))
        await callback.answer()
        return
    buttons = []
    for p in prods:
        buttons.append([InlineKeyboardButton(text=f"🛒 {p['title']}", callback_data=f"view_my_product|{p['id']}")])
    total_pages = (total + per_page - 1) // per_page
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"my_products|{uid}|{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"my_products|{uid}|{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_sell")])
    markup = simple_markup(buttons)
    await callback.message.answer(f"Мои товары (страница {page}/{total_pages}):", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("view_my_product|"))
async def cb_view_my_product(callback: CallbackQuery):
    pid = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM products WHERE id=?", (pid,)) as cur:
            p = await cur.fetchone()
    if not p or p['seller_id'] != (await get_seller_id(callback.from_user.id)):
        await callback.message.answer("Товар не найден или не ваш.")
        await callback.answer()
        return
    msk_tz = pytz.timezone('Europe/Moscow')
    created_at_msk = datetime.fromisoformat(p['created_at']).astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
    text = f"🛒 *{p['title']}*\n\n{p['description']}\n\n💵 Цена: *{format_money(p['price'])}*\n📦 Количество: {p['quantity']}\n📅 Создан: {created_at_msk}"
    buttons = [
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit_product|{pid}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"my_products|{callback.from_user.id}|1")]
    ]
    markup = simple_markup(buttons)
    if p['photo_file_id']:
        await bot.send_photo(callback.message.chat.id, p['photo_file_id'], caption=text, parse_mode="Markdown", reply_markup=markup)
    else:
        await callback.message.answer(text, parse_mode="Markdown", reply_markup=markup)
    await callback.answer()

async def get_seller_id(user_id):
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id FROM sellers WHERE user_id=?", (user_id,)) as cur:
            s = await cur.fetchone()
            return s['id'] if s else None

@dp.callback_query(lambda c: c.data.startswith("edit_product|"))
async def cb_edit_product(callback: CallbackQuery, state: FSMContext):
    pid = int(callback.data.split("|", 1)[1])
    seller_id = await get_seller_id(callback.from_user.id)
    if not seller_id:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT seller_id FROM products WHERE id=?", (pid,)) as cur:
            p = await cur.fetchone()
        if p['seller_id'] != seller_id:
            await callback.message.answer("Это не ваш товар.")
            await callback.answer()
            return
    markup = simple_markup([
        [InlineKeyboardButton(text="Название", callback_data=f"edit_field|{pid}|title"),
         InlineKeyboardButton(text="Описание", callback_data=f"edit_field|{pid}|desc")],
        [InlineKeyboardButton(text="Цена", callback_data=f"edit_field|{pid}|price"),
         InlineKeyboardButton(text="Количество", callback_data=f"edit_field|{pid}|quantity")],
        [InlineKeyboardButton(text="Содержимое", callback_data=f"edit_field|{pid}|content")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"view_my_product|{pid}")]
    ])
    await callback.message.answer("Что редактировать?", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("edit_field|"))
async def cb_edit_field(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("|")
    pid = int(parts[1])
    field = parts[2]
    await state.set_state(EditProduct.value)
    await state.set_data({"pid": pid, "field": field})
    prompt = {
        "title": "Введите новое название:",
        "desc": "Введите новое описание:",
        "price": "Введите новую цену:",
        "quantity": "Введите новое количество:",
        "content": "Отправьте новое содержимое (текст, фото или файл):"
    }[field]
    await callback.message.answer(prompt, reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(EditProduct.value)
async def process_edit_value(message: Message, state: FSMContext):
    data = await state.get_data()
    pid = data.get("pid")
    field = data.get("field")
    if message.text and message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("❌ Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    value = None
    if field == "content":
        content_text = None
        content_file_id = None
        if message.text:
            content_text = message.text.strip()
        elif message.photo:
            content_file_id = message.photo[-1].file_id
        elif message.document:
            content_file_id = message.document.file_id
        else:
            await message.answer("❌ Неверный формат.")
            return
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute("UPDATE products SET content_text=?, content_file_id=? WHERE id=?", (content_text, content_file_id, pid))
            await conn.commit()
    else:
        try:
            if field in ("price",):
                value = float(message.text.strip().replace(',', '.'))
            elif field in ("quantity",):
                value = int(message.text.strip())
            else:
                value = message.text.strip()
        except Exception:
            await message.answer("❌ Неверный формат.")
            return
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute(f"UPDATE products SET {field}=? WHERE id=?", (value, pid))
            await conn.commit()
    await message.answer("✅ Обновлено.", reply_markup=main_menu_markup(message.from_user.id))
    await state.clear()

# ---------------- My Sales ----------------
@dp.callback_query(lambda c: c.data.startswith("my_sales|"))
async def cb_my_sales(callback: CallbackQuery):
    if await maintenance_block(callback): return
    parts = callback.data.split("|")
    seller_id = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 1
    per_page = 10
    msk_tz = pytz.timezone('Europe/Moscow')
    today = datetime.now(msk_tz).strftime('%Y-%m-%d')
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT user_id FROM sellers WHERE id=?", (seller_id,)) as cur:
            s = await cur.fetchone()
        if not s or s['user_id'] != callback.from_user.id:
            await callback.message.answer("Доступ запрещён.")
            await callback.answer()
            return
        async with conn.execute("SELECT COUNT(*) as cnt FROM orders WHERE seller_id=? AND date(created_at)=?", 
                               (seller_id, today)) as cur:
            daily_sales = (await cur.fetchone())['cnt']
        async with conn.execute("SELECT SUM(price) as total FROM orders WHERE seller_id=? AND date(created_at)=?", 
                               (seller_id, today)) as cur:
            daily_profit = (await cur.fetchone())['total'] or 0.0
        async with conn.execute("SELECT COUNT(*) as cnt FROM orders WHERE seller_id=?", (seller_id,)) as cur:
            total = (await cur.fetchone())['cnt']
        async with conn.execute("""SELECT o.id, o.created_at, o.price, p.title 
                                FROM orders o JOIN products p ON o.product_id = p.id 
                                WHERE o.seller_id=? ORDER BY o.created_at DESC LIMIT ? OFFSET ?""", 
                                (seller_id, per_page, (page-1)*per_page)) as cur:
            sales = await cur.fetchall()
    text = f"💸 Мои продажи\n📈 Сегодня продано: {daily_sales} товаров\n💰 Прибыль за сегодня: {format_money(daily_profit)}\n\nСписок продаж (страница {page}):"
    if not sales:
        await callback.message.answer(f"{text}\nПока нет продаж.", reply_markup=main_menu_markup(callback.from_user.id))
        await callback.answer()
        return
    buttons = []
    for s in sales:
        created_at_msk = datetime.fromisoformat(s['created_at']).astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
        buttons.append([InlineKeyboardButton(text=f"🛒 {s['title']} ({created_at_msk})", callback_data=f"view_sale|{s['id']}")])
    total_pages = (total + per_page - 1) // per_page
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"my_sales|{seller_id}|{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"my_sales|{seller_id}|{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_sell")])
    markup = simple_markup(buttons)
    await callback.message.answer(text, parse_mode="Markdown", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("view_sale|"))
async def cb_view_sale(callback: CallbackQuery):
    if await maintenance_block(callback): return
    order_id = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("""SELECT o.*, p.title, p.description, u.username as buyer_username
                                FROM orders o 
                                JOIN products p ON o.product_id = p.id 
                                JOIN users u ON o.user_id = u.user_id
                                WHERE o.id=? AND o.seller_id=(SELECT id FROM sellers WHERE user_id=?)""", 
                                (order_id, callback.from_user.id)) as cur:
            s = await cur.fetchone()
    if not s:
        await callback.message.answer("Продажа не найдена или не ваша.")
        await callback.answer()
        return
    msk_tz = pytz.timezone('Europe/Moscow')
    created_at_msk = datetime.fromisoformat(s['created_at']).astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
    text = (f"🛒 Продажа #{s['id']}\n"
            f"📦 Товар: *{s['title']}* (ID: {s['product_id']})\n"
            f"📝 Описание: {s['description']}\n"
            f"💵 Цена: *{format_money(s['price'])}*\n"
            f"👤 Покупатель: @{s['buyer_username'] or 'анон'}\n"
            f"📅 Дата продажи: {created_at_msk}")
    markup = simple_markup([
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"my_sales|{s['seller_id']}|1")]
    ])
    await callback.message.answer(text, parse_mode="Markdown", reply_markup=markup)
    await callback.answer()

# ---------------- Settings ----------------
@dp.callback_query(lambda c: c.data == "menu_settings")
async def cb_settings(callback: CallbackQuery):
    if await maintenance_block(callback): return
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT notify_enabled FROM users WHERE user_id=?", (callback.from_user.id,)) as cur:
            r = await cur.fetchone()
            notify_status = "включены" if r['notify_enabled'] == 1 else "выключены"
    text = f"⚙️ Настройки\n📩 Уведомления: *{notify_status}*"
    markup = simple_markup([
        [InlineKeyboardButton(text="🔊 Вкл/выкл уведомления", callback_data="toggle_notifications")],
        [InlineKeyboardButton(text="✏️ Изменить описание профиля", callback_data="seller_edit_info")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_back_main")]
    ])
    await callback.message.answer(text, parse_mode="Markdown", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "toggle_notifications")
async def cb_toggle_notifications(callback: CallbackQuery):
    if await maintenance_block(callback): return
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT notify_enabled FROM users WHERE user_id=?", (callback.from_user.id,)) as cur:
            r = await cur.fetchone()
            new_status = 0 if r['notify_enabled'] == 1 else 1
        await conn.execute("UPDATE users SET notify_enabled=? WHERE user_id=?", (new_status, callback.from_user.id))
        await conn.commit()
    status_text = "включены" if new_status == 1 else "выключены"
    await callback.message.answer(f"📩 Уведомления {status_text}.", reply_markup=main_menu_markup(callback.from_user.id))
    await callback.answer()

# ---------------- Admin panel ----------------
@dp.callback_query(lambda c: c.data == "menu_admin")
async def cb_admin(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    markup = simple_markup([
        [InlineKeyboardButton(text="👥 Управление балансами", callback_data="admin_balances")],
        [InlineKeyboardButton(text="📁 Управление категориями", callback_data="admin_cats")],
        [InlineKeyboardButton(text="📂 Управление подкатегориями", callback_data="admin_subcats")],
        [InlineKeyboardButton(text="🗂 Товары (искать/удалять/править)", callback_data="admin_products")],
        [InlineKeyboardButton(text="⚖️ Споры", callback_data="admin_disputes")],
        [InlineKeyboardButton(text="🛠 Техработы: включить/выключить", callback_data="admin_toggle_maintenance")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_back_main")]
    ])
    await callback.message.answer("🔧 Админ панель", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_toggle_maintenance")
async def cb_admin_toggle_maintenance(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT value FROM settings WHERE key='maintenance'") as cur:
            val = (await cur.fetchone())['value']
        new = 'off' if val == 'on' else 'on'
        await conn.execute("UPDATE settings SET value=? WHERE key='maintenance'", (new,))
        await conn.commit()
    await callback.message.answer(f"🛠 Режим техработ установлен: {new}", reply_markup=main_menu_markup(callback.from_user.id))
    await callback.answer(f"Режим техработ: {new}")

@dp.callback_query(lambda c: c.data == "admin_balances")
async def cb_admin_balances(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    markup = simple_markup([
        [InlineKeyboardButton(text="🔍 Поиск по user_id", callback_data="admin_search_user")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_admin")]
    ])
    await callback.message.answer("👥 Управление балансами: выберите действие", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_search_user")
async def cb_admin_search_user(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    await state.set_state(AdminSearchUser.user_id)
    await callback.message.answer("Введите user_id пользователя для управления балансом:", reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(AdminSearchUser.user_id)
async def admin_process_user_search(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    try:
        target = int(message.text.strip())
    except Exception:
        await message.answer("Неверный user_id. Введите целое число.", reply_markup=cancel_markup("Отмена"))
        return
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT user_id, username, balance FROM users WHERE user_id=?", (target,)) as cur:
            user = await cur.fetchone()
    if not user:
        await message.answer("Пользователь не найден.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    await state.set_data({"target_user_id": target})
    markup = simple_markup([
        [InlineKeyboardButton(text="💰 Изменить баланс", callback_data=f"admin_change_balance|{target}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_balances")]
    ])
    await message.answer(
        f"👤 Пользователь: @{user['username'] or 'анон'}\n💰 Баланс: {format_money(user['balance'])}",
        reply_markup=markup,
        parse_mode="Markdown"
    )
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("admin_change_balance|"))
async def cb_admin_change_balance(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    target = int(callback.data.split("|", 1)[1])
    await state.set_state(AdminBalanceChange.amount)
    await state.set_data({"target_user_id": target})
    await callback.message.answer("Введите сумму для изменения баланса (положительная для добавления, отрицательная для вычета):", 
                                reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(AdminBalanceChange.amount)
async def admin_process_balance_change(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    try:
        amount = float(message.text.strip().replace(',', '.'))
    except Exception:
        await message.answer("Неверная сумма. Введите число (например, 100.50 или -50.25).", reply_markup=cancel_markup())
        return
    data = await state.get_data()
    target = data.get("target_user_id")
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT balance FROM users WHERE user_id=?", (target,)) as cur:
            user = await cur.fetchone()
        if not user:
            await message.answer("Пользователь не найден.", reply_markup=main_menu_markup(message.from_user.id))
            await state.clear()
            return
        new_balance = max(0.0, user['balance'] + amount)
        await conn.execute("UPDATE users SET balance=? WHERE user_id=?", (new_balance, target))
        await conn.commit()
    action = "добавлено" if amount > 0 else "вычтено"
    await message.answer(
        f"✅ Баланс изменён: {action} {format_money(abs(amount))}. Новый баланс: {format_money(new_balance)}",
        reply_markup=main_menu_markup(message.from_user.id)
    )
    if await is_notify_enabled(target):
        try:
            await bot.send_message(
                target, 
                f"💰 Ваш баланс изменён администратором: {action} {format_money(abs(amount))}. Новый баланс: {format_money(new_balance)}"
            )
        except Exception:
            pass
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_cats")
async def cb_admin_cats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    markup = await build_admin_categories_markup()
    await callback.message.answer("📁 Управление категориями:", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_subcats")
async def cb_admin_subcats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    markup = await build_categories_markup(admin_view=True)
    await callback.message.answer("📂 Выберите категорию для управления подкатегориями:", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("admin_view_cat|"))
async def cb_admin_view_cat(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    cat_id = int(callback.data.split("|", 1)[1])
    markup = await build_admin_subcategories_markup(cat_id)
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT name FROM categories WHERE id=?", (cat_id,)) as cur:
            cat = await cur.fetchone()
    if not cat:
        await callback.message.answer("Категория не найдена.")
        await callback.answer()
        return
    await callback.message.answer(f"📁 Подкатегории для '{cat['name']}':", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_create_category")
async def cb_admin_create_category(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    await state.set_state(AdminNewCategory.name)
    await callback.message.answer("📁 Введите название новой категории:", reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(AdminNewCategory.name)
async def admin_process_new_category(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    name = message.text.strip()
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute("INSERT INTO categories(name) VALUES (?)", (name,))
            await conn.commit()
        await message.answer("✅ Категория создана.", reply_markup=main_menu_markup(message.from_user.id))
    except sqlite3.IntegrityError:
        await message.answer("❌ Категория с таким именем уже существует.", reply_markup=cancel_markup())
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("admin_edit_cat|"))
async def cb_admin_edit_category(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    cat_id = int(callback.data.split("|", 1)[1])
    await state.set_state(AdminEditCategory.name)
    await state.set_data({"cat_id": cat_id})
    await callback.message.answer("📁 Введите новое название категории:", reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(AdminEditCategory.name)
async def admin_process_edit_category(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    name = message.text.strip()
    data = await state.get_data()
    cat_id = data.get("cat_id")
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute("UPDATE categories SET name=? WHERE id=?", (name, cat_id))
            await conn.commit()
        await message.answer("✅ Категория обновлена.", reply_markup=main_menu_markup(message.from_user.id))
    except sqlite3.IntegrityError:
        await message.answer("❌ Категория с таким именем уже существует.", reply_markup=cancel_markup())
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("admin_delete_cat|"))
async def cb_admin_delete_category(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    cat_id = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT COUNT(*) as cnt FROM products WHERE category_id=?", (cat_id,)) as cur:
            prod_count = (await cur.fetchone())['cnt']
        if prod_count > 0:
            await callback.message.answer("❌ Нельзя удалить категорию, в которой есть товары.")
            await callback.answer()
            return
        await conn.execute("DELETE FROM subcategories WHERE category_id=?", (cat_id,))
        await conn.execute("DELETE FROM categories WHERE id=?", (cat_id,))
        await conn.commit()
    await callback.message.answer("✅ Категория удалена.", reply_markup=await build_admin_categories_markup())
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("admin_create_sub|"))
async def cb_admin_create_subcategory(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    cat_id = int(callback.data.split("|", 1)[1])
    await state.set_state(AdminNewSub.name)
    await state.set_data({"cat_id": cat_id})
    await callback.message.answer("📂 Введите название новой подкатегории:", reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(AdminNewSub.name)
async def admin_process_new_subcategory(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    name = message.text.strip()
    data = await state.get_data()
    cat_id = data.get("cat_id")
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute("INSERT INTO subcategories(category_id, name) VALUES (?,?)", (cat_id, name))
        await conn.commit()
    await message.answer("✅ Подкатегория создана.", reply_markup=await build_admin_subcategories_markup(cat_id))
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("admin_edit_sub|"))
async def cb_admin_edit_subcategory(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    sub_id = int(callback.data.split("|", 1)[1])
    await state.set_state(AdminEditSub.name)
    await state.set_data({"sub_id": sub_id})
    await callback.message.answer("📂 Введите новое название подкатегории:", reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(AdminEditSub.name)
async def admin_process_edit_subcategory(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    name = message.text.strip()
    data = await state.get_data()
    sub_id = data.get("sub_id")
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT category_id FROM subcategories WHERE id=?", (sub_id,)) as cur:
            cat = await cur.fetchone()
        if not cat:
            await message.answer("Подкатегория не найдена.", reply_markup=main_menu_markup(message.from_user.id))
            await state.clear()
            return
        await conn.execute("UPDATE subcategories SET name=? WHERE id=?", (name, sub_id))
        await conn.commit()
    await message.answer("✅ Подкатегория обновлена.", reply_markup=await build_admin_subcategories_markup(cat['category_id']))
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("admin_delete_sub|"))
async def cb_admin_delete_subcategory(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    sub_id = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT category_id, (SELECT COUNT(*) FROM products WHERE subcategory_id=?) as cnt FROM subcategories WHERE id=?", 
                               (sub_id, sub_id)) as cur:
            sub = await cur.fetchone()
        if not sub:
            await callback.message.answer("Подкатегория не найдена.")
            await callback.answer()
            return
        if sub['cnt'] > 0:
            await callback.message.answer("❌ Нельзя удалить подкатегорию, в которой есть товары.")
            await callback.answer()
            return
        await conn.execute("DELETE FROM subcategories WHERE id=?", (sub_id,))
        await conn.commit()
    await callback.message.answer("✅ Подкатегория удалена.", reply_markup=await build_admin_subcategories_markup(sub['category_id']))
    await callback.answer()

@dp.callback_query(lambda c: c.data == "admin_products")
async def cb_admin_products(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    await state.set_state(AdminProdSearch.prod_id)
    await callback.message.answer("🛒 Введите ID товара для поиска:", reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(AdminProdSearch.prod_id)
async def admin_process_product_search(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    try:
        pid = int(message.text.strip())
    except Exception:
        await message.answer("Неверный ID товара. Введите целое число.", reply_markup=cancel_markup())
        return
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT p.*, s.username as seller_username FROM products p LEFT JOIN sellers s ON p.seller_id = s.id WHERE p.id=?", (pid,)) as cur:
            p = await cur.fetchone()
    if not p:
        await message.answer("Товар не найден.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    msk_tz = pytz.timezone('Europe/Moscow')
    created_at_msk = datetime.fromisoformat(p['created_at']).astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
    text = (
        f"🛒 *Товар* #{p['id']}: *{p['title']}*\n"
        f"📝 Описание: {p['description']}\n"
        f"💵 Цена: *{format_money(p['price'])}*\n"
        f"📦 Количество: {p['quantity']}\n"
        f"👤 Продавец: @{p['seller_username'] or 'анон'}\n"
        f"📅 Создан: {created_at_msk}"
    )
    markup = simple_markup([
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"admin_edit_product|{pid}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"admin_delete_product|{pid}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_products")]
    ])
    if p['photo_file_id']:
        await bot.send_photo(message.chat.id, p['photo_file_id'], caption=text, parse_mode="Markdown", reply_markup=markup)
    else:
        await message.answer(text, parse_mode="Markdown", reply_markup=markup)
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("admin_delete_product|"))
async def cb_admin_delete_product(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    pid = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT title, seller_id FROM products WHERE id=?", (pid,)) as cur:
            p = await cur.fetchone()
        if not p:
            await callback.message.answer("Товар не найден.")
            await callback.answer()
            return
        await conn.execute("DELETE FROM products WHERE id=?", (pid,))
        await conn.commit()
        async with conn.execute("SELECT user_id FROM sellers WHERE id=?", (p['seller_id'],)) as cur:
            seller = await cur.fetchone()
        if seller and await is_notify_enabled(seller['user_id']):
            try:
                await bot.send_message(seller['user_id'], f"🗑 Ваш товар '{p['title']}' удалён администратором.")
            except Exception:
                pass
    await callback.message.answer("✅ Товар удалён.", reply_markup=main_menu_markup(callback.from_user.id))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("admin_edit_product|"))
async def cb_admin_edit_product(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    pid = int(callback.data.split("|", 1)[1])
    await state.set_state(AdminEditProduct.name)
    await state.set_data({"pid": pid})
    await callback.message.answer("✏️ Введите новое название товара:", reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(AdminEditProduct.name)
async def admin_process_product_name(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    name = message.text.strip()
    await state.set_state(AdminEditProduct.desc)
    await state.update_data(name=name)
    await message.answer("📝 Введите новое описание товара:", reply_markup=cancel_markup("Отмена"))

@dp.message(AdminEditProduct.desc)
async def admin_process_product_desc(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    data = await state.get_data()
    pid = data.get("pid")
    name = data.get("name")
    desc = message.text.strip()
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute("UPDATE products SET title=?, description=? WHERE id=?", (name, desc, pid))
        await conn.commit()
    await message.answer("✅ Товар обновлён.", reply_markup=main_menu_markup(message.from_user.id))
    await state.clear()

@dp.callback_query(lambda c: c.data == "admin_disputes")
async def cb_admin_disputes(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT id, order_id, user_id, status, created_at FROM disputes WHERE status='open' ORDER BY created_at DESC") as cur:
            disputes = await cur.fetchall()
    if not disputes:
        await callback.message.answer("Открытых споров нет.", reply_markup=main_menu_markup(callback.from_user.id))
        await callback.answer()
        return
    buttons = []
    msk_tz = pytz.timezone('Europe/Moscow')
    for d in disputes:
        created_at_msk = datetime.fromisoformat(d['created_at']).astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
        buttons.append([InlineKeyboardButton(
            text=f"Спор #{d['id']} (Заказ #{d['order_id']}, {created_at_msk})", 
            callback_data=f"view_dispute|{d['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_admin")])
    markup = simple_markup(buttons)
    await callback.message.answer("⚖️ Открытые споры:", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("view_dispute|"))
async def cb_view_dispute(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    dispute_id = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            """SELECT d.*, u.username as user_username, p.title, p.seller_id, s.user_id as seller_user_id, s.username as seller_username
               FROM disputes d 
               JOIN orders o ON d.order_id = o.id
               JOIN users u ON d.user_id = u.user_id
               JOIN products p ON o.product_id = p.id
               JOIN sellers s ON p.seller_id = s.id
               WHERE d.id=?""", (dispute_id,)
        ) as cur:
            d = await cur.fetchone()
    if not d:
        await callback.message.answer("Спор не найден.")
        await callback.answer()
        return
    msk_tz = pytz.timezone('Europe/Moscow')
    created_at_msk = datetime.fromisoformat(d['created_at']).astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
    text = (
        f"⚖️ Спор #{d['id']}\n"
        f"📦 Заказ #{d['order_id']}\n"
        f"🛒 Товар: {d['title']}\n"
        f"👤 Покупатель: @{d['user_username'] or 'анон'}\n"
        f"👤 Продавец: @{d['seller_username'] or 'анон'}\n"
        f"📝 Проблема: {d['description']}\n"
        f"📅 Создан: {created_at_msk}\n"
        f"📌 Статус: {d['status']}"
    )
    markup = simple_markup([
        [InlineKeyboardButton(text="✅ Закрыть спор", callback_data=f"close_dispute|{d['id']}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_disputes")]
    ])
    await callback.message.answer(text, parse_mode="Markdown", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("close_dispute|"))
async def cb_close_dispute(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.message.answer("Доступ запрещён.")
        await callback.answer()
        return
    dispute_id = int(callback.data.split("|", 1)[1])
    await state.set_state(AdminCloseDispute.reason)
    await state.set_data({"dispute_id": dispute_id})
    await callback.message.answer("📝 Укажите причину закрытия спора:", reply_markup=cancel_markup("Отмена"))
    await callback.answer()

@dp.message(AdminCloseDispute.reason)
async def admin_process_close_dispute(message: Message, state: FSMContext):
    if message.text.strip().lower() in ("отмена", "cancel", "❌"):
        await message.answer("Отмена.", reply_markup=main_menu_markup(message.from_user.id))
        await state.clear()
        return
    reason = message.text.strip()
    data = await state.get_data()
    dispute_id = data.get("dispute_id")
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT user_id, order_id FROM disputes WHERE id=?", (dispute_id,)) as cur:
            d = await cur.fetchone()
        if not d:
            await message.answer("Спор не найден.", reply_markup=main_menu_markup(message.from_user.id))
            await state.clear()
            return
        await conn.execute("UPDATE disputes SET status='closed', close_reason=? WHERE id=?", (reason, dispute_id))
        await conn.commit()
        if await is_notify_enabled(d['user_id']):
            try:
                await bot.send_message(
                    d['user_id'], 
                    f"⚖️ Спор #{dispute_id} по заказу #{d['order_id']} закрыт.\nПричина: {reason}"
                )
            except Exception:
                pass
    await message.answer("✅ Спор закрыт.", reply_markup=main_menu_markup(message.from_user.id))
    await state.clear()

# ---------------- Support ----------------
@dp.callback_query(lambda c: c.data == "menu_support")
async def cb_support(callback: CallbackQuery):
    if await maintenance_block(callback): return
    markup = simple_markup([
        [InlineKeyboardButton(text="📲 Связаться с админом", url=f"https://t.me/mexanickq")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_back_main")]
    ])
    await callback.message.answer(
        f"👨‍💻 Поддержка {MARKET_NAME}\n\nСвяжитесь с администратором:",  
        reply_markup=markup
    )
    await callback.answer()

# ---------------- Orders ----------------
@dp.callback_query(lambda c: c.data == "menu_my_orders")
async def cb_my_orders(callback: CallbackQuery):
    if await maintenance_block(callback): return
    user_id = callback.from_user.id
    page = 1
    per_page = 10
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT COUNT(*) as cnt FROM orders WHERE user_id=?", (user_id,)) as cur:
            total = (await cur.fetchone())['cnt']
        async with conn.execute(
            """SELECT o.id, o.created_at, o.price, p.title, s.username as seller_username
               FROM orders o 
               JOIN products p ON o.product_id = p.id
               JOIN sellers s ON o.seller_id = s.id
               WHERE o.user_id=? 
               ORDER BY o.created_at DESC LIMIT ? OFFSET ?""", 
            (user_id, per_page, (page-1)*per_page)
        ) as cur:
            orders = await cur.fetchall()
    if not orders:
        await callback.message.answer("У вас пока нет покупок.", reply_markup=main_menu_markup(callback.from_user.id))
        await callback.answer()
        return
    buttons = []
    msk_tz = pytz.timezone('Europe/Moscow')
    for o in orders:
        created_at_msk = datetime.fromisoformat(o['created_at']).astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
        buttons.append([InlineKeyboardButton(
            text=f"🛒 {o['title']} ({format_money(o['price'])})", 
            callback_data=f"view_order|{o['id']}"
        )])
    total_pages = (total + per_page - 1) // per_page
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"my_orders|{user_id}|{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"my_orders|{user_id}|{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_back_main")])
    markup = simple_markup(buttons)
    await callback.message.answer(f"📋 Мои покупки (страница {page}/{total_pages}):", reply_markup=markup)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("view_order|"))
async def cb_view_order(callback: CallbackQuery):
    if await maintenance_block(callback): return
    order_id = int(callback.data.split("|", 1)[1])
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            """SELECT o.*, p.title, p.description, s.username as seller_username
               FROM orders o 
               JOIN products p ON o.product_id = p.id
               JOIN sellers s ON o.seller_id = s.id
               WHERE o.id=? AND o.user_id=?""", 
            (order_id, callback.from_user.id)
        ) as cur:
            o = await cur.fetchone()
    if not o:
        await callback.message.answer("Заказ не найден или не ваш.")
        await callback.answer()
        return
    msk_tz = pytz.timezone('Europe/Moscow')
    created_at_msk = datetime.fromisoformat(o['created_at']).astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
    text = (
        f"📋 Заказ #{o['id']}\n"
        f"🛒 Товар: *{o['title']}*\n"
        f"📝 Описание: {o['description']}\n"
        f"💵 Цена: *{format_money(o['price'])}*\n"
        f"👤 Продавец: @{o['seller_username'] or 'анон'}\n"
        f"📅 Дата покупки: {created_at_msk}"
    )
    markup = simple_markup([
        [InlineKeyboardButton(text="📝 Оставить отзыв", callback_data=f"review|{o['product_id']}")],
        [InlineKeyboardButton(text="⚖️ Открыть спор", callback_data=f"dispute|{o['id']}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_my_orders")]
    ])
    await callback.message.answer(text, parse_mode="Markdown", reply_markup=markup)
    await callback.answer()

# ---------------- Navigation ----------------
@dp.callback_query(lambda c: c.data == "menu_back_main")
async def cb_back_main(callback: CallbackQuery):
    if await maintenance_block(callback): return
    await callback.message.answer("🏠 Главное меню", reply_markup=main_menu_markup(callback.from_user.id))
    await callback.answer()

@dp.callback_query(lambda c: c.data == "action_cancel")
async def cb_action_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Действие отменено.", reply_markup=main_menu_markup(callback.from_user.id))
    await callback.answer()

# ---------------- Main ----------------
async def on_startup():
    asyncio.create_task(background_payment_checker())
    logging.info("Фоновая проверка платежей запущена.")

async def main():
    await init_db()
    dp.startup.register(on_startup)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":

    asyncio.run(main())
