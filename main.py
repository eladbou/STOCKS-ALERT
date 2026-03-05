# -*- coding: utf-8 -*-
import os
import asyncio
import logging
from fastapi import FastAPI
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import yfinance as yf
import asyncpg
from dotenv import load_dotenv
import re
import requests

# Load environment variables from .env file if present
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
# Try to get TELEGRAM_BOT_TOKEN first, fallback to TELEGRAM_TOKEN
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", os.getenv("TELEGRAM_TOKEN", ""))

# Database Environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stock_alerts_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Initialize Bot and Dispatcher
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# Global database pool
db_pool = None

# ==========================================
# Telegram Bot Handlers
# ==========================================
@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    global db_pool
    if not db_pool:
        await message.answer("שגיאה בהתחברות למסד הנתונים כעת.")
        return
        
    chat_id = str(message.chat.id)
    token = command.args
    
    async with db_pool.acquire() as conn:
        # First check if user is already linked
        existing_user = await conn.fetchrow("SELECT username FROM users WHERE telegram_chat_id = $1", chat_id)
        if existing_user:
            await message.answer(f"היי {existing_user['username']}! החשבון שלך כבר מקושר בהצלחה. תוכל לכתוב /help למידע נוסף.")
            return
            
        # If not linked, a token is required
        if not token:
            await message.answer("ברוך הבא! כדי לקשר את הטלגרם שלך לאתר, אנא היכנס לאתר שלנו ולחץ על כפתור הקישור בנייד / במחשב.")
            return

        # Attempt to link with the provided token
        user = await conn.fetchrow(
            "SELECT id, username FROM users WHERE telegram_link_token = $1 AND link_token_expires > NOW()", 
            token
        )
        if user:
            # Update user with telegram chat info
            await conn.execute(
                "UPDATE users SET telegram_chat_id = $1, telegram_link_token = NULL, link_token_expires = NULL WHERE id = $2",
                chat_id, user['id']
            )
            await message.answer(f"היי {user['username']}! החשבון שלך קושר בהצלחה. מעכשיו תקבל התראות לכאן. תוכל לכתוב /help כדי לראות מה אפשר לעשות.")
        else:
            await message.answer("הקישור שלך לא חוקי או שפג תוקפו (עבר יותר מ-15 דקות). אנא ייצר קישור חדש באתר.")

@dp.message(Command("status"))
async def cmd_status(message: Message):
    global db_pool
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id, username FROM users WHERE telegram_chat_id = $1", str(message.chat.id))
        if not user:
            await message.answer("החשבון שלך אינו מקושר. אנא היכנס לאתר כדי לקשר אותו.")
            return
            
        alerts_count = await conn.fetchval(
            "SELECT COUNT(*) FROM price_alerts WHERE user_id = $1 AND is_active = true",
            user['id']
        )
        await message.answer(f"📊 <b>סטטוס חשבון</b>\n\n👤 משתמש מחובר: <b>{user['username']}</b>\n🔔 התראות פעילות: <b>{alerts_count}</b>")

@dp.message(Command("help"))
@dp.message(F.text.lower() == "help")
async def cmd_help(message: Message):
    help_text = (
        "🤖 <b>מדריך פקודות לבוט התראות המניות:</b>\n\n"
        "🔗 <b>/status</b> - בדיקת מצב החשבון וכמות התראות פעילות.\n\n"
        "🔔 <b>ניהול התראות:</b>\n"
        "• <code>add [SYMBOL] [PRICE]</code> או פשוט <code>AAPL 150</code> - הוספת התראה חדשה.\n"
        "• <code>list</code> - הצגת כל ההתראות הפעילות שלך.\n"
        "• <code>del [ID]</code> / <code>delete [ID]</code> / <code>remove [ID]</code> - מחיקת התראה לפי מספר ID (אותו מקבלים מפקודת list).\n\n"
        "🔍 <b>בדיקות וחיפושים:</b>\n"
        "• <code>price [SYMBOL]</code> - בדיקת המחיר הנוכחי של מניה ללא שמירת התראה.\n"
        "• <code>search [QUERY]</code> - חיפוש מניה לפי שם או ביטוי."
    )
    await message.answer(help_text)

# We need a function to get current price
async def get_current_price(symbol: str):
    def fetch_price():
        stock = yf.Ticker(symbol)
        try:
            info = stock.fast_info
            if 'last_price' in info and info['last_price'] is not None:
                return float(info['last_price'])
        except Exception:
            pass
        history = stock.history(period="1d")
        if not history.empty:
            return float(history['Close'].iloc[-1])
        return None
    return await asyncio.to_thread(fetch_price)

@dp.message(lambda msg: msg.text and msg.text.lower() == "list")
async def cmd_list(message: Message):
    global db_pool
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id FROM users WHERE telegram_chat_id = $1", str(message.chat.id))
        if not user:
            await message.answer("החשבון שלך אינו מקושר. לא ניתן לראות רשימת התראות.")
            return
            
        alerts = await conn.fetch(
            "SELECT id, symbol, target_price, direction, TO_CHAR(created_at, 'DD/MM/YYYY HH24:MI') as date FROM price_alerts WHERE user_id = $1 AND is_active = true ORDER BY created_at DESC",
            user['id']
        )
        
        if not alerts:
            await message.answer("אין לך התראות פעילות כרגע.")
            return
            
        txt = "📋 <b>ההתראות הפעילות שלך:</b>\n\n"
        for i, a in enumerate(alerts, 1):
            cond_str = "מעל" if a['direction'].lower() == "above" else "מתחת ל-"
            txt += f"{i}. 📈 <b>{a['symbol']}</b> - 🎯 {cond_str} <b>${float(a['target_price']):.2f}</b>\n   🆔 ID: <code>{a['id']}</code>\n\n"
        
        await message.answer(txt)

@dp.message(lambda msg: msg.text and re.match(r'^(del|delete|remove)\s+(\d+)$', msg.text, re.IGNORECASE))
async def cmd_del(message: Message):
    match = re.match(r'^(del|delete|remove)\s+(\d+)$', message.text, re.IGNORECASE)
    alert_id = int(match.group(2))
    
    global db_pool
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id FROM users WHERE telegram_chat_id = $1", str(message.chat.id))
        if not user:
            return
            
        res = await conn.execute(
            "UPDATE price_alerts SET is_active = false WHERE id = $1 AND user_id = $2 AND is_active = true",
            alert_id, user['id']
        )
        if res == "UPDATE 1":
            await message.answer(f"✅ ההתראה <code>{alert_id}</code> בוטלה בהצלחה.")
        else:
            await message.answer(f"❌ לא נמצאה התראה פעילה עם מספר <code>{alert_id}</code> או שהיא אינה שייכת לך.")

@dp.message(lambda msg: msg.text and re.match(r'^price\s+([A-Za-z0-9^.-]+)$', msg.text, re.IGNORECASE))
async def cmd_price(message: Message):
    match = re.match(r'^price\s+([A-Za-z0-9^.-]+)$', message.text, re.IGNORECASE)
    symbol = match.group(1).upper()
    
    wait_msg = await message.answer("⏳ בודק מחיר...")
    
    price = await get_current_price(symbol)
    if price is None:
        await wait_msg.edit_text(f"❌ לא הצלחתי למצוא נתונים עבור הסימול <b>{symbol}</b>. נא לבדוק שהסימול חוקי.")
        return
        
    await wait_msg.edit_text(f"💰 המחיר הנוכחי של <b>{symbol}</b> הוא <b>${price:.2f}</b>.")

@dp.message(lambda msg: msg.text and re.match(r'^search\s+(.+)$', msg.text, re.IGNORECASE))
async def cmd_search(message: Message):
    match = re.match(r'^search\s+(.+)$', message.text, re.IGNORECASE)
    query = match.group(1)
    
    wait_msg = await message.answer("🔍 מחפש...")
    
    def search_yf():
        headers = {'User-Agent': 'Mozilla/5.0'}
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={query}"
        try:
            r = requests.get(url, headers=headers, timeout=5)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None
            
    data = await asyncio.to_thread(search_yf)
    if not data or 'quotes' not in data or not data['quotes']:
        await wait_msg.edit_text("❌ לא מצאתי תוצאות לחיפוש שלך.")
        return
        
    quotes = data['quotes']
    results = []
    for q in data['quotes'][:5]:
        if 'symbol' in q and 'shortname' in q:
             results.append(f"• <b>{q['symbol']}</b> - {q['shortname']}")
             
    if not results:
        await wait_msg.edit_text("❌ לא מצאתי חברות שמתאימות לחיפוש שלך.")
        return

    first_symbol = [q['symbol'] for q in data['quotes'] if 'symbol' in q][0]
    txt = "🔎 <b>תוצאות חיפוש:</b>\n\n" + "\n".join(results) + f"\n\nכדי להוסיף התראה, כתוב את הסימול והמחיר. (למשל: <code>{first_symbol} 150</code>)"
    await wait_msg.edit_text(txt)

@dp.message(lambda msg: msg.text and re.match(r'^(?:add\s+)?([A-Za-z0-9^.-]+)\s+([\d.]+)$', msg.text, re.IGNORECASE))
async def cmd_add(message: Message):
    match = re.match(r'^(?:add\s+)?([A-Za-z0-9^.-]+)\s+([\d.]+)$', message.text, re.IGNORECASE)
    symbol = match.group(1).upper()
    try:
        target_price = float(match.group(2))
    except ValueError:
        await message.answer("אנא הזן מחיר תקין.")
        return
    
    global db_pool
    if not db_pool:
        return
        
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id FROM users WHERE telegram_chat_id = $1", str(message.chat.id))
        if not user:
            await message.answer("החשבון שלך אינו מקושר. לא ניתן להוסיף התראות. נא קשר אותו דרך האתר לחוויה מלאה.")
            return
            
        wait_msg = await message.answer("⏳ בודק מחיר עדכני למניה כדי לקבוע כיוון התראה...")
        
        current_price = await get_current_price(symbol)
        if current_price is None:
            await wait_msg.edit_text(f"❌ לא הצלחתי למצוא נתונים עבור הסימול <b>{symbol}</b>. התראה לא נשמרה.\nנסה לחפש את המניה קודם באמצעות: <code>search {symbol}</code>")
            return
            
        direction = "above" if target_price > current_price else "below"
        
        # INSERT to db
        await conn.execute(
            """INSERT INTO price_alerts 
               (symbol, target_price, direction, is_active, created_at, user_id) 
               VALUES ($1, $2, $3, true, NOW(), $4)""",
            symbol, target_price, direction, user['id']
        )
        
        cond_str = "מעל" if direction == "above" else "מתחת ל-"
        await wait_msg.edit_text(
            f"✅ <b>התראה נוספה בהצלחה!</b>\n\n"
            f"📈 מניה: <b>{symbol}</b>\n"
            f"💰 מחיר נוכחי בעת הוספה: <b>${current_price:.2f}</b>\n"
            f"🎯 יעד התראה: <b>{cond_str} ${target_price:.2f}</b>"
        )

@dp.message()
async def auto_catch_all(message: Message):
    await message.answer("לא זיהיתי את הפקודה...\nשלח <code>help</code> כדי לראות את הפקודות הזמינות.")

# ==========================================
# Scheduler Job
# ==========================================
async def check_stocks_and_notify():
    global db_pool
    if db_pool is None:
        logger.error("Database pool is not initialized.")
        return

    logger.info("Scheduler tick: Checking active stocks from DB...")
    
    try:
        async with db_pool.acquire() as conn:
            # Query active alerts joined with users table to get telegram_chat_id
            query = """
                SELECT 
                    p.id, p.symbol, p.target_price, p.direction, p.user_id,
                    u.telegram_chat_id 
                FROM price_alerts p
                JOIN users u ON p.user_id = u.id
                WHERE p.is_active = true 
                  AND u.telegram_chat_id IS NOT NULL 
                  AND u.telegram_chat_id != ''
            """
            alerts = await conn.fetch(query)
            
            if not alerts:
                logger.info("No active alerts to check.")
                return
                
            for alert in alerts:
                alert_id = alert['id']
                symbol = alert['symbol']
                threshold = float(alert['target_price'])
                condition = alert['direction']
                chat_id = alert['telegram_chat_id']
                
                try:
                    # Fetch latest data from yfinance efficiently
                    stock = yf.Ticker(symbol)
                    
                    # Try using fast_info first (faster), fallback to history if needed
                    current_price = None
                    stock_info = stock.fast_info
                    if 'last_price' in stock_info and stock_info['last_price'] is not None:
                        current_price = stock_info['last_price']
                    else:
                        history = stock.history(period="1d")
                        if not history.empty:
                            current_price = float(history['Close'].iloc[-1])
                            
                    if current_price is None:
                        logger.warning(f"Could not retrieve price data for {symbol}")
                        continue
                        
                    logger.info(f"{symbol} current price: {current_price:.2f} | DB Threshold: {threshold} ({condition})")
                    
                    # Check conditions
                    triggered = False
                    if condition.lower() == "above" and current_price >= threshold:
                        triggered = True
                    elif condition.lower() == "below" and current_price <= threshold:
                        triggered = True
                        
                    if triggered:
                        # Send message using Telegram Bot
                        msg = (
                            f"🚨 <b>קפצה התראה!</b> 🚨\n\n"
                            f"📈 <b>{symbol}</b> הגיעה למחיר היעד.\n"
                            f"💰 מחיר נוכחי: <b>${current_price:.2f}</b>\n"
                            f"🎯 מחיר ההתרעה: <b>${threshold:.2f}</b> ({condition})"
                        )
                        
                        try:
                            await bot.send_message(chat_id=chat_id, text=msg)
                            logger.info(f"Alert sent to chat_id={chat_id} for {symbol}!")
                            
                            # Mark the alert as inactive and update alert_date so it doesn't spam
                            await conn.execute(
                                "UPDATE price_alerts SET is_active = false, alert_date = NOW() WHERE id = $1",
                                alert_id
                            )
                        except (TelegramBadRequest, TelegramForbiddenError) as e:
                            logger.error(f"Telegram error sending to {chat_id}: {e}")
                            
                except Exception as e:
                    logger.error(f"Error checking {symbol}: {e}")
                    
    except Exception as e:
        logger.error(f"Database error during check_stocks: {e}")

# ==========================================
# FastAPI Application
# ==========================================
app = FastAPI(title="Stock Alert & Telegram Bot")

@app.on_event("startup")
async def on_startup():
    global db_pool
    logger.info("Starting up FastAPI application...")
    
    # Initialize DB connection pool
    try:
        db_pool = await asyncpg.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            min_size=1,
            max_size=10,
            ssl="require",
            statement_cache_size=0
        )
        logger.info("Database connection pool created successfully.")
    except Exception as e:
        logger.error(f"Failed to create database pool: {e}")
    
    if not TELEGRAM_TOKEN:
        logger.warning("TELEGRAM_TOKEN is missing! Bot will not be able to send messages.")
    
    # 1. Start the Scheduler (runs every 30 seconds)
    scheduler.add_job(check_stocks_and_notify, 'interval', seconds=30)
    scheduler.start()
    logger.info("APScheduler started.")

    # 2. Start Bot Polling in the background so it doesn't block FastAPI Event Loop
    asyncio.create_task(dp.start_polling(bot))
    logger.info("Telegram Bot polling started... Waiting for messages.")

@app.on_event("shutdown")
async def on_shutdown():
    global db_pool
    logger.info("Shutting down gracefully...")
    scheduler.shutdown()
    await bot.session.close()
    if db_pool:
        await db_pool.close()
        logger.info("Database pool closed.")

# ==========================================
# Endpoints
# ==========================================
@app.get("/health")
async def health_check():
    """
    Simple health check endpoint avoiding the service from sleeping (e.g., in Render).
    """
    db_status = "connected" if db_pool else "disconnected"
    return {"status": "ok", "message": "Bot & Scheduler are running!", "db_status": db_status}
