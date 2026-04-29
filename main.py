# -*- coding: utf-8 -*-
import os
import asyncio
import logging
from fastapi import FastAPI
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, BufferedInputFile
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
from datetime import datetime
import pytz
import time
import aiohttp
import io

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
    global db_pool
    greeting = "🤖 <b>ברוכים הבאים לבוט התראות הבורסה!</b> 🚀"
    
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                user = await conn.fetchrow("SELECT id FROM users WHERE telegram_chat_id = $1", str(message.chat.id))
                if user:
                    user_id = user['id']
                    if user_id == 1:
                        greeting = "👑 <b>שלום אלעד מלכי</b>"
                    elif user_id == 2:
                        greeting = "🤡 <b>שלום ארביב הליצן</b>"
                    elif user_id == 3:
                        greeting = "🌈 <b>שלום הלל ההומו</b>"
        except Exception:
            pass # Fallback to default greeting if DB fails

    help_text = (
        f"{greeting}\n\n"
        "הנה כל מה שאתה יכול לעשות עם הבוט:\n\n"
        "📌 <b>חשבון וכללי</b>\n"
        "• <code>status</code> - בדיקת חיבור ומצב התראות\n"
        "• <code>help</code> - הצגת מדריך הפקודות הזה\n\n"
        "🔔 <b>ניהול התראות מחיר</b>\n"
        "• <code>[SYMBOL] [PRICE]</code> - הוספת התראה למחיר ספציפי\n"
        "  <i>לדוגמה: <code>AAPL 230</code></i>\n"
        "• <code>list</code> - הצגת כל ההתראות הפעילות שלך\n"
        "• <code>list [SYMBOL]</code> - הצגת התראות למניה ספציפית\n"
        "• <code>del [ID]</code> - מחיקת התראה (השתמש ב-ID מפקודת list)\n\n"
        "📊 <b>חדש! התראות ממוצע נע (SMA) בשביל ארביבון</b>\n"
        "• <code>add [SYMBOL] SMA [PERIOD]</code> - התראה כשהמחיר נוגע בממוצע\n"
        "  <i>לדוגמה: <code>NVDA SMA 50</code></i>\n"
        "  הבוט יתריע כשהמחיר יתקרב לטווח של 0.5% מהממוצע.\n\n"
        "🔍 <b>מידע וחיפוש</b>\n"
        "• <code>price [SYMBOL]</code> - קבלת מחיר נוכחי ומהיר\n"
        "• <code>search [NAME]</code> - חיפוש סימול לפי שם חברה (בעברית/אנגלית)\n\n"
        "🪙 <b>חדש! תמיכה בקריפטו (24/7)</b>\n"
        "• ניתן להוסיף התראות למטבעות קריפטו בפורמט <code>SYMBOL-USD</code>\n"
        "  <i>לדוגמה: <code>BTC-USD 95000</code> או <code>ETH-USD 2500</code></i>\n"
        "  התראות קריפטו פעילות 24/7 ללא תלות בשעות הבורסה!\n\n"
        "💡 <b>טיפ:</b> ניתן להוסיף התראה רגילה פשוט על ידי כתיבת שם המניה והמחיר, ללא המילה add."
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

async def send_stock_chart(chat_id: int, ticker: str, sma_period: int = None):
    """
    Fetches a stock chart from TradingView/Finviz into memory and sends it to Telegram.
    This bypasses Telegram's "failed to get content" errors.
    """
    global bot
    try:
        # Using Finviz for maximum reliability as TradingView API was failing DNS lookup.
        # ta=1 shows default SMAs (50, 200) - for 20,44,150 specialized logic 
        # usually requires paid APIs, sticking to reliable Finviz for now.
        chart_url = f"https://finviz.com/chart.ashx?t={ticker}&ty=c&ta=1&p=d&s=l"
            
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(chart_url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    image_data = await response.read()
                    
                    caption = f"📊 <b>הנה הגרף המעודכן עבור {ticker}</b>"
                    if sma_period:
                        caption += f" (מציג ממוצע נע {sma_period})"
                    
                    photo_file = BufferedInputFile(image_data, filename=f"{ticker}_chart.png")
                    await bot.send_photo(
                        chat_id=chat_id,
                        photo=photo_file,
                        caption=caption
                    )
                    logger.info(f"Chart sent successfully as buffer for {ticker}")
                else:
                    logger.error(f"Failed to fetch chart: HTTP {response.status}")
                    # Fallback to text if image fails
                    await bot.send_message(chat_id, f"⚠️ לא הצלחתי למשוך גרף עבור {ticker}, אבל ההתראה בתוקף.")
                    
    except Exception as e:
        logger.error(f"Error fetching/sending chart for {ticker}: {e}")

@dp.message(lambda msg: msg.text and re.match(r'^list(?:\s+([A-Za-z0-9^.-]+))?$', msg.text, re.IGNORECASE))
async def cmd_list(message: Message):
    match = re.match(r'^list(?:\s+([A-Za-z0-9^.-]+))?$', message.text, re.IGNORECASE)
    symbol_filter = match.group(1).upper() if match.group(1) else None

    global db_pool
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id FROM users WHERE telegram_chat_id = $1", str(message.chat.id))
        if not user:
            await message.answer("החשבון שלך אינו מקושר. לא ניתן לראות רשימת התראות.")
            return
            
        if symbol_filter:
            alerts = await conn.fetch(
                "SELECT id, symbol, target_price, direction, TO_CHAR(created_at, 'DD/MM/YYYY HH24:MI') as date FROM price_alerts WHERE user_id = $1 AND is_active = true AND symbol = $2 ORDER BY created_at DESC",
                user['id'], symbol_filter
            )
        else:
            alerts = await conn.fetch(
                "SELECT id, symbol, target_price, direction, TO_CHAR(created_at, 'DD/MM/YYYY HH24:MI') as date FROM price_alerts WHERE user_id = $1 AND is_active = true ORDER BY created_at DESC",
                user['id']
            )
        
        if not alerts:
            if symbol_filter:
                await message.answer(f"אין לך התראות פעילות עבור <b>{symbol_filter}</b> כרגע.")
            else:
                await message.answer("אין לך התראות פעילות כרגע.")
            return
            
        if symbol_filter:
            header = f"📋 <b>ההתראות הפעילות שלך עבור {symbol_filter}:</b>\n\n"
        else:
            header = "📋 <b>ההתראות הפעילות שלך:</b>\n\n"
        current_msg = header
        
        for i, a in enumerate(alerts, 1):
            if a['direction'] == "SMA_TOUCH":
                period = int(float(a['target_price']))
                alert_text = f"{i}. 📈 <b>{a['symbol']}</b> - 🎯 בקרבת <b>SMA {period}</b>\n   🆔 ID: <code>{a['id']}</code>\n\n"
            elif a['direction'].startswith("SMA_"):
                period = int(float(a['target_price']))
                dir_type = "מעל" if "ABOVE" in a['direction'] else "מתחת ל-"
                alert_text = f"{i}. 📈 <b>{a['symbol']}</b> - 🎯 {dir_type} <b>SMA {period}</b>\n   🆔 ID: <code>{a['id']}</code>\n\n"
            else:
                cond_str = "מעל" if a['direction'].lower() == "above" else "מתחת ל-"
                alert_text = f"{i}. 📈 <b>{a['symbol']}</b> - 🎯 {cond_str} <b>${float(a['target_price']):.2f}</b>\n   🆔 ID: <code>{a['id']}</code>\n\n"
            
            # Telegram has a limit of 4096 characters. We use 4000 to be safe.
            if len(current_msg) + len(alert_text) > 4000:
                await message.answer(current_msg)
                current_msg = alert_text # Start new message with this alert
            else:
                current_msg += alert_text
        
        # Send the last (or only) message
        if current_msg:
            await message.answer(current_msg)

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
    # Send chart with default SMAs 20, 44, 150
    await send_stock_chart(message.chat.id, symbol)

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

@dp.message(lambda msg: msg.text and re.match(r'^\s*(?:add\s+)?([A-Za-z0-9^.-]+)\s+SMA\s+(\d+)\s*$', msg.text, re.IGNORECASE))
async def cmd_add_sma(message: Message):
    logger.info(f"Received SMA alert request: {message.text}")
    match = re.match(r'^\s*(?:add\s+)?([A-Za-z0-9^.-]+)\s+SMA\s+(\d+)\s*$', message.text, re.IGNORECASE)
    symbol = match.group(1).upper()
    period = int(match.group(2))
    
    global db_pool
    if not db_pool: 
        logger.error("DB Pool is None in cmd_add_sma")
        return
    
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id FROM users WHERE telegram_chat_id = $1", str(message.chat.id))
        if not user:
            await message.answer("החשבון שלך אינו מקושר. נא קשר אותו דרך האתר.")
            return

        direction = "SMA_TOUCH"
        
        # We store period in target_price
        await conn.execute(
            """INSERT INTO price_alerts 
               (symbol, target_price, direction, is_active, created_at, user_id) 
               VALUES ($1, $2, $3, true, NOW(), $4)""",
            symbol, float(period), direction, user['id']
        )
        
        await message.answer(f"✅ <b>התראה נוספה!</b>\n\n📈 מניה: <b>{symbol}</b>\n🎯 יעד: <b>בקרבת SMA {period}</b>")

@dp.message(lambda msg: msg.text and re.match(r'^\s*(?:add\s+)?([A-Za-z0-9^.-]+)\s+([\d.\s,]+)$', msg.text, re.IGNORECASE))
async def cmd_add(message: Message):
    logger.info(f"Received regular alert request: {message.text}")
    if re.search(r'\bSMA\b', message.text, re.IGNORECASE):
        return
        
    match = re.match(r'^\s*(?:add\s+)?([A-Za-z0-9^.-]+)\s+([\d.\s,]+)$', message.text, re.IGNORECASE)
    if not match:
        return
        
    symbol = match.group(1).upper()
    prices_str = match.group(2)
    
    raw_prices = prices_str.replace(',', ' ').split()
    target_prices = []
    for p in raw_prices:
        try:
            target_prices.append(float(p))
        except ValueError:
            pass
            
    if not target_prices:
        await message.answer("אנא הזן מחירים תקינים.")
        return
        
    target_prices = list(set(target_prices))
    
    global db_pool
    if not db_pool:
        return
        
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id FROM users WHERE telegram_chat_id = $1", str(message.chat.id))
        if not user:
            await message.answer("החשבון שלך אינו מקושר. לא ניתן להוסיף התראות. נא קשר אותו דרך האתר לחוויה מלאה.")
            return
            
        wait_msg = await message.answer(f"⏳ בודק מחיר עדכני למניה <b>{symbol}</b>...")
        
        current_price = await get_current_price(symbol)
        if current_price is None:
            await wait_msg.edit_text(f"❌ לא הצלחתי למצוא נתונים עבור הסימול <b>{symbol}</b>. התראות לא נשמרו.\nנסה לחפש את המניה קודם באמצעות: <code>search {symbol}</code>")
            return
            
        results = []
        for target_price in target_prices:
            direction = "above" if target_price > current_price else "below"
            
            # INSERT to db
            await conn.execute(
                """INSERT INTO price_alerts 
                   (symbol, target_price, direction, is_active, created_at, user_id) 
                   VALUES ($1, $2, $3, true, NOW(), $4)""",
                symbol, target_price, direction, user['id']
            )
            
            cond_str = "מעל" if direction == "above" else "מתחת ל-"
            results.append(f"🎯 <b>{cond_str} ${target_price:.2f}</b>")
            
        targets_msg = "\n".join(results)
        success_title = "✅ <b>התראות נוספו בהצלחה!</b>" if len(target_prices) > 1 else "✅ <b>התראה נוספה בהצלחה!</b>"
        
        await wait_msg.edit_text(
            f"{success_title}\n\n"
            f"📈 מניה: <b>{symbol}</b>\n"
            f"💰 מחיר נוכחי בעת הוספה: <b>${current_price:.2f}</b>\n\n"
            f"יעדים:\n{targets_msg}"
        )

@dp.message(F.text.lower() == "revive")
async def cmd_revive_alerts(message: Message):
    global db_pool
    if not db_pool:
        return
        
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id FROM users WHERE telegram_chat_id = $1", str(message.chat.id))
        if not user:
            return
            
        res = await conn.execute(
            "UPDATE price_alerts SET is_active = true WHERE user_id = $1 AND is_active = false",
            user['id']
        )
        
        try:
            count = int(res.split()[1])
        except IndexError:
            count = 0
            
        if count > 0:
            await message.answer(f"🤫 <b>פקודה סודית הופעלה!</b>\n{count} התראות שקפצו בעבר הופעלו מחדש.")
        else:
            await message.answer("🤫 <b>פקודה סודית הופעלה!</b>\nלא היו התראות כבויות להפעיל.")

@dp.message()
async def auto_catch_all(message: Message):
    await message.answer("לא זיהיתי את הפקודה...\nשלח <code>help</code> כדי לראות את הפקודות הזמינות.")

# ==========================================
# Scheduler Job
# ==========================================
def is_market_open():
    """
    Checks if the US Stock Market (NYSE/NASDAQ) is currently open.
    Hours: Mon-Fri, 9:30 AM - 4:00 PM ET.
    """
    tz = pytz.timezone('US/Eastern')
    now = datetime.now(tz)
    
    # Check if it's a weekday (0=Monday, 6=Sunday)
    if now.weekday() > 4:
        return False
        
    # Check if within 9:30 AM - 4:00 PM
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    
    return market_open <= now <= market_close

async def check_single_alert(alert):
    """
    Checks a single stock alert and sends a notification if triggered.
    This runs in the background to avoid blocking other operations.
    """
    global db_pool, bot
    alert_id = alert['id']
    symbol = alert['symbol']
    threshold = float(alert['target_price'])
    condition = alert['direction']
    chat_id = alert['telegram_chat_id']
    
    try:
        if condition.startswith("SMA_"):
            period = int(float(threshold))
            # For SMA we need more than 1 day of data. 2y is safe for SMA 200.
            def fetch_sma():
                stock = yf.Ticker(symbol)
                # period="2y" to be safe for 200 SMA, interval="1d"
                hist = stock.history(period="2y")
                if len(hist) < period:
                    return None, None
                
                sma = hist['Close'].rolling(window=period).mean().iloc[-1]
                current = hist['Close'].iloc[-1]
                return float(sma), float(current)

            sma_val, current_price = await asyncio.to_thread(fetch_sma)
            
            if sma_val is None:
                logger.warning(f"Could not calculate SMA {period} for {symbol}")
                return
            
            threshold = sma_val # Overwrite threshold with SMA value for logic below
        else:
            # Use our existing async price fetcher (it handles threading and fallback)
            current_price = await get_current_price(symbol)
        
        if current_price is None:
            logger.warning(f"Could not retrieve price data for {symbol}")
            return
            
        logger.info(f"{symbol} current price: {current_price:.2f} | DB Threshold: {threshold} ({condition})")
        
        # Check conditions
        triggered = False
        if condition == "SMA_TOUCH":
            # Trigger if within 0.5% range
            diff_pct = abs(current_price - threshold) / threshold
            if diff_pct <= 0.005: # 0.5%
                triggered = True
                condition_msg = "Price is near"
            else:
                return # Don't log/proceed if not triggered for SMA_TOUCH to save logs
        elif condition.lower() == "above" and current_price >= threshold:
            triggered = True
            condition_msg = "crossed above"
        elif condition.lower() == "below" and current_price <= threshold:
            triggered = True
            condition_msg = "crossed below"
        elif condition == "SMA_ABOVE" and current_price >= threshold:
            triggered = True
            condition_msg = "is above"
        elif condition == "SMA_BELOW" and current_price <= threshold:
            triggered = True
            condition_msg = "is below"
            
        if triggered:
            # Send message using Telegram Bot
            display_condition = f"SMA {int(float(alert['target_price']))}" if condition.startswith("SMA_") else f"${threshold:.2f}"
            msg = (
                f"🚨 <b>Stock Limit Reached!</b> 🚨\n\n"
                f"📈 <b>{symbol}</b> {condition_msg} your target.\n"
                f"💰 Current Price: <b>${current_price:.2f}</b>\n"
                f"🎯 Target Set: <b>{display_condition}</b>"
            )
            
            try:
                await bot.send_message(chat_id=chat_id, text=msg)
                
                # If it was an SMA alert, send chart with ONLY that SMA. 
                # Otherwise send default chart (passed as None).
                target_sma = int(threshold) if condition.startswith("SMA_") else None
                await send_stock_chart(chat_id, symbol, sma_period=target_sma)
                
                logger.info(f"Alert and chart sent to chat_id={chat_id} for {symbol}!")
                
                # Mark the alert as inactive so it doesn't fire again immediately
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE price_alerts SET is_active = false, alert_date = NOW() WHERE id = $1",
                        alert_id
                    )
            except (TelegramBadRequest, TelegramForbiddenError) as e:
                logger.error(f"Telegram error sending to {chat_id}: {e}")
                
    except Exception as e:
        logger.error(f"Error checking {symbol}: {e}")

async def check_stocks_and_notify():
    global db_pool
    if db_pool is None:
        logger.error("Database pool is not initialized.")
        return

    # is_market_open check moved inside to allow crypto 24/7

    logger.info("Scheduler tick: Checking active stocks from DB...")
    
    try:
        is_open = is_market_open()
        
        async with db_pool.acquire() as conn:
            # Get all active alerts
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
            all_alerts = await conn.fetch(query)
            
        if not all_alerts:
            logger.info("No active alerts to check.")
            return

        # Filter alerts: Crypto runs 24/7, stocks only if market is open
        alerts_to_check = []
        for alert in all_alerts:
            symbol = alert['symbol'].upper()
            # Basic heuristic for crypto: ends with -USD, -EUR, -BTC or common crypto coins
            is_crypto = "-" in symbol or any(coin in symbol for coin in ["BTC", "ETH", "SOL", "BNB", "DOGE"])
            
            if is_crypto or is_open:
                alerts_to_check.append(alert)
                
        if not alerts_to_check:
            if not is_open:
                logger.info("Market is closed and no crypto alerts to check. Skipping.")
            else:
                logger.info("No active alerts to check (filtered).")
            return
                
        # Run all checks concurrently. 
        tasks = [check_single_alert(alert) for alert in alerts_to_check]
        await asyncio.gather(*tasks)
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
        # Check if we should use SSL based on environment (local vs production)
        ssl_mode = "require" if "localhost" not in DB_HOST else None
        
        db_pool = await asyncpg.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            min_size=1,
            max_size=10,
            ssl=ssl_mode,
            statement_cache_size=0
        )
        logger.info(f"Database connection pool created successfully (SSL: {ssl_mode}).")
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
