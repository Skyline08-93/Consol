# triangle_bybit_bot.py — торговый бот с командой /status
import ccxt.async_support as ccxt
import asyncio
import os
import time
import logging
import html
from datetime import datetime, timedelta
from telegram import Bot, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# === Настройка логирования ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot_debug.log")
    ]
)
logger = logging.getLogger('TriangleBot')
logger.setLevel(logging.DEBUG if os.getenv("DEBUG") else logging.INFO)

# === Конфигурация окружения ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
IS_TESTNET = os.getenv("TESTNET", "false").lower() == "true"

# === Параметры торговли ===
COMMISSION_RATE = 0.001
MIN_PROFIT = 0.15
MAX_PROFIT = 2.0
TARGET_VOLUME_USDT = float(os.getenv("TRADE_VOLUME", "10"))
START_COINS = ['USDT', 'BTC', 'ETH']
LOG_FILE = "trades.csv"
MAX_SLIPPAGE = 0.005
MAX_RETRIES = 3
RETRY_DELAY = 1.5
MAX_CONCURRENT_TRADES = 1
MIN_BALANCE_USDT = 15

# === Лимиты сделок для защиты от блокировки ===
MAX_TRADES_PER_MINUTE = int(os.getenv("MAX_TRADES_PER_MINUTE", "5"))
MAX_TRADES_PER_HOUR = int(os.getenv("MAX_TRADES_PER_HOUR", "30"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "100"))

# === Защитные механизмы ===
TRADE_COOLDOWN = timedelta(minutes=5)
BALANCE_REFRESH_INTERVAL = 3600
SYMBOL_REFRESH_INTERVAL = 86400
TRIANGLE_HOLD_TIME = 10

# === Глобальные состояния ===
active_trades = {}
trade_history = []
symbols_cache = {}
markets_cache = {}
triangles_cache = []
last_symbol_refresh = 0
last_balance_refresh = 0
current_balances = {}
trade_limits_suspended = False
balance_warning_sent = False
bot_start_time = time.time()
total_triangles_checked = 0
last_trade_time = None
last_trade_profit = 0.0
last_trade_route = ""
scanning_active = True

# === Инициализация биржи ===
def init_exchange():
    exchange_options = {
        "enableRateLimit": True,
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "options": {"defaultType": "spot"}
    }
    
    if IS_TESTNET:
        exchange_options["urls"] = {
            "api": {"public": "https://api-testnet.bybit.com", 
                    "private": "https://api-testnet.bybit.com"}
        }
        logger.info("Режим ТЕСТОВОЙ СЕТИ активирован")
    else:
        logger.info("Режим РЕАЛЬНОЙ СЕТИ активирован")
    
    return ccxt.bybit(exchange_options)

exchange = init_exchange()

# Инициализация файла лога
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w") as f:
        f.write("timestamp,route,profit_percent,volume_usdt,status,details\n")

# === Telegram Handlers ===
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /status"""
    try:
        # Рассчитываем время работы бота
        uptime_seconds = time.time() - bot_start_time
        uptime = timedelta(seconds=int(uptime_seconds))
        
        # Статус сканирования
        scan_status = "✅ АКТИВНО" if scanning_active else "⛔️ ОСТАНОВЛЕНО"
        
        # Статус торговли
        trade_status = "⛔️ ПРИОСТАНОВЛЕНА" if trade_limits_suspended else "✅ АКТИВНА"
        
        # Баланс USDT
        usdt_balance = current_balances.get('USDT', 0)
        balance_status = "✅ Достаточный" if usdt_balance >= MIN_BALANCE_USDT else f"⚠️ Низкий ({usdt_balance:.2f} USDT)"
        
        # Последняя сделка
        last_trade_info = "Нет данных"
        if last_trade_time:
            last_trade_delta = datetime.utcnow() - last_trade_time
            last_trade_info = (
                f"Время: {last_trade_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Маршрут: {last_trade_route}\n"
                f"Прибыль: {last_trade_profit:.2f}%"
            )
        
        # Формируем сообщение
        message = (
            "🤖 <b>СТАТУС БОТА</b>\n\n"
            f"⏱ <b>Время работы:</b> {uptime}\n"
            f"🔍 <b>Сканирование:</b> {scan_status}\n"
            f"🔁 <b>Проверено треугольников:</b> {total_triangles_checked}\n"
            f"💼 <b>Торговый статус:</b> {trade_status}\n"
            f"💰 <b>Баланс USDT:</b> {balance_status}\n\n"
            f"📊 <b>Последняя сделка:</b>\n{last_trade_info}\n\n"
            f"⚙️ <b>Текущие настройки:</b>\n"
            f"Объем: ${TARGET_VOLUME_USDT:.2f}\n"
            f"Мин. прибыль: {MIN_PROFIT}%\n"
            f"Макс. прибыль: {MAX_PROFIT}%"
        )
        
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка команды /status: {str(e)}")
        await update.message.reply_text("⚠️ Ошибка при получении статуса")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start"""
    try:
        message = (
            "🤖 <b>Треугольный арбитражный бот для Bybit</b>\n\n"
            "Доступные команды:\n"
            "/status - текущий статус бота\n"
            "/balance - текущий баланс\n"
            "/scan_on - возобновить сканирование\n"
            "/scan_off - приостановить сканирование"
        )
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка команды /start: {str(e)}")

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /balance"""
    try:
        await refresh_balances(force=True)
        if not current_balances:
            await update.message.reply_text("Не удалось получить баланс")
            return
            
        message = ["💰 <b>Текущий баланс:</b>"]
        for coin, amount in current_balances.items():
            if amount > 0.001:
                message.append(f"{coin}: {amount:.6f}")
        
        await update.message.reply_text("\n".join(message), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Ошибка команды /balance: {str(e)}")
        await update.message.reply_text("⚠️ Ошибка при получении баланса")

async def scan_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /scan_on"""
    global scanning_active
    scanning_active = True
    await update.message.reply_text("✅ Сканирование треугольников возобновлено")

async def scan_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /scan_off"""
    global scanning_active
    scanning_active = False
    await update.message.reply_text("⛔️ Сканирование треугольников приостановлено")

async def refresh_symbols(force=False):
    global symbols_cache, markets_cache, triangles_cache, last_symbol_refresh
    
    current_time = time.time()
    if not force and current_time - last_symbol_refresh < SYMBOL_REFRESH_INTERVAL:
        return symbols_cache, markets_cache, triangles_cache
    
    try:
        logger.debug("Обновление списка торговых пар...")
        markets = await exchange.load_markets()
        symbols = list(markets.keys())
        
        symbols_cache = symbols
        markets_cache = markets
        triangles_cache = await find_triangles(symbols)
        last_symbol_refresh = current_time
        
        logger.info(f"Обновлено {len(symbols)} пар, найдено {len(triangles_cache)} треугольников")
        return symbols_cache, markets_cache, triangles_cache
    except Exception as e:
        logger.error(f"Ошибка обновления пар: {str(e)}")
        return symbols_cache, markets_cache, triangles_cache

async def find_triangles(symbols):
    triangles = []
    for base in START_COINS:
        for sym1 in symbols:
            if not sym1.endswith('/' + base): 
                continue
            mid1 = sym1.split('/')[0]
            for sym2 in symbols:
                if not sym2.startswith(mid1 + '/'): 
                    continue
                mid2 = sym2.split('/')[1]
                third = f"{mid2}/{base}"
                if third in symbols:
                    triangles.append((base, mid1, mid2))
    return triangles

async def get_avg_price(orderbook_side, target_usdt):
    total_base = 0
    total_usd = 0
    max_liquidity = 0
    
    for price, volume in orderbook_side:
        price = float(price)
        volume = float(volume)
        usd = price * volume
        max_liquidity += usd
        
        if total_usd + usd >= target_usdt:
            remain_usd = target_usdt - total_usd
            total_base += remain_usd / price
            total_usd += remain_usd
            break
        else:
            total_base += volume
            total_usd += usd
    
    if total_usd < target_usdt * 0.9:
        return None, 0, max_liquidity
    
    avg_price = total_usd / total_base
    return avg_price, total_usd, max_liquidity

async def get_execution_price(symbol, side, target_usdt):
    for attempt in range(MAX_RETRIES):
        try:
            orderbook = await exchange.fetch_order_book(symbol, limit=20)
            if side == "buy":
                return await get_avg_price(orderbook['asks'], target_usdt)
            else:
                return await get_avg_price(orderbook['bids'], target_usdt)
        except Exception as e:
            logger.warning(f"Ошибка стакана {symbol} (попытка {attempt+1}): {str(e)}")
            await asyncio.sleep(RETRY_DELAY)
    
    return None, 0, 0

def format_line(index, pair, price, side, volume_usd, color, liquidity):
    emoji = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(color, "")
    
    # Экранирование специальных символов для HTML
    safe_pair = html.escape(pair)
    safe_side = html.escape(side)
    
    return f"{emoji} {index}. {safe_pair} - {price:.6f} ({safe_side}), исполнено ${volume_usd:.2f}, доступно ${liquidity:.2f}"

async def send_telegram_message(text, important=False):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    try:
        # Для важных сообщений используем HTML-разметку, но экранируем текст
        if important:
            # Экранируем весь текст, кроме тегов
            safe_text = text.replace('<', '&lt;').replace('>', '&gt;')
            safe_text = safe_text.replace('&lt;b&gt;', '<b>').replace('&lt;/b&gt;', '</b>')
            safe_text = safe_text.replace('&lt;i&gt;', '<i>').replace('&lt;/i&gt;', '</i>')
            parse_mode = ParseMode.HTML
        else:
            safe_text = text
            parse_mode = None
        
        logger.debug(f"Отправка Telegram: {safe_text[:100]}...")
        
        await telegram_app.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, 
            text=safe_text, 
            parse_mode=parse_mode,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Ошибка Telegram: {str(e)}")

def log_trade(base, mid1, mid2, profit, volume, status, details=""):
    try:
        with open(LOG_FILE, "a") as f:
            route = f"{base}->{mid1}->{mid2}->{base}"
            f.write(f"{datetime.utcnow()},{route},{profit:.4f},{volume},{status},{details}\n")
    except Exception as e:
        logger.error(f"Ошибка записи лога: {str(e)}")

async def refresh_balances(force=False):
    global current_balances, last_balance_refresh, balance_warning_sent
    
    current_time = time.time()
    if not force and current_time - last_balance_refresh < BALANCE_REFRESH_INTERVAL:
        return current_balances
    
    try:
        logger.debug("Обновление балансов...")
        balances = await exchange.fetch_balance()
        current_balances = {k: float(v) for k, v in balances["total"].items() if float(v) > 0}
        last_balance_refresh = current_time
        
        usdt_balance = current_balances.get('USDT', 0)
        if usdt_balance < MIN_BALANCE_USDT:
            if not balance_warning_sent:
                faucet_link = "https://testnet.bybit.com/ru-RU/testnet/faucet" if IS_TESTNET else ""
                msg = (
                    f"⚠️ НИЗКИЙ БАЛАНС! ⚠️\n"
                    f"USDT: {usdt_balance:.2f} < {MIN_BALANCE_USDT}\n"
                    f"Пополните баланс: {faucet_link}"
                )
                await send_telegram_message(msg, important=True)
                balance_warning_sent = True
        else:
            balance_warning_sent = False
        
        return current_balances
    except Exception as e:
        logger.error(f"Ошибка баланса: {str(e)}")
        return current_balances

def check_trade_limits():
    """Проверяет все лимиты на количество сделок"""
    global trade_limits_suspended
    
    now = time.time()
    
    # Проверка лимита на минуту
    recent_minute = [t for t in trade_history if now - t < 60]
    if len(recent_minute) >= MAX_TRADES_PER_MINUTE:
        reason = f"Превышен минутный лимит ({MAX_TRADES_PER_MINUTE})"
        if not trade_limits_suspended:
            trade_limits_suspended = True
            asyncio.create_task(send_telegram_message(
                f"⛔️ ТОРГОВЛЯ ПРИОСТАНОВЛЕНА ⛔️\n{reason}", 
                important=True
            ))
        return False, reason
    
    # Проверка лимита на час
    recent_hour = [t for t in trade_history if now - t < 3600]
    if len(recent_hour) >= MAX_TRADES_PER_HOUR:
        reason = f"Превышен часовой лимит ({MAX_TRADES_PER_HOUR})"
        if not trade_limits_suspended:
            trade_limits_suspended = True
            asyncio.create_task(send_telegram_message(
                f"⛔️ ТОРГОВЛЯ ПРИОСТАНОВЛЕНА ⛔️\n{reason}", 
                important=True
            ))
        return False, reason
    
    # Проверка лимита на день
    recent_day = [t for t in trade_history if now - t < 86400]
    if len(recent_day) >= MAX_TRADES_PER_DAY:
        reason = f"Превышен дневной лимит ({MAX_TRADES_PER_DAY})"
        if not trade_limits_suspended:
            trade_limits_suspended = True
            asyncio.create_task(send_telegram_message(
                f"⛔️ ТОРГОВЛЯ ПРИОСТАНОВЛЕНА ⛔️\n{reason}", 
                important=True
            ))
        return False, reason
    
    # Если все лимиты в норме, возобновляем торговлю
    if trade_limits_suspended:
        trade_limits_suspended = False
        asyncio.create_task(send_telegram_message(
            "✅ ТОРГОВЛЯ ВОЗОБНОВЛЕНА ✅\nЛимиты сброшены", 
            important=True
        ))
    
    return True, "OK"

def can_execute_trade(route_id):
    """Проверяет возможность выполнения сделки"""
    # Проверка активных сделок
    if len(active_trades) >= MAX_CONCURRENT_TRADES:
        logger.warning(f"Превышен лимит активных сделок: {route_id}")
        return False
    
    # Проверка времени последней сделки
    last_time = last_trade_time.get(route_id)
    if last_time and (datetime.utcnow() - last_time) < TRADE_COOLDOWN:
        logger.debug(f"Торговля в режиме охлаждения: {route_id}")
        return False
    
    # Проверка лимитов на количество сделок
    trade_allowed, reason = check_trade_limits()
    if not trade_allowed:
        logger.warning(f"Торговля приостановлена: {reason}")
        return False
    
    # Проверка баланса USDT
    usdt_balance = current_balances.get('USDT', 0)
    if usdt_balance < TARGET_VOLUME_USDT * 1.1:
        logger.warning(f"Недостаточный баланс USDT: {usdt_balance:.2f} < {TARGET_VOLUME_USDT * 1.1:.2f}")
        return False
    
    return True

async def execute_real_trade(route_id, steps):
    """Выполняет реальные торговые операции"""
    # Регистрируем начало сделки
    active_trades[route_id] = datetime.utcnow()
    trade_start = time.time()
    
    trade_details = []
    try:
        for i, (symbol, side, amount) in enumerate(steps):
            logger.info(f"Исполнение ордера {i+1}/{len(steps)}: {symbol} {side} {amount:.6f}")
            
            # Создание рыночного ордера
            order = await exchange.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=amount,
                params={'timeInForce': 'IOC'}
            )
            trade_details.append(order)
            
            # Пауза между ордерами для снижения нагрузки
            if i < len(steps) - 1:
                await asyncio.sleep(1.0)
                
        return True, trade_details
    except Exception as e:
        logger.error(f"Ошибка сделки: {str(e)}")
        return False, str(e)
    finally:
        # Снятие блокировки
        active_trades.pop(route_id, None)
        last_trade_time[route_id] = datetime.utcnow()
        
        # Регистрируем сделку в истории
        trade_history.append(trade_start)
        logger.info(f"Сделка завершена за {time.time() - trade_start:.2f} сек")

async def check_triangle(base, mid1, mid2, symbols, markets):
    global total_triangles_checked, last_trade_time, last_trade_profit, last_trade_route
    
    try:
        route_id = f"{base}->{mid1}->{mid2}->{base}"
        total_triangles_checked += 1
        
        # Проверка доступности маршрута
        s1 = f"{mid1}/{base}"
        s2 = f"{mid2}/{mid1}"
        s3 = f"{mid2}/{base}"
        
        if not (s1 in symbols and s2 in symbols and s3 in symbols):
            return
        
        # Проверка минимального объема
        market_info = markets.get(s1)
        if market_info and 'limits' in market_info:
            min_amount = market_info['limits']['amount']['min']
            if TARGET_VOLUME_USDT < min_amount * 10:
                return

        # Получение цен исполнения
        price1, vol1, liq1 = await get_execution_price(s1, "buy", TARGET_VOLUME_USDT)
        if not price1 or vol1 < TARGET_VOLUME_USDT * 0.8:
            return
            
        price2, vol2, liq2 = await get_execution_price(s2, "buy", vol1 * 0.99)
        if not price2:
            return
            
        price3, vol3, liq3 = await get_execution_price(s3, "sell", vol2 * 0.99)
        if not price3:
            return

        # Расчет прибыли с учетом комиссий
        step1 = (1 / price1) * (1 - COMMISSION_RATE)
        step2 = step1 * (1 / price2) * (1 - COMMISSION_RATE)
        step3 = step2 * price3 * (1 - COMMISSION_RATE)
        
        profit_percent = (step3 - 1) * 100
        if not (MIN_PROFIT <= profit_percent <= MAX_PROFIT): 
            return

        # Проверка условий для исполнения
        if not can_execute_trade(route_id):
            return

        min_liquidity = min(liq1, liq2, liq3)
        pure_profit_usdt = (step3 - 1) * TARGET_VOLUME_USDT

        # Формирование сообщения
        message_lines = [
            "🔁 Арбитражная возможность",
            format_line(1, s1, price1, "ASK", vol1, "green", liq1),
            format_line(2, s2, price2, "ASK", vol2, "yellow", liq2),
            format_line(3, s3, price3, "BID", vol3, "red", liq3),
            "",
            f"💰 Чистая прибыль: {pure_profit_usdt:.2f} USDT",
            f"📈 Спред: {profit_percent:.2f}%",
            f"💧 Мин. ликвидность: ${min_liquidity:.2f}"
        ]

        # Отправка в Telegram
        await send_telegram_message("\n".join(message_lines))
        log_trade(base, mid1, mid2, profit_percent, min_liquidity, "detected")

        # Подготовка шагов сделки
        steps = [
            (s1, "buy", TARGET_VOLUME_USDT),
            (s2, "buy", TARGET_VOLUME_USDT / price1 * (1 - COMMISSION_RATE)),
            (s3, "sell", TARGET_VOLUME_USDT / price1 / price2 * (1 - 2*COMMISSION_RATE))
        ]

        # Выполнение сделки
        trade_success, trade_result = await execute_real_trade(route_id, steps)
        
        if trade_success:
            last_trade_time = datetime.utcnow()
            last_trade_profit = profit_percent
            last_trade_route = route_id
            
            profit_msg = f"✅ Сделка выполнена\nПрибыль: {pure_profit_usdt:.2f} USDT ({profit_percent:.2f}%)"
            await send_telegram_message(profit_msg, important=True)
            log_trade(base, mid1, mid2, profit_percent, TARGET_VOLUME_USDT, "executed", str(trade_result))
        else:
            # Экранирование текста ошибки
            error_details = html.escape(str(trade_result)[:200])
            error_msg = f"❌ Ошибка сделки\n{error_details}"
            await send_telegram_message(error_msg, important=True)
            log_trade(base, mid1, mid2, profit_percent, TARGET_VOLUME_USDT, "failed", str(trade_result))
            
        # Принудительное обновление баланса после сделки
        await refresh_balances(force=True)
            
    except Exception as e:
        logger.error(f"Ошибка треугольника: {str(e)}", exc_info=True)
        # Экранирование текста ошибки
        error_details = html.escape(str(e))[:100]
        error_msg = f"⚠️ Ошибка обработки\n{error_details}"
        await send_telegram_message(error_msg, important=True)

async def check_exchange_connection():
    """Проверяет подключение к бирже"""
    try:
        await exchange.fetch_time()
        return True
    except Exception as e:
        logger.error(f"Ошибка подключения: {str(e)}")
        error_details = html.escape(str(e))[:200]
        error_msg = f"❌ Ошибка подключения к Bybit\n{error_details}"
        await send_telegram_message(error_msg, important=True)
        return False

async def cleanup_old_data():
    """Очищает старые данные для экономии памяти"""
    now = time.time()
    
    # Очистка истории сделок (старше 7 дней)
    global trade_history
    trade_history = [t for t in trade_history if now - t < 604800]
    
    # Очистка кеша последних сделок
    global last_trade_time
    cutoff = datetime.utcnow() - timedelta(days=7)
    last_trade_time = {k: v for k, v in last_trade_time.items() if v > cutoff}

async def safe_shutdown():
    """Безопасное завершение работы"""
    logger.info("Завершение работы...")
    try:
        await exchange.close()
        await telegram_app.stop()
        await telegram_app.shutdown()
    except Exception as e:
        logger.error(f"Ошибка завершения: {str(e)}")
    finally:
        logger.info("Бот остановлен")

async def main_loop():
    """Основной цикл работы бота"""
    global telegram_app, scanning_active
    
    telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    # Добавляем обработчики команд
    telegram_app.add_handler(CommandHandler("start", start_command))
    telegram_app.add_handler(CommandHandler("status", status_command))
    telegram_app.add_handler(CommandHandler("balance", balance_command))
    telegram_app.add_handler(CommandHandler("scan_on", scan_on_command))
    telegram_app.add_handler(CommandHandler("scan_off", scan_off_command))
    
    await telegram_app.initialize()
    await telegram_app.start()
    
    # Проверка подключения
    if not await check_exchange_connection():
        await safe_shutdown()
        return
    
    # Начальные загрузки
    await send_telegram_message("🤖 Торговый бот запущен")
    await refresh_symbols(force=True)
    await refresh_balances(force=True)
    
    # Основной цикл
    logger.info("Начало работы основного цикла")
    last_balance_update = time.time()
    last_cleanup = time.time()
    
    while True:
        try:
            current_time = time.time()
            
            # Периодическое обновление данных
            if current_time - last_balance_update > BALANCE_REFRESH_INTERVAL:
                await refresh_balances()
                last_balance_update = current_time
            
            # Очистка старых данных
            if current_time - last_cleanup > 3600:
                await cleanup_old_data()
                last_cleanup = current_time
            
            # Проверка минимального баланса
            usdt_balance = current_balances.get('USDT', 0)
            if usdt_balance < MIN_BALANCE_USDT:
                logger.warning(f"Недостаточный баланс USDT: {usdt_balance:.2f} < {MIN_BALANCE_USDT}")
                # Ждем 5 минут перед следующей проверкой
                await asyncio.sleep(300)
                await refresh_balances(force=True)
                continue
            
            # Обновление треугольников
            symbols, markets, triangles = await refresh_symbols()
            
            # Проверка треугольников только если сканирование активно
            if scanning_active:
                tasks = [check_triangle(base, mid1, mid2, symbols, markets) 
                         for base, mid1, mid2 in triangles]
                await asyncio.gather(*tasks)
            
            await asyncio.sleep(10)
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Ошибка главного цикла: {str(e)}", exc_info=True)
            await asyncio.sleep(30)

async def main():
    # Инициализация глобальных переменных
    global trade_history, last_trade_time
    global trade_limits_suspended, balance_warning_sent, scanning_active
    
    trade_history = []
    last_trade_time = {}
    trade_limits_suspended = False
    balance_warning_sent = False
    scanning_active = True
    
    try:
        await main_loop()
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания")
    finally:
        await safe_shutdown()

if __name__ == '__main__':
    asyncio.run(main())