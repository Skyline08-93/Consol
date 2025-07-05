# triangle_bybit_bot.py — оптимизированный торговый бот для Bybit
import ccxt.async_support as ccxt
import asyncio
import os
import hashlib
import time
import logging
from datetime import datetime, timedelta
from telegram import Bot
from telegram.constants import ParseMode
from telegram.ext import Application

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
COMMISSION_RATE = 0.001  # 0.1%
MIN_PROFIT = 0.15  # Минимальный порог прибыли
MAX_PROFIT = 2.0   # Максимальный порог прибыли
TARGET_VOLUME_USDT = float(os.getenv("TRADE_VOLUME", "10"))
START_COINS = ['USDT', 'BTC', 'ETH']
LOG_FILE = "trades.csv"
MAX_SLIPPAGE = 0.005  # 0.5%
MAX_RETRIES = 3
RETRY_DELAY = 1.5
MAX_CONCURRENT_TRADES = 1
MIN_BALANCE_USDT = 15

# === Защитные механизмы ===
TRADE_COOLDOWN = timedelta(minutes=5)
BALANCE_REFRESH_INTERVAL = 3600  # 1 час
SYMBOL_REFRESH_INTERVAL = 86400  # 1 день
TRIANGLE_HOLD_TIME = 10  # секунд

# === Глобальные состояния ===
active_trades = {}
last_trade_time = {}
symbols_cache = {}
markets_cache = {}
triangles_cache = []
last_symbol_refresh = 0
last_balance_refresh = 0
current_balances = {}

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

async def refresh_symbols(force=False):
    global symbols_cache, markets_cache, triangles_cache, last_symbol_refresh
    
    current_time = time.time()
    if not force and current_time - last_symbol_refresh < SYMBOL_REFRESH_INTERVAL:
        return symbols_cache, markets_cache, triangles_cache
    
    try:
        logger.debug("Обновление списка торговых пар...")
        markets = await exchange.load_markets()
        symbols = list(markets.keys())
        
        # Обновление кешей
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
    
    if total_usd < target_usdt * 0.9:  # Недостаточная ликвидность
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
    return f"{emoji} {index}. {pair} - {price:.6f} ({side}), исполнено ${volume_usd:.2f}, доступно ${liquidity:.2f}"

async def send_telegram_message(text, important=False):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    try:
        await telegram_app.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, 
            text=text, 
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        if important:
            logger.error(f"Ошибка Telegram: {str(e)}")

def log_trade(base, mid1, mid2, profit, volume, status, details=""):
    try:
        with open(LOG_FILE, "a") as f:
            route = f"{base}->{mid1}->{mid2}->{base}"
            f.write(f"{datetime.utcnow()},{route},{profit:.4f},{volume},{status},{details}\n")
    except Exception as e:
        logger.error(f"Ошибка записи лога: {str(e)}")

async def refresh_balances(force=False):
    global current_balances, last_balance_refresh
    
    current_time = time.time()
    if not force and current_time - last_balance_refresh < BALANCE_REFRESH_INTERVAL:
        return current_balances
    
    try:
        logger.debug("Обновление балансов...")
        balances = await exchange.fetch_balance()
        current_balances = {k: float(v) for k, v in balances["total"].items() if float(v) > 0}
        last_balance_refresh = current_time
        
        # Отправка уведомления при низком балансе
        usdt_balance = current_balances.get('USDT', 0)
        if usdt_balance < MIN_BALANCE_USDT:
            msg = f"⚠️ <b>НИЗКИЙ БАЛАНС!</b> ⚠️\nUSDT: {usdt_balance:.2f} < {MIN_BALANCE_USDT}"
            await send_telegram_message(msg, important=True)
        
        return current_balances
    except Exception as e:
        logger.error(f"Ошибка баланса: {str(e)}")
        return current_balances

def can_execute_trade(route_id):
    # Проверка активных сделок
    if len(active_trades) >= MAX_CONCURRENT_TRADES:
        logger.warning(f"Превышен лимит активных сделок: {route_id}")
        return False
    
    # Проверка времени последней сделки
    last_time = last_trade_time.get(route_id)
    if last_time and (datetime.utcnow() - last_time) < TRADE_COOLDOWN:
        logger.debug(f"Торговля в режиме охлаждения: {route_id}")
        return False
    
    # Проверка баланса USDT
    usdt_balance = current_balances.get('USDT', 0)
    if usdt_balance < TARGET_VOLUME_USDT * 1.1:
        logger.warning(f"Недостаточный баланс USDT: {usdt_balance:.2f} < {TARGET_VOLUME_USDT * 1.1:.2f}")
        return False
    
    return True

async def execute_real_trade(route_id, steps):
    """Выполняет реальные торговые операции"""
    # Блокировка маршрута
    active_trades[route_id] = datetime.utcnow()
    
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
            
            # Краткая пауза между ордерами
            if i < len(steps) - 1:
                await asyncio.sleep(0.7)
                
        return True, trade_details
    except Exception as e:
        logger.error(f"Ошибка сделки: {str(e)}")
        return False, str(e)
    finally:
        # Снятие блокировки
        active_trades.pop(route_id, None)
        last_trade_time[route_id] = datetime.utcnow()

async def check_triangle(base, mid1, mid2, symbols, markets):
    try:
        route_id = f"{base}->{mid1}->{mid2}->{base}"
        
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
            if TARGET_VOLUME_USDT < min_amount * 10:  # 10x минимального объема
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
            f"🔁 <b>Арбитражная возможность</b>",
            format_line(1, s1, price1, "ASK", vol1, "green", liq1),
            format_line(2, s2, price2, "ASK", vol2, "yellow", liq2),
            format_line(3, s3, price3, "BID", vol3, "red", liq3),
            "",
            f"💰 <b>Чистая прибыль:</b> {pure_profit_usdt:.2f} USDT",
            f"📈 <b>Спред:</b> {profit_percent:.2f}%",
            f"💧 <b>Мин. ликвидность:</b> ${min_liquidity:.2f}"
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
            profit_msg = f"✅ <b>Сделка выполнена</b>\nПрибыль: {pure_profit_usdt:.2f} USDT ({profit_percent:.2f}%)"
            await send_telegram_message(profit_msg, important=True)
            log_trade(base, mid1, mid2, profit_percent, TARGET_VOLUME_USDT, "executed", str(trade_result))
        else:
            error_msg = f"❌ <b>Ошибка сделки</b>\n{str(trade_result)[:200]}"
            await send_telegram_message(error_msg, important=True)
            log_trade(base, mid1, mid2, profit_percent, TARGET_VOLUME_USDT, "failed", str(trade_result))
            
        # Принудительное обновление баланса после сделки
        await refresh_balances(force=True)
            
    except Exception as e:
        logger.error(f"Ошибка треугольника: {str(e)}", exc_info=True)
        error_msg = f"⚠️ <b>Ошибка обработки</b>\n{str(e)[:100]}"
        await send_telegram_message(error_msg, important=True)

async def send_balance_update():
    """Отправляет текущий баланс в Telegram"""
    try:
        balances = await refresh_balances()
        if not balances:
            return
            
        msg = ["💰 <b>БАЛАНС:</b>"]
        for coin, amount in balances.items():
            if amount > 0.001:  # Фильтр малых сумм
                msg.append(f"{coin}: {amount:.6f}")
        
        await send_telegram_message("\n".join(msg))
    except Exception as e:
        logger.error(f"Ошибка баланса: {str(e)}")

async def check_exchange_connection():
    """Проверяет подключение к бирже"""
    try:
        await exchange.fetch_time()
        return True
    except Exception as e:
        logger.error(f"Ошибка подключения: {str(e)}")
        error_msg = f"❌ <b>Ошибка подключения к Bybit</b>\n{str(e)[:200]}"
        await send_telegram_message(error_msg, important=True)
        return False

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
    # Инициализация Telegram
    global telegram_app
    telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()
    await telegram_app.initialize()
    await telegram_app.start()
    
    # Проверка подключения
    if not await check_exchange_connection():
        await safe_shutdown()
        return
    
    # Начальные загрузки
    await send_telegram_message("🤖 <b>Торговый бот запущен</b>")
    await refresh_symbols(force=True)
    await refresh_balances(force=True)
    
    # Основной цикл
    logger.info("Начало работы основного цикла")
    last_balance_update = time.time()
    
    while True:
        try:
            # Периодическое обновление данных
            current_time = time.time()
            if current_time - last_balance_update > BALANCE_REFRESH_INTERVAL:
                await refresh_balances()
                last_balance_update = current_time
            
            # Обновление треугольников
            symbols, markets, triangles = await refresh_symbols()
            
            # Проверка треугольников
            tasks = [check_triangle(base, mid1, mid2, symbols, markets) 
                     for base, mid1, mid2 in triangles]
            await asyncio.gather(*tasks)
                
            await asyncio.sleep(8)
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Ошибка главного цикла: {str(e)}", exc_info=True)
            await asyncio.sleep(30)

async def main():
    try:
        await main_loop()
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания")
    finally:
        await safe_shutdown()

if __name__ == '__main__':
    asyncio.run(main())