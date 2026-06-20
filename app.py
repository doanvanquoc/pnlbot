import asyncio
import math
import hmac
import hashlib
import time
import os
import logging
import aiohttp
from aiohttp import web
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("bot")

# Global variables
positions = {}          # Key: f"{symbol}_{positionSide}", Value: dict vị thế
subscribed_symbols = set() # Các symbol (viết thường) đã subscribe Mark Price WS
mark_price_ws = None    # WS connection cho Mark Price stream
auto_chats = set()      # Danh sách chat_id nhận cập nhật tự động mỗi 5 phút
last_auto_messages = {} # Lưu message_id của tin nhắn auto cuối cùng (key: chat_id, value: message_id)
hedge_mode = False      # Chế độ Position Mode (True: Hedge Mode, False: One-way Mode)
symbol_precisions = {}  # Lưu độ chính xác số lượng coin (quantityPrecision) của từng symbol

# Hàm tạo chữ ký HMAC-SHA256 cho Binance API
def get_binance_signature(query_string, secret_key):
    return hmac.new(
        secret_key.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

# Gửi tin nhắn Telegram
async def send_telegram_message(session, chat_id, text):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get('result', {}).get('message_id')
            else:
                body = await resp.text()
                logger.error(f"Lỗi gửi tin nhắn Telegram: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi khi gửi tin nhắn Telegram: {e}")
    return None

# Xóa tin nhắn Telegram
async def delete_telegram_message(session, chat_id, message_id):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{token}/deleteMessage"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id
    }
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                body = await resp.text()
                logger.warning(f"Không thể xóa tin nhắn Telegram {message_id}: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi khi xóa tin nhắn Telegram: {e}")
    return False

# Subscribe Mark Price của một symbol qua WebSocket
async def subscribe_mark_price(symbol):
    symbol_lower = symbol.lower()
    if symbol_lower in subscribed_symbols:
        return
    
    subscribed_symbols.add(symbol_lower)
    if mark_price_ws and not mark_price_ws.closed:
        try:
            await mark_price_ws.send_json({
                "method": "SUBSCRIBE",
                "params": [f"{symbol_lower}@markPrice@1s"],
                "id": int(time.time() * 1000)
            })
            logger.info(f"Đã đăng ký nhận giá mark cho: {symbol}")
        except Exception as e:
            logger.error(f"Lỗi khi gửi lệnh SUBSCRIBE cho {symbol}: {e}")

# Unsubscribe Mark Price của một symbol
async def unsubscribe_mark_price(symbol):
    symbol_lower = symbol.lower()
    if symbol_lower not in subscribed_symbols:
        return
    
    subscribed_symbols.remove(symbol_lower)
    if mark_price_ws and not mark_price_ws.closed:
        try:
            await mark_price_ws.send_json({
                "method": "UNSUBSCRIBE",
                "params": [f"{symbol_lower}@markPrice@1s"],
                "id": int(time.time() * 1000)
            })
            logger.info(f"Đã hủy nhận giá mark cho: {symbol}")
        except Exception as e:
            logger.error(f"Lỗi khi gửi lệnh UNSUBSCRIBE cho {symbol}: {e}")

# Cập nhật cache vị thế cục bộ
async def update_position_cache(symbol, position_side, amount, entry_price, leverage):
    key = f"{symbol}_{position_side}"
    amount = float(amount)
    entry_price = float(entry_price)
    leverage = int(leverage)
    
    if amount == 0.0:
        # Vị thế bị đóng hoàn toàn
        if key in positions:
            del positions[key]
            logger.info(f"Đã đóng vị thế: {key}")
        
        # Kiểm tra xem symbol này còn vị thế nào khác đang mở hay không
        still_has_position = any(p['symbol'] == symbol for p in positions.values())
        if not still_has_position:
            await unsubscribe_mark_price(symbol)
    else:
        # Vị thế được mở hoặc thay đổi volume
        is_new = key not in positions
        positions[key] = {
            'symbol': symbol,
            'positionSide': position_side,
            'positionAmt': amount,
            'entryPrice': entry_price,
            'markPrice': positions.get(key, {}).get('markPrice', entry_price),
            'unrealizedPnL': positions.get(key, {}).get('unrealizedPnL', 0.0),
            'leverage': leverage
        }
        
        if is_new:
            logger.info(f"Đã mở vị thế mới: {key} (Size: {amount}, Entry: {entry_price})")
        else:
            logger.info(f"Cập nhật vị thế: {key} (Size: {amount}, Entry: {entry_price})")
            
        await subscribe_mark_price(symbol)

# Lấy snapshot vị thế ban đầu từ Binance Futures REST API
async def init_positions(session, api_key, api_secret):
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v2/positionRisk?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    logger.info("Đang lấy dữ liệu vị thế ban đầu từ Binance REST API...")
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            data = await resp.json()
            positions.clear()
            for p in data:
                amount = float(p.get('positionAmt', 0))
                if amount != 0.0:
                    symbol = p.get('symbol')
                    position_side = p.get('positionSide')
                    entry_price = float(p.get('entryPrice', 0))
                    leverage = int(p.get('leverage', 1))
                    mark_price = float(p.get('markPrice', 0))
                    
                    # Tính toán PnL ban đầu
                    side_sign = -1 if (position_side == 'SHORT' or amount < 0) else 1
                    unrealized_pnl = (mark_price - entry_price) * abs(amount) * side_sign
                    
                    key = f"{symbol}_{position_side}"
                    positions[key] = {
                        'symbol': symbol,
                        'positionSide': position_side,
                        'positionAmt': amount,
                        'entryPrice': entry_price,
                        'markPrice': mark_price,
                        'unrealizedPnL': unrealized_pnl,
                        'leverage': leverage
                    }
            logger.info(f"Nạp snapshot thành công. Số vị thế đang mở: {len(positions)}")
        else:
            text = await resp.text()
            raise Exception(f"Lỗi lấy snapshot vị thế từ Binance: HTTP {resp.status} - {text}")

# Lấy listenKey từ Binance
async def get_listen_key(session, api_key):
    url = "https://fapi.binance.com/fapi/v1/listenKey"
    headers = {"X-MBX-APIKEY": api_key}
    async with session.post(url, headers=headers) as resp:
        if resp.status == 200:
            data = await resp.json()
            return data['listenKey']
        else:
            text = await resp.text()
            raise Exception(f"Không thể lấy listenKey: HTTP {resp.status} - {text}")

# Ping gia hạn listenKey
async def keepalive_listen_key(session, api_key, listen_key):
    url = f"https://fapi.binance.com/fapi/v1/listenKey?listenKey={listen_key}"
    headers = {"X-MBX-APIKEY": api_key}
    async with session.put(url, headers=headers) as resp:
        return resp.status == 200

# Loop duy trì listenKey
async def listen_key_keepalive_loop(session, api_key, listen_key):
    try:
        while True:
            await asyncio.sleep(1800) # 30 phút
            logger.info("Đang gửi ping duy trì listenKey...")
            success = await keepalive_listen_key(session, api_key, listen_key)
            if success:
                logger.info("Gia hạn listenKey thành công.")
            else:
                logger.error("Gia hạn listenKey thất bại.")
    except asyncio.CancelledError:
        logger.info("Task gia hạn listenKey đã bị dừng.")

# WebSocket kết nối User Data Stream từ Binance
async def binance_user_data_stream(session, api_key):
    while True:
        try:
            listen_key = await get_listen_key(session, api_key)
            logger.info(f"Đã khởi tạo User Data Stream với listenKey: {listen_key}")
            
            # Khởi chạy task keepalive
            keepalive_task = asyncio.create_task(
                listen_key_keepalive_loop(session, api_key, listen_key)
            )
            
            url = f"wss://fstream.binance.com/private/ws/{listen_key}"
            logger.info("Đang kết nối WebSocket User Data Stream...")
            
            async with session.ws_connect(url) as ws:
                logger.info("WebSocket User Data Stream đã kết nối.")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        event_type = data.get('e')
                        
                        if event_type == 'ACCOUNT_UPDATE':
                            positions_data = data.get('a', {}).get('P', [])
                            for p in positions_data:
                                symbol = p.get('s')
                                position_side = p.get('ps')
                                amount = p.get('pa')
                                entry_price = p.get('ep')
                                
                                # Giữ đòn bẩy leverage cũ trong cache nếu có
                                key = f"{symbol}_{position_side}"
                                old_leverage = positions.get(key, {}).get('leverage', 1)
                                
                                await update_position_cache(
                                    symbol=symbol,
                                    position_side=position_side,
                                    amount=amount,
                                    entry_price=entry_price,
                                    leverage=old_leverage
                                )
                                
                        elif event_type == 'ACCOUNT_CONFIG_UPDATE':
                            config_data = data.get('ac', {})
                            symbol = config_data.get('s')
                            leverage = config_data.get('l')
                            if symbol and leverage is not None:
                                leverage = int(leverage)
                                for key, pos in list(positions.items()):
                                    if pos['symbol'] == symbol:
                                        pos['leverage'] = leverage
                                        logger.info(f"Đã cập nhật đòn bẩy {key} thành {leverage}x")
                                        
                        elif event_type == 'listenKeyExpired':
                            logger.warning("listenKey đã bị hết hạn trên Binance Server.")
                            break
                            
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        logger.warning("User Data Stream bị đóng hoặc lỗi.")
                        break
            
            keepalive_task.cancel()
        except Exception as e:
            logger.error(f"Lỗi trong User Data Stream WebSocket: {e}")
            
        logger.info("Sẽ thử kết nối lại User Data Stream sau 5 giây...")
        await asyncio.sleep(5)

# WebSocket kết nối lấy Mark Price
async def binance_mark_price_stream(session):
    global mark_price_ws
    url = "wss://fstream.binance.com/market/ws"
    
    while True:
        try:
            logger.info("Đang kết nối WebSocket Mark Price Stream...")
            async with session.ws_connect(url) as ws:
                mark_price_ws = ws
                logger.info("WebSocket Mark Price Stream đã kết nối.")
                
                # Subscribe lại toàn bộ các symbol đang có trong cache
                current_symbols = list(set(p['symbol'].lower() for p in positions.values()))
                if current_symbols:
                    subscribed_symbols.clear()
                    params = [f"{s}@markPrice@1s" for s in current_symbols]
                    for s in current_symbols:
                        subscribed_symbols.add(s)
                    
                    await ws.send_json({
                        "method": "SUBSCRIBE",
                        "params": params,
                        "id": int(time.time() * 1000)
                    })
                    logger.info(f"Đã subscribe lại markPrice cho các symbol: {current_symbols}")
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        if data.get('e') == 'markPriceUpdate':
                            symbol = data.get('s')
                            mark_price = float(data.get('p'))
                            
                            # Cập nhật markPrice và tính PnL cho các vị thế tương ứng
                            for key, pos in list(positions.items()):
                                if pos['symbol'] == symbol:
                                    pos['markPrice'] = mark_price
                                    
                                    amt = pos['positionAmt']
                                    entry = pos['entryPrice']
                                    side = pos['positionSide']
                                    
                                    side_sign = -1 if (side == 'SHORT' or amt < 0) else 1
                                    pos['unrealizedPnL'] = (mark_price - entry) * abs(amt) * side_sign
                                    
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        logger.warning("Mark Price WS bị đóng hoặc lỗi.")
                        break
        except Exception as e:
            logger.error(f"Lỗi trong Mark Price WS: {e}")
            
        mark_price_ws = None
        subscribed_symbols.clear()
        logger.info("Sẽ thử kết nối lại Mark Price WS sau 5 giây...")
        await asyncio.sleep(5)

# Vòng lặp gửi vị thế tự động mỗi 5 phút
async def auto_pos_sender_loop(app):
    try:
        while True:
            # Lưu ý: người dùng đang đặt là 30 giây để test nhanh
            await asyncio.sleep(60)  
            if auto_chats and positions:
                session = app['session']
                
                text_lines = []
                for key, pos in positions.items():
                    symbol = pos['symbol']
                    side = pos['positionSide']
                    amt = pos['positionAmt']
                    pnl = pos['unrealizedPnL']
                    
                    display_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
                    display_side = "LONG" if (side == 'LONG' or (side == 'BOTH' and amt > 0)) else "SHORT"
                    pnl_emoji = "🟩" if pnl >= 0 else "🟥"
                    sign = "+" if pnl >= 0 else ""
                    
                    pos_text = f"{display_symbol} ({display_side}) ➜ {pnl_emoji} *{sign}{pnl:,.2f} USDT*"
                    text_lines.append(pos_text)
                    
                text_lines.append("----------------------------------")
                total_pnl = sum(p.get('unrealizedPnL', 0.0) for p in positions.values())
                pnl_emoji = "🟢" if total_pnl >= 0 else "🔴"
                sign = "+" if total_pnl >= 0 else ""
                text_lines.append(f"📊 Tổng PnL: *{sign}{total_pnl:,.2f} USDT*")
                
                message = "\n\n".join(text_lines)
                
                # Gửi cho tất cả các chat_id đã đăng ký
                for chat_id in list(auto_chats):
                    # Xóa tin nhắn auto cũ trước đó nếu có
                    old_msg_id = last_auto_messages.get(chat_id)
                    if old_msg_id:
                        await delete_telegram_message(session, chat_id, old_msg_id)
                        
                    new_msg_id = await send_telegram_message(session, chat_id, message)
                    if new_msg_id:
                        last_auto_messages[chat_id] = new_msg_id
    except asyncio.CancelledError:
        logger.info("Task tự động gửi vị thế đã bị hủy.")
    except Exception as e:
        logger.error(f"Lỗi trong auto_pos_sender_loop: {e}")

# Xử lý lệnh /auto
async def handle_auto_command(session, chat_id):
    if chat_id in auto_chats:
        auto_chats.remove(chat_id)
        
        # Xóa tin nhắn auto cuối cùng nếu có khi tắt chế độ auto
        old_msg_id = last_auto_messages.pop(chat_id, None)
        if old_msg_id:
            await delete_telegram_message(session, chat_id, old_msg_id)
            
        await send_telegram_message(session, chat_id, "❌ Đã tắt tự động cập nhật vị thế mỗi 1 phút.")
    else:
        auto_chats.add(chat_id)
        await send_telegram_message(session, chat_id, "✅ Đã bật tự động cập nhật vị thế mỗi 1 phút.")
        
        # Gửi luôn vị thế hiện tại ngay lập tức và lưu message_id làm tin nhắn auto đầu tiên
        if positions:
            text_lines = ["🔍 *TỰ ĐỘNG CẬP NHẬT VỊ THẾ ĐANG MỞ (1P)*\n----------------------------------"]
            for key, pos in positions.items():
                symbol = pos['symbol']
                side = pos['positionSide']
                amt = pos['positionAmt']
                pnl = pos['unrealizedPnL']
                
                display_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
                display_side = "LONG" if (side == 'LONG' or (side == 'BOTH' and amt > 0)) else "SHORT"
                pnl_emoji = "🟩" if pnl >= 0 else "🟥"
                sign = "+" if pnl >= 0 else ""
                
                pos_text = f"{display_symbol} ({display_side}) ➜ {pnl_emoji} *{sign}{pnl:,.2f} USDT*"
                text_lines.append(pos_text)
                
            text_lines.append("----------------------------------")
            total_pnl = sum(p.get('unrealizedPnL', 0.0) for p in positions.values())
            pnl_emoji = "🟢" if total_pnl >= 0 else "🔴"
            sign = "+" if total_pnl >= 0 else ""
            text_lines.append(f"📊 Tổng PnL: *{sign}{total_pnl:,.2f} USDT*")
            
            message = "\n\n".join(text_lines)
            
            new_msg_id = await send_telegram_message(session, chat_id, message)
            if new_msg_id:
                last_auto_messages[chat_id] = new_msg_id
        else:
            await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures nào đang mở.")

# Đăng ký Webhook với Telegram
async def setup_telegram_webhook(session):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    webhook_url = os.getenv("WEBHOOK_URL")
    
    if not webhook_url:
        logger.warning("Cảnh báo: WEBHOOK_URL trống. Bạn cần cấu hình biến này trong .env để nhận lệnh qua Webhook.")
        return
        
    url = f"https://api.telegram.org/bot{token}/setWebhook"
    payload = {"url": webhook_url}
    
    logger.info(f"Đang tự động cấu hình setWebhook Telegram tới: {webhook_url}")
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('ok'):
                    logger.info("setWebhook thành công!")
                else:
                    logger.error(f"setWebhook thất bại: {data}")
            else:
                body = await resp.text()
                logger.error(f"Lỗi setWebhook: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi khi thực hiện setWebhook: {e}")

# Xử lý lệnh /pnl
async def handle_pnl_command(session, chat_id):
    if not positions:
        await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures nào đang mở.")
        return
        
    total_pnl = sum(pos.get('unrealizedPnL', 0.0) for pos in positions.values())
    pnl_emoji = "🟩" if total_pnl >= 0 else "🟥"
    sign = "+" if total_pnl >= 0 else ""
    
    message = (
        f"📊 *TỔNG PNL VỊ THẾ HIỆN TẠI*\n"
        f"----------------------------------\n"
        f"💰 Trạng thái: {pnl_emoji} *{sign}{total_pnl:,.2f} USDT*\n"
        f"🔥 Vị thế đang mở: *{len(positions)}*"
    )
    await send_telegram_message(session, chat_id, message)

# Xử lý lệnh /pos
async def handle_pos_command(session, chat_id):
    if not positions:
        await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures nào đang mở.")
        return
        
    text_lines = ["🔍 *CHI TIẾT VỊ THẾ ĐANG MỞ*\n----------------------------------"]
    
    for key, pos in positions.items():
        symbol = pos['symbol']
        side = pos['positionSide']
        amt = pos['positionAmt']
        pnl = pos['unrealizedPnL']
        
        display_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
        display_side = "LONG" if (side == 'LONG' or (side == 'BOTH' and amt > 0)) else "SHORT"
        pnl_emoji = "🟩" if pnl >= 0 else "🟥"
        sign = "+" if pnl >= 0 else ""
        
        pos_text = f"{display_symbol} ({display_side}) ➜ {pnl_emoji} *{sign}{pnl:,.2f} USDT*"
        text_lines.append(pos_text)
        
    text_lines.append("----------------------------------")
    total_pnl = sum(p.get('unrealizedPnL', 0.0) for p in positions.values())
    pnl_emoji = "🟢" if total_pnl >= 0 else "🔴"
    sign = "+" if total_pnl >= 0 else ""
    text_lines.append(f"📊 Tổng PnL: *{sign}{total_pnl:,.2f} USDT*")
    
    message = "\n\n".join(text_lines)
    await send_telegram_message(session, chat_id, message)


# API test cho cronjob call tới
async def test_handler(request):
    return web.Response(text="Hello world")


def format_price(price):
    if price is None:
        return "Không tìm thấy"
    if price >= 1000:
        return f"{price:,.2f}".rstrip('0').rstrip('.')
    elif price >= 1:
        return f"{price:,.4f}".rstrip('0').rstrip('.')
    else:
        return f"{price:,.8f}".rstrip('0').rstrip('.')


async def get_coin_prices(session, coin_names):
    # Chuẩn hóa tên coin cần tìm
    targets = {}
    for coin in coin_names:
        coin_upper = coin.upper()
        symbol = coin_upper if coin_upper.endswith("USDT") else f"{coin_upper}USDT"
        targets[symbol] = coin_upper

    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    api_key = os.getenv("BINANCE_API_KEY")
    if api_key:
        headers["X-MBX-APIKEY"] = api_key

    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                # Tạo map nhanh các symbol với giá và % thay đổi
                prices_map = {}
                for item in data:
                    symbol = item['symbol']
                    prices_map[symbol] = {
                        'price': float(item['lastPrice']),
                        'change': float(item['priceChangePercent'])
                    }
                
                results = []
                for symbol, coin_upper in targets.items():
                    info = prices_map.get(symbol)
                    results.append((coin_upper, info))
                return results
            else:
                body = await resp.text()
                logger.error(f"Lỗi gọi API Binance 24h: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi lấy giá coin hàng loạt: {e}")
    return [(coin.upper(), None) for coin in coin_names]


async def handle_balance_command(session, chat_id):
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v2/account?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                
                wallet_bal = float(data.get('totalWalletBalance', 0))
                pnl = float(data.get('totalUnrealizedProfit', 0))
                margin_bal = float(data.get('totalMarginBalance', 0))
                avail_bal = float(data.get('availableBalance', 0))
                
                pnl_emoji = "🟩" if pnl >= 0 else "🟥"
                pnl_sign = "+" if pnl >= 0 else ""
                
                message = (
                    f"💳 *THÔNG TIN TÀI KHOẢN FUTURES*\n"
                    f"----------------------------------\n"
                    f"💰 Số dư ví: *{wallet_bal:,.2f} USDT*\n"
                    f"📊 PnL chưa thực hiện: {pnl_emoji} *{pnl_sign}{pnl:,.2f} USDT*\n"
                    f"🛡️ Số dư ký quỹ (Margin Balance): *{margin_bal:,.2f} USDT*\n"
                    f"🟢 Khả dụng vào lệnh: *{avail_bal:,.2f} USDT*"
                )
                await send_telegram_message(session, chat_id, message)
            else:
                body = await resp.text()
                logger.error(f"Lỗi lấy số dư tài khoản: HTTP {resp.status} - {body}")
                await send_telegram_message(session, chat_id, "❌ Lỗi khi truy vấn số dư từ Binance.")
    except Exception as e:
        logger.error(f"Lỗi trong handle_balance_command: {e}")
        await send_telegram_message(session, chat_id, "❌ Đã xảy ra lỗi khi lấy số dư tài khoản.")


async def handle_top_command(session, chat_id):
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    api_key = os.getenv("BINANCE_API_KEY")
    if api_key:
        headers["X-MBX-APIKEY"] = api_key

    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                
                usdt_tickers = []
                for item in data:
                    symbol = item['symbol']
                    if symbol.endswith("USDT"):
                        usdt_tickers.append({
                            'symbol': symbol[:-4],
                            'price': float(item['lastPrice']),
                            'change': float(item['priceChangePercent'])
                        })
                
                usdt_tickers.sort(key=lambda x: x['change'], reverse=True)
                
                top_gainers = usdt_tickers[:5]
                top_losers = usdt_tickers[-5:]
                top_losers.reverse()
                
                lines = ["🔥 *TOP BIẾN ĐỘNG TRONG 24H (FUTURES)*\n----------------------------------"]
                
                lines.append("🚀 *Top 5 Tăng Mạnh Nhất:*")
                for i, item in enumerate(top_gainers, 1):
                    formatted_p = format_price(item['price'])
                    lines.append(f"{i}. {item['symbol']} ➜ *{formatted_p}* (🟢 +{item['change']:.2f}%)")
                    
                lines.append("\n📉 *Top 5 Giảm Mạnh Nhất:*")
                for i, item in enumerate(top_losers, 1):
                    formatted_p = format_price(item['price'])
                    lines.append(f"{i}. {item['symbol']} ➜ *{formatted_p}* (🔴 {item['change']:.2f}%)")
                
                message = "\n".join(lines)
                await send_telegram_message(session, chat_id, message)
            else:
                body = await resp.text()
                logger.error(f"Lỗi lấy top biến động: HTTP {resp.status} - {body}")
                await send_telegram_message(session, chat_id, "❌ Lỗi khi lấy dữ liệu biến động từ Binance.")
    except Exception as e:
        logger.error(f"Lỗi trong handle_top_command: {e}")
        await send_telegram_message(session, chat_id, "❌ Đã xảy ra lỗi khi xử lý dữ liệu biến động.")


# Kiểm tra Position Mode (Hedge hay One-way) của tài khoản
async def check_position_mode(session, api_key, api_secret):
    global hedge_mode
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v1/positionSide/dual?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                hedge_mode = data.get('dualSidePosition', False)
                logger.info(f"Chế độ Position Mode của tài khoản: {'Hedge Mode (Dual)' if hedge_mode else 'One-way Mode'}")
            else:
                body = await resp.text()
                logger.error(f"Lỗi kiểm tra Position Mode: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Không thể kiểm tra Position Mode: {e}. Mặc định là One-way Mode.")


def round_down(value, decimals):
    factor = 10 ** decimals
    return math.floor(value * factor) / factor


# Nạp thông tin độ chính xác số lượng từ Binance
async def init_exchange_info(session):
    global symbol_precisions
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                for s in data.get('symbols', []):
                    symbol = s['symbol']
                    precision = int(s.get('quantityPrecision', 0))
                    symbol_precisions[symbol] = precision
                logger.info(f"Đã nạp độ chính xác số lượng cho {len(symbol_precisions)} symbol.")
            else:
                body = await resp.text()
                logger.error(f"Lỗi nạp exchangeInfo: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi khi gọi exchangeInfo: {e}")


# Lấy đòn bẩy tối đa của symbol
async def get_max_leverage(session, api_key, api_secret, symbol):
    timestamp = int(time.time() * 1000)
    query_string = f"symbol={symbol}&timestamp={timestamp}"
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v1/leverageBracket?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list) and len(data) > 0:
                    brackets = data[0].get('brackets', [])
                    if brackets:
                        return int(brackets[0].get('initialLeverage', 20))
            else:
                body = await resp.text()
                logger.error(f"Lỗi lấy max leverage cho {symbol}: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Không thể lấy max leverage cho {symbol}: {e}")
    return 20 # Mặc định trả về 20 nếu lỗi


# Cài đặt đòn bẩy
async def set_leverage(session, api_key, api_secret, symbol, leverage):
    timestamp = int(time.time() * 1000)
    query_string = f"symbol={symbol}&leverage={leverage}&timestamp={timestamp}"
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v1/leverage?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    try:
        async with session.post(url, headers=headers) as resp:
            return resp.status == 200
    except Exception as e:
        logger.error(f"Lỗi set leverage {leverage} cho {symbol}: {e}")
    return False


# Lấy giá đơn lẻ của symbol
async def get_single_price(session, symbol):
    url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                return float(data.get('price', 0))
    except Exception as e:
        logger.error(f"Lỗi lấy giá single {symbol}: {e}")
    return 0.0


async def cancel_existing_tpsl(session, api_key, api_secret, symbol, position_side=None, cancel_tp=True, cancel_sl=True):
    """
    Tìm và hủy các lệnh TP/SL đang mở để tránh lỗi trùng lặp/GTE của Binance.
    """
    timestamp = int(time.time() * 1000)
    params = [
        f"symbol={symbol}",
        "algoType=CONDITIONAL",
        f"timestamp={timestamp}"
    ]
    query_string = "&".join(params)
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v1/openAlgoOrders?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                orders = await resp.json()
                if isinstance(orders, list):
                    for order in orders:
                        order_type = order.get('type')
                        order_pos_side = order.get('positionSide', 'BOTH')
                        
                        if position_side and order_pos_side != position_side:
                            continue
                            
                        is_tp = order_type == 'TAKE_PROFIT_MARKET'
                        is_sl = order_type == 'STOP_MARKET'
                        
                        if (is_tp and cancel_tp) or (is_sl and cancel_sl):
                            algo_id = order.get('algoId')
                            if algo_id:
                                del_timestamp = int(time.time() * 1000)
                                del_query = f"symbol={symbol}&algoId={algo_id}&timestamp={del_timestamp}"
                                del_sig = get_binance_signature(del_query, api_secret)
                                del_url = f"https://fapi.binance.com/fapi/v1/algoOrder?{del_query}&signature={del_sig}"
                                
                                async with session.delete(del_url, headers=headers) as del_resp:
                                    del_data = await del_resp.json()
                                    if del_resp.status == 200:
                                        logger.info(f"Đã tự động hủy lệnh TP/SL cũ: algoId={algo_id} của {symbol}")
                                    else:
                                        logger.warning(f"Không thể hủy lệnh TP/SL cũ: {del_data.get('msg')}")
            else:
                body = await resp.text()
                logger.error(f"Lỗi lấy openAlgoOrders: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi trong cancel_existing_tpsl: {e}")


async def handle_order_command(session, chat_id, side_type, coin_name, volume_str, price_str=None, tp_price_str=None, sl_price_str=None):
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    # Chuẩn hóa coin
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    try:
        volume = float(volume_str)
        if volume <= 0:
            raise ValueError()
    except ValueError:
        await send_telegram_message(session, chat_id, "❌ Số tiền volume không hợp lệ. Vui lòng nhập số dương lớn hơn 0.")
        return

    # Xác định giá đặt lệnh (nếu có price_str thì là LIMIT, ngược lại là MARKET)
    is_limit = price_str is not None
    limit_price = 0.0
    if is_limit:
        try:
            limit_price = float(price_str)
            if limit_price <= 0:
                raise ValueError()
        except ValueError:
            await send_telegram_message(session, chat_id, "❌ Giá đặt lệnh limit không hợp lệ. Vui lòng nhập số dương lớn hơn 0.")
            return

    tp_price = None
    if tp_price_str:
        try:
            tp_price = float(tp_price_str)
            if tp_price <= 0:
                raise ValueError()
        except ValueError:
            await send_telegram_message(session, chat_id, "❌ Giá chốt lời (TP) không hợp lệ. Vui lòng nhập số dương lớn hơn 0.")
            return

    sl_price = None
    if sl_price_str:
        try:
            sl_price = float(sl_price_str)
            if sl_price <= 0:
                raise ValueError()
        except ValueError:
            await send_telegram_message(session, chat_id, "❌ Giá cắt lỗ (SL) không hợp lệ. Vui lòng nhập số dương lớn hơn 0.")
            return

    # 1. Lấy đòn bẩy tối đa (Max Leverage) và tự động thiết lập cho symbol đó
    max_leverage = await get_max_leverage(session, api_key, api_secret, symbol)
    logger.info(f"Đòn bẩy tối đa của {symbol} là {max_leverage}x. Tiến hành cài đặt...")
    
    set_lev_ok = await set_leverage(session, api_key, api_secret, symbol, max_leverage)
    if not set_lev_ok:
        logger.warning(f"Không thể set đòn bẩy {max_leverage}x cho {symbol} trên Binance. Tiếp tục với đòn bẩy mặc định của tài khoản.")
    
    # 2. Xác định giá quy đổi số lượng coin
    if is_limit:
        exchange_price = limit_price
    else:
        current_price = await get_single_price(session, symbol)
        if current_price <= 0:
            await send_telegram_message(session, chat_id, f"❌ Không thể lấy giá hiện tại của {symbol} để quy đổi số lượng coin.")
            return
        exchange_price = current_price
        
    # 3. Tính toán số lượng coin (quantity = volume / exchange_price)
    raw_qty = volume / exchange_price
    
    # Lấy độ chính xác số lượng (quantityPrecision) từ cache exchangeInfo
    precision = symbol_precisions.get(symbol, 3)
    quantity = round_down(raw_qty, precision)
    
    if quantity <= 0:
        await send_telegram_message(
            session, 
            chat_id, 
            f"❌ Số lượng coin tính toán quá nhỏ ({raw_qty:.8f} {coin_name}).\n"
            f"Vui lòng tăng Volume đặt lệnh hoặc chọn coin có giá thấp hơn.\n"
            f"(Độ chính xác yêu cầu: {precision} số thập phân)"
        )
        return

    # Xác định side và positionSide dựa trên hedge_mode
    if side_type == 'LONG':
        side = 'BUY'
        pos_side = 'LONG' if hedge_mode else 'BOTH'
    else:
        side = 'SELL'
        pos_side = 'SHORT' if hedge_mode else 'BOTH'
        
    timestamp = int(time.time() * 1000)
    
    # Các tham số cho API đặt lệnh
    params = [
        f"symbol={symbol}",
        f"side={side}",
        f"type={'LIMIT' if is_limit else 'MARKET'}",
        f"quantity={quantity}",
        f"timestamp={timestamp}"
    ]
    if is_limit:
        params.append(f"price={limit_price}")
        params.append("timeInForce=GTC")
        
    if hedge_mode:
        params.append(f"positionSide={pos_side}")
        
    query_string = "&".join(params)
    signature = get_binance_signature(query_string, api_secret)
    
    url = f"https://fapi.binance.com/fapi/v1/order?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    try:
        async with session.post(url, headers=headers) as resp:
            data = await resp.json()
            if resp.status == 200:
                order_id = data.get('orderId')
                pnl_emoji = "🟢" if side_type == 'LONG' else "🔴"
                
                if is_limit:
                    msg = (
                        f"⏳ *TẠO LỆNH LIMIT THÀNH CÔNG!*\n"
                        f"----------------------------------\n"
                        f"🪙 Cặp: *{symbol}*\n"
                        f"⚡ Lệnh: {pnl_emoji} *{side_type} (LIMIT)*\n"
                        f"⚙️ Đòn bẩy áp dụng: *{max_leverage}x* (Tối đa)\n"
                        f"💵 Giá đặt Limit: *{limit_price:,.4f} USDT*\n"
                        f"📊 Volume lệnh: *{volume:,.2f} USDT*\n"
                        f"🔢 Số lượng: *{quantity} {coin_name}*\n"
                        f"🆔 Order ID: `{order_id}`"
                    )
                else:
                    avg_price = float(data.get('avgPrice', 0))
                    execute_qty = float(data.get('executedQty', 0))
                    actual_volume = execute_qty * avg_price
                    actual_margin = actual_volume / max_leverage
                    
                    msg = (
                        f"✅ *VÀO LỆNH MARKET THÀNH CÔNG!*\n"
                        f"----------------------------------\n"
                        f"🪙 Cặp: *{symbol}*\n"
                        f"⚡ Lệnh: {pnl_emoji} *{side_type} (MARKET)*\n"
                        f"⚙️ Đòn bẩy áp dụng: *{max_leverage}x* (Tối đa)\n"
                        f"📊 Volume khớp: *{actual_volume:,.2f} USDT*\n"
                        f"💵 Kí quỹ ước tính (Margin): ~*{actual_margin:,.4f} USDT*\n"
                        f"🔢 Số lượng: *{execute_qty} {coin_name}*\n"
                        f"💵 Giá khớp trung bình: *{avg_price:,.4f} USDT*\n"
                        f"🆔 Order ID: `{order_id}`"
                    )
                
                # Tự động hủy TP/SL cũ để tránh lỗi GTE của Binance
                if tp_price is not None or sl_price is not None:
                    await cancel_existing_tpsl(
                        session, 
                        api_key, 
                        api_secret, 
                        symbol, 
                        position_side=pos_side, 
                        cancel_tp=(tp_price is not None), 
                        cancel_sl=(sl_price is not None)
                    )

                tpsl_side = 'SELL' if side_type == 'LONG' else 'BUY'
                tp_sl_msg_parts = []
                
                # Cài đặt TP nếu có
                if tp_price is not None:
                    timestamp_tp = int(time.time() * 1000)
                    tp_params = [
                        f"symbol={symbol}",
                        f"side={tpsl_side}",
                        "type=TAKE_PROFIT_MARKET",
                        f"triggerPrice={tp_price}",
                        "algoType=CONDITIONAL",
                        f"timestamp={timestamp_tp}"
                    ]
                    if is_limit:
                        tp_params.append(f"quantity={quantity}")
                        tp_params.append("reduceOnly=true")
                    else:
                        tp_params.append("closePosition=true")
                        
                    if hedge_mode:
                        tp_params.append(f"positionSide={pos_side}")
                        
                    tp_query = "&".join(tp_params)
                    tp_sig = get_binance_signature(tp_query, api_secret)
                    tp_url = f"https://fapi.binance.com/fapi/v1/algoOrder?{tp_query}&signature={tp_sig}"
                    
                    try:
                        async with session.post(tp_url, headers=headers) as tp_resp:
                            tp_data = await tp_resp.json()
                            if tp_resp.status == 200:
                                tp_id = tp_data.get('orderId') or tp_data.get('algoId')
                                tp_sl_msg_parts.append(f"🎯 *TP:* Chốt lời ở giá *{tp_price:,.4f}* (Thành công, ID: `{tp_id}`)")
                            else:
                                tp_err = tp_data.get('msg', 'Lỗi không xác định')
                                tp_sl_msg_parts.append(f"❌ *Lỗi đặt TP:* `{tp_err}`")
                    except Exception as e:
                        tp_sl_msg_parts.append(f"❌ *Lỗi đặt TP:* `{e}`")

                # Cài đặt SL nếu có
                if sl_price is not None:
                    timestamp_sl = int(time.time() * 1000)
                    sl_params = [
                        f"symbol={symbol}",
                        f"side={tpsl_side}",
                        "type=STOP_MARKET",
                        f"triggerPrice={sl_price}",
                        "algoType=CONDITIONAL",
                        f"timestamp={timestamp_sl}"
                    ]
                    if is_limit:
                        sl_params.append(f"quantity={quantity}")
                        sl_params.append("reduceOnly=true")
                    else:
                        sl_params.append("closePosition=true")
                        
                    if hedge_mode:
                        sl_params.append(f"positionSide={pos_side}")
                        
                    sl_query = "&".join(sl_params)
                    sl_sig = get_binance_signature(sl_query, api_secret)
                    sl_url = f"https://fapi.binance.com/fapi/v1/algoOrder?{sl_query}&signature={sl_sig}"
                    
                    try:
                        async with session.post(sl_url, headers=headers) as sl_resp:
                            sl_data = await sl_resp.json()
                            if sl_resp.status == 200:
                                sl_id = sl_data.get('orderId') or sl_data.get('algoId')
                                tp_sl_msg_parts.append(f"🛡️ *SL:* Cắt lỗ ở giá *{sl_price:,.4f}* (Thành công, ID: `{sl_id}`)")
                            else:
                                sl_err = sl_data.get('msg', 'Lỗi không xác định')
                                tp_sl_msg_parts.append(f"❌ *Lỗi đặt SL:* `{sl_err}`")
                    except Exception as e:
                        tp_sl_msg_parts.append(f"❌ *Lỗi đặt SL:* `{e}`")

                if tp_sl_msg_parts:
                    msg += "\n\n" + "\n".join(tp_sl_msg_parts)

                await send_telegram_message(session, chat_id, msg)
            else:
                msg_err = data.get('msg', 'Lỗi không xác định')
                code_err = data.get('code', -1)
                await send_telegram_message(session, chat_id, f"❌ *Đặt lệnh thất bại!*\nBinance báo lỗi: `{msg_err}` (Code: {code_err})")
    except Exception as e:
        logger.error(f"Lỗi khi đặt lệnh {side_type} {symbol}: {e}")
        await send_telegram_message(session, chat_id, f"❌ Đã xảy ra lỗi hệ thống khi đặt lệnh: {e}")


async def handle_leverage_command(session, chat_id, coin_name, leverage_str):
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    # Chuẩn hóa coin
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    try:
        leverage = int(leverage_str)
        if leverage < 1 or leverage > 125:
            raise ValueError()
    except ValueError:
        await send_telegram_message(session, chat_id, "❌ Hệ số đòn bẩy không hợp lệ. Vui lòng nhập số nguyên từ 1 đến 125.")
        return
        
    timestamp = int(time.time() * 1000)
    query_string = f"symbol={symbol}&leverage={leverage}&timestamp={timestamp}"
    signature = get_binance_signature(query_string, api_secret)
    
    url = f"https://fapi.binance.com/fapi/v1/leverage?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    try:
        async with session.post(url, headers=headers) as resp:
            data = await resp.json()
            if resp.status == 200:
                ret_leverage = data.get('leverage')
                await send_telegram_message(
                    session, 
                    chat_id, 
                    f"✅ *CÀI ĐẶT ĐỒN BẨY THÀNH CÔNG!*\n"
                    f"----------------------------------\n"
                    f"🪙 Cặp: *{symbol}*\n"
                    f"⚙️ Đòn bẩy mới: *{ret_leverage}x*"
                )
            else:
                msg_err = data.get('msg', 'Lỗi không xác định')
                code_err = data.get('code', -1)
                await send_telegram_message(session, chat_id, f"❌ *Cài đặt đòn bẩy thất bại!*\nBinance báo lỗi: `{msg_err}` (Code: {code_err})")
    except Exception as e:
        logger.error(f"Lỗi khi cài đặt đòn bẩy cho {symbol}: {e}")
        await send_telegram_message(session, chat_id, f"❌ Đã xảy ra lỗi hệ thống khi cài đặt đòn bẩy: {e}")


async def handle_orders_command(session, chat_id):
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v1/openOrders?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    try:
        # 1. Gọi API lấy toàn bộ giá coin hiện tại để map với danh sách lệnh chờ
        prices_map = {}
        try:
            url_price = "https://fapi.binance.com/fapi/v1/ticker/price"
            async with session.get(url_price) as resp_price:
                if resp_price.status == 200:
                    price_data = await resp_price.json()
                    prices_map = {item['symbol']: float(item['price']) for item in price_data}
        except Exception as e:
            logger.error(f"Lỗi lấy giá hiện tại khi xem orders: {e}")

        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                
                if not data:
                    await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có lệnh chờ (Open Orders) nào trên tài khoản Futures.")
                    return
                
                lines = ["⏳ *DANH SÁCH LỆNH ĐANG CHỜ KHỚP*\n----------------------------------"]
                for i, order in enumerate(data, 1):
                    symbol = order.get('symbol')
                    order_id = order.get('orderId')
                    price = float(order.get('price', 0))
                    qty = float(order.get('origQty', 0))
                    side = order.get('side')
                    pos_side = order.get('positionSide', 'BOTH')
                    order_type = order.get('type')
                    
                    display_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
                    
                    if pos_side == 'LONG':
                        display_side = "LONG"
                    elif pos_side == 'SHORT':
                        display_side = "SHORT"
                    else:
                        display_side = "LONG" if side == 'BUY' else "SHORT"
                        
                    emoji = "🟢" if display_side == 'LONG' else "🔴"
                    notional = qty * price
                    current_price = prices_map.get(symbol)
                    
                    price_line = f"   • Giá đặt: *{price:,.4f} USDT*\n"
                    if current_price is not None:
                        price_line += f"   • Giá hiện tại: *{current_price:,.4f} USDT*\n"
                        
                    lines.append(
                        f"{i}. {display_symbol} ({emoji} *{display_side} - {order_type}*)\n"
                        f"{price_line}"
                        f"   • Số lượng: *{qty}* (~*{notional:,.2f} USDT*)\n"
                        f"   • ID: `{order_id}`\n"
                    )
                
                message = "\n".join(lines)
                await send_telegram_message(session, chat_id, message)
            else:
                body = await resp.text()
                logger.error(f"Lỗi lấy danh sách lệnh chờ: HTTP {resp.status} - {body}")
                await send_telegram_message(session, chat_id, "❌ Lỗi khi truy vấn danh sách lệnh từ Binance.")
    except Exception as e:
        logger.error(f"Lỗi trong handle_orders_command: {e}")
        await send_telegram_message(session, chat_id, "❌ Đã xảy ra lỗi hệ thống khi lấy danh sách lệnh chờ.")


async def handle_tpsl_command(session, chat_id, coin_name, tp_price_str=None, sl_price_str=None):
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    target_positions = [pos for pos in positions.values() if pos['symbol'] == symbol]
    
    if not target_positions:
        await send_telegram_message(
            session, 
            chat_id, 
            f"❌ Không tìm thấy vị thế *{symbol}* nào đang mở để cài đặt TP/SL."
        )
        return

    tp_price = None
    if tp_price_str:
        try:
            tp_price = float(tp_price_str)
            if tp_price <= 0:
                raise ValueError()
        except ValueError:
            await send_telegram_message(session, chat_id, "❌ Giá chốt lời (TP) không hợp lệ. Vui lòng nhập số dương lớn hơn 0.")
            return

    sl_price = None
    if sl_price_str:
        try:
            sl_price = float(sl_price_str)
            if sl_price <= 0:
                raise ValueError()
        except ValueError:
            await send_telegram_message(session, chat_id, "❌ Giá cắt lỗ (SL) không hợp lệ. Vui lòng nhập số dương lớn hơn 0.")
            return

    results = []
    headers = {"X-MBX-APIKEY": api_key}
    
    for pos in target_positions:
        side = pos['positionSide']
        amt = pos['positionAmt']
        
        is_long = amt > 0
        if side == 'LONG':
            is_long = True
        elif side == 'SHORT':
            is_long = False
            
        order_side = 'SELL' if is_long else 'BUY'
        pos_display = 'LONG' if is_long else 'SHORT'
        
        # Tự động hủy TP/SL cũ để tránh lỗi GTE của Binance
        if tp_price is not None or sl_price is not None:
            await cancel_existing_tpsl(
                session, 
                api_key, 
                api_secret, 
                symbol, 
                position_side=side, 
                cancel_tp=(tp_price is not None), 
                cancel_sl=(sl_price is not None)
            )
            
        if tp_price is not None:
            timestamp = int(time.time() * 1000)
            params = [
                f"symbol={symbol}",
                f"side={order_side}",
                "type=TAKE_PROFIT_MARKET",
                f"triggerPrice={tp_price}",
                "algoType=CONDITIONAL",
                "closePosition=true",
                f"timestamp={timestamp}"
            ]
            if hedge_mode:
                params.append(f"positionSide={side}")
                
            query_string = "&".join(params)
            signature = get_binance_signature(query_string, api_secret)
            url = f"https://fapi.binance.com/fapi/v1/algoOrder?{query_string}&signature={signature}"
            
            try:
                async with session.post(url, headers=headers) as resp:
                    data = await resp.json()
                    if resp.status == 200:
                        order_id = data.get('orderId') or data.get('algoId')
                        results.append(f"   • TP (*{pos_display}* tại giá *{tp_price:,.4f}*): 🟢 Thành công (ID: `{order_id}`)")
                    else:
                        msg_err = data.get('msg', 'Lỗi không xác định')
                        results.append(f"   • TP (*{pos_display}* tại giá *{tp_price:,.4f}*): 🔴 Thất bại: `{msg_err}`")
            except Exception as e:
                results.append(f"   • TP (*{pos_display}*): 🔴 Lỗi kết nối: {e}")
                
        if sl_price is not None:
            timestamp = int(time.time() * 1000)
            params = [
                f"symbol={symbol}",
                f"side={order_side}",
                "type=STOP_MARKET",
                f"triggerPrice={sl_price}",
                "algoType=CONDITIONAL",
                "closePosition=true",
                f"timestamp={timestamp}"
            ]
            if hedge_mode:
                params.append(f"positionSide={side}")
                
            query_string = "&".join(params)
            signature = get_binance_signature(query_string, api_secret)
            url = f"https://fapi.binance.com/fapi/v1/algoOrder?{query_string}&signature={signature}"
            
            try:
                async with session.post(url, headers=headers) as resp:
                    data = await resp.json()
                    if resp.status == 200:
                        order_id = data.get('orderId') or data.get('algoId')
                        results.append(f"   • SL (*{pos_display}* tại giá *{sl_price:,.4f}*): 🟢 Thành công (ID: `{order_id}`)")
                    else:
                        msg_err = data.get('msg', 'Lỗi không xác định')
                        results.append(f"   • SL (*{pos_display}* tại giá *{sl_price:,.4f}*): 🔴 Thất bại: `{msg_err}`")
            except Exception as e:
                results.append(f"   • SL (*{pos_display}*): 🔴 Lỗi kết nối: {e}")
                
    msg = (
        f"🎯 *KẾT QUẢ CÀI ĐẶT TP/SL CHO {symbol}*\n"
        f"----------------------------------\n" +
        "\n".join(results)
    )
    await send_telegram_message(session, chat_id, msg)


async def handle_cancel_command(session, chat_id, coin_name, order_id_str):
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    try:
        order_id = int(order_id_str)
    except ValueError:
        await send_telegram_message(session, chat_id, "❌ Order ID không hợp lệ. Vui lòng nhập số nguyên.")
        return
        
    timestamp = int(time.time() * 1000)
    query_string = f"symbol={symbol}&orderId={order_id}&timestamp={timestamp}"
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v1/order?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    try:
        async with session.delete(url, headers=headers) as resp:
            data = await resp.json()
            if resp.status == 200:
                await send_telegram_message(
                    session, 
                    chat_id, 
                    f"✅ *HỦY LỆNH THÀNH CÔNG!*\n"
                    f"----------------------------------\n"
                    f"🪙 Cặp: *{symbol}*\n"
                    f"🆔 Order ID đã hủy: `{order_id}`"
                )
            else:
                msg_err = data.get('msg', 'Lỗi không xác định')
                code_err = data.get('code', -1)
                await send_telegram_message(session, chat_id, f"❌ *Hủy lệnh thất bại!*\nBinance báo lỗi: `{msg_err}` (Code: {code_err})")
    except Exception as e:
        logger.error(f"Lỗi khi hủy lệnh {order_id} của {symbol}: {e}")
        await send_telegram_message(session, chat_id, f"❌ Đã xảy ra lỗi hệ thống khi hủy lệnh: {e}")


# Webhook Handler nhận POST từ Telegram
async def telegram_webhook_handler(request):
    try:
        data = await request.json()
    except Exception as e:
        logger.error(f"Lỗi parse JSON webhook body: {e}")
        return web.Response(status=400)
        
    message = data.get('message')
    if not message:
        return web.Response(status=200)
        
    chat = message.get('chat')
    if not chat:
        return web.Response(status=200)
        
    chat_id = chat.get('id')
    text = message.get('text', '').strip()
    
    if not text:
        return web.Response(status=200)
        
    # Nếu tin nhắn không bắt đầu bằng '/', coi đó là danh sách các coin cần lấy giá
    if not text.startswith('/'):
        coins = text.split()
        if coins:
            results = await get_coin_prices(request.app['session'], coins)
            
            response_lines = []
            for coin_name, info in results:
                if info is not None:
                    price = info['price']
                    change = info['change']
                    formatted = format_price(price)
                    emoji = "🟢" if change >= 0 else "🔴"
                    sign = "+" if change >= 0 else ""
                    response_lines.append(f"{coin_name.upper()}: {formatted} ({emoji} {sign}{change:.2f}%)")
                else:
                    response_lines.append(f"{coin_name.upper()}: Không tìm thấy")
            
            if response_lines:
                await send_telegram_message(request.app['session'], chat_id, "\n".join(response_lines))
        return web.Response(status=200)
        
    command = text.split()[0].lower()
    command_base = command.split('@')[0]
    
    if command_base in ('/start', '/help'):
        welcome_text = (
            "Chào mừng bạn đến với Binance Futures PnL Bot!\n\n"
            "Các câu lệnh hỗ trợ:\n"
            "📊 `/pnl` - Xem tổng PnL hiện tại.\n"
            "🔍 `/pos` - Xem chi tiết các vị thế đang mở.\n"
            "💳 `/balance` (hoặc `/wallet`) - Xem số dư tài khoản & ví Futures.\n"
            "🔥 `/top` (hoặc `/gainers`) - Top 5 tăng/giảm mạnh nhất 24h.\n"
            "⚙️ `/leverage <coin> <hệ_số>` (hoặc `/lev`) - Cài đặt đòn bẩy.\n"
            "⏳ `/orders` - Xem danh sách lệnh đang chờ khớp.\n"
            "❌ `/cancel <coin> <order_id>` - Hủy một lệnh đang chờ.\n"
            "🎯 `/tp <coin> <giá_tp>` - Cài đặt giá chốt lời (Take Profit).\n"
            "🛡️ `/sl <coin> <giá_sl>` - Cài đặt giá cắt lỗ (Stop Loss).\n"
            "🔮 `/tpsl <coin> <giá_tp> <giá_sl>` - Cài đặt đồng thời cả TP và SL.\n"
            "📈 `/long <coin> <volume> [giá]` (hoặc `/l`) - LONG (Market nếu không nhập giá, Limit nếu có giá).\n"
            "📉 `/short <coin> <volume> [giá]` (hoặc `/s`) - SHORT (Market nếu không nhập giá, Limit nếu có giá).\n"
            "⏱ `/auto` - Bật/Tắt tự động gửi vị thế mỗi 1 phút.\n\n"
            "💡 *Mẹo*:\n"
            "• Nhập trực tiếp tên coin (ví dụ: `btc` hoặc `btc eth sol`) để tra cứu giá nhanh kèm % biến động 24h.\n"
            "• Lệnh Market: `/long btc 1000` (LONG btc với volume 1000 USDT)\n"
            "• Lệnh Limit: `/long btc 1000 98000` (LONG btc với volume 1000 USDT tại giá 98000)"
        )
        await send_telegram_message(request.app['session'], chat_id, welcome_text)
        
    elif command_base == '/pnl':
        await handle_pnl_command(request.app['session'], chat_id)
        
    elif command_base == '/pos':
        await handle_pos_command(request.app['session'], chat_id)
        
    elif command_base in ('/balance', '/wallet'):
        await handle_balance_command(request.app['session'], chat_id)
        
    elif command_base in ('/top', '/gainers'):
        await handle_top_command(request.app['session'], chat_id)
        
    elif command_base == '/orders':
        await handle_orders_command(request.app['session'], chat_id)
        
    elif command_base == '/cancel':
        parts = text.split()
        if len(parts) < 3:
            await send_telegram_message(
                request.app['session'], 
                chat_id, 
                "❌ Sai cú pháp hủy lệnh!\nSử dụng: `/cancel <coin> <order_id>`\nVí dụ: `/cancel btc 1234567`"
            )
        else:
            coin_name = parts[1]
            order_id_str = parts[2]
            await handle_cancel_command(request.app['session'], chat_id, coin_name, order_id_str)
        
    elif command_base == '/tp':
        parts = text.split()
        if len(parts) < 3:
            await send_telegram_message(
                request.app['session'],
                chat_id,
                "❌ Sai cú pháp chốt lời!\nSử dụng: `/tp <coin> <giá_tp>`\nVí dụ: `/tp btc 68500`"
            )
        else:
            coin_name = parts[1]
            tp_price = parts[2]
            await handle_tpsl_command(request.app['session'], chat_id, coin_name, tp_price_str=tp_price)
            
    elif command_base == '/sl':
        parts = text.split()
        if len(parts) < 3:
            await send_telegram_message(
                request.app['session'],
                chat_id,
                "❌ Sai cú pháp cắt lỗ!\nSử dụng: `/sl <coin> <giá_sl>`\nVí dụ: `/sl btc 64000`"
            )
        else:
            coin_name = parts[1]
            sl_price = parts[2]
            await handle_tpsl_command(request.app['session'], chat_id, coin_name, sl_price_str=sl_price)
            
    elif command_base == '/tpsl':
        parts = text.split()
        if len(parts) < 4:
            await send_telegram_message(
                request.app['session'],
                chat_id,
                "❌ Sai cú pháp cài đặt TP/SL!\nSử dụng: `/tpsl <coin> <giá_tp> <giá_sl>`\nVí dụ: `/tpsl btc 68500 64000`"
            )
        else:
            coin_name = parts[1]
            tp_price = parts[2]
            sl_price = parts[3]
            await handle_tpsl_command(request.app['session'], chat_id, coin_name, tp_price_str=tp_price, sl_price_str=sl_price)
        
    elif command_base in ('/leverage', '/lev'):
        parts = text.split()
        if len(parts) < 3:
            await send_telegram_message(
                request.app['session'], 
                chat_id, 
                "❌ Sai cú pháp cài đặt đòn bẩy!\nSử dụng: `/leverage <coin> <hệ_số>`\nVí dụ: `/leverage btc 20`"
            )
        else:
            coin_name = parts[1]
            leverage_str = parts[2]
            await handle_leverage_command(request.app['session'], chat_id, coin_name, leverage_str)
        
    elif command_base in ('/long', '/l', '/short', '/s'):
        import re
        text_clean = re.sub(r'\b(tp|sl)\s*=\s*([0-9.]+)', r'\1=\2', text, flags=re.IGNORECASE)
        parts = text_clean.split()
        if len(parts) < 3:
            await send_telegram_message(
                request.app['session'], 
                chat_id, 
                "❌ Sai cú pháp đặt lệnh!\n"
                "• Lệnh Market: `/long <coin> <volume>`\n"
                "• Lệnh Limit: `/long <coin> <volume> <giá>`\n"
                "• Đi kèm TP/SL: `/long btc 400 60000 tp=65000 sl=58000` (hoặc `/long btc 400 tp=65000 sl=58000`)\n"
                "Ví dụ: `/long btc 1000` hoặc `/long btc 1000 98000`"
            )
        else:
            side_type = 'LONG' if command_base in ('/long', '/l') else 'SHORT'
            coin_name = parts[1]
            volume_str = parts[2]
            
            price_str = None
            tp_price_str = None
            sl_price_str = None
            
            for part in parts[3:]:
                part_lower = part.lower()
                if part_lower.startswith('tp='):
                    tp_price_str = part.split('=', 1)[1]
                elif part_lower.startswith('sl='):
                    sl_price_str = part.split('=', 1)[1]
                else:
                    price_str = part
                    
            await handle_order_command(
                request.app['session'], 
                chat_id, 
                side_type, 
                coin_name, 
                volume_str, 
                price_str, 
                tp_price_str, 
                sl_price_str
            )
        
    elif command_base == '/auto':
        await handle_auto_command(request.app['session'], chat_id)
        
    return web.Response(status=200)


# Lấy và log địa chỉ IP public của server
async def log_server_ip(session):
    try:
        async with session.get("https://api.ipify.org?format=json") as resp:
            if resp.status == 200:
                data = await resp.json()
                ip = data.get('ip')
                logger.info(f"👉👉 ĐỊA CHỈ IP PUBLIC CỦA SERVER RENDER LÀ: {ip} 👈👈")
                logger.info("Hãy copy IP này nhập vào phần IP access restrictions trên Binance API Key.")
            else:
                body = await resp.text()
                logger.warning(f"Không thể lấy IP public: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi khi lấy IP public của server: {e}")


# Lifecycle hooks của aiohttp
async def on_startup(app):
    app['session'] = aiohttp.ClientSession()
    
    # 0. Tự động lấy và log IP của server để cấu hình Binance
    await log_server_ip(app['session'])
    
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    # 1. Tự động setWebhook Telegram
    await setup_telegram_webhook(app['session'])
    
    # 2. Kiểm tra Position Mode (Hedge hay One-way) và lấy snapshot vị thế ban đầu từ Binance REST API
    try:
        await init_exchange_info(app['session'])
        await check_position_mode(app['session'], api_key, api_secret)
        await init_positions(app['session'], api_key, api_secret)
    except Exception as e:
        logger.error(f"Lỗi khởi tạo chế độ/vị thế ban đầu: {e}. Sẽ cập nhật lại khi có update từ WebSocket.")
        
    # 3. Chạy background tasks
    app['user_data_task'] = asyncio.create_task(
        binance_user_data_stream(app['session'], api_key)
    )
    app['mark_price_task'] = asyncio.create_task(
        binance_mark_price_stream(app['session'])
    )
    app['auto_pos_task'] = asyncio.create_task(
        auto_pos_sender_loop(app)
    )

async def on_cleanup(app):
    logger.info("Đang giải phóng tài nguyên...")
    if 'user_data_task' in app:
        app['user_data_task'].cancel()
    if 'mark_price_task' in app:
        app['mark_price_task'].cancel()
    if 'auto_pos_task' in app:
        app['auto_pos_task'].cancel()
        
    if 'session' in app:
        await app['session'].close()
    logger.info("Đã dọn dẹp hoàn tất.")

# Hàm main khởi động ứng dụng
def main():
    load_dotenv()
    
    required_env = ["TELEGRAM_BOT_TOKEN", "BINANCE_API_KEY", "BINANCE_API_SECRET"]
    missing = [env for env in required_env if not os.getenv(env)]
    if missing:
        logger.error(f"Thiếu các cấu hình bắt buộc trong file .env: {', '.join(missing)}")
        return
        
    app = web.Application()
    app.router.add_get('/test', test_handler)
    app.router.add_post('/webhook', telegram_webhook_handler)
    
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    
    port = int(os.getenv("PORT", 5000))
    logger.info(f"Khởi chạy web server lắng nghe webhook tại port {port}...")
    web.run_app(app, host='0.0.0.0', port=port)

if __name__ == '__main__':
    main()
