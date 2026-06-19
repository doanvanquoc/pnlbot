import asyncio
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

    url = "https://api.binance.com/api/v3/ticker/price"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                # Tạo map nhanh các symbol và giá của chúng
                prices_map = {item['symbol']: float(item['price']) for item in data}
                
                results = []
                for symbol, coin_upper in targets.items():
                    price = prices_map.get(symbol)
                    results.append((coin_upper, price))
                return results
            else:
                logger.error(f"Lỗi gọi API Binance: HTTP {resp.status}")
    except Exception as e:
        logger.error(f"Lỗi lấy giá coin hàng loạt: {e}")
    return [(coin.upper(), None) for coin in coin_names]


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
            for coin_name, price in results:
                if price is not None:
                    formatted = format_price(price)
                    response_lines.append(f"{coin_name.upper()}: {formatted}")
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
            "⏱ `/auto` - Bật/Tắt tự động gửi vị thế mỗi 5 phút."
        )
        await send_telegram_message(request.app['session'], chat_id, welcome_text)
        
    elif command_base == '/pnl':
        await handle_pnl_command(request.app['session'], chat_id)
        
    elif command_base == '/pos':
        await handle_pos_command(request.app['session'], chat_id)
        
    elif command_base == '/auto':
        await handle_auto_command(request.app['session'], chat_id)
        
    return web.Response(status=200)

# Lifecycle hooks của aiohttp
async def on_startup(app):
    app['session'] = aiohttp.ClientSession()
    
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    # 1. Tự động setWebhook Telegram
    await setup_telegram_webhook(app['session'])
    
    # 2. Lấy snapshot vị thế ban đầu từ Binance REST API
    try:
        await init_positions(app['session'], api_key, api_secret)
    except Exception as e:
        logger.error(f"Lỗi nạp vị thế snapshot ban đầu: {e}. Sẽ cập nhật lại khi có update từ WebSocket.")
        
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
