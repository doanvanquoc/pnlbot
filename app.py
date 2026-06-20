import asyncio
import math
import hmac
import hashlib
import time
import os
import random
import json
from datetime import datetime, timezone, timedelta
import logging
import aiohttp
from aiohttp import web
from dotenv import load_dotenv
import io
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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
has_new_activity = {}   # Đánh dấu có hoạt động mới trong chat (key: chat_id, value: bool)
notified_thresholds = {} # Các ngưỡng % đã thông báo cho từng vị thế (key: pos_key, value: set)
PRICE_ALERT_THRESHOLDS = [10, 20, 30] # Ngưỡng % biến động giá cần cảnh báo
hedge_mode = False      # Chế độ Position Mode (True: Hedge Mode, False: One-way Mode)
symbol_precisions = {}  # Lưu độ chính xác số lượng coin (quantityPrecision) của từng symbol
symbol_price_precisions = {}  # Lưu độ chính xác giá (pricePrecision) của từng symbol
symbol_tick_sizes = {}  # Lưu tickSize của từng symbol

ACTIVE_CHATS_FILE = "active_chats.json"
active_chats = set()

def load_active_chats():
    global active_chats
    try:
        if os.path.exists(ACTIVE_CHATS_FILE):
            with open(ACTIVE_CHATS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                active_chats = set(int(cid) for cid in data)
                logger.info(f"Đã tải {len(active_chats)} chat_id hoạt động từ file.")
    except Exception as e:
        logger.error(f"Lỗi khi tải active_chats: {e}")

def save_active_chats():
    try:
        with open(ACTIVE_CHATS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(active_chats), f)
    except Exception as e:
        logger.error(f"Lỗi khi lưu active_chats: {e}")


# Hàm tạo chữ ký HMAC-SHA256 cho Binance API
def get_binance_signature(query_string, secret_key):
    return hmac.new(
        secret_key.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

# Gửi tin nhắn Telegram
async def send_telegram_message(session, chat_id, text, is_auto=False):
    if not is_auto:
        has_new_activity[chat_id] = True
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

# Sửa tin nhắn Telegram
async def edit_telegram_message(session, chat_id, message_id, text):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{token}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
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
                # Không log cảnh báo nếu nội dung không đổi
                if "message is not modified" not in body:
                    logger.warning(f"Không thể sửa tin nhắn Telegram {message_id}: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi khi sửa tin nhắn Telegram: {e}")
    return None

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
            notified_thresholds.pop(key, None)
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
                                        
                        elif event_type == 'ORDER_TRADE_UPDATE':
                            order_data = data.get('o', {})
                            exec_type = order_data.get('x') # Execution Type: 'TRADE', 'CALCULATED', etc.
                            status = order_data.get('X')    # Trạng thái: 'FILLED'
                            client_order_id = order_data.get('c', '')
                            orig_type = order_data.get('ot', '')
                            
                            message = None
                            
                            # 1. Lệnh Limit/DCA Limit đặt qua Bot khớp hoàn toàn
                            if exec_type == 'TRADE' and status == 'FILLED' and client_order_id.startswith('pnlbot_'):
                                symbol = order_data.get('s')
                                side = order_data.get('S')        # BUY, SELL
                                pos_side = order_data.get('ps')   # LONG, SHORT, BOTH
                                price = float(order_data.get('ap', 0)) or float(order_data.get('p', 0))
                                qty = float(order_data.get('z', 0))
                                notional = qty * price
                                order_id = order_data.get('i')
                                
                                order_type_display = "Limit"
                                if "dca" in client_order_id:
                                    order_type_display = "DCA Limit"
                                
                                side_display = f"{side} ({pos_side})" if pos_side != 'BOTH' else side
                                
                                message = (
                                    f"┌──────────────────────────────┐\n"
                                    f"   🔔 *LỆNH KHỚP THÀNH CÔNG*\n"
                                    f"└──────────────────────────────┘\n"
                                    f"🪙 Cặp: `{symbol}`\n"
                                    f"⚡ Loại: `{order_type_display} ({side_display})`\n"
                                    f"📊 Trạng thái: 🟢 `FILLED`\n"
                                    f"💵 Giá khớp: `{format_price(price)} USDT`\n"
                                    f"🔢 Số lượng: `{qty}` (~`{notional:,.2f} USDT`)\n"
                                    f"🆔 Order ID: `{order_id}`"
                                )
                                
                            # 2. Lệnh TP/SL kích hoạt khớp hoàn toàn
                            elif exec_type == 'TRADE' and status == 'FILLED' and orig_type in ('TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'STOP', 'STOP_MARKET'):
                                symbol = order_data.get('s')
                                side = order_data.get('S')        # BUY, SELL
                                pos_side = order_data.get('ps')   # LONG, SHORT, BOTH
                                price = float(order_data.get('ap', 0)) or float(order_data.get('p', 0))
                                qty = float(order_data.get('z', 0))
                                notional = qty * price
                                order_id = order_data.get('i')
                                
                                is_tp = 'TAKE_PROFIT' in orig_type
                                pos_display = "SHORT" if side == 'BUY' else "LONG"
                                if pos_side != 'BOTH':
                                    pos_display = pos_side
                                    
                                if is_tp:
                                    message = (
                                        f"🎯🎯 *【CHỐT LỜI - TAKE PROFIT】* 🎯🎯\n"
                                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                        f"🪙 Cặp: `{symbol}`\n"
                                        f"⚡ Vị thế đóng: `{pos_display}`\n"
                                        f"📊 Trạng thái: 🟢 `FILLED`\n"
                                        f"💵 Giá khớp: `{format_price(price)} USDT`\n"
                                        f"🔢 Số lượng: `{qty}` (~`{notional:,.2f} USDT`)\n"
                                        f"🆔 Order ID: `{order_id}`"
                                    )
                                else:
                                    message = (
                                        f"🛡️🛡️ *【CẮT LỖ - STOP LOSS】* 🛡️🛡️\n"
                                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                        f"🪙 Cặp: `{symbol}`\n"
                                        f"⚡ Vị thế đóng: `{pos_display}`\n"
                                        f"📊 Trạng thái: 🔴 `FILLED`\n"
                                        f"💵 Giá khớp: `{format_price(price)} USDT`\n"
                                        f"🔢 Số lượng: `{qty}` (~`{notional:,.2f} USDT`)\n"
                                        f"🆔 Order ID: `{order_id}`"
                                    )
                                    
                            # 3. Sự kiện thanh lý vị thế
                            elif exec_type == 'CALCULATED' and status == 'FILLED':
                                symbol = order_data.get('s')
                                side = order_data.get('S')        # BUY, SELL
                                pos_side = order_data.get('ps')   # LONG, SHORT, BOTH
                                price = float(order_data.get('ap', 0)) or float(order_data.get('p', 0))
                                qty = float(order_data.get('z', 0))
                                notional = qty * price
                                order_id = order_data.get('i')
                                
                                pos_display = "SHORT" if side == 'BUY' else "LONG"
                                if pos_side != 'BOTH':
                                    pos_display = pos_side
                                    
                                message = (
                                    f"🚨🚨 *【CẢNH BÁO THANH LÝ】* 🚨🚨\n"
                                    f"💀💀💀💀💀💀💀💀💀💀💀💀💀💀\n"
                                    f"🪙 Cặp: `{symbol}`\n"
                                    f"💥 Vị thế cháy: 🔴 `{pos_display}`\n"
                                    f"💵 Giá thanh lý: `{format_price(price)} USDT`\n"
                                    f"🔢 Số lượng thanh lý: `{qty}` (~`{notional:,.2f} USDT`)\n"
                                    f"🆔 Order ID: `{order_id}`"
                                )
                                
                            # Gửi thông báo cho tất cả active_chats
                            if message and active_chats:
                                for chat_id in list(active_chats):
                                    try:
                                        await send_telegram_message(session, chat_id, message)
                                    except Exception as send_err:
                                        logger.error(f"Không thể gửi thông báo sự kiện đến {chat_id}: {send_err}")
                                        
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
                                    
                                    # Kiểm tra ngưỡng biến động giá % so với entry
                                    if entry > 0:
                                        pct_change = ((mark_price - entry) / entry) * 100 * side_sign
                                        if key not in notified_thresholds:
                                            notified_thresholds[key] = set()
                                        
                                        for threshold in PRICE_ALERT_THRESHOLDS:
                                            # Kiểm tra cả chiều lời (+) và lỗ (-)
                                            for direction in [threshold, -threshold]:
                                                if direction not in notified_thresholds[key]:
                                                    if (direction > 0 and pct_change >= direction) or (direction < 0 and pct_change <= direction):
                                                        # Chỉ đánh dấu đã thông báo khi có active_chats để gửi
                                                        if not active_chats:
                                                            continue
                                                        
                                                        display_symbol = symbol[:-4] if symbol.endswith('USDT') else symbol
                                                        display_side = 'LONG' if side_sign > 0 else 'SHORT'
                                                        
                                                        if direction > 0:
                                                            alert_msg = (
                                                                f"📈📈 *【CẢNH BÁO BIẾN ĐỘNG GIÁ】* 📈📈\n"
                                                                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                                                f"🪙 Cặp: `{display_symbol}` ({display_side})\n"
                                                                f"🟢 Giá đã tăng *+{pct_change:.1f}%* so với Entry\n"
                                                                f"💵 Entry: `{format_price(entry)} USDT`\n"
                                                                f"💵 Hiện tại: `{format_price(mark_price)} USDT`\n"
                                                                f"💰 PnL: `{'+' if pos['unrealizedPnL'] >= 0 else ''}{pos['unrealizedPnL']:,.2f} USDT`"
                                                            )
                                                        else:
                                                            alert_msg = (
                                                                f"📉📉 *【CẢNH BÁO BIẾN ĐỘNG GIÁ】* 📉📉\n"
                                                                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                                                f"🪙 Cặp: `{display_symbol}` ({display_side})\n"
                                                                f"🔴 Giá đã giảm *{pct_change:.1f}%* so với Entry\n"
                                                                f"💵 Entry: `{format_price(entry)} USDT`\n"
                                                                f"💵 Hiện tại: `{format_price(mark_price)} USDT`\n"
                                                                f"💰 PnL: `{'+' if pos['unrealizedPnL'] >= 0 else ''}{pos['unrealizedPnL']:,.2f} USDT`"
                                                            )
                                                        
                                                        sent_ok = False
                                                        for chat_id in list(active_chats):
                                                            try:
                                                                await send_telegram_message(session, chat_id, alert_msg)
                                                                sent_ok = True
                                                            except Exception:
                                                                pass
                                                        
                                                        # Chỉ đánh dấu đã thông báo nếu gửi thành công ít nhất 1 chat
                                                        if sent_ok:
                                                            notified_thresholds[key].add(direction)
                                    
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
                
                tz_vn = timezone(timedelta(hours=7))
                now_str = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")
                
                text_lines = [f"🕒 *Cập nhật lúc:* `{now_str}`\n"]
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
                
                # Gửi hoặc sửa tin nhắn cho tất cả các chat_id đã đăng ký
                for chat_id in list(auto_chats):
                    old_msg_id = last_auto_messages.get(chat_id)
                    
                    # Nếu có hoạt động mới trong chat, xóa tin nhắn PnL cũ và gửi tin mới xuống dưới cùng
                    if has_new_activity.get(chat_id, True):
                        if old_msg_id:
                            await delete_telegram_message(session, chat_id, old_msg_id)
                        
                        new_msg_id = await send_telegram_message(session, chat_id, message, is_auto=True)
                        if new_msg_id:
                            last_auto_messages[chat_id] = new_msg_id
                            has_new_activity[chat_id] = False
                    else:
                        # Nếu không có hoạt động mới, chỉnh sửa trực tiếp tin nhắn cũ
                        if old_msg_id:
                            edited_msg_id = await edit_telegram_message(session, chat_id, old_msg_id, message)
                            if edited_msg_id:
                                last_auto_messages[chat_id] = edited_msg_id
                            else:
                                new_msg_id = await send_telegram_message(session, chat_id, message, is_auto=True)
                                if new_msg_id:
                                    last_auto_messages[chat_id] = new_msg_id
                                    has_new_activity[chat_id] = False
                        else:
                            new_msg_id = await send_telegram_message(session, chat_id, message, is_auto=True)
                            if new_msg_id:
                                last_auto_messages[chat_id] = new_msg_id
                                has_new_activity[chat_id] = False
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
            
            new_msg_id = await send_telegram_message(session, chat_id, message, is_auto=True)
            if new_msg_id:
                last_auto_messages[chat_id] = new_msg_id
                has_new_activity[chat_id] = False
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


# Nạp thông tin độ chính xác từ Binance
async def init_exchange_info(session):
    global symbol_precisions, symbol_price_precisions, symbol_tick_sizes
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                for s in data.get('symbols', []):
                    sym = s['symbol']
                    symbol_precisions[sym] = int(s.get('quantityPrecision', 0))
                    symbol_price_precisions[sym] = int(s.get('pricePrecision', 0))
                    
                    # Tìm tickSize trong PRICE_FILTER
                    tick_size = 0.0
                    for f in s.get('filters', []):
                        if f.get('filterType') == 'PRICE_FILTER':
                            tick_size = float(f.get('tickSize', 0))
                            break
                    if tick_size <= 0:
                        tick_size = 10 ** (-int(s.get('pricePrecision', 0)))
                    symbol_tick_sizes[sym] = tick_size
                logger.info(f"Đã nạp độ chính xác số lượng ({len(symbol_precisions)}), giá ({len(symbol_price_precisions)}) và tickSize ({len(symbol_tick_sizes)}) từ Binance.")
            else:
                body = await resp.text()
                logger.error(f"Lỗi nạp exchangeInfo: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi khi gọi exchangeInfo: {e}")


def round_price_step(price, tick_size, price_precision):
    """
    Làm tròn giá về bội số gần nhất của tick_size để không bị lỗi -4014.
    """
    if tick_size <= 0:
        return round(price, price_precision)
    rounded = round(round(price / tick_size) * tick_size, price_precision)
    return rounded


async def get_symbol_precisions(session, symbol):
    """
    Trả về (quantityPrecision, pricePrecision, tickSize) của symbol.
    Nếu chưa có trong cache, sẽ gọi API exchangeInfo để nạp lại.
    """
    qty_p = symbol_precisions.get(symbol)
    price_p = symbol_price_precisions.get(symbol)
    tick_size = symbol_tick_sizes.get(symbol)
    
    if qty_p is None or price_p is None or tick_size is None:
        await init_exchange_info(session)
        qty_p = symbol_precisions.get(symbol, 3)
        price_p = symbol_price_precisions.get(symbol, 4)
        tick_size = symbol_tick_sizes.get(symbol, 10 ** (-price_p))
        
    return qty_p, price_p, tick_size


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


def calculate_tpsl_price(input_str, entry_price, quantity, leverage, is_long, is_tp):
    """
    Tính toán giá TP/SL tuyệt đối dựa trên giá trị nhập vào:
    - Suffix '%': phần trăm biến động giá (vd: '5%')
    - Suffix 'r' hoặc 'roe': phần trăm ROE (vd: '100r', '50roe')
    - Suffix 'u' hoặc 'usdt': số tiền USDT PnL tuyệt đối (vd: '20u', '50usdt')
    - Raw number: Giá tuyệt đối (vd: '68500')
    """
    input_str = input_str.strip().lower()
    
    # 1. ROE %: vd "100r", "50roe"
    if input_str.endswith('roe') or input_str.endswith('r'):
        clean_str = input_str.replace('roe', '').replace('r', '').replace('%', '').strip()
        roe_val = abs(float(clean_str))
        # Price Change % = ROE / Leverage
        price_change_pct = (roe_val / leverage) / 100.0
        if is_tp:
            if is_long:
                return entry_price * (1 + price_change_pct)
            else:
                return entry_price * (1 - price_change_pct)
        else:
            if is_long:
                return entry_price * (1 - price_change_pct)
            else:
                return entry_price * (1 + price_change_pct)
                
    # 2. % Biến động giá: vd "5%"
    elif input_str.endswith('%') or input_str.endswith('pct'):
        clean_str = input_str.replace('%', '').replace('pct', '').strip()
        pct_val = abs(float(clean_str)) / 100.0
        if is_tp:
            if is_long:
                return entry_price * (1 + pct_val)
            else:
                return entry_price * (1 - pct_val)
        else:
            if is_long:
                return entry_price * (1 - pct_val)
            else:
                return entry_price * (1 + pct_val)
                
    # 3. USDT PnL: vd "50u", "10u"
    elif input_str.endswith('u') or input_str.endswith('usdt'):
        clean_str = input_str.replace('usdt', '').replace('u', '').strip()
        pnl_val = abs(float(clean_str))
        if quantity <= 0:
            raise ValueError("Số lượng phải lớn hơn 0 để tính theo USDT PnL.")
        price_diff = pnl_val / quantity
        if is_tp:
            if is_long:
                return entry_price + price_diff
            else:
                return entry_price - price_diff
        else:
            if is_long:
                return entry_price - price_diff
            else:
                return entry_price + price_diff
                
    # 4. Giá tuyệt đối
    else:
        return float(input_str)


async def draw_candlestick_chart(session, symbol, interval):
    """
    Lấy dữ liệu nến từ Binance Futures và vẽ biểu đồ candlestick lưu vào BytesIO.
    """
    # 1. Gọi API lấy dữ liệu klines (mặc định lấy 80 nến để hiển thị đẹp nhất)
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit=80"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise Exception(f"Binance API trả về lỗi HTTP {resp.status}: {body}")
            
            klines_data = await resp.json()
            if not isinstance(klines_data, list) or len(klines_data) == 0:
                raise Exception("Dữ liệu nến trống hoặc không hợp lệ từ Binance.")
    except Exception as e:
        logger.error(f"Lỗi lấy klines cho {symbol}: {e}")
        raise e

    # 2. Xử lý dữ liệu nến bằng pandas
    df = pd.DataFrame(klines_data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base', 'taker_buy_quote', 'ignore'
    ])
    
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)
    
    up_color = '#0ecb81'   # Binance Green
    down_color = '#f6465d' # Binance Red
    df['color'] = df.apply(lambda row: up_color if row['close'] >= row['open'] else down_color, axis=1)

    # Tính độ rộng của cột (width) dựa trên khoảng cách giữa các nến (đơn vị ngày trong matplotlib)
    if len(df) > 1:
        diff_sec = (df['open_time'].iloc[1] - df['open_time'].iloc[0]).total_seconds()
        width = (diff_sec / 86400.0) * 0.7
    else:
        width = 0.0005

    # 3. Vẽ biểu đồ bằng matplotlib
    plt.style.use('dark_background')
    fig, (ax, ax_vol) = plt.subplots(
        2, 1, figsize=(10, 6), sharex=True,
        gridspec_kw={'height_ratios': [3, 1]}
    )
    fig.subplots_adjust(hspace=0.05)

    # Vẽ râu nến (shadows)
    ax.vlines(df['open_time'], df['low'], df['high'], color=df['color'], linewidth=1)
    
    # Vẽ thân nến (bodies)
    bottoms = df[['open', 'close']].min(axis=1)
    heights = (df['close'] - df['open']).abs()
    
    # Xử lý nến doji hoặc nến có open == close
    zero_height_mask = heights == 0
    if zero_height_mask.any():
        mini_height = (df['high'] - df['low']) * 0.03
        mini_height = mini_height.where(mini_height > 0, 0.0001)
        heights = heights.where(~zero_height_mask, mini_height)
        
    ax.bar(df['open_time'], heights, bottom=bottoms, width=width, color=df['color'], edgecolor=df['color'], linewidth=0.5)
    
    # Vẽ volume
    ax_vol.bar(df['open_time'], df['volume'], width=width, color=df['color'])

    # 4. Định dạng biểu đồ
    ax.set_title(f"📊 {symbol} ({interval.upper()}) - Binance Futures", fontsize=14, color='white', fontweight='bold', pad=15)
    ax.grid(True, color='#2F3336', linestyle='--', linewidth=0.5)
    ax_vol.grid(True, color='#2F3336', linestyle='--', linewidth=0.5)
    
    for s in ['top', 'right', 'left', 'bottom']:
        ax.spines[s].set_color('#2f3336')
        ax_vol.spines[s].set_color('#2f3336')
        
    ax.tick_params(colors='white', labelsize=10)
    ax_vol.tick_params(colors='white', labelsize=10)
    
    # Đưa nhãn trục Y của giá sang bên phải
    ax.yaxis.tick_right()
    ax.yaxis.set_label_position("right")
    ax_vol.yaxis.tick_right()
    
    # Tự động định dạng thời gian trên trục X
    if 'm' in interval.lower() or 'h' in interval.lower():
        date_format = mdates.DateFormatter('%m-%d %H:%M')
    else:
        date_format = mdates.DateFormatter('%Y-%m-%d')
    ax_vol.xaxis.set_major_formatter(date_format)
    fig.autofmt_xdate()

    # 5. Xuất hình ảnh ra BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=120)
    buf.seek(0)
    plt.close(fig)
    return buf


async def send_telegram_photo(session, chat_id, photo_bytes, caption=None):
    """
    Gửi ảnh đến Telegram chat bằng API sendPhoto.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    
    data = aiohttp.FormData()
    data.add_field('chat_id', str(chat_id))
    data.add_field('photo', photo_bytes, filename='chart.png', content_type='image/png')
    if caption:
        data.add_field('caption', caption)
        data.add_field('parse_mode', 'Markdown')
        
    try:
        async with session.post(url, data=data) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.error(f"Lỗi gửi ảnh Telegram: HTTP {resp.status} - {body}")
                return False
            return True
    except Exception as e:
        logger.error(f"Lỗi kết nối khi gửi ảnh: {e}")
        return False


async def cancel_existing_tpsl(session, api_key, api_secret, symbol, position_side=None, cancel_tp=True, cancel_sl=True):
    """
    Tìm và hủy các lệnh TP/SL đang mở (bao gồm cả Algo Orders và Regular Orders) để tránh lỗi trùng lặp/GTE của Binance.
    """
    timestamp = int(time.time() * 1000)
    headers = {"X-MBX-APIKEY": api_key}
    
    # 1. Hủy các lệnh điều kiện của Algo Service
    params = [
        f"symbol={symbol}",
        "algoType=CONDITIONAL",
        f"timestamp={timestamp}"
    ]
    query_string = "&".join(params)
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v1/openAlgoOrders?{query_string}&signature={signature}"
    
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                orders = await resp.json()
                if isinstance(orders, list):
                    for order in orders:
                        order_type = (order.get('orderType') or order.get('type') or '').upper()
                        order_pos_side = order.get('positionSide', 'BOTH')
                        
                        if position_side and order_pos_side != position_side:
                            continue
                            
                        is_tp = 'TAKE_PROFIT' in order_type
                        is_sl = 'STOP' in order_type
                        
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
                                        logger.info(f"Đã tự động hủy lệnh Algo TP/SL cũ: algoId={algo_id} của {symbol}")
                                    else:
                                        logger.warning(f"Không thể hủy lệnh Algo TP/SL cũ: {del_data.get('msg')}")
            else:
                body = await resp.text()
                logger.error(f"Lỗi lấy openAlgoOrders: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi trong cancel_existing_tpsl (Algo): {e}")

    # 2. Hủy các lệnh dừng/chốt lời thông thường (Regular Orders)
    try:
        timestamp_reg = int(time.time() * 1000)
        params_reg = [
            f"symbol={symbol}",
            f"timestamp={timestamp_reg}"
        ]
        query_reg = "&".join(params_reg)
        sig_reg = get_binance_signature(query_reg, api_secret)
        url_reg = f"https://fapi.binance.com/fapi/v1/openOrders?{query_reg}&signature={sig_reg}"
        
        async with session.get(url_reg, headers=headers) as resp_reg:
            if resp_reg.status == 200:
                orders_reg = await resp_reg.json()
                if isinstance(orders_reg, list):
                    for order in orders_reg:
                        order_type = (order.get('type') or order.get('origType') or '').upper()
                        order_pos_side = order.get('positionSide', 'BOTH')
                        
                        if position_side and order_pos_side != position_side:
                            continue
                            
                        is_tp = 'TAKE_PROFIT' in order_type
                        is_sl = 'STOP' in order_type
                        
                        if (is_tp and cancel_tp) or (is_sl and cancel_sl):
                            order_id = order.get('orderId')
                            if order_id:
                                del_timestamp = int(time.time() * 1000)
                                del_query = f"symbol={symbol}&orderId={order_id}&timestamp={del_timestamp}"
                                del_sig = get_binance_signature(del_query, api_secret)
                                del_url = f"https://fapi.binance.com/fapi/v1/order?{del_query}&signature={del_sig}"
                                
                                async with session.delete(del_url, headers=headers) as del_resp:
                                    del_data = await del_resp.json()
                                    if del_resp.status == 200:
                                        logger.info(f"Đã tự động hủy lệnh Regular TP/SL cũ: orderId={order_id} của {symbol}")
                                    else:
                                        logger.warning(f"Không thể hủy lệnh Regular TP/SL cũ: {del_data.get('msg')}")
            else:
                body = await resp_reg.text()
                logger.error(f"Lỗi lấy openOrders: HTTP {resp_reg.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi trong cancel_existing_tpsl (Regular): {e}")


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

    # Lấy độ chính xác số lượng và giá của symbol
    qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)

    # Xác định giá đặt lệnh (nếu có price_str thì là LIMIT, ngược lại là MARKET)
    is_limit = price_str is not None
    limit_price = 0.0
    if is_limit:
        try:
            limit_price = float(price_str)
            if limit_price <= 0:
                raise ValueError()
            limit_price = round_price_step(limit_price, tick_size, price_p)
        except ValueError:
            await send_telegram_message(session, chat_id, "❌ Giá đặt lệnh limit không hợp lệ. Vui lòng nhập số dương lớn hơn 0.")
            return

    # Không ép kiểu float ngay lập tức vì hỗ trợ định dạng % (phần trăm) và u (USDT PnL)
    if tp_price_str:
        tp_price_str = tp_price_str.strip()
    if sl_price_str:
        sl_price_str = sl_price_str.strip()

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
    
    precision = qty_p
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
        client_order_id = f"pnlbot_limit_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        params.append(f"newClientOrderId={client_order_id}")
        
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
                        f"💵 Giá đặt Limit: *{format_price(limit_price)} USDT*\n"
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
                        f"💵 Giá khớp trung bình: *{format_price(avg_price)} USDT*\n"
                        f"🆔 Order ID: `{order_id}`"
                    )
                
                tp_sl_msg_parts = []

                # Tính toán giá TP/SL nếu có (hỗ trợ %, u, r)
                final_tp_price = None
                if tp_price_str:
                    try:
                        ref_price = limit_price if is_limit else avg_price
                        ref_qty = quantity if is_limit else execute_qty
                        final_tp_price = calculate_tpsl_price(
                            tp_price_str,
                            entry_price=ref_price,
                            quantity=ref_qty,
                            leverage=max_leverage,
                            is_long=(side_type == 'LONG'),
                            is_tp=True
                        )
                        final_tp_price = round_price_step(final_tp_price, tick_size, price_p)
                    except Exception as e:
                        tp_sl_msg_parts.append(f"❌ *Lỗi tính toán TP '{tp_price_str}':* `{e}`")

                final_sl_price = None
                if sl_price_str:
                    try:
                        ref_price = limit_price if is_limit else avg_price
                        ref_qty = quantity if is_limit else execute_qty
                        final_sl_price = calculate_tpsl_price(
                            sl_price_str,
                            entry_price=ref_price,
                            quantity=ref_qty,
                            leverage=max_leverage,
                            is_long=(side_type == 'LONG'),
                            is_tp=False
                        )
                        final_sl_price = round_price_step(final_sl_price, tick_size, price_p)
                    except Exception as e:
                        tp_sl_msg_parts.append(f"❌ *Lỗi tính toán SL '{sl_price_str}':* `{e}`")

                # Tự động hủy TP/SL cũ để tránh lỗi GTE của Binance
                if final_tp_price is not None or final_sl_price is not None:
                    await cancel_existing_tpsl(
                        session, 
                        api_key, 
                        api_secret, 
                        symbol, 
                        position_side=pos_side, 
                        cancel_tp=(final_tp_price is not None), 
                        cancel_sl=(final_sl_price is not None)
                    )

                tpsl_side = 'SELL' if side_type == 'LONG' else 'BUY'
                
                # Cài đặt TP nếu có
                if final_tp_price is not None:
                    timestamp_tp = int(time.time() * 1000)
                    tp_params = [
                        f"symbol={symbol}",
                        f"side={tpsl_side}",
                        "type=TAKE_PROFIT_MARKET",
                        f"triggerPrice={final_tp_price}",
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
                                tp_sl_msg_parts.append(f"🎯 *TP:* Chốt lời ở giá *{final_tp_price:,.4f}* (Thành công, ID: `{tp_id}`)")
                            else:
                                tp_err = tp_data.get('msg', 'Lỗi không xác định')
                                tp_sl_msg_parts.append(f"❌ *Lỗi đặt TP:* `{tp_err}`")
                    except Exception as e:
                        tp_sl_msg_parts.append(f"❌ *Lỗi đặt TP:* `{e}`")

                # Cài đặt SL nếu có
                if final_sl_price is not None:
                    timestamp_sl = int(time.time() * 1000)
                    sl_params = [
                        f"symbol={symbol}",
                        f"side={tpsl_side}",
                        "type=STOP_MARKET",
                        f"triggerPrice={final_sl_price}",
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
                                tp_sl_msg_parts.append(f"🛡️ *SL:* Cắt lỗ ở giá *{final_sl_price:,.4f}* (Thành công, ID: `{sl_id}`)")
                            else:
                                sl_err = sl_data.get('msg', 'Lỗi không xác định')
                                tp_sl_msg_parts.append(f"❌ *Lỗi đặt SL:* `{sl_err}`")
                    except Exception as e:
                        tp_sl_msg_parts.append(f"❌ *Lỗi đặt SL:* `{e}`")

                if tp_sl_msg_parts:
                    msg += "\n\n" + "\n".join(tp_sl_msg_parts)
                    
                    if any("GTE" in r or "closePosition" in r for r in tp_sl_msg_parts):
                        msg += (
                            f"\n\n⚠️ *Lưu ý lỗi GTE/closePosition từ Binance:*\n"
                            f"Binance quy định chỉ được phép tồn tại *1 lệnh đóng vị thế (closePosition)* có cùng điều kiện kích hoạt GTE (hoặc LTE).\n"
                            f"Khi bạn đặt TP/SL mà cả TP và SL đều nằm cùng một phía so với giá hiện tại (cả hai đều cao hơn hoặc đều thấp hơn giá thị trường), chúng sẽ trùng điều kiện kích hoạt (GTE/LTE) dẫn đến lệnh thứ hai bị từ chối.\n"
                            f"👉 *Giải pháp:* Cài đặt TP/SL khi giá hiện tại nằm giữa khoảng TP và SL, hoặc hủy bớt lệnh cũ trên app Binance rồi thử lại."
                        )

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
            headers_public = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            async with session.get(url_price, headers=headers_public) as resp_price:
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
                    if current_price is None or current_price == 0:
                        current_price = await get_single_price(session, symbol)
                        if current_price > 0:
                            prices_map[symbol] = current_price
                        else:
                            current_price = None
                    
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

    qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)

    # Không ép kiểu float ngay lập tức vì hỗ trợ định dạng % (phần trăm) và u (USDT PnL)
    if tp_price_str:
        tp_price_str = tp_price_str.strip()
    if sl_price_str:
        sl_price_str = sl_price_str.strip()

    results = []
    headers = {"X-MBX-APIKEY": api_key}
    
    for pos in target_positions:
        side = pos['positionSide']
        amt = pos['positionAmt']
        entry_price = pos['entryPrice']
        leverage = pos.get('leverage', 1)
        quantity = abs(amt)
        
        is_long = amt > 0
        if side == 'LONG':
            is_long = True
        elif side == 'SHORT':
            is_long = False
            
        order_side = 'SELL' if is_long else 'BUY'
        pos_display = 'LONG' if is_long else 'SHORT'
        
        # Tính toán giá TP/SL nếu có (hỗ trợ %, u, r)
        final_tp_price = None
        if tp_price_str:
            try:
                final_tp_price = calculate_tpsl_price(
                    tp_price_str,
                    entry_price=entry_price,
                    quantity=quantity,
                    leverage=leverage,
                    is_long=is_long,
                    is_tp=True
                )
                final_tp_price = round_price_step(final_tp_price, tick_size, price_p)
            except Exception as e:
                results.append(f"   • TP (*{pos_display}*): 🔴 Lỗi tính toán '{tp_price_str}': {e}")

        final_sl_price = None
        if sl_price_str:
            try:
                final_sl_price = calculate_tpsl_price(
                    sl_price_str,
                    entry_price=entry_price,
                    quantity=quantity,
                    leverage=leverage,
                    is_long=is_long,
                    is_tp=False
                )
                final_sl_price = round_price_step(final_sl_price, tick_size, price_p)
            except Exception as e:
                results.append(f"   • SL (*{pos_display}*): 🔴 Lỗi tính toán '{sl_price_str}': {e}")
        
        # Tự động hủy TP/SL cũ để tránh lỗi GTE của Binance
        if final_tp_price is not None or final_sl_price is not None:
            await cancel_existing_tpsl(
                session, 
                api_key, 
                api_secret, 
                symbol, 
                position_side=side, 
                cancel_tp=(final_tp_price is not None), 
                cancel_sl=(final_sl_price is not None)
            )
            
        if final_tp_price is not None:
            timestamp = int(time.time() * 1000)
            params = [
                f"symbol={symbol}",
                f"side={order_side}",
                "type=TAKE_PROFIT_MARKET",
                f"triggerPrice={final_tp_price}",
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
                        results.append(f"   • TP (*{pos_display}* tại giá *{format_price(final_tp_price)}*): 🟢 Thành công (ID: `{order_id}`)")
                    else:
                        msg_err = data.get('msg', 'Lỗi không xác định')
                        results.append(f"   • TP (*{pos_display}* tại giá *{format_price(final_tp_price)}*): 🔴 Thất bại: `{msg_err}`")
            except Exception as e:
                results.append(f"   • TP (*{pos_display}*): 🔴 Lỗi kết nối: {e}")
                
        if final_sl_price is not None:
            timestamp = int(time.time() * 1000)
            params = [
                f"symbol={symbol}",
                f"side={order_side}",
                "type=STOP_MARKET",
                f"triggerPrice={final_sl_price}",
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
                        results.append(f"   • SL (*{pos_display}* tại giá *{format_price(final_sl_price)}*): 🟢 Thành công (ID: `{order_id}`)")
                    else:
                        msg_err = data.get('msg', 'Lỗi không xác định')
                        results.append(f"   • SL (*{pos_display}* tại giá *{format_price(final_sl_price)}*): 🔴 Thất bại: `{msg_err}`")
            except Exception as e:
                results.append(f"   • SL (*{pos_display}*): 🔴 Lỗi kết nối: {e}")
                
    msg = (
        f"🎯 *KẾT QUẢ CÀI ĐẶT TP/SL CHO {symbol}*\n"
        f"----------------------------------\n" +
        "\n".join(results)
    )
    
    if any("GTE" in r or "closePosition" in r for r in results):
        msg += (
            f"\n\n⚠️ *Lưu ý lỗi GTE/closePosition từ Binance:*\n"
            f"Binance quy định chỉ được phép tồn tại *1 lệnh đóng vị thế (closePosition)* có cùng điều kiện kích hoạt GTE (hoặc LTE).\n"
            f"Khi bạn đặt TP/SL mà cả TP và SL đều nằm cùng một phía so với giá hiện tại (cả hai đều cao hơn hoặc đều thấp hơn giá thị trường), chúng sẽ trùng điều kiện kích hoạt (GTE/LTE) dẫn đến lệnh thứ hai bị từ chối.\n"
            f"👉 *Giải pháp:* Cài đặt TP/SL khi giá hiện tại nằm giữa khoảng TP và SL, hoặc hủy bớt lệnh cũ trên app Binance rồi thử lại."
        )
    await send_telegram_message(session, chat_id, msg)


async def handle_dca_command(session, chat_id, coin_name, volume_str, diff_str):
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    try:
        volume = float(volume_str)
        if volume <= 0:
            raise ValueError()
    except ValueError:
        await send_telegram_message(session, chat_id, "❌ Số tiền volume không hợp lệ. Vui lòng nhập số dương lớn hơn 0.")
        return

    # Lọc các vị thế đang mở (Amt khác 0)
    target_positions = [pos for pos in positions.values() if pos['symbol'] == symbol and float(pos.get('positionAmt', 0)) != 0]
    
    if not target_positions:
        await send_telegram_message(
            session, 
            chat_id, 
            f"❌ Không tìm thấy vị thế *{symbol}* nào đang mở để thực hiện DCA."
        )
        return

    headers = {"X-MBX-APIKEY": api_key}
    
    for pos in target_positions:
        side = pos['positionSide']
        amt = float(pos['positionAmt'])
        entry_price = float(pos['entryPrice'])
        leverage = int(pos.get('leverage', 1))
        quantity_current = abs(amt)
        
        is_long = amt > 0
        if side == 'LONG':
            is_long = True
        elif side == 'SHORT':
            is_long = False
            
        pos_display = 'LONG' if is_long else 'SHORT'
        
        # 1. Tính toán giá Limit DCA tương ứng với khoảng cách (loss)
        qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)
        try:
            dca_price = calculate_tpsl_price(
                diff_str,
                entry_price=entry_price,
                quantity=quantity_current,
                leverage=leverage,
                is_long=is_long,
                is_tp=False # DCA đặt ở vùng lỗ
            )
            dca_price = round_price_step(dca_price, tick_size, price_p)
            
        except Exception as e:
            await send_telegram_message(
                session, 
                chat_id, 
                f"❌ *Lỗi tính toán giá DCA '{diff_str}':* `{e}`"
            )
            continue
            
        # 2. Quy đổi volume ra quantity của lệnh DCA mới
        raw_qty = volume / dca_price
        precision = qty_p
        quantity_dca = round_down(raw_qty, precision)
        
        if quantity_dca <= 0:
            await send_telegram_message(
                session, 
                chat_id, 
                f"❌ Số lượng coin tính toán cho lệnh DCA quá nhỏ ({raw_qty:.8f} {coin_name}).\n"
                f"Vui lòng tăng Volume đặt DCA hoặc chọn coin có giá thấp hơn."
            )
            continue
            
        # 3. Đặt lệnh LIMIT cùng chiều với vị thế hiện tại để DCA tăng vị thế
        order_side = 'BUY' if is_long else 'SELL'
        pos_side = side # LONG, SHORT, BOTH
        
        timestamp = int(time.time() * 1000)
        client_order_id = f"pnlbot_dca_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        params = [
            f"symbol={symbol}",
            f"side={order_side}",
            "type=LIMIT",
            f"quantity={quantity_dca}",
            f"price={dca_price}",
            "timeInForce=GTC",
            f"newClientOrderId={client_order_id}",
            f"timestamp={timestamp}"
        ]
        
        if hedge_mode:
            params.append(f"positionSide={pos_side}")
            
        query_string = "&".join(params)
        signature = get_binance_signature(query_string, api_secret)
        url = f"https://fapi.binance.com/fapi/v1/order?{query_string}&signature={signature}"
        
        try:
            async with session.post(url, headers=headers) as resp:
                data = await resp.json()
                if resp.status == 200:
                    order_id = data.get('orderId')
                    pnl_emoji = "🟢" if is_long else "🔴"
                    await send_telegram_message(
                        session, 
                        chat_id, 
                        f"✅ *ĐẶT LỆNH LIMIT DCA THÀNH CÔNG!*\n"
                        f"----------------------------------\n"
                        f"🪙 Cặp: *{symbol}*\n"
                        f"⚡ DCA vị thế: {pnl_emoji} *{pos_display}*\n"
                        f"💵 Giá đặt DCA Limit: *{format_price(dca_price)} USDT*\n"
                        f"📊 Volume DCA: *{volume:,.2f} USDT*\n"
                        f"🔢 Số lượng DCA thêm: *{quantity_dca} {coin_name}*\n"
                        f"🆔 Order ID: `{order_id}`"
                    )
                else:
                    msg_err = data.get('msg', 'Lỗi không xác định')
                    code_err = data.get('code', -1)
                    await send_telegram_message(
                        session, 
                        chat_id, 
                        f"❌ *Đặt lệnh DCA Limit thất bại!*\nBinance báo lỗi: `{msg_err}` (Code: {code_err})"
                    )
        except Exception as e:
            logger.error(f"Lỗi khi gửi lệnh DCA cho {symbol}: {e}")
            await send_telegram_message(session, chat_id, f"❌ Đã xảy ra lỗi hệ thống khi đặt lệnh DCA: {e}")


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
    has_new_activity[chat_id] = True
    if chat_id not in active_chats:
        active_chats.add(chat_id)
        save_active_chats()
        
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
            "📊 `/chart [khung_thời_gian] <coin>` - Xem biểu đồ nến (ví dụ: `/chart 1d btc`, `/chart btc 15m`).\n"
            "⚖️ `/dca <coin> <volume> <khoảng_cách>` - Đặt lệnh Limit DCA vùng lỗ (ví dụ: `/dca btc 200 40u`, `/dca eth 100 2%`).\n"
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
        
    elif command_base == '/chart':
        parts = text.split()
        if len(parts) < 2:
            await send_telegram_message(
                request.app['session'],
                chat_id,
                "❌ Sai cú pháp!\nSử dụng: `/chart [khung_thời_gian] <coin>` hoặc `/chart <coin> [khung_thời_gian]`\n"
                "Khung thời gian hỗ trợ: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1M`\n"
                "Ví dụ: `/chart btc` hoặc `/chart 1d btc` hoặc `/chart sol 15m`"
            )
        else:
            import re
            timeframe_pattern = r'^(1m|3m|5m|15m|30m|1h|2h|4h|6h|8h|12h|1d|3d|1w|1M)$'
            
            interval = '1h'
            coin_name = None
            
            for part in parts[1:]:
                part_clean = part.strip()
                if re.match(timeframe_pattern, part_clean, re.IGNORECASE):
                    if part_clean.lower() == '1m':
                        interval = '1M' if part_clean == '1M' else '1m'
                    else:
                        interval = part_clean.lower() if part_clean != '1M' else '1M'
                else:
                    coin_name = part_clean.upper()
            
            if not coin_name:
                await send_telegram_message(
                    request.app['session'],
                    chat_id,
                    "❌ Vui lòng nhập tên coin (ví dụ: btc, eth, sol)."
                )
            else:
                symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
                
                loading_msg_id = await send_telegram_message(
                    request.app['session'],
                    chat_id,
                    f"⏳ Đang tải và vẽ biểu đồ *{symbol}* ({interval.upper()})..."
                )
                
                try:
                    photo_buf = await draw_candlestick_chart(request.app['session'], symbol, interval)
                    
                    caption = f"📊 Biểu đồ nến *{symbol}* ({interval.upper()})\n⚡ Sàn: Binance Futures"
                    success = await send_telegram_photo(request.app['session'], chat_id, photo_buf, caption=caption)
                    
                    if loading_msg_id:
                        await delete_telegram_message(request.app['session'], chat_id, loading_msg_id)
                except Exception as e:
                    logger.error(f"Lỗi vẽ hoặc gửi biểu đồ cho {symbol}: {e}")
                    if loading_msg_id:
                        await delete_telegram_message(request.app['session'], chat_id, loading_msg_id)
                    await send_telegram_message(
                        request.app['session'],
                        chat_id,
                        f"❌ Không thể vẽ biểu đồ cho *{symbol}*.\nLý do: `{e}`"
                    )
    elif command_base == '/dca':
        parts = text.split()
        if len(parts) < 4:
            await send_telegram_message(
                request.app['session'],
                chat_id,
                "❌ Sai cú pháp đặt DCA!\nSử dụng: `/dca <coin> <volume> <khoảng_cách>`\nVí dụ: `/dca btc 200 40u` hoặc `/dca eth 100 2%`"
            )
        else:
            coin_name = parts[1]
            volume_str = parts[2]
            diff_str = parts[3]
            await handle_dca_command(request.app['session'], chat_id, coin_name, volume_str, diff_str)
            
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
    load_active_chats()
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
