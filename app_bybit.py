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
logger = logging.getLogger("bot_bybit")

# Global variables
positions = {}          # Key: f"{symbol}_{positionSide}", Value: dict vị thế
subscribed_symbols = set() # Các symbol (viết hoa) đã subscribe Ticker WS
mark_price_ws = None    # WS connection cho Public tickers stream
auto_chats = set()      # Danh sách chat_id nhận cập nhật tự động mỗi 5 phút
last_auto_messages = {} # Lưu message_id của tin nhắn auto cuối cùng (key: chat_id, value: message_id)
has_new_activity = {}   # Đánh dấu có hoạt động mới trong chat (key: chat_id, value: bool)
notified_thresholds = {} # Các ngưỡng % đã thông báo cho từng vị thế (key: pos_key, value: set)
PRICE_ALERT_THRESHOLDS = list(range(5, 505, 5)) # Cảnh báo mỗi 5% biến động vị thế (từ 5% đến 500%)
hedge_mode = False      # Chế độ Position Mode (True: Hedge Mode, False: One-way Mode)
symbol_precisions = {}  # Lưu độ chính xác số lượng coin (quantityPrecision)
symbol_price_precisions = {}  # Lưu độ chính xác giá (pricePrecision)
symbol_tick_sizes = {}  # Lưu tickSize của từng symbol
order_client_ids = {}   # Lưu map tạm thời orderId -> clientOrderId để biết loại lệnh khi khớp (execution)
notified_filled_orders = set()  # Lưu các orderId đã thông báo Filled để tránh trùng lặp
order_realized_pnls = {}     # Lưu map orderId -> realizedPnL từ execution stream

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


# Hàm tạo chữ ký HMAC-SHA256 cho Bybit V5 API
def get_bybit_signature(api_secret, timestamp, api_key, recv_window, payload=""):
    val = str(timestamp) + api_key + str(recv_window) + payload
    return hmac.new(
        api_secret.encode('utf-8'),
        val.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

# Helper gọi REST API Bybit V5
async def bybit_api_request(session, method, path, params=None, body=None, is_private=False):
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    url = f"https://api.bybit.com{path}"
    headers = {}
    
    if is_private:
        if not api_key or not api_secret:
            logger.error("Thiếu cấu hình BYBIT_API_KEY hoặc BYBIT_API_SECRET trong .env")
            return 500, {"retCode": -1, "retMsg": "Missing API Key/Secret"}
        timestamp = int(time.time() * 1000)
        recv_window = 5000
        
        if method == "GET":
            query_str = ""
            if params:
                query_str = "&".join([f"{k}={v}" for k, v in params.items()])
            payload = query_str
        else:
            payload = json.dumps(body) if body else ""
            
        signature = get_bybit_signature(api_secret, timestamp, api_key, recv_window, payload)
        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-TIMESTAMP": str(timestamp),
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": str(recv_window),
            "Content-Type": "application/json"
        }
    else:
        headers = {
            "Content-Type": "application/json"
        }
        
    try:
        if method == "GET":
            async with session.get(url, params=params, headers=headers) as resp:
                status = resp.status
                text = await resp.text()
                try:
                    data = json.loads(text)
                except:
                    data = text
                return status, data
        elif method == "POST":
            async with session.post(url, json=body, headers=headers) as resp:
                status = resp.status
                text = await resp.text()
                try:
                    data = json.loads(text)
                except:
                    data = text
                return status, data
    except Exception as e:
        logger.error(f"Lỗi khi gửi request Bybit {method} {path}: {e}")
        return 500, {"retCode": -1, "retMsg": str(e)}

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
                if "message is not modified" not in body:
                    logger.warning(f"Không thể sửa tin nhắn Telegram {message_id}: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi khi sửa tin nhắn Telegram: {e}")
    return None

# Subscribe Ticker của một symbol qua WebSocket Bybit
async def subscribe_mark_price(symbol):
    symbol_upper = symbol.upper()
    if symbol_upper in subscribed_symbols:
        return
    
    subscribed_symbols.add(symbol_upper)
    if mark_price_ws and not mark_price_ws.closed:
        try:
            await mark_price_ws.send_json({
                "op": "subscribe",
                "args": [f"tickers.{symbol_upper}"]
            })
            logger.info(f"Đã đăng ký nhận giá Bybit cho: {symbol_upper}")
        except Exception as e:
            logger.error(f"Lỗi khi gửi lệnh SUBSCRIBE Bybit cho {symbol_upper}: {e}")

# Unsubscribe Ticker của một symbol
async def unsubscribe_mark_price(symbol):
    symbol_upper = symbol.upper()
    if symbol_upper not in subscribed_symbols:
        return
    
    subscribed_symbols.remove(symbol_upper)
    if mark_price_ws and not mark_price_ws.closed:
        try:
            await mark_price_ws.send_json({
                "op": "unsubscribe",
                "args": [f"tickers.{symbol_upper}"]
            })
            logger.info(f"Đã hủy nhận giá Bybit cho: {symbol_upper}")
        except Exception as e:
            logger.error(f"Lỗi khi gửi lệnh UNSUBSCRIBE Bybit cho {symbol_upper}: {e}")

# Cập nhật cache vị thế cục bộ
async def update_position_cache(symbol, position_side, amount, entry_price, leverage, position_idx=0):
    key = f"{symbol}_{position_side}"
    amount = float(amount)
    entry_price = float(entry_price)
    leverage = int(leverage)
    
    if amount == 0.0:
        if key in positions:
            del positions[key]
            notified_thresholds.pop(key, None)
            logger.info(f"Đã đóng vị thế Bybit: {key}")
        
        still_has_position = any(p['symbol'] == symbol for p in positions.values())
        if not still_has_position:
            await unsubscribe_mark_price(symbol)
    else:
        is_new = key not in positions
        positions[key] = {
            'symbol': symbol,
            'positionSide': position_side,
            'positionAmt': amount,
            'entryPrice': entry_price,
            'markPrice': positions.get(key, {}).get('markPrice', entry_price),
            'unrealizedPnL': positions.get(key, {}).get('unrealizedPnL', 0.0),
            'leverage': leverage,
            'positionIdx': position_idx
        }
        
        if is_new:
            logger.info(f"Đã mở vị thế Bybit mới: {key} (Size: {amount}, Entry: {entry_price})")
        else:
            logger.info(f"Cập nhật vị thế Bybit: {key} (Size: {amount}, Entry: {entry_price})")
            
        await subscribe_mark_price(symbol)

# Lấy snapshot vị thế ban đầu từ Bybit REST API V5
async def init_positions(session, api_key, api_secret):
    logger.info("Đang lấy dữ liệu vị thế ban đầu từ Bybit REST API...")
    status, data = await bybit_api_request(
        session, "GET", "/v5/position/list",
        params={"category": "linear", "settleCoin": "USDT"},
        is_private=True
    )
    if status == 200 and data.get("retCode") == 0:
        positions.clear()
        pos_list = data.get("result", {}).get("list", [])
        for p in pos_list:
            size = float(p.get('size', 0))
            if size != 0.0:
                symbol = p.get('symbol')
                position_idx = int(p.get('positionIdx', 0))
                entry_price = float(p.get('entryPrice', 0))
                leverage = int(float(p.get('leverage', 1)))
                mark_price = float(p.get('markPrice', 0))
                unrealised_pnl = float(p.get('unrealisedPnl', 0))
                side = p.get('side')
                
                # Ánh xạ positionSide theo Mode
                if position_idx == 1:
                    position_side = 'LONG'
                    amount = size
                elif position_idx == 2:
                    position_side = 'SHORT'
                    amount = -size
                else:
                    position_side = 'BOTH'
                    amount = size if side == 'Buy' else -size
                    
                key = f"{symbol}_{position_side}"
                positions[key] = {
                    'symbol': symbol,
                    'positionSide': position_side,
                    'positionAmt': amount,
                    'entryPrice': entry_price,
                    'markPrice': mark_price,
                    'unrealizedPnL': unrealised_pnl,
                    'leverage': leverage,
                    'positionIdx': position_idx
                }
        logger.info(f"Nạp snapshot thành công. Số vị thế đang mở trên Bybit: {len(positions)}")
    else:
        raise Exception(f"Lỗi lấy snapshot vị thế từ Bybit: {data}")

# WebSocket kết nối User Data Stream (Private) từ Bybit
async def bybit_user_data_stream(session, api_key, api_secret):
    while True:
        try:
            url = "wss://stream.bybit.com/v5/private"
            logger.info("Đang kết nối WebSocket Private Bybit...")
            
            async with session.ws_connect(url) as ws:
                logger.info("WebSocket Private Bybit đã kết nối. Tiến hành xác thực...")
                
                # 1. Gửi gói tin auth
                expires = int((time.time() + 60) * 1000)
                raw_sig = f"GET/realtime{expires}"
                signature = hmac.new(
                    api_secret.encode('utf-8'),
                    raw_sig.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                
                auth_payload = {
                    "op": "auth",
                    "args": [api_key, expires, str(signature)]
                }
                await ws.send_json(auth_payload)
                
                # Chờ phản hồi auth
                auth_resp = await ws.receive_json()
                if not auth_resp.get("success"):
                    logger.error(f"Xác thực WebSocket Private Bybit thất bại: {auth_resp}")
                    await asyncio.sleep(5)
                    continue
                    
                logger.info("Xác thực WebSocket Private Bybit thành công. Đăng ký nhận sự kiện...")
                
                # 2. Subscribe các topic
                sub_payload = {
                    "op": "subscribe",
                    "args": ["position", "order", "execution"]
                }
                await ws.send_json(sub_payload)
                
                # Task gửi ping định kỳ giữ kết nối (mỗi 20s)
                async def send_ping():
                    while not ws.closed:
                        await asyncio.sleep(20)
                        await ws.send_json({"op": "ping"})
                
                ping_task = asyncio.create_task(send_ping())
                
                # 3. Lắng nghe dữ liệu
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data_json = msg.json()
                        topic = data_json.get("topic")
                        
                        # Xử lý sự kiện position thay đổi
                        if topic == "position":
                            for p in data_json.get("data", []):
                                symbol = p.get('symbol')
                                size = float(p.get('size', 0))
                                entry_price = float(p.get('entryPrice', 0))
                                leverage = int(float(p.get('leverage', 1)))
                                position_idx = int(p.get('positionIdx', 0))
                                side = p.get('side')
                                
                                if position_idx == 1:
                                    position_side = 'LONG'
                                    amount = size
                                elif position_idx == 2:
                                    position_side = 'SHORT'
                                    amount = -size
                                else:
                                    position_side = 'BOTH'
                                    amount = size if side == 'Buy' else -size
                                    
                                await update_position_cache(
                                    symbol=symbol,
                                    position_side=position_side,
                                    amount=amount,
                                    entry_price=entry_price,
                                    leverage=leverage,
                                    position_idx=position_idx
                                )
                                
                        # Xử lý sự kiện order (để lưu map clientOrderId)
                        # Xử lý sự kiện order (để lưu map clientOrderId và thông báo khớp lệnh khi Filled)
                        elif topic == "order":
                            for o in data_json.get("data", []):
                                order_id = o.get('orderId')
                                client_id = o.get('orderLinkId', '')
                                if order_id and client_id:
                                    order_client_ids[order_id] = client_id
                                    # Giới hạn kích thước map để tránh leak bộ nhớ
                                    if len(order_client_ids) > 1000:
                                        key_to_del = next(iter(order_client_ids))
                                        order_client_ids.pop(key_to_del, None)
                                        
                                status = o.get('orderStatus')
                                if status == 'Filled':
                                    if order_id in notified_filled_orders:
                                        continue
                                    notified_filled_orders.add(order_id)
                                    if len(notified_filled_orders) > 500:
                                        notified_filled_orders.remove(next(iter(notified_filled_orders)))
                                        
                                    symbol = o.get('symbol')
                                    side = o.get('side')  # Buy, Sell
                                    pos_idx = int(o.get('positionIdx', 0))
                                    price = float(o.get('avgPrice', 0) or o.get('price', 0) or 0)
                                    qty = float(o.get('cumExecQty', 0) or o.get('qty', 0) or 0)
                                    notional = qty * price
                                    
                                    # Xác định chiều của lệnh chính
                                    if pos_idx == 1:
                                        side_display = "LONG"
                                    elif pos_idx == 2:
                                        side_display = "SHORT"
                                    else:
                                        side_display = "LONG" if side == 'Buy' else "SHORT"
                                        
                                    # Phân loại loại lệnh hiển thị
                                    order_type_display = "Market/Limit"
                                    stop_order_type = o.get('stopOrderType', '')
                                    if stop_order_type == 'TakeProfit' or "tp" in client_id.lower():
                                        order_type_display = "🎯 CHỐT LỜI (Take Profit)"
                                    elif stop_order_type == 'StopLoss' or "sl" in client_id.lower():
                                        order_type_display = "🛡️ CẮT LỖ (Stop Loss)"
                                    elif "dca" in client_id.lower():
                                        order_type_display = "⚖️ DCA Limit"
                                    elif "limit" in client_id.lower():
                                        order_type_display = "⏳ Limit"
                                        
                                    realized_pnl = order_realized_pnls.pop(order_id, 0.0)
                                    
                                    msg_lines = [
                                        f"┌──────────────────────────────┐",
                                        f"   🔔 *THÔNG BÁO KHỚP LỆNH (BYBIT)*",
                                        f"└──────────────────────────────┘",
                                        f"🪙 Cặp: `{symbol}`",
                                        f"⚡ Loại: `{order_type_display} ({side_display})`",
                                        f"📊 Trạng thái: 🟢 `FILLED`",
                                        f"💵 Giá khớp: `{format_price(price)} USDT`",
                                        f"🔢 Số lượng: `{qty}` (~`{notional:,.2f} USDT`)"
                                    ]
                                    
                                    if realized_pnl != 0.0:
                                        pnl_emoji = "🟩" if realized_pnl >= 0 else "🟥"
                                        pnl_sign = "+" if realized_pnl >= 0 else ""
                                        msg_lines.append(f"💰 PnL đóng: {pnl_emoji} `*{pnl_sign}{realized_pnl:,.2f} USDT*`")
                                        
                                    msg_lines.append(f"🆔 Order ID: `{order_id}`")
                                    message = "\n".join(msg_lines)
                                    
                                    if active_chats:
                                        for chat_id in list(active_chats):
                                            try:
                                                await send_telegram_message(session, chat_id, message)
                                            except Exception as send_err:
                                                logger.error(f"Lỗi gửi thông báo event đến {chat_id}: {send_err}")
                                                
                        # Xử lý sự kiện execution (thông báo thanh lý & lưu realizedPnL của trade)
                        elif topic == "execution":
                            for ex in data_json.get("data", []):
                                exec_type = ex.get('execType')
                                symbol = ex.get('symbol')
                                order_id = ex.get('orderId')
                                side = ex.get('side')
                                price = float(ex.get('execPrice', 0))
                                qty = float(ex.get('execQty', 0))
                                realized_pnl = float(ex.get('execRealizedPnl', 0))
                                notional = qty * price
                                
                                # Lưu realized_pnl của trade này
                                if realized_pnl != 0.0:
                                    order_realized_pnls[order_id] = order_realized_pnls.get(order_id, 0.0) + realized_pnl
                                    # Tránh phình bộ nhớ cho order_realized_pnls
                                    if len(order_realized_pnls) > 1000:
                                        key_to_del = next(iter(order_realized_pnls))
                                        order_realized_pnls.pop(key_to_del, None)
                                        
                                # 1. Thanh lý vị thế (BustTrade)
                                if exec_type == 'BustTrade':
                                    pos_display = "LONG" if side == 'Sell' else "SHORT"
                                    message = (
                                        f"🚨🚨 *【CẢNH BÁO THANH LÝ (BYBIT)】* 🚨🚨\n"
                                        f"💀💀💀💀💀💀💀💀💀💀💀💀💀💀\n"
                                        f"🪙 Cặp: `{symbol}`\n"
                                        f"💥 Vị thế cháy: 🔴 `{pos_display}`\n"
                                        f"💵 Giá thanh lý: `{format_price(price)} USDT`\n"
                                        f"🔢 Số lượng thanh lý: `{qty}` (~`{notional:,.2f} USDT`)\n"
                                        f"🆔 Order ID: `{order_id}`"
                                    )
                                    
                                    if active_chats:
                                        for chat_id in list(active_chats):
                                            try:
                                                await send_telegram_message(session, chat_id, message)
                                            except Exception as send_err:
                                                logger.error(f"Lỗi gửi thông báo thanh lý đến {chat_id}: {send_err}")
                                            
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        logger.warning("WebSocket Private Bybit bị đóng hoặc lỗi.")
                        break
                        
                ping_task.cancel()
        except Exception as e:
            logger.error(f"Lỗi trong loop WebSocket Private Bybit: {e}")
            
        logger.info("Thử kết nối lại WebSocket Private sau 5 giây...")
        await asyncio.sleep(5)

# WebSocket kết nối lấy Mark Price (Public tickers stream) của Bybit V5
async def bybit_mark_price_stream(session):
    global mark_price_ws
    url = "wss://stream.bybit.com/v5/public/linear"
    
    while True:
        try:
            logger.info("Đang kết nối WebSocket Public Bybit...")
            async with session.ws_connect(url) as ws:
                mark_price_ws = ws
                logger.info("WebSocket Public Bybit đã kết nối.")
                
                # Subscribe lại các symbol đang có trong cache vị thế
                current_symbols = list(set(p['symbol'].upper() for p in positions.values()))
                if current_symbols:
                    subscribed_symbols.clear()
                    args = [f"tickers.{s}" for s in current_symbols]
                    for s in current_symbols:
                        subscribed_symbols.add(s)
                        
                    await ws.send_json({
                        "op": "subscribe",
                        "args": args
                    })
                    logger.info(f"Đã subscribe lại tickers cho các symbol Bybit: {current_symbols}")
                    
                # Task ping duy trì
                async def send_ping():
                    while not ws.closed:
                        await asyncio.sleep(20)
                        await ws.send_json({"op": "ping"})
                        
                ping_task = asyncio.create_task(send_ping())
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data_json = msg.json()
                        topic = data_json.get("topic")
                        
                        if topic and topic.startswith("tickers."):
                            data = data_json.get("data", {})
                            symbol = data.get("symbol")
                            mark_price_str = data.get("markPrice")
                            
                            if symbol and mark_price_str:
                                mark_price = float(mark_price_str)
                                
                                # Cập nhật cache vị thế
                                for key, pos in list(positions.items()):
                                    if pos['symbol'] == symbol:
                                        pos['markPrice'] = mark_price
                                        
                                        amt = pos['positionAmt']
                                        entry = pos['entryPrice']
                                        side_idx = pos.get('positionIdx', 0)
                                        
                                        # Xác định chiều để tính PnL
                                        # amt dương -> Long, amt âm -> Short
                                        side_sign = -1 if amt < 0 else 1
                                        pos['unrealizedPnL'] = (mark_price - entry) * abs(amt) * side_sign
                                        
                                        # Cảnh báo biến động %
                                        if entry > 0:
                                            pct_change = ((mark_price - entry) / entry) * 100 * side_sign
                                            if key not in notified_thresholds:
                                                notified_thresholds[key] = set()
                                                
                                            for threshold in PRICE_ALERT_THRESHOLDS:
                                                for direction in [threshold, -threshold]:
                                                    if direction not in notified_thresholds[key]:
                                                        if (direction > 0 and pct_change >= direction) or (direction < 0 and pct_change <= direction):
                                                            if not active_chats:
                                                                continue
                                                            
                                                            display_symbol = symbol[:-4] if symbol.endswith('USDT') else symbol
                                                            display_side = 'LONG' if side_sign > 0 else 'SHORT'
                                                            
                                                            if direction > 0:
                                                                alert_msg = (
                                                                    f"📈📈 *【CẢNH BÁO BIẾN ĐỘNG GIÁ (BYBIT)】* 📈📈\n"
                                                                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                                                    f"🪙 Cặp: `{display_symbol}` ({display_side})\n"
                                                                    f"🟢 Vị thế đã dương *+{pct_change:.1f}%* so với Entry\n"
                                                                    f"💵 Entry: `{format_price(entry)} USDT`\n"
                                                                    f"💵 Hiện tại: `{format_price(mark_price)} USDT`\n"
                                                                    f"💰 PnL: `{'+' if pos['unrealizedPnL'] >= 0 else ''}{pos['unrealizedPnL']:,.2f} USDT`"
                                                                )
                                                            else:
                                                                alert_msg = (
                                                                    f"📉📉 *【CẢNH BÁO BIẾN ĐỘNG GIÁ (BYBIT)】* 📉📉\n"
                                                                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                                                    f"🪙 Cặp: `{display_symbol}` ({display_side})\n"
                                                                    f"🔴 Vị thế đã âm *{abs(pct_change):.1f}%* so với Entry\n"
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
                                                                    
                                                            if sent_ok:
                                                                notified_thresholds[key].add(direction)
                                                                
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        logger.warning("Mark Price WS Bybit bị đóng hoặc lỗi.")
                        break
                        
                ping_task.cancel()
        except Exception as e:
            logger.error(f"Lỗi trong Mark Price WS Bybit: {e}")
            
        mark_price_ws = None
        subscribed_symbols.clear()
        logger.info("Thử kết nối lại Mark Price WS Bybit sau 5 giây...")
        await asyncio.sleep(5)

# Vòng lặp gửi vị thế tự động mỗi 5 phút (ở đây set 60s để khớp cấu hình test cũ)
async def auto_pos_sender_loop(app):
    try:
        while True:
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
                    display_side = "LONG" if amt > 0 else "SHORT"
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
                
                for chat_id in list(auto_chats):
                    old_msg_id = last_auto_messages.get(chat_id)
                    
                    if has_new_activity.get(chat_id, True):
                        if old_msg_id:
                            await delete_telegram_message(session, chat_id, old_msg_id)
                        
                        new_msg_id = await send_telegram_message(session, chat_id, message, is_auto=True)
                        if new_msg_id:
                            last_auto_messages[chat_id] = new_msg_id
                            has_new_activity[chat_id] = False
                    else:
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
        logger.info("Task tự động gửi vị thế Bybit đã bị hủy.")
    except Exception as e:
        logger.error(f"Lỗi trong auto_pos_sender_loop Bybit: {e}")

# Xử lý lệnh /auto
async def handle_auto_command(session, chat_id):
    if chat_id in auto_chats:
        auto_chats.remove(chat_id)
        old_msg_id = last_auto_messages.pop(chat_id, None)
        if old_msg_id:
            await delete_telegram_message(session, chat_id, old_msg_id)
        await send_telegram_message(session, chat_id, "❌ Đã tắt tự động cập nhật vị thế mỗi 1 phút.")
    else:
        auto_chats.add(chat_id)
        await send_telegram_message(session, chat_id, "✅ Đã bật tự động cập nhật vị thế mỗi 1 phút.")
        
        if positions:
            text_lines = ["🔍 *TỰ ĐỘNG CẬP NHẬT VỊ THẾ ĐANG MỞ (1P)*\n----------------------------------"]
            for key, pos in positions.items():
                symbol = pos['symbol']
                side = pos['positionSide']
                amt = pos['positionAmt']
                pnl = pos['unrealizedPnL']
                
                display_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
                display_side = "LONG" if amt > 0 else "SHORT"
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
            await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures Bybit nào đang mở.")

# Đăng ký Webhook với Telegram
async def setup_telegram_webhook(session):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    webhook_url = os.getenv("WEBHOOK_URL")
    
    if not webhook_url:
        logger.warning("Cảnh báo: WEBHOOK_URL trống trong file .env.")
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
        await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures Bybit nào đang mở.")
        return
        
    total_pnl = sum(pos.get('unrealizedPnL', 0.0) for pos in positions.values())
    pnl_emoji = "🟩" if total_pnl >= 0 else "🟥"
    sign = "+" if total_pnl >= 0 else ""
    
    message = (
        f"📊 *TỔNG PNL VỊ THẾ HIỆN TẠI (BYBIT)*\n"
        f"----------------------------------\n"
        f"💰 Trạng thái: {pnl_emoji} *{sign}{total_pnl:,.2f} USDT*\n"
        f"🔥 Vị thế đang mở: *{len(positions)}*"
    )
    await send_telegram_message(session, chat_id, message)

# Xử lý lệnh /pos
async def handle_pos_command(session, chat_id):
    if not positions:
        await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures Bybit nào đang mở.")
        return
        
    text_lines = ["🔍 *CHI TIẾT VỊ THẾ ĐANG MỞ (BYBIT)*\n----------------------------------"]
    for key, pos in positions.items():
        symbol = pos['symbol']
        side = pos['positionSide']
        amt = pos['positionAmt']
        pnl = pos['unrealizedPnL']
        
        display_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
        display_side = "LONG" if amt > 0 else "SHORT"
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

# Lấy số dư tài khoản Bybit
async def handle_balance_command(session, chat_id):
    status, data = await bybit_api_request(
        session, "GET", "/v5/account/wallet-balance",
        params={"accountType": "UNIFIED"},
        is_private=True
    )
    if status != 200 or data.get("retCode") != 0:
        status, data = await bybit_api_request(
            session, "GET", "/v5/account/wallet-balance",
            params={"accountType": "CONTRACT"},
            is_private=True
        )
        
    if status == 200 and data.get("retCode") == 0:
        acc_info = data.get("result", {}).get("list", [{}])[0]
        wallet_bal = float(acc_info.get('totalWalletBalance', 0) or 0)
        pnl = float(acc_info.get('totalUnrealisedPnl', 0) or 0)
        margin_bal = float(acc_info.get('totalMarginBalance', 0) or 0)
        avail_bal = float(acc_info.get('totalAvailableBalance', 0) or 0)
        
        if wallet_bal == 0.0 and avail_bal == 0.0:
            for c_info in acc_info.get('coin', []):
                if c_info.get('coin') == 'USDT':
                    wallet_bal = float(c_info.get('walletBalance', 0) or 0)
                    pnl = float(c_info.get('unrealisedPnl', 0) or 0)
                    avail_bal = float(c_info.get('availableToWithdraw', 0) or 0)
                    margin_bal = wallet_bal + pnl
                    break
                    
        pnl_emoji = "🟩" if pnl >= 0 else "🟥"
        pnl_sign = "+" if pnl >= 0 else ""
        
        message = (
            f"💳 *THÔNG TIN TÀI KHOẢN FUTURES (BYBIT)*\n"
            f"----------------------------------\n"
            f"💰 Số dư ví: *{wallet_bal:,.2f} USDT*\n"
            f"📊 PnL chưa thực hiện: {pnl_emoji} *{pnl_sign}{pnl:,.2f} USDT*\n"
            f"🛡️ Số dư ký quỹ (Margin Balance): *{margin_bal:,.2f} USDT*\n"
            f"🟢 Khả dụng vào lệnh: *{avail_bal:,.2f} USDT*"
        )
        await send_telegram_message(session, chat_id, message)
    else:
        msg_err = data.get('retMsg', 'Lỗi không xác định')
        logger.error(f"Lỗi lấy số dư tài khoản Bybit: {data}")
        await send_telegram_message(session, chat_id, f"❌ Lỗi khi truy vấn số dư từ Bybit: `{msg_err}`")

# Top biến động 24h Bybit
async def handle_top_command(session, chat_id):
    status, data = await bybit_api_request(
        session, "GET", "/v5/market/tickers",
        params={"category": "linear"}
    )
    if status == 200 and data.get("retCode") == 0:
        tickers = data.get("result", {}).get("list", [])
        
        usdt_tickers = []
        for item in tickers:
            symbol = item['symbol']
            if symbol.endswith("USDT"):
                try:
                    change = float(item.get('price24hPcnt', 0)) * 100
                except:
                    change = 0.0
                    
                usdt_tickers.append({
                    'symbol': symbol[:-4],
                    'price': float(item.get('lastPrice', 0)),
                    'change': change
                })
                
        usdt_tickers.sort(key=lambda x: x['change'], reverse=True)
        
        top_gainers = usdt_tickers[:5]
        top_losers = usdt_tickers[-5:]
        top_losers.reverse()
        
        lines = ["🔥 *TOP BIẾN ĐỘNG TRONG 24H (BYBIT FUTURES)*\n----------------------------------"]
        
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
        logger.error(f"Lỗi lấy top biến động Bybit: {data}")
        await send_telegram_message(session, chat_id, "❌ Lỗi khi lấy dữ liệu biến động từ Bybit.")

# Xem các lệnh đang chờ
async def handle_orders_command(session, chat_id):
    prices_map = {}
    try:
        status_p, data_p = await bybit_api_request(session, "GET", "/v5/market/tickers", params={"category": "linear"})
        if status_p == 200 and data_p.get("retCode") == 0:
            prices_map = {item['symbol']: float(item['lastPrice']) for item in data_p.get("result", {}).get("list", [])}
    except Exception as e:
        logger.error(f"Lỗi lấy giá hiện tại khi xem orders Bybit: {e}")
        
    status, data = await bybit_api_request(
        session, "GET", "/v5/order/realtime",
        params={"category": "linear", "settleCoin": "USDT"},
        is_private=True
    )
    
    if status == 200 and data.get("retCode") == 0:
        orders_list = data.get("result", {}).get("list", [])
        
        if not orders_list:
            await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có lệnh chờ (Open Orders) nào trên tài khoản Bybit.")
            return
            
        lines = ["⏳ *DANH SÁCH LỆNH ĐANG CHỜ KHỚP (BYBIT)*\n----------------------------------"]
        for i, order in enumerate(orders_list, 1):
            symbol = order.get('symbol')
            order_id = order.get('orderId')
            price = float(order.get('price', 0) or 0)
            qty = float(order.get('qty', 0) or 0)
            side = order.get('side')
            pos_idx = int(order.get('positionIdx', 0))
            order_type = order.get('orderType')
            
            display_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
            
            if pos_idx == 1:
                display_side = "LONG"
            elif pos_idx == 2:
                display_side = "SHORT"
            else:
                display_side = "LONG" if side == 'Buy' else "SHORT"
                
            emoji = "🟢" if display_side == "LONG" else "🔴"
            notional = qty * price
            current_price = prices_map.get(symbol)
            if current_price is None or current_price == 0:
                current_price = await get_single_price(session, symbol)
                if current_price > 0:
                    prices_map[symbol] = current_price
                    
            price_str_val = f"{price:,.4f} USDT" if price > 0 else "Market"
            price_line = f"   • Giá đặt: *{price_str_val}*\n"
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
        msg_err = data.get('retMsg', 'Lỗi không xác định')
        await send_telegram_message(session, chat_id, f"❌ Lỗi khi truy vấn danh sách lệnh từ Bybit: `{msg_err}`")

# Hủy lệnh chờ
async def handle_cancel_command(session, chat_id, coin_name, order_id_str):
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    body = {
        "category": "linear",
        "symbol": symbol,
        "orderId": order_id_str
    }
    status, data = await bybit_api_request(session, "POST", "/v5/order/cancel", body=body, is_private=True)
    if status == 200 and data.get("retCode") == 0:
        await send_telegram_message(
            session, 
            chat_id, 
            f"✅ *HỦY LỆNH THÀNH CÔNG (BYBIT)!*\n"
            f"----------------------------------\n"
            f"🪙 Cặp: *{symbol}*\n"
            f"🆔 Order ID đã hủy: `{order_id_str}`"
        )
    else:
        msg_err = data.get('retMsg', 'Lỗi không xác định')
        code_err = data.get('retCode', -1)
        await send_telegram_message(session, chat_id, f"❌ *Hủy lệnh thất bại!*\nBybit báo lỗi: `{msg_err}` (Code: {code_err})")

# Lịch sử chốt vị thế
async def handle_history_command(session, chat_id, coin_name=None):
    symbol = None
    if coin_name:
        coin_name = coin_name.upper()
        symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
        
    params = {
        "category": "linear",
        "limit": 50
    }
    if symbol:
        params["symbol"] = symbol
        
    status, data = await bybit_api_request(session, "GET", "/v5/position/closed-pnl", params=params, is_private=True)
    if status == 200 and data.get("retCode") == 0:
        history_list = data.get("result", {}).get("list", [])
        if not history_list:
            await send_telegram_message(
                session, 
                chat_id, 
                f"ℹ️ Không tìm thấy lịch sử chốt vị thế (Closed PnL) nào{' cho ' + symbol if symbol else ''} trên Bybit."
            )
            return
            
        grouped_data = []
        for item in history_list:
            sym = item.get('symbol')
            closed_pnl = float(item.get('closedPnl', 0))
            time_ms = int(item.get('updatedTime') or item.get('createdTime') or 0)
            
            found = False
            for g in grouped_data:
                if g['symbol'] == sym and abs(g['time'] - time_ms) <= 10000:
                    g['income'] += closed_pnl
                    if time_ms > g['time']:
                        g['time'] = time_ms
                    found = True
                    break
                    
            if not found:
                grouped_data.append({
                    'symbol': sym,
                    'income': closed_pnl,
                    'time': time_ms
                })
                
        grouped_data.sort(key=lambda x: x['time'], reverse=True)
        display_data = grouped_data[:10]
        
        tz_vn = timezone(timedelta(hours=7))
        lines = ["📜 *LỊCH SỬ CHỐT VỊ THẾ GẦN NHẤT (CLOSED PNL - BYBIT)*\n----------------------------------"]
        
        total_realized_pnl = 0.0
        for i, item in enumerate(display_data, 1):
            sym = item['symbol']
            income = item['income']
            time_ms = item['time']
            
            total_realized_pnl += income
            
            time_dt = datetime.fromtimestamp(time_ms / 1000.0, tz=tz_vn)
            time_str = time_dt.strftime("%d/%m/%Y %H:%M:%S")
            
            display_sym = sym[:-4] if sym.endswith("USDT") else sym
            pnl_emoji = "🟩" if income >= 0 else "🟥"
            pnl_sign = "+" if income >= 0 else ""
            
            lines.append(
                f"{i}. *{display_sym}* ➜ {pnl_emoji} `{pnl_sign}{income:,.2f} USDT`\n"
                f"Thời gian: `{time_str}`"
            )
            
        lines.append("----------------------------------")
        total_emoji = "🟩" if total_realized_pnl >= 0 else "🟥"
        total_sign = "+" if total_realized_pnl >= 0 else ""
        lines.append(f"📊 *Tổng kết {len(display_data)} vị thế gần nhất:*")
        lines.append(f"💰 Tổng Closed PnL: {total_emoji} `{total_sign}{total_realized_pnl:,.2f} USDT`")
        
        message = "\n\n".join(lines)
        await send_telegram_message(session, chat_id, message)
    else:
        msg_err = data.get('retMsg', 'Lỗi không xác định')
        await send_telegram_message(session, chat_id, f"❌ Lỗi khi truy vấn lịch sử vị thế từ Bybit: `{msg_err}`")

# Xem giá thanh lý các vị thế đang mở
async def handle_liq_command(session, chat_id):
    status, data = await bybit_api_request(
        session, "GET", "/v5/position/list",
        params={"category": "linear", "settleCoin": "USDT"},
        is_private=True
    )
    if status == 200 and data.get("retCode") == 0:
        pos_list = data.get("result", {}).get("list", [])
        open_positions = [p for p in pos_list if float(p.get('size', 0)) != 0.0]
        
        if not open_positions:
            await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures Bybit nào đang mở.")
            return
            
        lines = ["☣️ *GIÁ THANH LÝ CÁC VỊ THẾ ĐANG MỞ (BYBIT)*\n----------------------------------"]
        for p in open_positions:
            symbol = p.get('symbol')
            position_idx = int(p.get('positionIdx', 0))
            size = float(p.get('size', 0))
            entry_price = float(p.get('entryPrice', 0))
            mark_price = float(p.get('markPrice', 0))
            unrealised_pnl = float(p.get('unrealisedPnl', 0))
            leverage = p.get('leverage')
            liq_price = float(p.get('liqPrice', 0) or 0)
            side = p.get('side')
            
            display_symbol = symbol[:-4] if symbol.endswith('USDT') else symbol
            
            if position_idx == 1:
                display_side = "LONG"
            elif position_idx == 2:
                display_side = "SHORT"
            else:
                display_side = "LONG" if side == 'Buy' else "SHORT"
                
            pnl_emoji = "🟩" if unrealised_pnl >= 0 else "🟥"
            pnl_sign = "+" if unrealised_pnl >= 0 else ""
            
            liq_price_str = format_price(liq_price) if liq_price > 0 else "Không có ( CROSS/Safe )"
            
            pos_lines = (
                f"🪙 *{display_symbol}* ({display_side})\n"
                f"• Entry: `{format_price(entry_price)} USDT`\n"
                f"• Mark Price: `{format_price(mark_price)} USDT`\n"
                f"• PnL: {pnl_emoji} `{pnl_sign}{unrealised_pnl:,.2f} USDT`\n"
                f"• Leverage: `{leverage}x`\n"
                f"• **Giá thanh lý:** 💀 `{liq_price_str}`"
            )
            lines.append(pos_lines)
            
        message = "\n\n".join(lines)
        await send_telegram_message(session, chat_id, message)
    else:
        msg_err = data.get('retMsg', 'Lỗi không xác định')
        await send_telegram_message(session, chat_id, f"❌ Lỗi khi lấy thông tin thanh lý từ Bybit: `{msg_err}`")

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

# Lấy thông tin giá coin hàng loạt (24h) từ Bybit
async def get_coin_prices(session, coin_names):
    targets = {}
    for coin in coin_names:
        coin_upper = coin.upper()
        symbol = coin_upper if coin_upper.endswith("USDT") else f"{coin_upper}USDT"
        targets[symbol] = coin_upper

    status, data = await bybit_api_request(session, "GET", "/v5/market/tickers", params={"category": "linear"})
    if status == 200 and data.get("retCode") == 0:
        tickers = data.get("result", {}).get("list", [])
        prices_map = {}
        for item in tickers:
            symbol = item['symbol']
            try:
                change = float(item.get('price24hPcnt', 0)) * 100
            except:
                change = 0.0
            prices_map[symbol] = {
                'price': float(item.get('lastPrice', 0)),
                'change': change
            }
        
        results = []
        for symbol, coin_upper in targets.items():
            info = prices_map.get(symbol)
            results.append((coin_upper, info))
        return results
    else:
        logger.error(f"Lỗi lấy giá tickers từ Bybit: {data}")
    return [(coin.upper(), None) for coin in coin_names]

def convert_interval_binance_to_bybit(interval):
    mapping = {
        '1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30',
        '1h': '60', '2h': '120', '4h': '240', '6h': '360', '12h': '720',
        '1d': 'D', '3d': 'D',
        '1w': 'W', '1M': 'M'
    }
    return mapping.get(interval.lower(), '60')

# Phân tích kỹ thuật chi tiết
async def analyze_market(session, symbol, interval='1h'):
    bybit_interval = convert_interval_binance_to_bybit(interval)
    status, data = await bybit_api_request(
        session, "GET", "/v5/market/kline",
        params={"category": "linear", "symbol": symbol, "interval": bybit_interval, "limit": 100}
    )
    if status != 200 or data.get("retCode") != 0:
        logger.error(f"Lỗi khi lấy klines Bybit cho {symbol}: {data}")
        return None
        
    klines_data = data.get("result", {}).get("list", [])
    if not klines_data:
        return None
        
    # Bybit trả về từ nến mới nhất đến cũ nhất, ta phải đảo ngược lại
    klines_data.reverse()
    
    # Cấu trúc: [startTime, openPrice, highPrice, lowPrice, closePrice, volume, turnover]
    df = pd.DataFrame(klines_data, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
    df['close'] = df['close'].astype(float)
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    
    # 1. RSI (14)
    close_delta = df['close'].diff()
    up = close_delta.clip(lower=0)
    down = -1 * close_delta.clip(upper=0)
    ma_up = up.ewm(com=13, adjust=False).mean()
    ma_down = down.ewm(com=13, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-10)
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # 2. EMA (9, 21)
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    
    # 3. Bollinger Bands (20, 2)
    df['ma20'] = df['close'].rolling(window=20).mean()
    df['std20'] = df['close'].rolling(window=20).std()
    df['upper_band'] = df['ma20'] + (df['std20'] * 2)
    df['lower_band'] = df['ma20'] - (df['std20'] * 2)
    
    # 4. MACD (12, 26, 9)
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = exp1 - exp2
    df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['hist'] = df['macd'] - df['signal']
    
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    close_price = latest['close']
    rsi_val = latest['rsi']
    ema9_val = latest['ema9']
    ema21_val = latest['ema21']
    upper_b = latest['upper_band']
    lower_b = latest['lower_band']
    macd_val = latest['macd']
    sig_val = latest['signal']
    hist_val = latest['hist']
    prev_hist = prev['hist']
    
    long_score = 0.0
    short_score = 0.0
    
    if rsi_val <= 30:
        long_score += 2.0
    elif rsi_val >= 70:
        short_score += 2.0
    elif rsi_val < 40:
        long_score += 0.8
    elif rsi_val > 60:
        short_score += 0.8
        
    if close_price > ema9_val > ema21_val:
        long_score += 1.2
    elif close_price < ema9_val < ema21_val:
        short_score += 1.2
        
    if close_price <= lower_b:
        long_score += 1.5
    elif close_price >= upper_b:
        short_score += 1.5
        
    if hist_val > 0 and prev_hist <= 0:
        long_score += 1.0
    elif hist_val < 0 and prev_hist >= 0:
        short_score += 1.0
    elif hist_val > 0:
        long_score += 0.5
    elif hist_val < 0:
        short_score += 0.5
        
    signal = 'NEUTRAL'
    confidence = 'Thấp'
    
    if long_score > short_score:
        if long_score >= 3.0:
            signal = 'LONG'
            confidence = 'Mạnh' if long_score >= 4.0 else 'Trung bình'
    elif short_score > long_score:
        if short_score >= 3.0:
            signal = 'SHORT'
            confidence = 'Mạnh' if short_score >= 4.0 else 'Trung bình'
            
    tp_price = 0.0
    sl_price = 0.0
    bb_width = upper_b - lower_b
    
    qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)
    
    if signal == 'LONG':
        sl_price = close_price - (bb_width * 0.6)
        if sl_price <= 0 or sl_price >= close_price:
            sl_price = close_price * 0.95
        tp_price = close_price + (close_price - sl_price) * 1.5
    elif signal == 'SHORT':
        sl_price = close_price + (bb_width * 0.6)
        if sl_price <= close_price:
            sl_price = close_price * 1.05
        tp_price = close_price - (sl_price - close_price) * 1.5
        if tp_price <= 0:
            tp_price = close_price * 0.90
        
    if tp_price > 0:
        tp_price = round_price_step(tp_price, tick_size, price_p)
    if sl_price > 0:
        sl_price = round_price_step(sl_price, tick_size, price_p)
        
    return {
        'symbol': symbol,
        'close': close_price,
        'rsi': rsi_val,
        'ema9': ema9_val,
        'ema21': ema21_val,
        'upper_band': upper_b,
        'lower_band': lower_b,
        'macd': macd_val,
        'signal_line': sig_val,
        'hist': hist_val,
        'signal': signal,
        'confidence': confidence,
        'long_score': long_score,
        'short_score': short_score,
        'tp': tp_price,
        'sl': sl_price
    }

# Quét top 100 volume 24h
async def scan_market_signals(session):
    status, data = await bybit_api_request(session, "GET", "/v5/market/tickers", params={"category": "linear"})
    coins_to_scan = []
    if status == 200 and data.get("retCode") == 0:
        tickers = data.get("result", {}).get("list", [])
        usdt_tickers = [t for t in tickers if t['symbol'].endswith('USDT')]
        # Sắp xếp theo volume 24h (turnover24h đại diện cho volume USDT)
        usdt_tickers.sort(key=lambda x: float(x.get('turnover24h', 0)), reverse=True)
        coins_to_scan = [t['symbol'] for t in usdt_tickers[:100]]
    
    if not coins_to_scan:
        fallback_coins = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE', 'ADA', 'LINK', 'NEAR', 'SUI', 'AVAX', 'OP']
        coins_to_scan = [f"{c}USDT" for c in fallback_coins]
        
    tasks = []
    for symbol in coins_to_scan:
        tasks.append(analyze_market(session, symbol, interval='1h'))
        
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    long_signals = []
    short_signals = []
    
    for res in results:
        if isinstance(res, dict) and res.get('signal') in ('LONG', 'SHORT'):
            if res['signal'] == 'LONG':
                long_signals.append(res)
            else:
                short_signals.append(res)
                
    long_signals.sort(key=lambda x: x['long_score'], reverse=True)
    short_signals.sort(key=lambda x: x['short_score'], reverse=True)
    
    return long_signals[:5], short_signals[:5]

# Lệnh /analyze
async def handle_analyze_command(session, chat_id, coin_name=None):
    if coin_name:
        coin_name = coin_name.upper()
        symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
        
        loading_msg_id = await send_telegram_message(
            session,
            chat_id,
            f"⏳ Đang phân tích kỹ thuật Bybit cho *{symbol}* (khung 1h)..."
        )
        
        try:
            res = await analyze_market(session, symbol, interval='1h')
            if loading_msg_id:
                await delete_telegram_message(session, chat_id, loading_msg_id)
                
            if not res:
                await send_telegram_message(
                    session,
                    chat_id,
                    f"❌ Không thể lấy dữ liệu phân tích cho *{symbol}*. Vui lòng kiểm tra lại tên coin."
                )
                return
                
            price_str = format_price(res['close'])
            rsi_str = f"{res['rsi']:.1f}"
            ema9_str = format_price(res['ema9'])
            ema21_str = format_price(res['ema21'])
            upper_str = format_price(res['upper_band'])
            lower_str = format_price(res['lower_band'])
            macd_hist_str = f"{res['hist']:+,.4f}".rstrip('0').rstrip('.')
            
            rsi_desc = "Quá bán (Bullish)" if res['rsi'] <= 30 else ("Quá mua (Bearish)" if res['rsi'] >= 70 else "Trung tính")
            ema_desc = "Tăng (Bullish)" if res['close'] > res['ema9'] > res['ema21'] else ("Giảm (Bearish)" if res['close'] < res['ema9'] < res['ema21'] else "Trung tính")
            bb_desc = "Chạm biên dưới (Bullish)" if res['close'] <= res['lower_band'] else ("Chạm biên trên (Bearish)" if res['close'] >= res['upper_band'] else "Trung tính")
            macd_desc = "Bullish" if res['hist'] > 0 else "Bearish"
            
            sig_emoji = "🟩 LONG" if res['signal'] == 'LONG' else ("🟥 SHORT" if res['signal'] == 'SHORT' else "⬜ NEUTRAL (Đứng ngoài)")
            conf_color = "🟢" if res['confidence'] == 'Mạnh' else ("🟡" if res['confidence'] == 'Trung bình' else "⚪")
            
            msg = (
                f"📊 *PHÂN TÍCH KỸ THUẬT: {symbol} (1h) - BYBIT*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💵 Giá hiện tại: `*{price_str} USDT*`\n\n"
                f"🔍 *Các chỉ báo chính:*\n"
                f"• *RSI (14):* `{rsi_str}` ➜ _{rsi_desc}_\n"
                f"• *EMA Trend:* Giá vs EMA9 (`{ema9_str}`) & EMA21 (`{ema21_str}`) ➜ _{ema_desc}_\n"
                f"• *Bollinger Bands:* Biên `{lower_str}` - `{upper_str}` ➜ _{bb_desc}_\n"
                f"• *MACD:* Hist `{macd_hist_str}` ➜ _{macd_desc}_\n\n"
                f"🎯 *KẾT LUẬN TÍN HIỆU:*\n"
                f"👉 Khuyến nghị: **{sig_emoji}**\n"
                f"🔥 Độ tin cậy: {conf_color} *{res['confidence']}* (L: `{res['long_score']:.1f}` | S: `{res['short_score']:.1f}`)\n\n"
            )
            
            if res['signal'] != 'NEUTRAL':
                tp_str = format_price(res['tp'])
                sl_str = format_price(res['sl'])
                tp_change = ((res['tp'] - res['close']) / res['close']) * 100
                sl_change = ((res['sl'] - res['close']) / res['close']) * 100
                msg += (
                    f"🛡️ *Kế hoạch giao dịch gợi ý:*\n"
                    f"• *Entry:* quanh `{price_str} USDT`\n"
                    f"• *Target TP:* `{tp_str} USDT` ({tp_change:+.2f}%)\n"
                    f"• *Stop Loss:* `{sl_str} USDT` ({sl_change:+.2f}%)"
                )
            else:
                msg += "💡 *Gợi ý:* Thị trường chưa có xu hướng rõ ràng, nên kiên nhẫn đứng ngoài quan sát thêm."
                
            await send_telegram_message(session, chat_id, msg)
            
        except Exception as e:
            logger.error(f"Lỗi khi phân tích cho {symbol}: {e}")
            if loading_msg_id:
                await delete_telegram_message(session, chat_id, loading_msg_id)
            await send_telegram_message(session, chat_id, f"❌ Đã xảy ra lỗi khi phân tích: {e}")
            
    else:
        loading_msg_id = await send_telegram_message(
            session,
            chat_id,
            "🔍 Đang quét thị trường Bybit tìm cơ hội giao dịch tỉ lệ thắng cao..."
        )
        
        try:
            long_signals, short_signals = await scan_market_signals(session)
            if loading_msg_id:
                await delete_telegram_message(session, chat_id, loading_msg_id)
                
            msg_lines = [
                "🔍 *QUÉT TÍN HIỆU CƠ HỘI GIAO DỊCH (BYBIT 1h)*",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                "Dưới đây là các coin trong Top 100 Volume 24h Bybit có tín hiệu tốt nhất hiện tại:\n"
            ]
            
            has_signals = False
            
            if long_signals:
                has_signals = True
                msg_lines.append("🚀 *CƠ HỘI LONG (Tỉ lệ thắng cao):*")
                for i, res in enumerate(long_signals, 1):
                    coin = res['symbol'][:-4] if res['symbol'].endswith("USDT") else res['symbol']
                    price_str = format_price(res['close'])
                    rsi_str = f"{res['rsi']:.1f}"
                    tp_str = format_price(res['tp'])
                    sl_str = format_price(res['sl'])
                    conf = "Mạnh 🔥" if res['confidence'] == 'Mạnh' else "Trung bình"
                    tp_change = ((res['tp'] - res['close']) / res['close']) * 100
                    sl_change = ((res['sl'] - res['close']) / res['close']) * 100
                    msg_lines.append(
                        f"{i}. *{coin}* ➜ Price: `{price_str}` (RSI: `{rsi_str}`)\n"
                        f"   • Khuyến nghị: *LONG* (Độ tin cậy: `{conf}`)\n"
                        f"   • Gợi ý: TP `{tp_str}` ({tp_change:+.2f}%) | SL `{sl_str}` ({sl_change:+.2f}%)"
                    )
                msg_lines.append("")
                
            if short_signals:
                has_signals = True
                msg_lines.append("📉 *CƠ HỘI SHORT (Tỉ lệ thắng cao):*")
                for i, res in enumerate(short_signals, 1):
                    coin = res['symbol'][:-4] if res['symbol'].endswith("USDT") else res['symbol']
                    price_str = format_price(res['close'])
                    rsi_str = f"{res['rsi']:.1f}"
                    tp_str = format_price(res['tp'])
                    sl_str = format_price(res['sl'])
                    conf = "Mạnh ⚡" if res['confidence'] == 'Mạnh' else "Trung bình"
                    tp_change = ((res['tp'] - res['close']) / res['close']) * 100
                    sl_change = ((res['sl'] - res['close']) / res['close']) * 100
                    msg_lines.append(
                        f"{i}. *{coin}* ➜ Price: `{price_str}` (RSI: `{rsi_str}`)\n"
                        f"   • Khuyến nghị: *SHORT* (Độ tin cậy: `{conf}`)\n"
                        f"   • Gợi ý: TP `{tp_str}` ({tp_change:+.2f}%) | SL `{sl_str}` ({sl_change:+.2f}%)"
                    )
                    
            if not has_signals:
                msg_lines.append("⬜ *Hiện tại chưa phát hiện tín hiệu LONG/SHORT rõ rệt.*")
                
            msg_lines.append("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            msg_lines.append("💡 *Mẹo:* Sử dụng `/analyze <coin>` để phân tích chi tiết cho một coin cụ thể.")
            
            await send_telegram_message(session, chat_id, "\n".join(msg_lines))
            
        except Exception as e:
            logger.error(f"Lỗi khi quét tín hiệu Bybit: {e}")
            if loading_msg_id:
                await delete_telegram_message(session, chat_id, loading_msg_id)
            await send_telegram_message(session, chat_id, f"❌ Lỗi khi quét tín hiệu thị trường: {e}")

# Kiểm tra Position Mode (Hedge hay One-way) của tài khoản Bybit
async def check_position_mode(session, api_key, api_secret):
    global hedge_mode
    status, data = await bybit_api_request(
        session, "GET", "/v5/position/list",
        params={"category": "linear", "symbol": "BTCUSDT", "limit": 10},
        is_private=True
    )
    if status == 200 and data.get("retCode") == 0:
        pos_list = data.get("result", {}).get("list", [])
        has_hedge = any(int(p.get('positionIdx', 0)) in (1, 2) for p in pos_list)
        hedge_mode = has_hedge
        logger.info(f"Chế độ Position Mode của tài khoản Bybit: {'Hedge Mode (Dual)' if hedge_mode else 'One-way Mode'}")
    else:
        logger.error(f"Lỗi kiểm tra Position Mode từ Bybit: {data}. Mặc định là One-way Mode.")

def round_down(value, decimals):
    factor = 10 ** decimals
    return math.floor(value * factor) / factor

# Nạp thông tin độ chính xác từ Bybit V5
async def init_exchange_info(session):
    global symbol_precisions, symbol_price_precisions, symbol_tick_sizes
    status, data = await bybit_api_request(session, "GET", "/v5/market/instruments-info", params={"category": "linear"})
    if status == 200 and data.get("retCode") == 0:
        instruments = data.get("result", {}).get("list", [])
        for inst in instruments:
            sym = inst['symbol']
            tick_size_str = inst.get('priceFilter', {}).get('tickSize', '0')
            qty_step_str = inst.get('lotSizeFilter', {}).get('qtyStep', '0')
            
            tick_size = float(tick_size_str)
            
            price_precision = 0
            if '.' in tick_size_str:
                price_precision = len(tick_size_str.split('.')[1].rstrip('0'))
                
            qty_precision = 0
            if '.' in qty_step_str:
                qty_precision = len(qty_step_str.split('.')[1].rstrip('0'))
                
            symbol_precisions[sym] = qty_precision
            symbol_price_precisions[sym] = price_precision
            symbol_tick_sizes[sym] = tick_size if tick_size > 0 else 10**(-price_precision)
        logger.info(f"Đã nạp thành công độ chính xác ({len(symbol_precisions)} symbols) từ Bybit.")
    else:
        logger.error(f"Lỗi nạp exchangeInfo từ Bybit: {data}")

def round_price_step(price, tick_size, price_precision):
    if tick_size <= 0:
        return round(price, price_precision)
    rounded = round(round(price / tick_size) * tick_size, price_precision)
    return rounded

async def get_symbol_precisions(session, symbol):
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
async def get_max_leverage(session, symbol):
    status, data = await bybit_api_request(
        session, "GET", "/v5/market/instruments-info",
        params={"category": "linear", "symbol": symbol}
    )
    if status == 200 and data.get("retCode") == 0:
        list_data = data.get("result", {}).get("list", [])
        if list_data:
            lev_filter = list_data[0].get('leverageFilter', {})
            max_lev = lev_filter.get('maxLeverage')
            if max_lev:
                return int(float(max_lev))
    return 20 # Mặc định trả về 20 nếu lỗi

# Cài đặt đòn bẩy
async def set_leverage(session, symbol, leverage):
    body = {
        "category": "linear",
        "symbol": symbol,
        "buyLeverage": str(leverage),
        "sellLeverage": str(leverage)
    }
    status, data = await bybit_api_request(session, "POST", "/v5/position/set-leverage", body=body, is_private=True)
    # Bybit trả về code 110043 nếu đòn bẩy không thay đổi, ta coi đó là thành công
    return status == 200 and (data.get("retCode") == 0 or data.get("retCode") == 110043)

# Lấy giá đơn lẻ của symbol
async def get_single_price(session, symbol):
    status, data = await bybit_api_request(session, "GET", "/v5/market/tickers", params={"category": "linear", "symbol": symbol})
    if status == 200 and data.get("retCode") == 0:
        list_data = data.get("result", {}).get("list", [])
        if list_data:
            return float(list_data[0].get('lastPrice', 0))
    return 0.0

def calculate_tpsl_price(input_str, entry_price, quantity, leverage, is_long, is_tp):
    input_str = input_str.strip().lower()
    
    # 1. ROE %: vd "100r", "50roe"
    if input_str.endswith('roe') or input_str.endswith('r'):
        clean_str = input_str.replace('roe', '').replace('r', '').replace('%', '').strip()
        roe_val = abs(float(clean_str))
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

# Vẽ biểu đồ hình nến (Klines Bybit)
async def draw_candlestick_chart(session, symbol, interval):
    bybit_interval = convert_interval_binance_to_bybit(interval)
    status, data = await bybit_api_request(
        session, "GET", "/v5/market/kline",
        params={"category": "linear", "symbol": symbol, "interval": bybit_interval, "limit": 80}
    )
    if status != 200 or data.get("retCode") != 0:
        raise Exception(f"Bybit API trả về lỗi: {data.get('retMsg', 'HTTP Error')}")
        
    klines_data = data.get("result", {}).get("list", [])
    if not klines_data:
        raise Exception("Dữ liệu nến trống.")
        
    # Đảo ngược nến Bybit về thứ tự thời gian tăng dần
    klines_data.reverse()
    
    df = pd.DataFrame(klines_data, columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
    df['open_time'] = pd.to_datetime(df['open_time'].astype(float), unit='ms')
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)
    
    up_color = '#0ecb81'   # Green
    down_color = '#f6465d' # Red
    df['color'] = df.apply(lambda row: up_color if row['close'] >= row['open'] else down_color, axis=1)

    if len(df) > 1:
        diff_sec = (df['open_time'].iloc[1] - df['open_time'].iloc[0]).total_seconds()
        width = (diff_sec / 86400.0) * 0.7
    else:
        width = 0.0005

    plt.style.use('dark_background')
    fig, (ax, ax_vol) = plt.subplots(
        2, 1, figsize=(10, 6), sharex=True,
        gridspec_kw={'height_ratios': [3, 1]}
    )
    fig.subplots_adjust(hspace=0.05)

    ax.vlines(df['open_time'], df['low'], df['high'], color=df['color'], linewidth=1)
    
    bottoms = df[['open', 'close']].min(axis=1)
    heights = (df['close'] - df['open']).abs()
    
    zero_height_mask = heights == 0
    if zero_height_mask.any():
        mini_height = (df['high'] - df['low']) * 0.03
        mini_height = mini_height.where(mini_height > 0, 0.0001)
        heights = heights.where(~zero_height_mask, mini_height)
        
    ax.bar(df['open_time'], heights, bottom=bottoms, width=width, color=df['color'], edgecolor=df['color'], linewidth=0.5)
    ax_vol.bar(df['open_time'], df['volume'], width=width, color=df['color'])

    ax.set_title(f"📊 {symbol} ({interval.upper()}) - Bybit Futures", fontsize=14, color='white', fontweight='bold', pad=15)
    ax.grid(True, color='#2F3336', linestyle='--', linewidth=0.5)
    ax_vol.grid(True, color='#2F3336', linestyle='--', linewidth=0.5)
    
    for s in ['top', 'right', 'left', 'bottom']:
        ax.spines[s].set_color('#2f3336')
        ax_vol.spines[s].set_color('#2f3336')
        
    ax.tick_params(colors='white', labelsize=10)
    ax_vol.tick_params(colors='white', labelsize=10)
    
    ax.yaxis.tick_right()
    ax.yaxis.set_label_position("right")
    ax_vol.yaxis.tick_right()
    
    if 'm' in interval.lower() or 'h' in interval.lower():
        date_format = mdates.DateFormatter('%m-%d %H:%M')
    else:
        date_format = mdates.DateFormatter('%Y-%m-%d')
    ax_vol.xaxis.set_major_formatter(date_format)
    fig.autofmt_xdate()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=120)
    buf.seek(0)
    plt.close(fig)
    return buf

async def send_telegram_photo(session, chat_id, photo_bytes, caption=None):
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
            return resp.status == 200
    except Exception as e:
        logger.error(f"Lỗi kết nối khi gửi ảnh: {e}")
        return False

# Đặt lệnh chính (LIMIT/MARKET) và TP/SL kèm theo
async def handle_order_command(session, chat_id, side_type, coin_name, volume_str, price_str=None, tp_price_str=None, sl_price_str=None):
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    try:
        volume = float(volume_str)
        if volume <= 0:
            raise ValueError()
    except ValueError:
        await send_telegram_message(session, chat_id, "❌ Số tiền volume không hợp lệ. Vui lòng nhập số dương lớn hơn 0.")
        return

    qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)

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

    if tp_price_str:
        tp_price_str = tp_price_str.strip()
    if sl_price_str:
        sl_price_str = sl_price_str.strip()

    # 1. Lấy leverage tối đa và tự set cho symbol
    max_leverage = await get_max_leverage(session, symbol)
    logger.info(f"Đòn bẩy tối đa của {symbol} là {max_leverage}x. Tiến hành cài đặt...")
    await set_leverage(session, symbol, max_leverage)
    
    # 2. Quy đổi quantity
    if is_limit:
        exchange_price = limit_price
    else:
        current_price = await get_single_price(session, symbol)
        if current_price <= 0:
            await send_telegram_message(session, chat_id, f"❌ Không thể lấy giá hiện tại của {symbol} để quy đổi.")
            return
        exchange_price = current_price
        
    raw_qty = volume / exchange_price
    quantity = round_down(raw_qty, qty_p)
    
    if quantity <= 0:
        await send_telegram_message(
            session, 
            chat_id, 
            f"❌ Số lượng coin quá nhỏ ({raw_qty:.8f} {coin_name}).\n"
            f"Vui lòng tăng Volume đặt lệnh.\n"
            f"(Precision: {qty_p} số thập phân)"
        )
        return

    # Kiểm tra Position Mode thực tế của symbol này trên Bybit
    symbol_hedge_mode = False
    status_mode, data_mode = await bybit_api_request(
        session, "GET", "/v5/position/list",
        params={"category": "linear", "symbol": symbol},
        is_private=True
    )
    if status_mode == 200 and data_mode.get("retCode") == 0:
        pos_list = data_mode.get("result", {}).get("list", [])
        symbol_hedge_mode = any(int(p.get('positionIdx', 0)) in (1, 2) for p in pos_list)
    else:
        symbol_hedge_mode = hedge_mode

    # Xác định index và side theo symbol_hedge_mode
    if side_type == 'LONG':
        side = 'Buy'
        position_idx = 1 if symbol_hedge_mode else 0
    else:
        side = 'Sell'
        position_idx = 2 if symbol_hedge_mode else 0
        
    # Tính toán TP/SL nếu được truyền kèm
    final_tp_price = None
    if tp_price_str:
        try:
            ref_price = limit_price if is_limit else exchange_price
            final_tp_price = calculate_tpsl_price(
                tp_price_str,
                entry_price=ref_price,
                quantity=quantity,
                leverage=max_leverage,
                is_long=(side_type == 'LONG'),
                is_tp=True
            )
            final_tp_price = round_price_step(final_tp_price, tick_size, price_p)
        except Exception as e:
            await send_telegram_message(session, chat_id, f"❌ *Lỗi tính toán TP '{tp_price_str}':* `{e}`")
            return

    final_sl_price = None
    if sl_price_str:
        try:
            ref_price = limit_price if is_limit else exchange_price
            final_sl_price = calculate_tpsl_price(
                sl_price_str,
                entry_price=ref_price,
                quantity=quantity,
                leverage=max_leverage,
                is_long=(side_type == 'LONG'),
                is_tp=False
            )
            final_sl_price = round_price_step(final_sl_price, tick_size, price_p)
        except Exception as e:
            await send_telegram_message(session, chat_id, f"❌ *Lỗi tính toán SL '{sl_price_str}':* `{e}`")
            return

    # Khởi tạo body cho lệnh Bybit V5
    qty_str = str(int(quantity)) if qty_p == 0 else f"{quantity:.{qty_p}f}"
    logger.info(f"Đặt lệnh Bybit {symbol}: volume={volume}, qty_p={qty_p}, quantity={quantity}, qty_str={qty_str}")
    order_link_id = f"pnlbot_limit_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Limit" if is_limit else "Market",
        "qty": qty_str,
        "positionIdx": position_idx,
        "orderLinkId": order_link_id
    }
    
    if is_limit:
        body["price"] = str(limit_price)
        body["timeInForce"] = "GTC"
        
    # Bybit cho phép đính kèm TP/SL trực tiếp khi tạo lệnh, an toàn tuyệt đối
    if final_tp_price is not None:
        body["takeProfit"] = str(final_tp_price)
        body["tpTriggerBy"] = "MarkPrice"
    if final_sl_price is not None:
        body["stopLoss"] = str(final_sl_price)
        body["slTriggerBy"] = "MarkPrice"
        
    status, data = await bybit_api_request(session, "POST", "/v5/order/create", body=body, is_private=True)
    if status == 200 and data.get("retCode") == 0:
        order_id = data.get("result", {}).get("orderId")
        pnl_emoji = "🟢" if side_type == 'LONG' else "🔴"
        
        # Lưu clientOrderId vào map để nhận diện khi khớp
        if order_id:
            order_client_ids[order_id] = order_link_id
            
        tp_sl_info = ""
        if final_tp_price:
            tp_sl_info += f"\n🎯 *TP:* Chốt lời ở giá *{final_tp_price:,.4f}*"
        if final_sl_price:
            tp_sl_info += f"\n🛡️ *SL:* Cắt lỗ ở giá *{final_sl_price:,.4f}*"
            
        if is_limit:
            msg = (
                f"⏳ *TẠO LỆNH LIMIT THÀNH CÔNG (BYBIT)!*\n"
                f"----------------------------------\n"
                f"🪙 Cặp: *{symbol}*\n"
                f"⚡ Lệnh: {pnl_emoji} *{side_type} (LIMIT)*\n"
                f"⚙️ Đòn bẩy: *{max_leverage}x*\n"
                f"💵 Giá đặt Limit: *{format_price(limit_price)} USDT*\n"
                f"📊 Volume lệnh: *{volume:,.2f} USDT*\n"
                f"🔢 Số lượng: *{quantity} {coin_name}*"
                f"{tp_sl_info}\n"
                f"🆔 Order ID: `{order_id}`"
            )
        else:
            msg = (
                f"✅ *VÀO LỆNH MARKET THÀNH CÔNG (BYBIT)!*\n"
                f"----------------------------------\n"
                f"🪙 Cặp: *{symbol}*\n"
                f"⚡ Lệnh: {pnl_emoji} *{side_type} (MARKET)*\n"
                f"⚙️ Đòn bẩy: *{max_leverage}x*\n"
                f"📊 Volume ước tính: *{volume:,.2f} USDT*\n"
                f"🔢 Số lượng: *{quantity} {coin_name}*"
                f"{tp_sl_info}\n"
                f"🆔 Order ID: `{order_id}`"
            )
        await send_telegram_message(session, chat_id, msg)
    else:
        msg_err = data.get('retMsg', 'Lỗi không xác định')
        code_err = data.get('retCode', -1)
        await send_telegram_message(session, chat_id, f"❌ *Đặt lệnh thất bại!*\nBybit báo lỗi: `{msg_err}` (Code: {code_err})")

# Cài đặt đòn bẩy qua Telegram
async def handle_leverage_command(session, chat_id, coin_name, leverage_str):
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    try:
        leverage = int(leverage_str)
        if leverage < 1 or leverage > 150:
            raise ValueError()
    except ValueError:
        await send_telegram_message(session, chat_id, "❌ Hệ số đòn bẩy không hợp lệ. Vui lòng nhập số nguyên từ 1 đến 150.")
        return
        
    ok = await set_leverage(session, symbol, leverage)
    if ok:
        await send_telegram_message(
            session, 
            chat_id, 
            f"✅ *CÀI ĐẶT ĐỒN BẨY THÀNH CÔNG!*\n"
            f"----------------------------------\n"
            f"🪙 Cặp: *{symbol}*\n"
            f"⚙️ Đòn bẩy mới: *{leverage}x*"
        )
    else:
        await send_telegram_message(session, chat_id, f"❌ *Cài đặt đòn bẩy thất bại cho {symbol}!* Vui lòng kiểm tra lại.")

# Cài đặt TP/SL cho vị thế đang mở
async def handle_tpsl_command(session, chat_id, coin_name, tp_price_str=None, sl_price_str=None):
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

    if tp_price_str:
        tp_price_str = tp_price_str.strip()
    if sl_price_str:
        sl_price_str = sl_price_str.strip()

    results = []
    
    for pos in target_positions:
        idx = pos.get('positionIdx', 0)
        amt = pos['positionAmt']
        entry_price = pos['entryPrice']
        leverage = pos.get('leverage', 1)
        quantity = abs(amt)
        is_long = amt > 0
        pos_display = 'LONG' if is_long else 'SHORT'
        
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
        
        # Gọi API set trading-stop của Bybit để gắn TP/SL cho vị thế đang mở
        body = {
            "category": "linear",
            "symbol": symbol,
            "positionIdx": idx
        }
        if final_tp_price is not None:
            body["takeProfit"] = str(final_tp_price)
            body["tpTriggerBy"] = "MarkPrice"
        if final_sl_price is not None:
            body["stopLoss"] = str(final_sl_price)
            body["slTriggerBy"] = "MarkPrice"
            
        status, data = await bybit_api_request(session, "POST", "/v5/position/trading-stop", body=body, is_private=True)
        if status == 200 and data.get("retCode") == 0:
            if final_tp_price:
                results.append(f"   • TP (*{pos_display}* tại giá *{format_price(final_tp_price)}*): 🟢 Thành công")
            if final_sl_price:
                results.append(f"   • SL (*{pos_display}* tại giá *{format_price(final_sl_price)}*): 🟢 Thành công")
        else:
            msg_err = data.get('retMsg', 'Lỗi không xác định')
            results.append(f"   • TP/SL (*{pos_display}*): 🔴 Thất bại: `{msg_err}`")
            
    msg = (
        f"🎯 *KẾT QUẢ CÀI ĐẶT TP/SL CHO {symbol} (BYBIT)*\n"
        f"----------------------------------\n" +
        "\n".join(results)
    )
    await send_telegram_message(session, chat_id, msg)

# Đặt lệnh Limit DCA vùng lỗ
async def handle_dca_command(session, chat_id, coin_name, volume_str, diff_str):
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    try:
        volume = float(volume_str)
        if volume <= 0:
            raise ValueError()
    except ValueError:
        await send_telegram_message(session, chat_id, "❌ Số tiền volume không hợp lệ. Vui lòng nhập số dương lớn hơn 0.")
        return

    target_positions = [pos for pos in positions.values() if pos['symbol'] == symbol and float(pos.get('positionAmt', 0)) != 0]
    
    if not target_positions:
        await send_telegram_message(
            session, 
            chat_id, 
            f"❌ Không tìm thấy vị thế *{symbol}* nào đang mở để thực hiện DCA."
        )
        return

    for pos in target_positions:
        idx = pos.get('positionIdx', 0)
        amt = float(pos['positionAmt'])
        entry_price = float(pos['entryPrice'])
        leverage = int(pos.get('leverage', 1))
        quantity_current = abs(amt)
        is_long = amt > 0
        pos_display = 'LONG' if is_long else 'SHORT'
        
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
            await send_telegram_message(session, chat_id, f"❌ *Lỗi tính toán giá DCA '{diff_str}':* `{e}`")
            continue
            
        raw_qty = volume / dca_price
        quantity_dca = round_down(raw_qty, qty_p)
        
        if quantity_dca <= 0:
            await send_telegram_message(
                session, 
                chat_id, 
                f"❌ Số lượng coin tính toán cho DCA quá nhỏ ({raw_qty:.8f} {coin_name}).\n"
                f"Vui lòng tăng Volume đặt DCA."
            )
            continue
            
        order_side = 'Buy' if is_long else 'Sell'
        client_order_id = f"pnlbot_dca_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        qty_str = str(int(quantity_dca)) if qty_p == 0 else f"{quantity_dca:.{qty_p}f}"
        
        body = {
            "category": "linear",
            "symbol": symbol,
            "side": order_side,
            "orderType": "Limit",
            "qty": qty_str,
            "price": str(dca_price),
            "timeInForce": "GTC",
            "positionIdx": idx,
            "orderLinkId": client_order_id
        }
        
        status, data = await bybit_api_request(session, "POST", "/v5/order/create", body=body, is_private=True)
        if status == 200 and data.get("retCode") == 0:
            order_id = data.get("result", {}).get("orderId")
            pnl_emoji = "🟢" if is_long else "🔴"
            
            if order_id:
                order_client_ids[order_id] = client_order_id
                
            await send_telegram_message(
                session, 
                chat_id, 
                f"✅ *ĐẶT LỆNH LIMIT DCA THÀNH CÔNG (BYBIT)!*\n"
                f"----------------------------------\n"
                f"🪙 Cặp: *{symbol}*\n"
                f"⚡ DCA vị thế: {pnl_emoji} *{pos_display}*\n"
                f"💵 Giá đặt DCA Limit: *{format_price(dca_price)} USDT*\n"
                f"📊 Volume DCA: *{volume:,.2f} USDT*\n"
                f"🔢 Số lượng DCA thêm: *{quantity_dca} {coin_name}*\n"
                f"🆔 Order ID: `{order_id}`"
            )
        else:
            msg_err = data.get('retMsg', 'Lỗi không xác định')
            code_err = data.get('retCode', -1)
            await send_telegram_message(session, chat_id, f"❌ *Đặt lệnh DCA Limit thất bại!*\nBybit báo lỗi: `{msg_err}` (Code: {code_err})")

# Đóng vị thế MARKET
async def handle_close_command(session, chat_id, coin_name, side_str=None):
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    matched_positions = []
    for key, pos in positions.items():
        if pos['symbol'] == symbol:
            matched_positions.append(pos)
            
    if not matched_positions:
        await send_telegram_message(
            session,
            chat_id,
            f"❌ Không tìm thấy vị thế nào đang mở cho coin *{symbol}*."
        )
        return
        
    target_pos = None
    if len(matched_positions) > 1:
        if not side_str:
            await send_telegram_message(
                session,
                chat_id,
                f"⚠️ Phát hiện cả vị thế LONG và SHORT cho *{symbol}* đang mở.\n"
                f"Vui lòng ghi rõ chiều muốn đóng.\n"
                f"Cú pháp: `/close <coin> <long|short>`\n"
                f"Ví dụ: `/close {coin_name.lower()} long`"
            )
            return
        side_upper = side_str.upper()
        for pos in matched_positions:
            amt = pos['positionAmt']
            actual_side = "LONG" if amt > 0 else "SHORT"
            if actual_side == side_upper:
                target_pos = pos
                break
        if not target_pos:
            await send_telegram_message(
                session,
                chat_id,
                f"❌ Không tìm thấy vị thế *{side_upper}* nào đang mở cho *{symbol}*."
            )
            return
    else:
        if side_str:
            side_upper = side_str.upper()
            pos = matched_positions[0]
            amt = pos['positionAmt']
            actual_side = "LONG" if amt > 0 else "SHORT"
            if actual_side != side_upper:
                await send_telegram_message(
                    session,
                    chat_id,
                    f"❌ Vị thế đang mở của *{symbol}* là *{actual_side}*, không phải *{side_upper}*."
                )
                return
        target_pos = matched_positions[0]
        
    idx = target_pos.get('positionIdx', 0)
    amt = target_pos['positionAmt']
    abs_amt = abs(amt)
    
    if abs_amt <= 0:
        await send_telegram_message(session, chat_id, f"❌ Kích thước vị thế của *{symbol}* bằng 0.")
        return
        
    qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)
    qty_str = str(int(abs_amt)) if qty_p == 0 else f"{abs_amt:.{qty_p}f}"
        
    # Bybit đóng vị thế bằng cách đặt lệnh ngược chiều (hoặc dùng reduceOnly)
    is_long = amt > 0
    side = 'Sell' if is_long else 'Buy'
    
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": qty_str,
        "positionIdx": idx,
        "reduceOnly": True
    }
    
    try:
        await send_telegram_message(session, chat_id, f"🔄 Đang gửi lệnh đóng vị thế MARKET cho *{symbol}*...")
        status, data = await bybit_api_request(session, "POST", "/v5/order/create", body=body, is_private=True)
        if status == 200 and data.get("retCode") == 0:
            order_id = data.get("result", {}).get("orderId")
            pnl_emoji = "🟢" if is_long else "🔴"
            display_side = "LONG" if is_long else "SHORT"
            await send_telegram_message(
                session,
                chat_id,
                f"✅ *ĐÓNG VỊ THẾ THÀNH CÔNG (BYBIT)!*\n"
                f"----------------------------------\n"
                f"🪙 Cặp: *{symbol}*\n"
                f"⚡ Đã đóng vị thế: {pnl_emoji} *{display_side} (MARKET)*\n"
                f"🔢 Số lượng đã đóng: *{abs_amt}*\n"
                f"🆔 Order ID: `{order_id}`"
            )
        else:
            msg_err = data.get('retMsg', 'Lỗi không xác định')
            code_err = data.get('retCode', -1)
            await send_telegram_message(session, chat_id, f"❌ *Đóng vị thế thất bại!*\nBybit báo lỗi: `{msg_err}` (Code: {code_err})")
    except Exception as e:
        logger.error(f"Lỗi khi đóng vị thế Bybit cho {symbol}: {e}")
        await send_telegram_message(session, chat_id, f"❌ Đã xảy ra lỗi hệ thống khi đóng vị thế: {e}")

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
        
    should_delete = False
    if not text.startswith('/'):
        should_delete = True
    else:
        command = text.split()[0].lower()
        command_base = command.split('@')[0]
        supported_commands = {
            '/start', '/help', '/pnl', '/pos', '/balance', '/wallet', '/sodu',
            '/top', '/gainers', '/orders', '/lenh', '/cancel', '/huy',
            '/close', '/c', '/tp', '/sl', '/tpsl', '/leverage', '/lev',
            '/long', '/l', '/short', '/s', '/chart', '/dca', '/auto',
            '/analyze', '/a', '/history', '/lichsu', '/his', '/liq'
        }
        if command_base in supported_commands:
            should_delete = True
            
    if should_delete:
        message_id = message.get('message_id')
        if message_id:
            asyncio.create_task(delete_telegram_message(request.app['session'], chat_id, message_id))
            
    # Tra cứu giá coin nhanh
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
            "Chào mừng bạn đến với Bybit Futures PnL Bot!\n\n"
            "Các câu lệnh hỗ trợ:\n"
            "📊 `/pnl` - Xem tổng PnL hiện tại.\n"
            "🔍 `/pos` - Xem chi tiết các vị thế đang mở.\n"
            "💀 `/liq` - Xem các vị thế đang mở kèm giá thanh lý.\n"
            "💳 `/balance` (hoặc `/wallet`) - Xem số dư ví Bybit.\n"
            "🔥 `/top` (hoặc `/gainers`) - Top 5 tăng/giảm mạnh nhất 24h.\n"
            "⚙️ `/leverage <coin> <hệ_số>` (hoặc `/lev`) - Cài đặt đòn bẩy.\n"
            "⏳ `/orders` - Xem danh sách lệnh đang chờ khớp.\n"
            "❌ `/cancel <coin> <order_id>` - Hủy một lệnh đang chờ.\n"
            "🎯 `/tp <coin> <giá_tp>` - Cài đặt TP (Take Profit).\n"
            "🛡️ `/sl <coin> <giá_sl>` - Cài đặt SL (Stop Loss).\n"
            "🔮 `/tpsl <coin> <giá_tp> <giá_sl>` - Cài đặt đồng thời cả TP và SL.\n"
            "📈 `/long <coin> <volume> [giá] [tp=..] [sl=..]` (hoặc `/l`) - Mở LONG.\n"
            "📉 `/short <coin> <volume> [giá] [tp=..] [sl=..]` (hoặc `/s`) - Mở SHORT.\n"
            "📊 `/chart [khung_thời_gian] <coin>` - Xem biểu đồ nến.\n"
            "⚖️ `/dca <coin> <volume> <khoảng_cách>` - Đặt lệnh Limit DCA vùng lỗ.\n"
            "⏱ `/auto` - Bật/Tắt tự động cập nhật vị thế mỗi 1 phút.\n"
            "📈 `/analyze [coin]` (hoặc `/a`) - Phân tích kỹ thuật chi tiết.\n"
            "📜 `/history [coin]` (hoặc `/lichsu`) - Xem lịch sử 10 vị thế đã chốt gần nhất.\n\n"
            "💡 *Mẹo*:\n"
            "• Nhập trực tiếp tên coin (ví dụ: `btc` hoặc `btc eth`) để tra cứu giá nhanh.\n"
            "• Lệnh Market: `/long btc 100` (LONG btc với volume 100 USDT)\n"
            "• Lệnh Limit: `/long btc 100 95000` (LONG btc với volume 100 USDT tại giá 95000)\n"
            "• Lệnh Limit kèm TP/SL: `/long btc 100 95000 tp=105000 sl=90000`"
        )
        await send_telegram_message(request.app['session'], chat_id, welcome_text)
        
    elif command_base == '/pnl':
        await handle_pnl_command(request.app['session'], chat_id)
        
    elif command_base == '/pos':
        await handle_pos_command(request.app['session'], chat_id)
        
    elif command_base in ('/balance', '/wallet', '/sodu'):
        await handle_balance_command(request.app['session'], chat_id)
        
    elif command_base in ('/top', '/gainers'):
        await handle_top_command(request.app['session'], chat_id)
        
    elif command_base in ('/orders', '/lenh'):
        await handle_orders_command(request.app['session'], chat_id)
        
    elif command_base in ('/cancel', '/huy'):
        parts = text.split()
        if len(parts) < 3:
            await send_telegram_message(
                request.app['session'], 
                chat_id, 
                "❌ Sai cú pháp!\nSử dụng: `/cancel <coin> <order_id>`\nVí dụ: `/cancel btc 1234567`"
            )
        else:
            coin_name = parts[1]
            order_id_str = parts[2]
            await handle_cancel_command(request.app['session'], chat_id, coin_name, order_id_str)
            
    elif command_base in ('/close', '/c'):
        parts = text.split()
        if len(parts) < 2:
            await send_telegram_message(
                request.app['session'], 
                chat_id, 
                "❌ Sai cú pháp!\nSử dụng: `/close <coin> [long|short]`\nVí dụ: `/close btc`"
            )
        else:
            coin_name = parts[1]
            side_str = parts[2] if len(parts) > 2 else None
            await handle_close_command(request.app['session'], chat_id, coin_name, side_str)
        
    elif command_base == '/tp':
        parts = text.split()
        if len(parts) < 3:
            await send_telegram_message(
                request.app['session'],
                chat_id,
                "❌ Sai cú pháp!\nSử dụng: `/tp <coin> <giá_tp>`"
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
                "❌ Sai cú pháp!\nSử dụng: `/sl <coin> <giá_sl>`"
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
                "❌ Sai cú pháp!\nSử dụng: `/tpsl <coin> <giá_tp> <giá_sl>`"
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
                "❌ Sai cú pháp!\nSử dụng: `/leverage <coin> <hệ_số>`"
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
                "❌ Sai cú pháp!\n"
                "• Lệnh Market: `/long <coin> <volume>`\n"
                "• Lệnh Limit: `/long <coin> <volume> <giá>`\n"
                "• Đi kèm TP/SL: `/long btc 100 tp=98000 sl=92000`"
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
                "❌ Sai cú pháp!\nSử dụng: `/chart [khung_thời_gian] <coin>`"
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
                await send_telegram_message(request.app['session'], chat_id, "❌ Vui lòng nhập tên coin.")
            else:
                symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
                loading_msg_id = await send_telegram_message(
                    request.app['session'],
                    chat_id,
                    f"⏳ Đang tải và vẽ biểu đồ Bybit cho *{symbol}* ({interval.upper()})..."
                )
                try:
                    photo_buf = await draw_candlestick_chart(request.app['session'], symbol, interval)
                    caption = f"📊 Biểu đồ nến *{symbol}* ({interval.upper()})\n⚡ Sàn: Bybit Futures"
                    await send_telegram_photo(request.app['session'], chat_id, photo_buf, caption=caption)
                    if loading_msg_id:
                        await delete_telegram_message(request.app['session'], chat_id, loading_msg_id)
                except Exception as e:
                    logger.error(f"Lỗi vẽ biểu đồ cho {symbol}: {e}")
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
                "❌ Sai cú pháp!\nSử dụng: `/dca <coin> <volume> <khoảng_cách>`\nVí dụ: `/dca btc 200 40u`"
            )
        else:
            coin_name = parts[1]
            volume_str = parts[2]
            diff_str = parts[3]
            await handle_dca_command(request.app['session'], chat_id, coin_name, volume_str, diff_str)
            
    elif command_base == '/auto':
        await handle_auto_command(request.app['session'], chat_id)
        
    elif command_base in ('/analyze', '/a'):
        parts = text.split()
        coin_name = parts[1] if len(parts) > 1 else None
        await handle_analyze_command(request.app['session'], chat_id, coin_name)
        
    elif command_base in ('/history', '/lichsu', '/his'):
        parts = text.split()
        coin_name = parts[1] if len(parts) > 1 else None
        await handle_history_command(request.app['session'], chat_id, coin_name)
        
    elif command_base == '/liq':
        await handle_liq_command(request.app['session'], chat_id)
        
    return web.Response(status=200)

async def log_server_ip(session):
    try:
        async with session.get("https://api.ipify.org?format=json") as resp:
            if resp.status == 200:
                data = await resp.json()
                ip = data.get('ip')
                logger.info(f"👉👉 ĐỊA CHỈ IP PUBLIC CỦA SERVER LÀ: {ip} 👈👈")
                logger.info("Hãy copy IP này cấu hình trong API Key Bybit nếu có cấu hình IP restriction.")
            else:
                body = await resp.text()
                logger.warning(f"Không thể lấy IP public: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi khi lấy IP public: {e}")

# Lifecycle hooks
async def on_startup(app):
    load_active_chats()
    app['session'] = aiohttp.ClientSession()
    
    await log_server_ip(app['session'])
    
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    
    await setup_telegram_webhook(app['session'])
    
    try:
        await init_exchange_info(app['session'])
        await check_position_mode(app['session'], api_key, api_secret)
        await init_positions(app['session'], api_key, api_secret)
    except Exception as e:
        logger.error(f"Lỗi khởi tạo Bybit ban đầu: {e}. Sẽ nạp lại qua WebSocket.")
        
    app['user_data_task'] = asyncio.create_task(
        bybit_user_data_stream(app['session'], api_key, api_secret)
    )
    app['mark_price_task'] = asyncio.create_task(
        bybit_mark_price_stream(app['session'])
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

def main():
    load_dotenv()
    
    required_env = ["TELEGRAM_BOT_TOKEN", "BYBIT_API_KEY", "BYBIT_API_SECRET"]
    missing = [env for env in required_env if not os.getenv(env)]
    if missing:
        logger.error(f"Thiếu cấu hình bắt buộc trong file .env: {', '.join(missing)}")
        return
        
    app = web.Application()
    app.router.add_get('/test', test_handler)
    app.router.add_post('/webhook', telegram_webhook_handler)
    
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    
    port = int(os.getenv("PORT", 5000))
    logger.info(f"Khởi chạy web server Bybit tại port {port}...")
    web.run_app(app, host='0.0.0.0', port=port)

if __name__ == '__main__':
    main()
