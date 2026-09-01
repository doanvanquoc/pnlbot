import asyncio
import math
import hmac
import hashlib
import time
import os
import random
import re
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
PRICE_ALERT_THRESHOLDS = list(range(5, 505, 5)) # Cảnh báo mỗi 5% biến động vị thế (từ 5% đến 500%)
hedge_mode = False      # Chế độ Position Mode (True: Hedge Mode, False: One-way Mode)
symbol_precisions = {}  # Lưu độ chính xác số lượng coin (quantityPrecision) của từng symbol
symbol_price_precisions = {}  # Lưu độ chính xác giá (pricePrecision) của từng symbol
symbol_tick_sizes = {}  # Lưu tickSize của từng symbol
order_realized_pnl = {} # Lưu realized PnL cộng dồn cho từng order_id (tránh lỗi fragmented trades PnL)
tracking_coins = {}     # Lưu danh sách coin đang tracking giá: {symbol: {'ref_price': float, 'chat_ids': set()}}

# Cache cho kết quả quét thị trường của lệnh /analyze
market_scan_cache = {
    "signals": None,      # Lưu: (long_signals, short_signals)
    "timestamp": 0.0,     # Unix timestamp lúc quét xong
    "lock": asyncio.Lock()
}

# Cache snapshot giá toàn sàn (dùng chung cho tra giá coin, /top, /orders)
TICKER_CACHE_TTL = 30 # giây
market_snapshot_cache = {
    "tickers": None,      # {symbol: {'price': float, 'change': float}}
    "funding": None,      # {symbol: float}
    "timestamp": 0.0,
    "lock": asyncio.Lock()
}

# Cảnh báo lỗi GTE/closePosition dùng chung cho các lệnh cài TP/SL
GTE_WARNING = (
    "\n\n⚠️ *Lưu ý lỗi GTE/closePosition từ Binance:*\n"
    "Binance quy định chỉ được phép tồn tại *1 lệnh đóng vị thế (closePosition)* có cùng điều kiện kích hoạt GTE (hoặc LTE).\n"
    "Khi bạn đặt TP/SL mà cả TP và SL đều nằm cùng một phía so với giá hiện tại (cả hai đều cao hơn hoặc đều thấp hơn giá thị trường), chúng sẽ trùng điều kiện kích hoạt (GTE/LTE) dẫn đến lệnh thứ hai bị từ chối.\n"
    "👉 *Giải pháp:* Cài đặt TP/SL khi giá hiện tại nằm giữa khoảng TP và SL, hoặc hủy bớt lệnh cũ trên app Binance rồi thử lại."
)

# Map sao độ tin cậy tín hiệu (dùng chung cho /analyze và quét thị trường)
CONF_MAP = {'Rất mạnh': '⭐⭐⭐⭐⭐', 'Mạnh': '⭐⭐⭐⭐', 'Trung bình': '⭐⭐⭐', 'Yếu': '⭐⭐', 'Thấp': '⭐'}

ACTIVE_CHATS_FILE = "active_chats.json"
AUTO_CHATS_FILE = "auto_chats.json"
TRACKING_COINS_FILE = "tracking_coins.json"
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

def load_auto_chats():
    global auto_chats, last_auto_messages
    try:
        if os.path.exists(AUTO_CHATS_FILE):
            with open(AUTO_CHATS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                auto_chats = set(int(cid) for cid in data.get('chats', []))
                last_auto_messages = {int(cid): mid for cid, mid in data.get('last_messages', {}).items()}
                logger.info(f"Đã tải {len(auto_chats)} chat tự động cập nhật từ file.")
    except Exception as e:
        logger.error(f"Lỗi khi tải auto_chats: {e}")

def save_auto_chats():
    try:
        with open(AUTO_CHATS_FILE, "w", encoding="utf-8") as f:
            json.dump({"chats": list(auto_chats), "last_messages": last_auto_messages}, f)
    except Exception as e:
        logger.error(f"Lỗi khi lưu auto_chats: {e}")

def load_tracking_coins():
    global tracking_coins
    try:
        if os.path.exists(TRACKING_COINS_FILE):
            with open(TRACKING_COINS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                for symbol, info in data.items():
                    chat_ids = set(int(cid) for cid in info.get('chat_ids', []))
                    if chat_ids:
                        tracking_coins[symbol] = {
                            'ref_price': float(info.get('ref_price', 0)),
                            'chat_ids': chat_ids
                        }
                logger.info(f"Đã tải {len(tracking_coins)} coin đang tracking từ file.")
    except Exception as e:
        logger.error(f"Lỗi khi tải tracking_coins: {e}")

def save_tracking_coins():
    try:
        data = {
            symbol: {'ref_price': info['ref_price'], 'chat_ids': list(info['chat_ids'])}
            for symbol, info in tracking_coins.items()
        }
        with open(TRACKING_COINS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Lỗi khi lưu tracking_coins: {e}")


SIGNAL_HISTORY_FILE = "signal_history.json"
SIGNAL_MAX_AGE_DAYS = 30
SIGNAL_TIMEOUT_HOURS = 72
signal_history = []

def load_signal_history():
    global signal_history
    try:
        if os.path.exists(SIGNAL_HISTORY_FILE):
            with open(SIGNAL_HISTORY_FILE, "r", encoding="utf-8") as f:
                signal_history = json.load(f)
            logger.info(f"Đã tải {len(signal_history)} tín hiệu từ lịch sử.")
    except Exception as e:
        logger.error(f"Lỗi khi tải signal_history: {e}")

def save_signal_history():
    try:
        with open(SIGNAL_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(signal_history, f)
    except Exception as e:
        logger.error(f"Lỗi khi lưu signal_history: {e}")

def prune_signal_history(max_keep=500):
    global signal_history
    cutoff = time.time() - SIGNAL_MAX_AGE_DAYS * 86400
    signal_history = [s for s in signal_history if s.get('ts', 0) >= cutoff]
    if len(signal_history) > max_keep:
        signal_history = signal_history[-max_keep:]

def record_signal(res, ai_verdict=None):
    """Lưu tín hiệu mạnh (Mạnh/Rất mạnh, có TP/SL) vào lịch sử để theo dõi win-rate thực tế."""
    if not isinstance(res, dict):
        return
    if res.get('signal') not in ('LONG', 'SHORT'):
        return
    if res.get('confidence') not in ('Mạnh', 'Rất mạnh'):
        return
    if not res.get('tp') or not res.get('sl'):
        return
    # Tránh ghi trùng: cùng symbol + side còn mở trong 4 giờ gần nhất
    now = time.time()
    for s in signal_history:
        if (s.get('status') == 'open' and s.get('symbol') == res['symbol']
                and s.get('side') == res['signal'] and now - s.get('ts', 0) < 4 * 3600):
            return
    signal_history.append({
        'id': f"{res['symbol']}_{res['signal']}_{int(now)}",
        'ts': now,
        'symbol': res['symbol'],
        'side': res['signal'],
        'entry': res['close'],
        'tp': res['tp'],
        'sl': res['sl'],
        'score': res['long_score'] if res['signal'] == 'LONG' else res['short_score'],
        'confidence': res['confidence'],
        'ai': (ai_verdict or {}).get('direction'),
        'status': 'open'
    })
    prune_signal_history()
    save_signal_history()

def get_signal_stats(days=SIGNAL_MAX_AGE_DAYS):
    """Thống kê win/loss theo band độ tin cậy (4⭐/5⭐) trong `days` ngày gần nhất."""
    cutoff = time.time() - days * 86400
    stats = {}
    for s in signal_history:
        if s.get('status') not in ('win', 'loss') or s.get('ts', 0) < cutoff:
            continue
        band = '5⭐' if s.get('confidence') == 'Rất mạnh' else '4⭐'
        st = stats.setdefault(band, {'win': 0, 'loss': 0})
        st['win' if s['status'] == 'win' else 'loss'] += 1
    return stats

def format_signal_stats(days=SIGNAL_MAX_AGE_DAYS):
    """Dòng thống kê win-rate thực tế để hiển thị trong output /a. Rỗng nếu chưa có dữ liệu."""
    stats = get_signal_stats(days)
    if not stats:
        return ""
    parts = []
    for band in ('5⭐', '4⭐'):
        if band in stats:
            st = stats[band]
            total = st['win'] + st['loss']
            wr = st['win'] / total * 100
            parts.append(f"{band} {st['win']}/{total} ({wr:.0f}%)")
    if not parts:
        return ""
    return f"📈 *Win-rate thực tế {days} ngày:* " + " | ".join(parts)

def band_winrate_ok(confidence, min_samples=10, min_wr=0.5):
    """Adaptive gate: chặn nhóm tín hiệu có win-rate thực tế dưới 50% (tối thiểu min_samples mẫu)."""
    cutoff = time.time() - SIGNAL_MAX_AGE_DAYS * 86400
    wins = losses = 0
    for s in signal_history:
        if (s.get('confidence') != confidence or s.get('status') not in ('win', 'loss')
                or s.get('ts', 0) < cutoff):
            continue
        if s['status'] == 'win':
            wins += 1
        else:
            losses += 1
    total = wins + losses
    if total < min_samples:
        return True
    return (wins / total) >= min_wr

async def signal_tracking_loop(app):
    """Task nền: theo dõi kết quả các tín hiệu đang mở (TP chạm trước hay SL trước)."""
    await asyncio.sleep(10)
    while True:
        try:
            open_signals = [s for s in signal_history if s.get('status') == 'open']
            if open_signals:
                session = app['session']
                tickers_map, _ = await get_market_snapshot(session)
                changed = False
                now = time.time()
                for sig in open_signals:
                    info = tickers_map.get(sig['symbol'])
                    price = info['price'] if info else 0
                    if price > 0:
                        if sig['side'] == 'LONG':
                            if price >= sig['tp']:
                                sig['status'] = 'win'
                            elif price <= sig['sl']:
                                sig['status'] = 'loss'
                        else:
                            if price <= sig['tp']:
                                sig['status'] = 'win'
                            elif price >= sig['sl']:
                                sig['status'] = 'loss'
                    if sig['status'] == 'open' and now - sig.get('ts', 0) > SIGNAL_TIMEOUT_HOURS * 3600:
                        sig['status'] = 'expired'
                    if sig['status'] != 'open':
                        sig['closed_ts'] = now
                        changed = True
                if changed:
                    prune_signal_history()
                    save_signal_history()
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Lỗi trong signal_tracking_loop: {e}")
            await asyncio.sleep(30)


# ─── AI phân tích realtime (endpoint OpenAI-compatible) ───
AI_CACHE_TTL = 600
ai_verdict_cache = {}

def _extract_json(text):
    """Trích JSON object đầu tiên từ nội dung trả lời của LLM (bỏ qua markdown fence...)."""
    if not text:
        return None
    start = text.find('{')
    end = text.rfind('}')
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        return json.loads(text[start:end + 1])
    except Exception:
        return None

async def get_ai_analysis(session, digest):
    """Gọi LLM phân tích digest chỉ báo. Trả về {direction, confidence, reason} hoặc None."""
    api_key = os.getenv("DASH_TOKEN")
    if not api_key or not digest:
        return None
    model = os.getenv("DASH_MODEL", "glm-5.3-flash")
    url = "https://opencode.ai/zen/go/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    system_prompt = (
        "Bạn là một phân tích viên giao dịch crypto futures chuyên nghiệp, kỷ luật và thận trọng. "
        "Dựa trên các số liệu chỉ báo kỹ thuật đa khung thời gian (nến đã đóng) được cung cấp, hãy đánh giá hướng đi ngắn hạn. "
        "Ưu tiên bảo toàn vốn: khi tín hiệu mâu thuẫn, xu hướng chưa rõ hoặc biến động quá mạnh, hãy chọn NEUTRAL. "
        "Chỉ trả lời bằng MỘT JSON hợp lệ duy nhất, không thêm bất kỳ chữ nào, đúng định dạng: "
        '{"direction": "LONG" | "SHORT" | "NEUTRAL", "confidence": "cao" | "trung bình" | "thấp", "reason": "lý do ngắn gọn bằng tiếng Việt, dưới 200 ký tự"}'
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": digest}
        ],
        "temperature": 0.2,
        # glm-5.3-flash là thinking model: reasoning_content tiêu tốn token nên
        # cần budget đủ lớn để phần JSON cuối cùng không bị cắt (finish_reason=length)
        "max_tokens": 2000
    }
    try:
        timeout = aiohttp.ClientTimeout(total=60)
        async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.warning(f"AI API trả lỗi HTTP {resp.status}: {body[:200]}")
                return None
            data = await resp.json()
            content = data.get('choices', [{}])[0].get('message', {}).get('content', '')
            verdict = _extract_json(content)
            if verdict and verdict.get('direction') in ('LONG', 'SHORT', 'NEUTRAL'):
                verdict['confidence'] = verdict.get('confidence', 'trung bình')
                verdict['reason'] = str(verdict.get('reason', ''))[:250]
                return verdict
            # glm-5.3-flash là thinking model: nếu max_tokens quá nhỏ, phần suy luận
            # (reasoning_content) ăn hết budget và content trả về rỗng
            logger.warning(f"AI trả lời không parse được JSON (content rỗng hoặc sai định dạng): {str(content)[:150]}")
            return None
    except Exception as e:
        logger.warning(f"Lỗi gọi AI analysis: {e}")
        return None

def build_ai_digest(symbol, timeframe_results, oi_change=None, taker_ratio=None, funding_rate=None):
    """Dựng text digest chỉ báo đa khung (số liệu, không phải giá thô) để gửi cho AI."""
    lines = [f"Phân tích kỹ thuật {symbol} — Binance Futures, nến đã đóng, điểm Rule engine (thang 10):"]
    for tf_name, res in timeframe_results:
        if not res:
            continue
        lines.append(
            f"- Khung {tf_name}: giá {format_price(res['close'])}, RSI {res['rsi']:.1f}, StochK {res['stoch_k']:.1f}, "
            f"MACD hist {res['hist']:+.5g}, ADX {res['adx']:.1f}, ATR {((res['atr'] / res['close']) * 100):.2f}%, "
            f"EMA9 {format_price(res['ema9'])} / EMA21 {format_price(res['ema21'])} / EMA50 {format_price(res['ema50'])} / EMA200 {format_price(res['ema200'])}, "
            f"BB {res['bb_pct'] * 100:.0f}%, Vol x{res['vol_ratio']:.2f}, "
            f"S/R: {format_price(res['support'])}-{format_price(res['resistance'])}, "
            f"Rule: {res['signal']} (L:{res['long_score']:.1f}/S:{res['short_score']:.1f})"
        )
        if res.get('rsi_div'):
            lines.append(f"  · Divergence RSI khung {tf_name}: {res['rsi_div']}")
        if res.get('pattern'):
            lines.append(f"  · Pattern nến khung {tf_name}: {res['pattern']}")
    if oi_change is not None:
        lines.append(f"- Open Interest 24h: {oi_change:+.1f}%")
    if taker_ratio is not None:
        lines.append(f"- Taker buy/sell ratio: {taker_ratio:.2f}")
    if funding_rate is not None:
        lines.append(f"- Funding rate: {funding_rate * 100:+.4f}%")
    lines.append("Hãy kết luận hướng đi ngắn hạn theo đúng JSON yêu cầu.")
    return "\n".join(lines)

async def get_ai_verdict_cached(session, cache_key, digest):
    """Gọi AI có cache TTL 10 phút để tiết kiệm usage."""
    now = time.time()
    cached = ai_verdict_cache.get(cache_key)
    if cached and now - cached['ts'] < AI_CACHE_TTL:
        return cached['verdict']
    verdict = await get_ai_analysis(session, digest)
    if verdict:
        ai_verdict_cache[cache_key] = {'verdict': verdict, 'ts': now}
    return verdict


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
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('result', {}).get('message_id')
                # Telegram trả 429 (rate limit): chờ retry_after rồi thử lại
                if resp.status == 429 and attempt < max_attempts - 1:
                    try:
                        err = await resp.json()
                        retry_after = int(err.get('parameters', {}).get('retry_after', 1))
                    except Exception:
                        retry_after = 1
                    logger.warning(f"Telegram 429 rate limit: thử lại sau {retry_after}s (lần {attempt + 1}/{max_attempts})")
                    await asyncio.sleep(min(retry_after, 5))
                    continue
                body = await resp.text()
                logger.error(f"Lỗi gửi tin nhắn Telegram: HTTP {resp.status} - {body}")
                return None
        except Exception as e:
            logger.error(f"Lỗi khi gửi tin nhắn Telegram: {e}")
            return None
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

# Hàm hủy DCA dùng chung
async def cancel_dca_orders(session, api_key, api_secret, symbol):
    """
    Hủy tất cả các lệnh DCA đang mở của một symbol.
    """
    headers = {"X-MBX-APIKEY": api_key}
    try:
        timestamp = int(time.time() * 1000)
        params = [
            f"symbol={symbol}",
            f"timestamp={timestamp}"
        ]
        query = "&".join(params)
        sig = get_binance_signature(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v1/openOrders?{query}&signature={sig}"
        
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                orders = await resp.json()
                if isinstance(orders, list):
                    cancelled_count = 0
                    for order in orders:
                        client_order_id = order.get('clientOrderId', '')
                        if "dca" in client_order_id.lower():
                            order_id = order.get('orderId')
                            if order_id:
                                del_timestamp = int(time.time() * 1000)
                                del_query = f"symbol={symbol}&orderId={order_id}&timestamp={del_timestamp}"
                                del_sig = get_binance_signature(del_query, api_secret)
                                del_url = f"https://fapi.binance.com/fapi/v1/order?{del_query}&signature={del_sig}"
                                
                                async with session.delete(del_url, headers=headers) as del_resp:
                                    del_data = await del_resp.json()
                                    if del_resp.status == 200:
                                        cancelled_count += 1
                                        logger.info(f"Đã tự động hủy lệnh DCA: orderId={order_id} của {symbol}")
                                    else:
                                        logger.warning(f"Không thể hủy lệnh DCA {order_id}: {del_data.get('msg')}")
                    if cancelled_count > 0:
                        logger.info(f"Đã hủy {cancelled_count} lệnh DCA của {symbol}")
                        return True
            else:
                body = await resp.text()
                logger.error(f"Lỗi lấy openOrders để hủy DCA: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi trong cancel_dca_orders cho {symbol}: {e}")
    return False

# Cập nhật cache vị thế cục bộ
async def update_position_cache(symbol, position_side, amount, entry_price, leverage, session=None):
    key = f"{symbol}_{position_side}"
    amount = float(amount)
    entry_price = float(entry_price)
    leverage = int(leverage)
    
    if amount == 0.0:
        # Vị thế bị đóng hoàn toàn
        if key in positions:
            # Tự động hủy lệnh DCA khi vị thế đóng (do TP/SL hoặc thanh lý)
            if session:
                try:
                    api_key = os.getenv("BINANCE_API_KEY")
                    api_secret = os.getenv("BINANCE_API_SECRET")
                    await cancel_dca_orders(session, api_key, api_secret, symbol)
                except Exception as e:
                    logger.error(f"Lỗi khi tự động hủy DCA cho {symbol}: {e}")
            else:
                logger.warning(f"Không có session để hủy DCA cho {symbol}")
            
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
            'leverage': leverage,
            'fundingRate': positions.get(key, {}).get('fundingRate', 0.0)
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
                        'leverage': leverage,
                        'fundingRate': 0.0
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
        keepalive_task = None
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
                                    leverage=old_leverage,
                                    session=session
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
                            order_type = order_data.get('o', '') # LIMIT, MARKET, etc.
                            order_id = order_data.get('i')
                            realized_pnl = float(order_data.get('rp', 0))

                            # Cộng dồn realized_pnl cho mỗi trade fill của cùng 1 order_id
                            if realized_pnl != 0.0:
                                order_realized_pnl[order_id] = order_realized_pnl.get(order_id, 0.0) + realized_pnl
                            
                            message = None
                            
                            # 1. Sự kiện thanh lý vị thế
                            if exec_type == 'CALCULATED' and status == 'FILLED':
                                symbol = order_data.get('s')
                                side = order_data.get('S')        # BUY, SELL
                                pos_side = order_data.get('ps')   # LONG, SHORT, BOTH
                                price = float(order_data.get('ap', 0)) or float(order_data.get('L', 0)) or float(order_data.get('p', 0))
                                qty = float(order_data.get('z', 0))
                                notional = qty * price
                                
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
                                
                            # 2. Xử lý các lệnh giao dịch khớp hoàn toàn (FILLED), mới tạo (NEW - chỉ cho LIMIT), hoặc bị hủy (CANCELED)
                            elif status in ('FILLED', 'NEW', 'CANCELED'):
                                # Tránh gửi thông báo NEW cho các lệnh không phải LIMIT (như MARKET, TP, SL lúc mới tạo)
                                if status == 'NEW' and order_type != 'LIMIT':
                                    pass
                                else:
                                    symbol = order_data.get('s')
                                    side = order_data.get('S')        # BUY, SELL
                                    pos_side = order_data.get('ps')   # LONG, SHORT, BOTH
                                    price = float(order_data.get('ap', 0)) or float(order_data.get('L', 0)) or float(order_data.get('p', 0))
                                    if price == 0:
                                        price = float(order_data.get('p', 0)) # fallback sang giá đặt ban đầu
                                    
                                    qty = float(order_data.get('z', 0)) or float(order_data.get('q', 0))
                                    notional = qty * price
                                    
                                    # Lấy tổng realized pnl đã cộng dồn (và xóa khỏi cache nếu lệnh kết thúc)
                                    if status in ('FILLED', 'CANCELED'):
                                        total_realized_pnl = order_realized_pnl.pop(order_id, realized_pnl)
                                    else:
                                        total_realized_pnl = realized_pnl
                                    
                                    # Xác định loại lệnh hiển thị
                                    if orig_type in ('TAKE_PROFIT', 'TAKE_PROFIT_MARKET'):
                                        order_type_display = "🎯 CHỐT LỜI (Take Profit)"
                                    elif orig_type in ('STOP', 'STOP_MARKET'):
                                        order_type_display = "🛡️ CẮT LỖ (Stop Loss)"
                                    elif "dca" in client_order_id.lower():
                                        order_type_display = "⚖️ DCA Limit"
                                    elif client_order_id.startswith('pnlbot_limit'):
                                        order_type_display = "⏳ Limit"
                                    else:
                                        order_type_display = f"{order_type}"
                                    
                                    side_display = f"{side} ({pos_side})" if pos_side != 'BOTH' else side
                                    
                                    if status == 'FILLED':
                                        title = "🔔 *THÔNG BÁO KHỚP LỆNH*"
                                        status_emoji = "🟢 `FILLED`"
                                        price_label = "Giá khớp"
                                    elif status == 'NEW':
                                        title = "⏳ *THÔNG BÁO TẠO LỆNH*"
                                        status_emoji = "🟡 `NEW` (Chờ khớp)"
                                        price_label = "Giá đặt"
                                    else: # CANCELED
                                        title = "❌ *THÔNG BÁO HỦY LỆNH*"
                                        status_emoji = "🔴 `CANCELED`"
                                        price_label = "Giá đặt"
                                        
                                    msg_lines = [
                                        f"┌──────────────────────────────┐",
                                        f"   {title}",
                                        f"└──────────────────────────────┘",
                                        f"🪙 Cặp: `{symbol}`",
                                        f"⚡ Loại: `{order_type_display} ({side_display})`",
                                        f"📊 Trạng thái: {status_emoji}",
                                        f"💵 {price_label}: `{format_price(price)} USDT`",
                                        f"🔢 Số lượng: `{qty}` (~`{notional:,.2f} USDT`)"
                                    ]
                                    
                                    # Thêm PNL đóng nếu có realized_pnl hoặc là lệnh TP/SL/đóng
                                    is_close_or_reduce = (total_realized_pnl != 0.0) or (orig_type in ('TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'STOP', 'STOP_MARKET'))
                                    if status == 'FILLED' and is_close_or_reduce:
                                        pnl_sign_emoji = pnl_emoji(total_realized_pnl)
                                        msg_lines.append(f"💰 PnL đóng: {pnl_sign_emoji} `*{fmt_signed(total_realized_pnl)} USDT*`")
                                        
                                    msg_lines.append(f"🆔 Order ID: `{order_id}`")
                                    message = "\n".join(msg_lines)

                            # Dọn dẹp cache nếu lệnh kết thúc bằng cách khác (CANCELED/EXPIRED)
                            if status in ('CANCELED', 'EXPIRED'):
                                order_realized_pnl.pop(order_id, None)
                                
                            # Gửi thông báo song song cho tất cả active_chats
                            if message and active_chats:
                                send_results = await asyncio.gather(
                                    *[send_telegram_message(session, cid, message) for cid in list(active_chats)],
                                    return_exceptions=True
                                )
                                for cid, send_err in zip(list(active_chats), send_results):
                                    if isinstance(send_err, Exception):
                                        logger.error(f"Không thể gửi thông báo sự kiện đến {cid}: {send_err}")
                                        
                        elif event_type == 'listenKeyExpired':
                            logger.warning("listenKey đã bị hết hạn trên Binance Server.")
                            break
                            
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        logger.warning("User Data Stream bị đóng hoặc lỗi.")
                        break
        except Exception as e:
            logger.error(f"Lỗi trong User Data Stream WebSocket: {e}")
        finally:
            # Đảm bảo task keepalive luôn được dừng kể cả khi WS lỗi
            if keepalive_task:
                keepalive_task.cancel()
            
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
                                    pos['fundingRate'] = float(data.get('r', 0))
                                    
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
                                                        
                                                        sym_display = display_symbol(symbol)
                                                        display_side = 'LONG' if side_sign > 0 else 'SHORT'
                                                        
                                                        if direction > 0:
                                                            alert_msg = (
                                                                f"📈📈 *【CẢNH BÁO BIẾN ĐỘNG GIÁ】* 📈📈\n"
                                                                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                                                f"🪙 Cặp: `{sym_display}` ({display_side})\n"
                                                                f"🟢 Vị thế đã dương *+{pct_change:.1f}%* so với Entry\n"
                                                                f"💵 Entry: `{format_price(entry)} USDT`\n"
                                                                f"💵 Hiện tại: `{format_price(mark_price)} USDT`\n"
                                                                f"💰 PnL: `{'+' if pos['unrealizedPnL'] >= 0 else ''}{pos['unrealizedPnL']:,.2f} USDT`"
                                                            )
                                                        else:
                                                            alert_msg = (
                                                                f"📉📉 *【CẢNH BÁO BIẾN ĐỘNG GIÁ】* 📉📉\n"
                                                                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                                                f"🪙 Cặp: `{sym_display}` ({display_side})\n"
                                                                f"🔴 Vị thế đã âm *{abs(pct_change):.1f}%* so với Entry\n"
                                                                f"💵 Entry: `{format_price(entry)} USDT`\n"
                                                                f"💵 Hiện tại: `{format_price(mark_price)} USDT`\n"
                                                                f"💰 PnL: `{'+' if pos['unrealizedPnL'] >= 0 else ''}{pos['unrealizedPnL']:,.2f} USDT`"
                                                            )
                                                        
                                                        send_results = await asyncio.gather(
                                                            *[send_telegram_message(session, cid, alert_msg) for cid in list(active_chats)],
                                                            return_exceptions=True
                                                        )
                                                        
                                                        # Chỉ đánh dấu đã thông báo nếu gửi thành công ít nhất 1 chat
                                                        if any(r is not None and not isinstance(r, Exception) for r in send_results):
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
                message = build_positions_text()
                
                # Gửi hoặc sửa tin nhắn cho tất cả các chat_id đã đăng ký (song song)
                async def update_auto_chat(chat_id):
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
                
                await asyncio.gather(*(update_auto_chat(cid) for cid in list(auto_chats)), return_exceptions=True)
                save_auto_chats()
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
        save_auto_chats()
            
        await send_telegram_message(session, chat_id, "❌ Đã tắt tự động cập nhật vị thế mỗi 1 phút.")
    else:
        auto_chats.add(chat_id)
        save_auto_chats()
        await send_telegram_message(session, chat_id, "✅ Đã bật tự động cập nhật vị thế mỗi 1 phút.")
        
        # Gửi luôn vị thế hiện tại ngay lập tức và lưu message_id làm tin nhắn auto đầu tiên
        if positions:
            message = build_positions_text("🔍 *TỰ ĐỘNG CẬP NHẬT VỊ THẾ ĐANG MỞ (1P)*\n----------------------------------")
            
            new_msg_id = await send_telegram_message(session, chat_id, message, is_auto=True)
            if new_msg_id:
                last_auto_messages[chat_id] = new_msg_id
                has_new_activity[chat_id] = False
                save_auto_chats()
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
    
    message = (
        f"📊 *TỔNG PNL VỊ THẾ HIỆN TẠI*\n"
        f"----------------------------------\n"
        f"💰 Trạng thái: {pnl_emoji(total_pnl)} *{fmt_signed(total_pnl)} USDT*\n"
        f"🔥 Vị thế đang mở: *{len(positions)}*"
    )
    await send_telegram_message(session, chat_id, message)

# Xử lý lệnh /pos
async def handle_pos_command(session, chat_id):
    if not positions:
        await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures nào đang mở.")
        return
        
    message = build_positions_text("🔍 *CHI TIẾT VỊ THẾ ĐANG MỞ*\n----------------------------------")
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


def display_symbol(symbol):
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def pos_side_display(position_side, amount):
    return "LONG" if (position_side == 'LONG' or (position_side == 'BOTH' and amount > 0)) else "SHORT"


def pnl_emoji(value):
    return "🟩" if value >= 0 else "🟥"


def fmt_signed(value):
    return f"{'+' if value >= 0 else ''}{value:,.2f}"


def funding_str(rate):
    return f" (FR: {rate * 100:+.4f}%)" if abs(rate) >= 0.005 else ""


def build_positions_text(header=None):
    """
    Dựng nội dung tin nhắn danh sách vị thế đang mở + tổng PnL.
    Dùng chung cho /pos, /auto và vòng lặp auto cập nhật.
    """
    if header is None:
        tz_vn = timezone(timedelta(hours=7))
        now_str = datetime.now(tz_vn).strftime("%d/%m/%Y %H:%M:%S")
        header = f"🕒 *Cập nhật lúc:* `{now_str}`\n"
    text_lines = [header]
    for pos in positions.values():
        pnl = pos['unrealizedPnL']
        text_lines.append(
            f"{display_symbol(pos['symbol'])} ({pos_side_display(pos['positionSide'], pos['positionAmt'])}) ➜ "
            f"{pnl_emoji(pnl)} *{fmt_signed(pnl)} USDT*{funding_str(pos.get('fundingRate', 0.0))}"
        )
    text_lines.append("----------------------------------")
    total_pnl = sum(p.get('unrealizedPnL', 0.0) for p in positions.values())
    text_lines.append(f"📊 Tổng PnL: *{fmt_signed(total_pnl)} USDT*")
    return "\n\n".join(text_lines)


async def get_market_snapshot(session):
    """
    Lấy giá + % thay đổi 24h + funding rate toàn sàn, cache 30s.
    Trả về (tickers_map, funding_map); nếu API lỗi trả về map rỗng.
    """
    now = time.time()
    if (market_snapshot_cache["tickers"] is not None
            and now - market_snapshot_cache["timestamp"] < TICKER_CACHE_TTL):
        return market_snapshot_cache["tickers"], market_snapshot_cache["funding"]

    async with market_snapshot_cache["lock"]:
        now = time.time()
        if (market_snapshot_cache["tickers"] is not None
                and now - market_snapshot_cache["timestamp"] < TICKER_CACHE_TTL):
            return market_snapshot_cache["tickers"], market_snapshot_cache["funding"]

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        api_key = os.getenv("BINANCE_API_KEY")
        if api_key:
            headers["X-MBX-APIKEY"] = api_key

        async def fetch_24h():
            try:
                async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr", headers=headers) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except Exception as e:
                logger.error(f"Lỗi lấy 24hr ticker: {e}")
            return None

        async def fetch_premium():
            try:
                async with session.get("https://fapi.binance.com/fapi/v1/premiumIndex", headers=headers) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except Exception as e:
                logger.error(f"Lỗi lấy premiumIndex: {e}")
            return None

        res_24h, res_premium = await asyncio.gather(fetch_24h(), fetch_premium())

        tickers_map = {}
        funding_map = {}
        if res_24h:
            for item in res_24h:
                tickers_map[item['symbol']] = {
                    'price': float(item['lastPrice']),
                    'change': float(item['priceChangePercent'])
                }
        if res_premium and isinstance(res_premium, list):
            for item in res_premium:
                funding_map[item.get('symbol')] = float(item.get('lastFundingRate', 0))

        # Chỉ cập nhật cache khi có dữ liệu để lỗi tạm thời không bị cache
        if tickers_map:
            market_snapshot_cache["tickers"] = tickers_map
            market_snapshot_cache["funding"] = funding_map
            market_snapshot_cache["timestamp"] = time.time()

        return tickers_map, funding_map


async def get_coin_prices(session, coin_names):
    # Chuẩn hóa tên coin cần tìm
    targets = {}
    for coin in coin_names:
        coin_upper = coin.upper()
        symbol = coin_upper if coin_upper.endswith("USDT") else f"{coin_upper}USDT"
        targets[symbol] = coin_upper

    tickers_map, funding_map = await get_market_snapshot(session)

    results = []
    for symbol, coin_upper in targets.items():
        info = tickers_map.get(symbol)
        if info is not None:
            info = {
                'price': info['price'],
                'change': info['change'],
                'funding_rate': funding_map.get(symbol, 0.0)
            }
        results.append((coin_upper, info))
    return results


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
                
                message = (
                    f"💳 *THÔNG TIN TÀI KHOẢN FUTURES*\n"
                    f"----------------------------------\n"
                    f"💰 Số dư ví: *{wallet_bal:,.2f} USDT*\n"
                    f"📊 PnL chưa thực hiện: {pnl_emoji(pnl)} *{fmt_signed(pnl)} USDT*\n"
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
    try:
        tickers_map, _ = await get_market_snapshot(session)
        
        usdt_tickers = [
            {'symbol': symbol[:-4], 'price': info['price'], 'change': info['change']}
            for symbol, info in tickers_map.items()
            if symbol.endswith("USDT")
        ]
        
        if not usdt_tickers:
            await send_telegram_message(session, chat_id, "❌ Lỗi khi lấy dữ liệu biến động từ Binance.")
            return
        
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
    except Exception as e:
        logger.error(f"Lỗi trong handle_top_command: {e}")
        await send_telegram_message(session, chat_id, "❌ Đã xảy ra lỗi khi xử lý dữ liệu biến động.")


async def handle_history_command(session, chat_id, coin_name=None):
    """
    Lấy lịch sử chốt vị thế (Realized PnL) từ Binance Futures.
    """
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    symbol = None
    if coin_name:
        coin_name = coin_name.upper()
        symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
        
    timestamp = int(time.time() * 1000)
    params = [
        "incomeType=REALIZED_PNL",
        "limit=100",  # Lấy nhiều bản ghi thô hơn để sau khi gom nhóm không bị thiếu
        f"timestamp={timestamp}"
    ]
    if symbol:
        params.insert(0, f"symbol={symbol}")
        
    query_string = "&".join(params)
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v1/income?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                if not data:
                    await send_telegram_message(
                        session, 
                        chat_id, 
                        f"ℹ️ Không tìm thấy lịch sử chốt vị thế (Realized PnL) nào{' cho ' + symbol if symbol else ''}."
                    )
                    return
                
                # Gom nhóm các bản ghi PnL rời rạc (fragmented trades) có cùng symbol và cách nhau dưới 10 giây
                grouped_data = []
                for item in data:
                    sym = item.get('symbol')
                    income = float(item.get('income', 0))
                    time_ms = item.get('time')
                    
                    found = False
                    for g in grouped_data:
                        # Nếu cùng symbol và chênh lệch thời gian không quá 10 giây (10000ms), coi như cùng 1 lệnh chốt vị thế
                        if g['symbol'] == sym and abs(g['time'] - time_ms) <= 10000:
                            g['income'] += income
                            # Giữ thời gian mới nhất trong nhóm
                            if time_ms > g['time']:
                                g['time'] = time_ms
                            found = True
                            break
                    
                    if not found:
                        grouped_data.append({
                            'symbol': sym,
                            'income': income,
                            'time': time_ms
                        })
                
                # Sắp xếp lại theo thời gian mới nhất (gần nhất) lên đầu
                grouped_data.sort(key=lambda x: x['time'], reverse=True)
                
                # Chỉ lấy tối đa 10 vị thế chốt tổng gần nhất để hiển thị
                display_data = grouped_data[:10]
                
                tz_vn = timezone(timedelta(hours=7))
                lines = ["📜 *LỊCH SỬ CHỐT VỊ THẾ GẦN NHẤT (REALIZED PNL)*\n----------------------------------"]
                
                total_realized_pnl = 0.0
                
                for i, item in enumerate(display_data, 1):
                    sym = item['symbol']
                    income = item['income']
                    time_ms = item['time']
                    
                    total_realized_pnl += income
                    
                    time_dt = datetime.fromtimestamp(time_ms / 1000.0, tz=tz_vn)
                    time_str = time_dt.strftime("%d/%m/%Y %H:%M:%S")
                    
                    display_sym = sym[:-4] if sym.endswith("USDT") else sym
                    
                    lines.append(
                        f"{i}. *{display_sym}* ➜ {pnl_emoji(income)} `{fmt_signed(income)} USDT`\n"
                        f"Thời gian: `{time_str}`"
                    )
                    
                lines.append("----------------------------------")
                lines.append(f"📊 *Tổng kết {len(display_data)} vị thế gần nhất:*")
                lines.append(f"💰 Tổng Realized PnL: {pnl_emoji(total_realized_pnl)} `{fmt_signed(total_realized_pnl)} USDT`")
                
                message = "\n\n".join(lines)
                await send_telegram_message(session, chat_id, message)
            else:
                body = await resp.text()
                logger.error(f"Lỗi lấy lịch sử vị thế: HTTP {resp.status} - {body}")
                await send_telegram_message(session, chat_id, "❌ Lỗi khi truy vấn lịch sử vị thế từ Binance.")
    except Exception as e:
        logger.error(f"Lỗi trong handle_history_command: {e}")
        await send_telegram_message(session, chat_id, "❌ Đã xảy ra lỗi hệ thống khi lấy lịch sử vị thế.")


async def handle_liq_command(session, chat_id):
    """
    Xem các vị thế đang mở và giá thanh lý của từng vị thế.
    """
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}"
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v2/positionRisk?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                
                open_positions = []
                for p in data:
                    amount = float(p.get('positionAmt', 0))
                    if amount != 0.0:
                        open_positions.append(p)
                        
                if not open_positions:
                    await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures nào đang mở.")
                    return
                    
                lines = ["☣️ *GIÁ THANH LÝ CÁC VỊ THẾ ĐANG MỞ*\n----------------------------------"]
                for p in open_positions:
                    symbol = p.get('symbol')
                    side = p.get('positionSide')
                    amount = float(p.get('positionAmt', 0))
                    entry_price = float(p.get('entryPrice', 0))
                    mark_price = float(p.get('markPrice', 0))
                    unrealized_pnl = float(p.get('unrealizedProfit', 0))
                    leverage = p.get('leverage')
                    liq_price = float(p.get('liquidationPrice', 0))
                    
                    # Binance trả về 0 nếu CROSS và tài khoản rất an toàn hoặc không có giá thanh lý
                    liq_price_str = format_price(liq_price) if liq_price > 0 else "Không có ( CROSS/Safe )"
                    
                    # Lấy funding rate hiện tại từ cache
                    pos_key = f"{symbol}_{side}"
                    funding_rate = positions.get(pos_key, {}).get('fundingRate', 0.0)
                    
                    funding_part = ""
                    if abs(funding_rate) >= 0.005:
                        funding_part = f" | Funding: `{funding_rate * 100:+.4f}%`"
                    
                    pos_lines = (
                        f"🪙 *{display_symbol(symbol)}* ({pos_side_display(side, amount)})\n"
                        f"• Entry: `{format_price(entry_price)} USDT`\n"
                        f"• Mark Price: `{format_price(mark_price)} USDT`\n"
                        f"• PnL: {pnl_emoji(unrealized_pnl)} `{fmt_signed(unrealized_pnl)} USDT`\n"
                        f"• Leverage: `{leverage}x`{funding_part}\n"
                        f"• **Giá thanh lý:** 💀 `{liq_price_str}`"
                    )
                    lines.append(pos_lines)
                    
                message = "\n\n".join(lines)
                await send_telegram_message(session, chat_id, message)
            else:
                body = await resp.text()
                logger.error(f"Lỗi lấy dữ liệu positionRisk: HTTP {resp.status} - {body}")
                await send_telegram_message(session, chat_id, "❌ Lỗi khi lấy thông tin thanh lý từ Binance.")
    except Exception as e:
        logger.error(f"Lỗi trong handle_liq_command: {e}")
        await send_telegram_message(session, chat_id, "❌ Đã xảy ra lỗi hệ thống khi kiểm tra giá thanh lý.")


def detect_divergence(price, osc, lookback=30):
    """
    Phát hiện divergence giữa giá và oscillator (RSI / MACD hist) trong lookback nến gần nhất.
    Trả về 'bullish', 'bearish' hoặc None.
    """
    if len(price) < lookback or len(osc) < lookback:
        return None
    p_l = list(price.iloc[-lookback:])
    o_l = list(osc.iloc[-lookback:])
    half = lookback // 2

    def idx_min(seq):
        return seq.index(min(seq))

    def idx_max(seq):
        return seq.index(max(seq))

    # So sánh đáy nửa sau với đáy nửa trước
    p_low_1 = idx_min(p_l[:half])
    p_low_2 = half + idx_min(p_l[half:])
    o_low_1 = idx_min(o_l[:half])
    o_low_2 = half + idx_min(o_l[half:])
    bullish = p_l[p_low_2] < p_l[p_low_1] and o_l[o_low_2] > o_l[o_low_1]
    # So sánh đỉnh nửa sau với đỉnh nửa trước
    p_high_1 = idx_max(p_l[:half])
    p_high_2 = half + idx_max(p_l[half:])
    o_high_1 = idx_max(o_l[:half])
    o_high_2 = half + idx_max(o_l[half:])
    bearish = p_l[p_high_2] > p_l[p_high_1] and o_l[o_high_2] < o_l[o_high_1]
    if bullish and not bearish:
        return 'bullish'
    if bearish and not bullish:
        return 'bearish'
    return None


def detect_candle_pattern(o, h, l, c, prev_o, prev_c):
    """
    Nhận diện candlestick pattern đơn giản: engulfing, pin bar (hammer/shooting star).
    Trả về tên pattern hoặc None.
    """
    body = abs(c - o)
    prev_body = abs(prev_c - prev_o)
    rng = h - l
    if rng <= 0 or body <= 0:
        return None
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    # Bullish / Bearish engulfing: thân nến sau bao trùm thân nến trước và ngược màu
    if c > o and prev_c < prev_o and c >= prev_o and o <= prev_c and body > prev_body:
        return 'bullish_engulfing'
    if c < o and prev_c > prev_o and c <= prev_o and o >= prev_c and body > prev_body:
        return 'bearish_engulfing'
    # Pin bar: râu dài >= 2 lần thân và >= 60% range
    if lower_wick >= 2 * body and lower_wick >= 0.6 * rng:
        return 'hammer'
    if upper_wick >= 2 * body and upper_wick >= 0.6 * rng:
        return 'shooting_star'
    return None


async def fetch_futures_extras(session, symbol):
    """
    Lấy dữ liệu futures bổ trợ: % thay đổi Open Interest 24h, taker buy/sell ratio, funding rate.
    Trả về dict rỗng nếu lỗi (scoring sẽ bỏ qua).
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    extras = {'oi_change': None, 'taker_ratio': None, 'funding_rate': None}
    
    async def fetch_oi():
        try:
            url = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period=1h&limit=25"
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list) and len(data) >= 2:
                        first = float(data[0].get('sumOpenInterest', 0))
                        last = float(data[-1].get('sumOpenInterest', 0))
                        if first > 0:
                            return (last - first) / first * 100
        except Exception as e:
            logger.warning(f"Lỗi lấy openInterestHist cho {symbol}: {e}")
        return None

    async def fetch_taker():
        try:
            url = f"https://fapi.binance.com/futures/data/takerlongshortRatio?symbol={symbol}&period=1h&limit=1"
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list) and data:
                        return float(data[0].get('buySellRatio', 0)) or None
        except Exception as e:
            logger.warning(f"Lỗi lấy takerlongshortRatio cho {symbol}: {e}")
        return None

    async def fetch_funding():
        try:
            return await get_single_funding_rate(session, symbol)
        except Exception:
            return None

    oi, taker, funding = await asyncio.gather(fetch_oi(), fetch_taker(), fetch_funding())
    extras['oi_change'] = oi
    extras['taker_ratio'] = taker
    extras['funding_rate'] = funding if funding != 0.0 else None
    return extras


def score_confidence(score):
    """Quy đổi điểm số thành nhãn độ tin cậy (dùng khi điểm bị điều chỉnh sau analyze)."""
    if score >= 6.0:
        return 'Rất mạnh'
    if score >= 4.5:
        return 'Mạnh'
    if score >= 3.0:
        return 'Trung bình'
    if score >= 2.5:
        return 'Yếu'
    return 'Thấp'


btc_filter_cache = {}

async def get_btc_filter(session, interval='4h', ttl=600):
    """Lấy phân tích BTCUSDT cho lọc xu hướng, cache TTL giây."""
    now = time.time()
    cached = btc_filter_cache.get(interval)
    if cached and now - cached['ts'] < ttl:
        return cached['res']
    res = await analyze_market(session, 'BTCUSDT', interval=interval, fetch_extras=False)
    btc_filter_cache[interval] = {'res': res, 'ts': now}
    return res


def apply_btc_penalty(res, btc_res, penalty=1.5):
    """
    Trừ điểm nặng nếu tín hiệu alt đi ngược xu hướng BTC mạnh.
    Sau khi trừ, hạ lại confidence và vô hiệu tín hiệu nếu điểm dưới 2.5.
    """
    if not res or not btc_res or res.get('signal') == 'NEUTRAL' or res.get('btc_penalty'):
        return
    if res['signal'] == 'LONG':
        btc_downtrend = (btc_res['close'] < btc_res['ema9'] < btc_res['ema21'] < btc_res['ema50']) or btc_res['short_score'] >= 4.0
        if btc_downtrend:
            res['long_score'] = max(0.0, res['long_score'] - penalty)
            res['btc_penalty'] = True
    elif res['signal'] == 'SHORT':
        btc_uptrend = (btc_res['close'] > btc_res['ema9'] > btc_res['ema21'] > btc_res['ema50']) or btc_res['long_score'] >= 4.0
        if btc_uptrend:
            res['short_score'] = max(0.0, res['short_score'] - penalty)
            res['btc_penalty'] = True
    if res.get('btc_penalty'):
        side_score = res['long_score'] if res['signal'] == 'LONG' else res['short_score']
        res['confidence'] = score_confidence(side_score)
        if side_score < 2.5:
            res['signal'] = 'NEUTRAL'
            res['confidence'] = 'Thấp'


async def analyze_market(session, symbol, interval='1h', df=None, fetch_extras=True):
    """
    Phân tích kỹ thuật chi tiết cho một symbol.
    Chỉ báo: RSI, Stochastic RSI, EMA(9/21/50/200), VWAP, Bollinger Bands, MACD, ATR, ADX,
    Volume, Support/Resistance, Divergence, Candlestick Pattern + dữ liệu futures (OI, taker ratio).
    Chỉ dùng NẾN ĐÃ ĐÓNG (bỏ nến đang hình thành) để tránh repaint tín hiệu.
    Truyền df (DataFrame klines đã đóng) để tái sử dụng engine cho backtest mà không gọi API.
    """
    if df is None:
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit=500"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"Lỗi API Binance khi lấy klines cho {symbol}: {body}")
                    return None
                klines_data = await resp.json()
                if not isinstance(klines_data, list) or len(klines_data) == 0:
                    return None
        except Exception as e:
            logger.error(f"Lỗi kết nối klines cho {symbol}: {e}")
            return None
            
        df = pd.DataFrame(klines_data, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base', 'taker_buy_quote', 'ignore'
        ])
    
    df = df.copy()
    df['close'] = df['close'].astype(float)
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(float)
    
    # Chỉ dùng nến đã đóng: bỏ nến cuối (đang hình thành hoặc nến giả lập cho backtest)
    if len(df) > 2:
        df = df.iloc[:-1]
        if len(df) < 60:
            return None
    
    # ─── 1. RSI (14) ───
    close_delta = df['close'].diff()
    up = close_delta.clip(lower=0)
    down = -1 * close_delta.clip(upper=0)
    ma_up = up.ewm(com=13, adjust=False).mean()
    ma_down = down.ewm(com=13, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-10)
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # ─── 2. Stochastic RSI (14, 14, 3, 3) ───
    rsi_series = df['rsi']
    rsi_min = rsi_series.rolling(window=14).min()
    rsi_max = rsi_series.rolling(window=14).max()
    stoch_rsi = (rsi_series - rsi_min) / (rsi_max - rsi_min + 1e-10)
    df['stoch_k'] = stoch_rsi.rolling(window=3).mean() * 100
    df['stoch_d'] = df['stoch_k'].rolling(window=3).mean()
    
    # ─── 3. EMA (9, 21, 50, 200) ───
    df['ema9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema21'] = df['close'].ewm(span=21, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    
    # ─── 3b. VWAP (rolling 24 nến) ───
    tp = (df['high'] + df['low'] + df['close']) / 3
    df['vwap'] = (tp * df['volume']).rolling(window=24).sum() / (df['volume'].rolling(window=24).sum() + 1e-10)
    
    # ─── 4. Bollinger Bands (20, 2) ───
    df['ma20'] = df['close'].rolling(window=20).mean()
    df['std20'] = df['close'].rolling(window=20).std()
    df['upper_band'] = df['ma20'] + (df['std20'] * 2)
    df['lower_band'] = df['ma20'] - (df['std20'] * 2)
    
    # ─── 5. MACD (12, 26, 9) ───
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = exp1 - exp2
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['hist'] = df['macd'] - df['macd_signal']
    
    # ─── 6. ATR (14) ───
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift(1)).abs()
    low_close = (df['low'] - df['close'].shift(1)).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr'] = true_range.ewm(span=14, adjust=False).mean()
    
    # ─── 7. ADX (14) ───
    plus_dm = df['high'].diff()
    minus_dm = -df['low'].diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
    atr_14 = df['atr']
    plus_di = 100 * (plus_dm.ewm(span=14, adjust=False).mean() / (atr_14 + 1e-10))
    minus_di = 100 * (minus_dm.ewm(span=14, adjust=False).mean() / (atr_14 + 1e-10))
    dx = 100 * ((plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10))
    df['adx'] = dx.ewm(span=14, adjust=False).mean()
    df['plus_di'] = plus_di
    df['minus_di'] = minus_di
    
    # ─── 8. Volume Analysis ───
    df['vol_ma20'] = df['volume'].rolling(window=20).mean()
    
    # ─── 9. Support / Resistance (từ đỉnh/đáy gần nhất trong 50 nến) ───
    lookback = min(50, len(df) - 2)
    recent = df.tail(lookback)
    support = recent['low'].min()
    resistance = recent['high'].max()
    
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    close_price = latest['close']
    rsi_val = latest['rsi']
    stoch_k = latest['stoch_k']
    stoch_d = latest['stoch_d']
    ema9_val = latest['ema9']
    ema21_val = latest['ema21']
    ema50_val = latest['ema50']
    ema200_val = latest['ema200']
    vwap_val = latest['vwap']
    upper_b = latest['upper_band']
    lower_b = latest['lower_band']
    macd_val = latest['macd']
    sig_val = latest['macd_signal']
    hist_val = latest['hist']
    prev_hist = prev['hist']
    atr_val = latest['atr']
    adx_val = latest['adx']
    plus_di_val = latest['plus_di']
    minus_di_val = latest['minus_di']
    vol_now = latest['volume']
    vol_avg = latest['vol_ma20']
    vol_ratio = vol_now / (vol_avg + 1e-10)
    
    # ─── 10. Divergence giá vs RSI / MACD hist (30 nến gần nhất) ───
    rsi_div = detect_divergence(df['close'], df['rsi'], 30)
    macd_div = detect_divergence(df['close'], df['hist'], 30)
    
    # ─── 11. Candlestick pattern (nến hiện tại + nến trước) ───
    candle_pattern = detect_candle_pattern(
        latest['open'], latest['high'], latest['low'], latest['close'],
        prev['open'], prev['close']
    )
    
    # ─── 12. Dữ liệu futures (OI, taker ratio) — chỉ tải cho khung 1h ───
    extras = {}
    if fetch_extras and interval == '1h':
        extras = await fetch_futures_extras(session, symbol)
    oi_change = extras.get('oi_change')
    taker_ratio = extras.get('taker_ratio')
    funding_rate_val = extras.get('funding_rate')
    
    # ═══════════════════════════════════
    # HỆ THỐNG CHẤM ĐIỂM (Thang 10)
    # ═══════════════════════════════════
    long_score = 0.0
    short_score = 0.0
    
    # ── RSI (max 1.5đ) ──
    if rsi_val <= 25:
        long_score += 1.5
    elif rsi_val <= 35:
        long_score += 1.0
    elif rsi_val <= 45:
        long_score += 0.3
    elif rsi_val >= 75:
        short_score += 1.5
    elif rsi_val >= 65:
        short_score += 1.0
    elif rsi_val >= 55:
        short_score += 0.3
        
    # ── Stochastic RSI (max 1.0đ) ──
    if stoch_k <= 20 and stoch_d <= 20:
        long_score += 1.0
    elif stoch_k <= 30:
        long_score += 0.4
    elif stoch_k >= 80 and stoch_d >= 80:
        short_score += 1.0
    elif stoch_k >= 70:
        short_score += 0.4
    # Crossover bonus
    prev_stoch_k = prev['stoch_k']
    prev_stoch_d = prev['stoch_d']
    if stoch_k > stoch_d and prev_stoch_k <= prev_stoch_d and stoch_k <= 40:
        long_score += 0.5  # Bullish cross ở vùng oversold
    elif stoch_k < stoch_d and prev_stoch_k >= prev_stoch_d and stoch_k >= 60:
        short_score += 0.5  # Bearish cross ở vùng overbought
        
    # ── EMA Trend (max 2.0đ) ──
    if close_price > ema9_val > ema21_val > ema50_val:
        long_score += 2.0  # Uptrend hoàn hảo
    elif close_price > ema9_val > ema21_val:
        long_score += 1.2
    elif close_price > ema21_val:
        long_score += 0.5
    elif close_price < ema9_val < ema21_val < ema50_val:
        short_score += 2.0  # Downtrend hoàn hảo
    elif close_price < ema9_val < ema21_val:
        short_score += 1.2
    elif close_price < ema21_val:
        short_score += 0.5
        
    # ── Bollinger Bands (max 1.5đ) ──
    bb_width = upper_b - lower_b
    bb_pct = (close_price - lower_b) / (bb_width + 1e-10)  # 0 = lower band, 1 = upper band
    if bb_pct <= 0.0:
        long_score += 1.5  # Chạm/phá biên dưới
    elif bb_pct <= 0.15:
        long_score += 0.8
    elif bb_pct >= 1.0:
        short_score += 1.5  # Chạm/phá biên trên
    elif bb_pct >= 0.85:
        short_score += 0.8
        
    # ── Confluence Bonus (Sự đồng thuận chỉ báo) ──
    if bb_pct <= 0.05 and rsi_val <= 30:
        long_score += 0.8  # Quá bán + Chạm biên dưới -> Tăng uy tín đảo chiều tăng
    if bb_pct >= 0.95 and rsi_val >= 70:
        short_score += 0.8  # Quá mua + Chạm biên trên -> Tăng uy tín đảo chiều giảm
        
    # ── MACD (max 1.5đ) ──
    if hist_val > 0 and prev_hist <= 0:
        long_score += 1.5  # Bullish crossover
    elif hist_val < 0 and prev_hist >= 0:
        short_score += 1.5  # Bearish crossover
    elif hist_val > 0 and hist_val > prev_hist:
        long_score += 0.7  # Momentum tăng
    elif hist_val > 0:
        long_score += 0.3
    elif hist_val < 0 and hist_val < prev_hist:
        short_score += 0.7  # Momentum giảm
    elif hist_val < 0:
        short_score += 0.3
        
    # ── EMA200 - Xu hướng lớn (max 0.4đ) ──
    if close_price > ema200_val:
        long_score += 0.4
    elif close_price < ema200_val:
        short_score += 0.4
        
    # ── VWAP (max 0.3đ) ──
    if not math.isnan(vwap_val):
        if close_price > vwap_val:
            long_score += 0.3
        elif close_price < vwap_val:
            short_score += 0.3
            
    # ── ADX - Sức mạnh xu hướng (max 1.0đ) ──
    if adx_val >= 25:
        # Xu hướng mạnh → tăng điểm cho bên có DI chiếm ưu thế
        if plus_di_val > minus_di_val:
            long_score += min(1.0, (adx_val - 25) / 25 + 0.5)
        else:
            short_score += min(1.0, (adx_val - 25) / 25 + 0.5)
    # ADX thấp (< 20) → thị trường sideway, penalty cả 2 bên
    if adx_val < 20:
        long_score *= 0.85
        short_score *= 0.85
        
    # ── Volume Confirmation (max 1.5đ) ──
    if vol_ratio >= 2.0:
        # Volume cao bất thường → tăng điểm cho phe đang thắng
        if long_score > short_score:
            long_score += 1.5
        elif short_score > long_score:
            short_score += 1.5
    elif vol_ratio >= 1.3:
        if long_score > short_score:
            long_score += 0.7
        elif short_score > long_score:
            short_score += 0.7
    elif vol_ratio < 0.5:
        # Volume quá thấp → tín hiệu yếu, penalty
        long_score *= 0.7
        short_score *= 0.7
        
    # ═══ Volume confirmation đặc biệt (Multiplier) ═══
    if vol_ratio >= 1.8:
        if long_score > short_score:
            long_score *= 1.15
        elif short_score > long_score:
            short_score *= 1.15
    elif vol_ratio < 0.6:
        # volume cực thấp, hạ thêm điểm tin cậy
        long_score *= 0.85
        short_score *= 0.85
        
    # ═══ Divergence (tín hiệu đảo chiều) ═══
    if rsi_div == 'bullish':
        long_score += 1.0
    elif rsi_div == 'bearish':
        short_score += 1.0
    if macd_div == 'bullish':
        long_score += 0.7
    elif macd_div == 'bearish':
        short_score += 0.7
        
    # ═══ Candlestick pattern ═══
    if candle_pattern in ('bullish_engulfing', 'hammer'):
        long_score += 0.5
    elif candle_pattern in ('bearish_engulfing', 'shooting_star'):
        short_score += 0.5
        
    # ═══ Dữ liệu futures (OI / taker ratio / funding) ═══
    price_change_24 = ((close_price - df['close'].iloc[-25]) / df['close'].iloc[-25] * 100) if len(df) >= 25 else 0.0
    if oi_change is not None and oi_change >= 5.0:
        # OI tăng + giá tăng → tiền mới vào phe long; OI tăng + giá giảm → phe short áp đảo
        if price_change_24 > 0:
            long_score += 0.4
        else:
            short_score += 0.4
    if taker_ratio is not None:
        if taker_ratio >= 1.15:
            long_score += 0.3
        elif taker_ratio <= 0.87:
            short_score += 0.3
    if funding_rate_val is not None:
        # Funding cực đoan → đám đông quá đông một chiều, cảnh báo đảo chiều
        if funding_rate_val >= 0.00075:
            long_score *= 0.85
        elif funding_rate_val <= -0.00075:
            short_score *= 0.85
        
    # ═══ PENALTY: Tín hiệu mâu thuẫn ═══
    # Nếu MACD bearish nhưng RSI bullish (hoặc ngược lại) → giảm điểm
    macd_bullish = hist_val > 0
    rsi_bullish = rsi_val < 45
    macd_bearish = hist_val < 0
    rsi_bearish = rsi_val > 55
    if macd_bullish and rsi_bearish:
        long_score *= 0.8
    if macd_bearish and rsi_bullish:
        short_score *= 0.8
    # Nếu giá trên EMA nhưng RSI quá mua → cảnh báo
    if close_price > ema21_val and rsi_val >= 70:
        long_score *= 0.75
    if close_price < ema21_val and rsi_val <= 30:
        short_score *= 0.75

    # ═══ TREND FILTER (Ngăn chặn giao dịch ngược xu hướng mạnh) ═══
    # Nếu đang downtrend rất mạnh, giới hạn điểm LONG tối đa để tránh bắt dao rơi
    if close_price < ema9_val < ema21_val < ema50_val:
        long_score = min(long_score, 3.0)
    # Nếu đang uptrend rất mạnh, giới hạn điểm SHORT tối đa để tránh cản tàu
    if close_price > ema9_val > ema21_val > ema50_val:
        short_score = min(short_score, 3.0)
        
    # ═══ KẾT LUẬN TÍN HIỆU (ngưỡng theo regime thị trường) ═══
    signal = 'NEUTRAL'
    confidence = 'Thấp'
    
    max_score = max(long_score, short_score)
    atr_pct_val = (atr_val / close_price) * 100 if close_price > 0 else 0.0
    
    # Sideway (ADX thấp) hoặc biến động quá mạnh (ATR cao) → yêu cầu điểm cao hơn
    min_signal_score = 2.5
    if adx_val < 20:
        min_signal_score = 3.2
    elif atr_pct_val > 3.0:
        min_signal_score = 3.0
    
    if long_score > short_score and long_score >= min_signal_score:
        signal = 'LONG'
        if long_score >= 6.0:
            confidence = 'Rất mạnh'
        elif long_score >= 4.5:
            confidence = 'Mạnh'
        elif long_score >= 3.0:
            confidence = 'Trung bình'
        else:
            confidence = 'Yếu'
    elif short_score > long_score and short_score >= min_signal_score:
        signal = 'SHORT'
        if short_score >= 6.0:
            confidence = 'Rất mạnh'
        elif short_score >= 4.5:
            confidence = 'Mạnh'
        elif short_score >= 3.0:
            confidence = 'Trung bình'
        else:
            confidence = 'Yếu'
            
    # ═══ TÍNH TP/SL DỰA TRÊN ATR ═══
    tp_price = 0.0
    sl_price = 0.0
    # RR 1:1 được backtest 571 tín hiệu (10 coin × 2000 nến 1h) xác nhận là cấu hình
    # có edge dương duy nhất: win-rate 54.9%, EV +0.10R (RR 1:2 chỉ còn 30.7%, EV âm)
    rr_ratio = 1.0
    
    # Lấy thông tin làm tròn
    qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)
    
    if signal == 'LONG':
        sl_price = close_price - (atr_val * 1.5)
        if sl_price <= 0 or sl_price >= close_price:
            sl_price = close_price * 0.97
        # SL không vượt quá support gần nhất (nếu support gần)
        if support > 0 and support < close_price:
            sl_from_support = support - (atr_val * 0.3)
            if sl_from_support > 0:
                sl_price = min(sl_price, sl_from_support)
        risk = close_price - sl_price
        tp_price = close_price + (risk * rr_ratio)
        
    elif signal == 'SHORT':
        sl_price = close_price + (atr_val * 1.5)
        if sl_price <= close_price:
            sl_price = close_price * 1.03
        # SL không vượt quá resistance gần nhất
        if resistance > 0 and resistance > close_price:
            sl_from_resistance = resistance + (atr_val * 0.3)
            sl_price = max(sl_price, sl_from_resistance)
        risk = sl_price - close_price
        tp_price = close_price - (risk * rr_ratio)
        if tp_price <= 0:
            tp_price = close_price * 0.94
        
    if tp_price > 0:
        tp_price = round_price_step(tp_price, tick_size, price_p)
    if sl_price > 0:
        sl_price = round_price_step(sl_price, tick_size, price_p)
        
    return {
        'symbol': symbol,
        'interval': interval,
        'close': close_price,
        'rsi': rsi_val,
        'stoch_k': stoch_k,
        'stoch_d': stoch_d,
        'ema9': ema9_val,
        'ema21': ema21_val,
        'ema50': ema50_val,
        'ema200': ema200_val,
        'vwap': vwap_val,
        'upper_band': upper_b,
        'lower_band': lower_b,
        'bb_pct': bb_pct,
        'macd': macd_val,
        'signal_line': sig_val,
        'hist': hist_val,
        'atr': atr_val,
        'adx': adx_val,
        'plus_di': plus_di_val,
        'minus_di': minus_di_val,
        'vol_ratio': vol_ratio,
        'support': support,
        'resistance': resistance,
        'rsi_div': rsi_div,
        'macd_div': macd_div,
        'pattern': candle_pattern,
        'oi_change': oi_change,
        'taker_ratio': taker_ratio,
        'funding_rate': funding_rate_val,
        'signal': signal,
        'confidence': confidence,
        'long_score': long_score,
        'short_score': short_score,
        'tp': tp_price,
        'sl': sl_price
    }


async def scan_market_signals(session):
    """
    Quét qua top 50 coin theo volume 24h để tìm cơ hội giao dịch có tỉ lệ thắng cao.
    Sử dụng Semaphore để giới hạn request song song và lọc xu hướng khung 4h (MTF Confluence) để tăng win rate.
    Chỉ trả về các tín hiệu có độ tin cậy từ 4 sao trở lên (Mạnh và Rất mạnh).
    """
    url_ticker = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    coins_to_scan = []
    try:
        async with session.get(url_ticker, headers=headers) as resp:
            if resp.status == 200:
                tickers = await resp.json()
                usdt_tickers = [t for t in tickers if t['symbol'].endswith('USDT')]
                # Sắp xếp theo quoteVolume 24h giảm dần
                usdt_tickers.sort(key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
                coins_to_scan = [t['symbol'] for t in usdt_tickers[:50]]
            else:
                body = await resp.text()
                logger.error(f"Lỗi gọi API ticker 24h: {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách top 50 volume: {e}")
        
    if not coins_to_scan:
        # Fallback danh sách coin phổ biến
        fallback_coins = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE', 'ADA', 'LINK', 'NEAR', 'SUI', 'AVAX', 'OP']
        coins_to_scan = [f"{c}USDT" for c in fallback_coins]
        
    # Sử dụng Semaphore để khống chế tốc độ request, tránh lỗi 429
    sem = asyncio.Semaphore(10)
    
    async def analyze_with_sem(symbol, interval, fetch_extras=True):
        async with sem:
            return await analyze_market(session, symbol, interval=interval, fetch_extras=fetch_extras)
            
    # Bước 1: Quét khung 1h tìm các coin tiềm năng
    tasks_1h = [analyze_with_sem(symbol, '1h') for symbol in coins_to_scan]
    results_1h = await asyncio.gather(*tasks_1h, return_exceptions=True)
    
    potential_symbols = []
    potential_res_1h = {}
    
    for res in results_1h:
        if isinstance(res, dict) and res.get('signal') in ('LONG', 'SHORT'):
            score = res['long_score'] if res['signal'] == 'LONG' else res['short_score']
            if score >= 4.0:
                potential_symbols.append(res['symbol'])
                potential_res_1h[res['symbol']] = res
                
    # Bước 2: Quét thêm khung 4h + 1d của các coin tiềm năng để xác nhận xu hướng
    results_4h_map = {}
    results_1d_map = {}
    if potential_symbols:
        logger.info(f"Phát hiện {len(potential_symbols)} coin tiềm năng. Tiến hành check xu hướng khung 4h/1d: {potential_symbols}")
        tasks_4h = [analyze_with_sem(symbol, '4h', fetch_extras=False) for symbol in potential_symbols]
        tasks_1d = [analyze_with_sem(symbol, '1d', fetch_extras=False) for symbol in potential_symbols]
        results_4h = await asyncio.gather(*tasks_4h, return_exceptions=True)
        results_1d = await asyncio.gather(*tasks_1d, return_exceptions=True)
        for symbol, res_4h in zip(potential_symbols, results_4h):
            if isinstance(res_4h, dict):
                results_4h_map[symbol] = res_4h
        for symbol, res_1d in zip(potential_symbols, results_1d):
            if isinstance(res_1d, dict):
                results_1d_map[symbol] = res_1d
                
    # Bước 3: Áp dụng bộ lọc đa khung thời gian (MTF Confluence: 1h + 4h + 1d)
    candidates = []
    
    for symbol, res_1h in potential_res_1h.items():
        res_4h = results_4h_map.get(symbol)
        res_1d = results_1d_map.get(symbol)
        mtf_pass = True
        
        if res_4h:
            close_4h = res_4h['close']
            ema9_4h = res_4h['ema9']
            ema21_4h = res_4h['ema21']
            ema50_4h = res_4h['ema50']
            
            is_4h_downtrend_strong = (close_4h < ema9_4h < ema21_4h < ema50_4h) or (close_4h < ema50_4h and res_4h['short_score'] >= 4.0)
            is_4h_uptrend_strong = (close_4h > ema9_4h > ema21_4h > ema50_4h) or (close_4h > ema50_4h and res_4h['long_score'] >= 4.0)
            
            if res_1h['signal'] == 'LONG':
                if is_4h_downtrend_strong:
                    mtf_pass = False
                    logger.info(f"Lọc bỏ tín hiệu LONG của {symbol} do khung 4h đang Downtrend mạnh.")
            elif res_1h['signal'] == 'SHORT':
                if is_4h_uptrend_strong:
                    mtf_pass = False
                    logger.info(f"Lọc bỏ tín hiệu SHORT của {symbol} do khung 4h đang Uptrend mạnh.")
        
        # Lọc khung 1d: tín hiệu đi ngược xu hướng ngày bị loại
        if mtf_pass and res_1d:
            close_1d = res_1d['close']
            is_1d_downtrend_strong = (close_1d < res_1d['ema9'] < res_1d['ema21'] < res_1d['ema50']) or (close_1d < res_1d['ema50'] and res_1d['short_score'] >= 4.0)
            is_1d_uptrend_strong = (close_1d > res_1d['ema9'] > res_1d['ema21'] > res_1d['ema50']) or (close_1d > res_1d['ema50'] and res_1d['long_score'] >= 4.0)
            
            if res_1h['signal'] == 'LONG' and is_1d_downtrend_strong:
                mtf_pass = False
                logger.info(f"Lọc bỏ tín hiệu LONG của {symbol} do khung 1d đang Downtrend mạnh.")
            elif res_1h['signal'] == 'SHORT' and is_1d_uptrend_strong:
                mtf_pass = False
                logger.info(f"Lọc bỏ tín hiệu SHORT của {symbol} do khung 1d đang Uptrend mạnh.")
        
        if mtf_pass:
            res_1h['res_4h'] = res_4h
            res_1h['res_1d'] = res_1d
            candidates.append(res_1h)
            
    # Bước 4: Lọc xu hướng BTC (tín hiệu alt đi ngược BTC 4h mạnh bị trừ điểm nặng)
    btc_res_4h = await get_btc_filter(session, '4h')
    survivors = []
    for res in candidates:
        if res['symbol'] != 'BTCUSDT':
            apply_btc_penalty(res, btc_res_4h)
        if res['signal'] in ('LONG', 'SHORT') and res['confidence'] in ('Mạnh', 'Rất mạnh') and band_winrate_ok(res['confidence']):
            survivors.append(res)
            
    # Sắp xếp tín hiệu theo điểm số từ cao xuống thấp
    long_signals = sorted([r for r in survivors if r['signal'] == 'LONG'], key=lambda x: x['long_score'], reverse=True)
    short_signals = sorted([r for r in survivors if r['signal'] == 'SHORT'], key=lambda x: x['short_score'], reverse=True)
    
    # Bước 5: Gate chặt bằng AI — chỉ giữ tín hiệu AI cùng chiều (top 10 ứng viên tốt nhất)
    ai_enabled = bool(os.getenv("DASH_TOKEN"))
    if ai_enabled and (long_signals or short_signals):
        top_candidates = []
        for r in long_signals[:5]:
            top_candidates.append(r)
        for r in short_signals[:5]:
            top_candidates.append(r)
        top_candidates = top_candidates[:10]
        
        ai_sem = asyncio.Semaphore(4)
        
        async def ai_gate(res):
            async with ai_sem:
                # Digest phải GIỐNG HỆT với nhánh /a <coin> (đủ 4 khung + funding)
                # để AI cho ra kết luận nhất quán giữa quét và phân tích chi tiết
                res_15m = await analyze_with_sem(res['symbol'], '15m', fetch_extras=False)
                digest = build_ai_digest(res['symbol'],
                                          [("15m", res_15m), ("1h", res), ("4h", res.get('res_4h')), ("1d", res.get('res_1d'))],
                                          oi_change=res.get('oi_change'), taker_ratio=res.get('taker_ratio'),
                                          funding_rate=res.get('funding_rate'))
                verdict = await get_ai_verdict_cached(session, f"ai_{res['symbol']}", digest)
            res['ai'] = verdict
            return res
        
        gated = await asyncio.gather(*(ai_gate(r) for r in top_candidates), return_exceptions=True)
        
        long_signals = []
        short_signals = []
        for r in gated:
            if isinstance(r, Exception) or not isinstance(r, dict):
                continue
            verdict = r.get('ai')
            # Fallback rule-only nếu AI lỗi/không trả lời; gate chặt khi AI có kết luận
            if verdict is None or verdict.get('direction') == r['signal']:
                if r['signal'] == 'LONG':
                    long_signals.append(r)
                else:
                    short_signals.append(r)
            else:
                logger.info(f"AI gate loại bỏ tín hiệu {r['signal']} của {r['symbol']} (AI: {verdict.get('direction')})")
        long_signals.sort(key=lambda x: x['long_score'], reverse=True)
        short_signals.sort(key=lambda x: x['short_score'], reverse=True)
    
    # Giới hạn lấy tối đa 5 cơ hội tốt nhất cho mỗi chiều để tin nhắn gọn gàng
    return long_signals[:5], short_signals[:5]


async def get_single_funding_rate(session, symbol):
    url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, dict):
                    return float(data.get('lastFundingRate', 0))
    except Exception as e:
        logger.error(f"Lỗi lấy funding rate cho {symbol}: {e}")
    return 0.0


async def handle_tracking_command(session, chat_id, coin_name):
    """Bắt đầu theo dõi biến động giá của coin. Gửi thông báo khi biến động >= 5% so với giá tham chiếu trước đó."""
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    # Lấy giá hiện tại làm giá tham chiếu ban đầu
    url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}"
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                await send_telegram_message(session, chat_id, f"❌ Không tìm thấy coin *{symbol}*. Vui lòng kiểm tra lại tên coin.")
                return
            data = await resp.json()
            current_price = float(data.get('price', 0))
            if current_price <= 0:
                await send_telegram_message(session, chat_id, f"❌ Không lấy được giá cho *{symbol}*.")
                return
    except Exception as e:
        await send_telegram_message(session, chat_id, f"❌ Lỗi khi lấy giá *{symbol}*: {e}")
        return
    
    if symbol in tracking_coins:
        tracking_coins[symbol]['chat_ids'].add(chat_id)
    else:
        tracking_coins[symbol] = {
            'ref_price': current_price,
            'chat_ids': {chat_id}
        }
    save_tracking_coins()
    
    display = display_symbol(symbol)
    await send_telegram_message(
        session, chat_id,
        f"🔔 *BẮT ĐẦU THEO DÕI: {display}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💵 Giá tham chiếu: `{format_price(current_price)} USDT`\n"
        f"📊 Sẽ thông báo khi biến động ≥ *5%* so với giá tham chiếu\n"
        f"❌ Hủy theo dõi: `/ct {display.lower()}`"
    )


async def handle_cancel_tracking_command(session, chat_id, coin_name=None):
    """Hủy theo dõi coin."""
    if coin_name:
        coin_name = coin_name.upper()
        symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
        
        if symbol in tracking_coins and chat_id in tracking_coins[symbol]['chat_ids']:
            tracking_coins[symbol]['chat_ids'].discard(chat_id)
            if not tracking_coins[symbol]['chat_ids']:
                del tracking_coins[symbol]
            save_tracking_coins()
            display = display_symbol(symbol)
            await send_telegram_message(session, chat_id, f"✅ Đã hủy theo dõi *{display}*.")
        else:
            await send_telegram_message(session, chat_id, f"❌ Bạn chưa theo dõi coin *{symbol}*.")
    else:
        # Hủy tất cả coin đang tracking cho chat_id này
        removed = []
        for symbol in list(tracking_coins.keys()):
            if chat_id in tracking_coins[symbol]['chat_ids']:
                tracking_coins[symbol]['chat_ids'].discard(chat_id)
                removed.append(display_symbol(symbol))
                if not tracking_coins[symbol]['chat_ids']:
                    del tracking_coins[symbol]
        
        if removed:
            save_tracking_coins()
            await send_telegram_message(session, chat_id, f"✅ Đã hủy theo dõi: *{', '.join(removed)}*")
        else:
            await send_telegram_message(session, chat_id, "❌ Bạn chưa theo dõi coin nào.")


async def tracking_price_loop(app):
    """Background loop kiểm tra giá các coin đang tracking mỗi 30 giây."""
    await asyncio.sleep(5)  # Chờ khởi tạo
    while True:
        try:
            if not tracking_coins:
                await asyncio.sleep(15)
                continue
            
            session = app['session']
            symbols_to_check = list(tracking_coins.keys())
            
            for symbol in symbols_to_check:
                if symbol not in tracking_coins:
                    continue
                    
                url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}"
                try:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json()
                        current_price = float(data.get('price', 0))
                        if current_price <= 0:
                            continue
                except Exception:
                    continue
                
                ref_price = tracking_coins[symbol]['ref_price']
                change_pct = ((current_price - ref_price) / ref_price) * 100
                
                if abs(change_pct) >= 5.0:
                    # Cập nhật giá tham chiếu mới
                    tracking_coins[symbol]['ref_price'] = current_price
                    save_tracking_coins()
                    
                    display = display_symbol(symbol)
                    direction = "📈 TĂNG" if change_pct > 0 else "📉 GIẢM"
                    emoji = "🟢" if change_pct > 0 else "🔴"
                    
                    msg = (
                        f"🔔 *CẢNH BÁO BIẾN ĐỘNG: {display}*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"{direction} {emoji} *{change_pct:+.2f}%*\n"
                        f"💵 Giá hiện tại: `{format_price(current_price)} USDT`\n"
                        f"📊 Giá tham chiếu cũ: `{format_price(ref_price)} USDT`\n"
                        f"🔄 Giá tham chiếu mới: `{format_price(current_price)} USDT`"
                    )
                    
                    chat_ids = list(tracking_coins.get(symbol, {}).get('chat_ids', set()))
                    send_results = await asyncio.gather(
                        *[send_telegram_message(session, cid, msg) for cid in chat_ids],
                        return_exceptions=True
                    )
                    for cid, send_err in zip(chat_ids, send_results):
                        if isinstance(send_err, Exception):
                            logger.error(f"Lỗi gửi tracking alert đến {cid}: {send_err}")
            
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Lỗi trong tracking_price_loop: {e}")
            await asyncio.sleep(30)

def format_scan_item(i, res, direction):
    """Dựng 1 khối tín hiệu quét thị trường — thiết kế gọn để xem trên điện thoại."""
    coin = display_symbol(res['symbol'])
    conf = CONF_MAP.get(res['confidence'], '⭐')
    score = res['long_score'] if direction == 'LONG' else res['short_score']
    tp_change = ((res['tp'] - res['close']) / res['close']) * 100
    sl_change = ((res['sl'] - res['close']) / res['close']) * 100
    ai_tag = " 🤖" if res.get('ai') else ""
    return (
        f"{i}. *{coin}* {conf} S:`{score:.1f}` @ `{format_price(res['close'])}`{ai_tag}\n"
        f"   TP `{format_price(res['tp'])}` ({tp_change:+.1f}%)\n"
        f"   SL `{format_price(res['sl'])}` ({sl_change:+.1f}%)"
    )


async def _send_scan_results(session, chat_id, long_signals, short_signals, cache_age=0):
    ai_enabled = bool(os.getenv("DASH_TOKEN"))
    filter_desc = "MTF · BTC · Win-rate · AI 🤖" if ai_enabled else "MTF · BTC · Win-rate"
    msg_lines = [
        "🔍 *QUÉT CƠ HỘI (1h)*",
        f"🧭 Lọc: {filter_desc}",
        ""
    ]
    
    has_signals = False
    
    if long_signals:
        has_signals = True
        msg_lines.append("🚀 *CƠ HỘI LONG:*")
        for i, res in enumerate(long_signals, 1):
            msg_lines.append(format_scan_item(i, res, 'LONG'))
        msg_lines.append("")
        
    if short_signals:
        has_signals = True
        msg_lines.append("📉 *CƠ HỘI SHORT:*")
        for i, res in enumerate(short_signals, 1):
            msg_lines.append(format_scan_item(i, res, 'SHORT'))
            
    if not has_signals:
        msg_lines.append("⬜ *Chưa có tín hiệu 4-5 sao nào.*")
        msg_lines.append("_Thị trường sideway hoặc chưa rõ hướng — nên kiên nhẫn quan sát thêm._")
        
    msg_lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    stats_line = format_signal_stats()
    if stats_line:
        msg_lines.append(stats_line)
    if cache_age > 0:
        msg_lines.append(f"🕒 _Cập nhật cách đây {cache_age}s (cache 5p)_")
    else:
        msg_lines.append("🕒 _Quét trực tiếp thời gian thực_")
    msg_lines.append("💡 Chi tiết: `/a <coin>`")
    
    await send_telegram_message(session, chat_id, "\n".join(msg_lines))


async def handle_analyze_command(session, chat_id, coin_name=None):
    """
    Xử lý câu lệnh phân tích kỹ thuật và quét tín hiệu.
    Chỉ trả về các tín hiệu có độ tin cậy từ 4 sao trở lên (Mạnh và Rất mạnh).
    """
    if coin_name:
        coin_name = coin_name.upper()
        symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
        
        loading_msg_id = await send_telegram_message(
            session,
            chat_id,
            f"⏳ Đang phân tích kỹ thuật đa khung thời gian cho *{symbol}*..."
        )
        
        try:
            # Phân tích đa khung thời gian (MTF): 15m, 1h, 4h, 1d + dữ liệu BTC để lọc xu hướng
            res_15m_task = asyncio.create_task(analyze_market(session, symbol, interval='15m', fetch_extras=False))
            res_1h_task = asyncio.create_task(analyze_market(session, symbol, interval='1h'))
            res_4h_task = asyncio.create_task(analyze_market(session, symbol, interval='4h', fetch_extras=False))
            res_1d_task = asyncio.create_task(analyze_market(session, symbol, interval='1d', fetch_extras=False))
            btc_task = asyncio.create_task(get_btc_filter(session, '4h'))
            
            res_15m = await res_15m_task
            res = await res_1h_task  # Khung chính
            res_4h = await res_4h_task
            res_1d = await res_1d_task
            btc_res = await btc_task
            
            if loading_msg_id:
                await delete_telegram_message(session, chat_id, loading_msg_id)
                
            if not res:
                await send_telegram_message(
                    session,
                    chat_id,
                    f"❌ Không thể lấy dữ liệu phân tích cho *{symbol}*. Vui lòng kiểm tra lại tên coin."
                )
                return
            
            # Lọc xu hướng BTC: tín hiệu ngược BTC 4h mạnh bị trừ điểm nặng
            apply_btc_penalty(res, btc_res)
                
            price_str = format_price(res['close'])
            funding_rate = res.get('funding_rate')
            if funding_rate is None:
                funding_rate = await get_single_funding_rate(session, symbol)
            funding_line = f"⏳ Funding Rate: `{funding_rate * 100:+.4f}%`\n" if abs(funding_rate) >= 0.005 else ""
            oi_change = res.get('oi_change')
            taker_ratio = res.get('taker_ratio')
            
            # Mô tả chỉ báo
            rsi_str = f"{res['rsi']:.1f}"
            rsi_desc = "Quá bán 🟢" if res['rsi'] <= 30 else ("Quá mua 🔴" if res['rsi'] >= 70 else "Trung tính")
            
            stoch_str = f"K:{res['stoch_k']:.1f} D:{res['stoch_d']:.1f}"
            stoch_desc = "Quá bán 🟢" if res['stoch_k'] <= 20 else ("Quá mua 🔴" if res['stoch_k'] >= 80 else "Trung tính")
            
            if res['close'] > res['ema9'] > res['ema21'] > res['ema50']:
                ema_desc = "Uptrend 🟢"
            elif res['close'] > res['ema9'] > res['ema21']:
                ema_desc = "Tăng nhẹ"
            elif res['close'] < res['ema9'] < res['ema21'] < res['ema50']:
                ema_desc = "Downtrend 🔴"
            elif res['close'] < res['ema9'] < res['ema21']:
                ema_desc = "Giảm nhẹ"
            else:
                ema_desc = "Sideway"
            
            bb_pct_str = f"{res['bb_pct'] * 100:.0f}%"
            bb_desc = "Chạm biên dưới 🟢" if res['bb_pct'] <= 0.05 else ("Chạm biên trên 🔴" if res['bb_pct'] >= 0.95 else "Trung tính")
            
            macd_hist_str = f"{res['hist']:+,.4f}".rstrip('0').rstrip('.')
            macd_desc = "Bullish" if res['hist'] > 0 else "Bearish"
            
            adx_str = f"{res['adx']:.1f}"
            adx_desc = "Trending mạnh 🔥" if res['adx'] >= 40 else ("Trending" if res['adx'] >= 25 else "Sideway ⚠️")
            
            atr_str = format_price(res['atr'])
            atr_pct = (res['atr'] / res['close']) * 100
            
            vol_desc = "Rất cao 🔥" if res['vol_ratio'] >= 2.0 else ("Cao" if res['vol_ratio'] >= 1.3 else ("Thấp ⚠️" if res['vol_ratio'] < 0.5 else "Bình thường"))
            
            extras_lines = ""
            if oi_change is not None:
                extras_lines += f"• *Open Interest 24h:* `{oi_change:+.1f}%`\n"
            if taker_ratio is not None:
                extras_lines += f"• *Taker Buy/Sell:* `{taker_ratio:.2f}`\n"
            if res.get('rsi_div'):
                div_desc = "Bullish 🟢 (tín hiệu đảo chiều tăng)" if res['rsi_div'] == 'bullish' else "Bearish 🔴 (tín hiệu đảo chiều giảm)"
                extras_lines += f"• *RSI Divergence:* _{div_desc}_\n"
            if res.get('pattern'):
                pattern_names = {'bullish_engulfing': 'Bullish Engulfing 🟢', 'bearish_engulfing': 'Bearish Engulfing 🔴',
                                 'hammer': 'Hammer 🟢', 'shooting_star': 'Shooting Star 🔴'}
                extras_lines += f"• *Pattern nến:* _{pattern_names.get(res['pattern'], res['pattern'])}_\n"
            if res.get('btc_penalty'):
                extras_lines += "• ⚠️ _Bị trừ điểm do đi ngược xu hướng BTC 4h mạnh_\n"
            
            sig_emoji = "🟩 LONG" if res['signal'] == 'LONG' else ("🟥 SHORT" if res['signal'] == 'SHORT' else "⬜ NEUTRAL")
            conf_icon = CONF_MAP.get(res['confidence'], '⭐')
            
            msg = (
                f"📊 *PHÂN TÍCH KỸ THUẬT: {symbol}*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💵 Giá hiện tại: `{price_str} USDT`\n"
                f"{funding_line}"
                f"\n🔍 *Chỉ báo chính (1h):*\n"
                f"• *RSI (14):* `{rsi_str}` ➜ _{rsi_desc}_\n"
                f"• *Stoch RSI:* `{stoch_str}` ➜ _{stoch_desc}_\n"
                f"• *EMA (9/21/50/200):* _{ema_desc}_\n"
                f"• *Bollinger:* `{bb_pct_str}` ➜ _{bb_desc}_\n"
                f"• *MACD Hist:* `{macd_hist_str}` ➜ _{macd_desc}_\n"
                f"• *ADX:* `{adx_str}` ➜ _{adx_desc}_\n"
                f"• *ATR:* `{atr_str}` ({atr_pct:.2f}%)\n"
                f"• *Volume:* `{res['vol_ratio']:.1f}x` trung bình ➜ _{vol_desc}_\n"
                f"• *S/R:* Support `{format_price(res['support'])}` | Resistance `{format_price(res['resistance'])}`\n"
            )
            if extras_lines:
                msg += f"\n📡 *Dữ liệu bổ trợ:*\n{extras_lines}"
            
            # MTF Summary
            msg += "\n⏱ *Phân tích đa khung (MTF):*\n"
            for tf_name, tf_res in [("15m", res_15m), ("1h", res), ("4h", res_4h), ("1d", res_1d)]:
                if tf_res:
                    tf_sig = "🟩L" if tf_res['signal'] == 'LONG' else ("🟥S" if tf_res['signal'] == 'SHORT' else "⬜N")
                    tf_conf_star = CONF_MAP.get(tf_res['confidence'], '⭐')
                    msg += f"• *{tf_name}:* {tf_sig} ({tf_conf_star}) | L:`{tf_res['long_score']:.1f}` S:`{tf_res['short_score']:.1f}`\n"
                else:
                    msg += f"• *{tf_name}:* ❌ Không có dữ liệu\n"
            
            # Kết luận
            msg += (
                f"\n🎯 *KẾT LUẬN (Khung 1h):*\n"
                f"👉 Khuyến nghị: *{sig_emoji}*\n"
                f"🔥 Độ tin cậy: {conf_icon} (L:`{res['long_score']:.1f}` | S:`{res['short_score']:.1f}`)\n"
            )
            
            # AI phân tích độc lập (nếu cấu hình DASH_TOKEN)
            ai_verdict = None
            if os.getenv("DASH_TOKEN"):
                digest = build_ai_digest(symbol, [("15m", res_15m), ("1h", res), ("4h", res_4h), ("1d", res_1d)],
                                          oi_change=oi_change, taker_ratio=taker_ratio, funding_rate=funding_rate)
                ai_verdict = await get_ai_verdict_cached(session, f"ai_{symbol}", digest)
                if ai_verdict:
                    ai_dir = ai_verdict.get('direction', 'NEUTRAL')
                    ai_emoji = "🟩 LONG" if ai_dir == 'LONG' else ("🟥 SHORT" if ai_dir == 'SHORT' else "⬜ NEUTRAL")
                    msg += (
                        f"\n🤖 *AI phân tích:*\n"
                        f"👉 AI khuyến nghị: *{ai_emoji}* _({ai_verdict.get('confidence', 'trung bình')})_\n"
                        f"💬 _{ai_verdict.get('reason', '')}_\n"
                    )
                    if res['signal'] != 'NEUTRAL' and ai_dir != 'NEUTRAL' and ai_dir != res['signal']:
                        msg += "⚠️ _AI mâu thuẫn với rule engine — cân nhắc bỏ qua tín hiệu này._\n"
                else:
                    msg += "\n🤖 _AI không phản hồi hoặc lỗi — kết luận dựa trên rule engine._\n"
            
            # Adaptive gate: cảnh báo nếu nhóm tín hiệu này đang có win-rate kém
            if res['signal'] != 'NEUTRAL' and not band_winrate_ok(res['confidence']):
                msg += "\n⛔ *CẢNH BÁO:* Nhóm tín hiệu này đang có win-rate thực tế dưới 50% — hệ thống khuyến nghị KHÔNG vào lệnh.\n"
            
            stats_line = format_signal_stats()
            if stats_line:
                msg += f"\n{stats_line}\n"
            
            if res['signal'] != 'NEUTRAL' and res['confidence'] in ('Mạnh', 'Rất mạnh') and band_winrate_ok(res['confidence']):
                record_signal(res, ai_verdict)
            if res['signal'] != 'NEUTRAL' and res['confidence'] in ('Mạnh', 'Rất mạnh'):
                tp_str = format_price(res['tp'])
                sl_str = format_price(res['sl'])
                tp_change = ((res['tp'] - res['close']) / res['close']) * 100
                sl_change = ((res['sl'] - res['close']) / res['close']) * 100
                risk = abs(res['close'] - res['sl'])
                reward = abs(res['tp'] - res['close'])
                rr = reward / (risk + 1e-10)
                msg += (
                    f"\n🛡️ *Kế hoạch giao dịch gợi ý:*\n"
                    f"• *Entry:* quanh `{price_str} USDT`\n"
                    f"• *Target TP:* `{tp_str} USDT` ({tp_change:+.2f}%)\n"
                    f"• *Stop Loss:* `{sl_str} USDT` ({sl_change:+.2f}%)\n"
                    f"• *Risk:Reward =* `1:{rr:.1f}`"
                )
            else:
                msg += "\n💡 *Gợi ý:* Tín hiệu chưa đủ mạnh (dưới 4 sao) hoặc thị trường chưa có xu hướng rõ ràng. Nên kiên nhẫn đứng ngoài quan sát thêm."
                
            await send_telegram_message(session, chat_id, msg)
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý lệnh analyze cho {symbol}: {e}")
            if loading_msg_id:
                await delete_telegram_message(session, chat_id, loading_msg_id)
            await send_telegram_message(session, chat_id, f"❌ Đã xảy ra lỗi khi phân tích: {e}")
    else:
        now = time.time()
        # 1. Kiểm tra cache trước
        if market_scan_cache["signals"] is not None and now - market_scan_cache["timestamp"] < 300:
            long_signals, short_signals = market_scan_cache["signals"]
            cache_age = int(now - market_scan_cache["timestamp"])
            await _send_scan_results(session, chat_id, long_signals, short_signals, cache_age)
            return

        # 2. Cache hết hạn hoặc trống, tiến hành quét mới
        loading_msg_id = await send_telegram_message(
            session,
            chat_id,
            "🔍 Đang quét thị trường tìm cơ hội giao dịch tỉ lệ thắng cao (chỉ hiển thị tín hiệu 4-5 sao)..."
        )
        
        try:
            # Dùng lock để tránh việc nhiều request cùng quét đồng thời
            async with market_scan_cache["lock"]:
                # Kiểm tra lại một lần nữa phòng trường hợp task khác vừa quét xong trong khi chờ lock
                now_check = time.time()
                if market_scan_cache["signals"] is not None and now_check - market_scan_cache["timestamp"] < 300:
                    long_signals, short_signals = market_scan_cache["signals"]
                    cache_age = int(now_check - market_scan_cache["timestamp"])
                else:
                    long_signals, short_signals = await scan_market_signals(session)
                    market_scan_cache["signals"] = (long_signals, short_signals)
                    market_scan_cache["timestamp"] = time.time()
                    cache_age = 0
                    # Lưu tín hiệu quét mới vào lịch sử để theo dõi win-rate
                    for sig_res in list(long_signals) + list(short_signals):
                        record_signal(sig_res, sig_res.get('ai'))
            
            if loading_msg_id:
                await delete_telegram_message(session, chat_id, loading_msg_id)
                
            await _send_scan_results(session, chat_id, long_signals, short_signals, cache_age)
            
        except Exception as e:
            logger.error(f"Lỗi khi quét tín hiệu: {e}")
            if loading_msg_id:
                await delete_telegram_message(session, chat_id, loading_msg_id)
            await send_telegram_message(session, chat_id, f"❌ Lỗi khi quét tín hiệu thị trường: {e}")


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


async def place_algo_tpsl(session, api_key, api_secret, symbol, order_side, order_type, trigger_price, pos_side=None, quantity=None, close_position=False):
    """
    Đặt một lệnh Algo TP/SL (TAKE_PROFIT_MARKET / STOP_MARKET) trên Binance.
    Trả về (thành_công, order_id hoặc thông báo lỗi).
    """
    timestamp = int(time.time() * 1000)
    params = [
        f"symbol={symbol}",
        f"side={order_side}",
        f"type={order_type}",
        f"triggerPrice={trigger_price}",
        "algoType=CONDITIONAL",
        f"timestamp={timestamp}"
    ]
    if quantity is not None:
        params.append(f"quantity={quantity}")
        params.append("reduceOnly=true")
    elif close_position:
        params.append("closePosition=true")
    if pos_side and pos_side != 'BOTH':
        params.append(f"positionSide={pos_side}")
        
    query_string = "&".join(params)
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v1/algoOrder?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    try:
        async with session.post(url, headers=headers) as resp:
            data = await resp.json()
            if resp.status == 200:
                return True, data.get('orderId') or data.get('algoId')
            return False, data.get('msg', 'Lỗi không xác định')
    except Exception as e:
        logger.error(f"Lỗi khi đặt lệnh {order_type} cho {symbol}: {e}")
        return False, str(e)


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
                avg_price = 0.0
                execute_qty = quantity
                if not is_limit:
                    avg_price = float(data.get('avgPrice', 0))
                    if avg_price == 0:
                        cum_quote = float(data.get('cumQuote', 0))
                        executed_qty = float(data.get('executedQty', 0)) or float(data.get('cumQty', 0))
                        if executed_qty > 0:
                            avg_price = cum_quote / executed_qty
                    execute_qty = float(data.get('executedQty', 0))
                
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
                    tp_ok, tp_val = await place_algo_tpsl(
                        session, api_key, api_secret, symbol,
                        order_side=tpsl_side, order_type="TAKE_PROFIT_MARKET",
                        trigger_price=final_tp_price, pos_side=pos_side,
                        quantity=(quantity if is_limit else None),
                        close_position=(not is_limit)
                    )
                    if tp_ok:
                        tp_sl_msg_parts.append(f"🎯 *TP:* Chốt lời ở giá *{final_tp_price:,.4f}* (Thành công, ID: `{tp_val}`)")
                    else:
                        tp_sl_msg_parts.append(f"❌ *Lỗi đặt TP:* `{tp_val}`")

                # Cài đặt SL nếu có
                if final_sl_price is not None:
                    sl_ok, sl_val = await place_algo_tpsl(
                        session, api_key, api_secret, symbol,
                        order_side=tpsl_side, order_type="STOP_MARKET",
                        trigger_price=final_sl_price, pos_side=pos_side,
                        quantity=(quantity if is_limit else None),
                        close_position=(not is_limit)
                    )
                    if sl_ok:
                        tp_sl_msg_parts.append(f"🛡️ *SL:* Cắt lỗ ở giá *{final_sl_price:,.4f}* (Thành công, ID: `{sl_val}`)")
                    else:
                        tp_sl_msg_parts.append(f"❌ *Lỗi đặt SL:* `{sl_val}`")

                if tp_sl_msg_parts:
                    msg = "\n".join(tp_sl_msg_parts)
                    
                    if any("GTE" in r or "closePosition" in r for r in tp_sl_msg_parts):
                        msg += GTE_WARNING
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
        # 1. Lấy giá coin hiện tại và funding rate toàn sàn (có cache 30s)
        prices_map = {}
        funding_map = {}
        try:
            tickers_map, funding_map = await get_market_snapshot(session)
            prices_map = {sym: info['price'] for sym, info in tickers_map.items()}
        except Exception as e:
            logger.error(f"Lỗi lấy thông tin thị trường khi xem orders: {e}")

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
                    
                    sym_display = display_symbol(symbol)
                    
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
                        
                    funding_rate = funding_map.get(symbol, 0.0)
                    funding_str = f"   • Funding Rate: *{funding_rate * 100:+.4f}%*\n" if abs(funding_rate) >= 0.005 else ""
                    
                    lines.append(
                        f"{i}. {sym_display} ({emoji} *{display_side} - {order_type}*)\n"
                        f"{price_line}"
                        f"{funding_str}"
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
            tp_ok, tp_val = await place_algo_tpsl(
                session, api_key, api_secret, symbol,
                order_side=order_side, order_type="TAKE_PROFIT_MARKET",
                trigger_price=final_tp_price, pos_side=side,
                close_position=True
            )
            if tp_ok:
                results.append(f"   • TP (*{pos_display}* tại giá *{format_price(final_tp_price)}*): 🟢 Thành công (ID: `{tp_val}`)")
            else:
                results.append(f"   • TP (*{pos_display}* tại giá *{format_price(final_tp_price)}*): 🔴 Thất bại: `{tp_val}`")
                
        if final_sl_price is not None:
            sl_ok, sl_val = await place_algo_tpsl(
                session, api_key, api_secret, symbol,
                order_side=order_side, order_type="STOP_MARKET",
                trigger_price=final_sl_price, pos_side=side,
                close_position=True
            )
            if sl_ok:
                results.append(f"   • SL (*{pos_display}* tại giá *{format_price(final_sl_price)}*): 🟢 Thành công (ID: `{sl_val}`)")
            else:
                results.append(f"   • SL (*{pos_display}* tại giá *{format_price(final_sl_price)}*): 🔴 Thất bại: `{sl_val}`")
                
    msg = (
        f"🎯 *KẾT QUẢ CÀI ĐẶT TP/SL CHO {symbol}*\n"
        f"----------------------------------\n" +
        "\n".join(results)
    )
    
    if any("GTE" in r or "closePosition" in r for r in results):
        msg += GTE_WARNING
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
                    logger.info(f"Đặt lệnh DCA Limit thành công cho {symbol}: orderId={order_id}")
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


async def handle_close_command(session, chat_id, coin_name, side_str=None):
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    
    # 1. Tìm các vị thế đang mở khớp với symbol
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
        
    # 2. Lọc vị thế theo side nếu người dùng có nhập thêm side (long/short)
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
            actual_side = pos_side_display(pos['positionSide'], pos['positionAmt'])
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
        # Chỉ có 1 vị thế
        if side_str:
            side_upper = side_str.upper()
            pos = matched_positions[0]
            actual_side = pos_side_display(pos['positionSide'], pos['positionAmt'])
            if actual_side != side_upper:
                await send_telegram_message(
                    session,
                    chat_id,
                    f"❌ Vị thế đang mở của *{symbol}* là *{actual_side}*, không phải *{side_upper}*."
                )
                return
        target_pos = matched_positions[0]
        
    # 3. Tiến hành đóng vị thế bằng lệnh MARKET ngược chiều
    pos_side = target_pos['positionSide']
    amt = target_pos['positionAmt']
    abs_amt = abs(amt)
    
    if abs_amt <= 0:
        await send_telegram_message(
            session,
            chat_id,
            f"❌ Kích thước vị thế của *{symbol}* bằng 0 hoặc không hợp lệ."
        )
        return
        
    is_long = (pos_side == 'LONG' or (pos_side == 'BOTH' and amt > 0))
    side = 'SELL' if is_long else 'BUY'
    
    timestamp = int(time.time() * 1000)
    params = [
        f"symbol={symbol}",
        f"side={side}",
        "type=MARKET",
        f"quantity={abs_amt}",
        f"timestamp={timestamp}"
    ]
    
    if pos_side != 'BOTH':
        params.append(f"positionSide={pos_side}")
    else:
        params.append("reduceOnly=true")
        
    query_string = "&".join(params)
    signature = get_binance_signature(query_string, api_secret)
    url = f"https://fapi.binance.com/fapi/v1/order?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    
    try:
        async with session.post(url, headers=headers) as resp:
            data = await resp.json()
            if resp.status == 200:
                logger.info(f"Đã gửi lệnh đóng vị thế thành công cho {symbol}")
                
                # Tự động hủy toàn bộ các lệnh DCA đang mở của symbol đó
                await cancel_dca_orders(session, api_key, api_secret, symbol)
            else:
                msg_err = data.get('msg', 'Lỗi không xác định')
                code_err = data.get('code', -1)
                await send_telegram_message(
                    session,
                    chat_id,
                    f"❌ *Đóng vị thế thất bại!*\nBinance báo lỗi: `{msg_err}` (Code: {code_err})"
                )
    except Exception as e:
        logger.error(f"Lỗi khi gửi lệnh đóng vị thế cho {symbol}: {e}")
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
        
    # Xóa tin nhắn của user nếu là command hoặc tin nhắn tra cứu giá coin được hỗ trợ
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
            '/analyze', '/a', '/history', '/lichsu', '/his', '/liq',
            '/tracking', '/t', '/ct', '/canceltracking'
        }
        if command_base in supported_commands:
            should_delete = True
            
    if should_delete:
        message_id = message.get('message_id')
        if message_id:
            asyncio.create_task(delete_telegram_message(request.app['session'], chat_id, message_id))
            
    # Xử lý lệnh ở nền để trả 200 ngay, tránh Telegram timeout rồi gửi lại webhook gây trùng lặp
    async def run_command():
        try:
            await process_telegram_message(request, chat_id, text)
        except Exception as e:
            logger.error(f"Lỗi xử lý tin nhắn từ {chat_id}: {e}")
            
    asyncio.create_task(run_command())
    return web.Response(status=200)


# Xử lý nội dung tin nhắn/lệnh Telegram (chạy nền sau khi webhook đã phản hồi 200)
async def process_telegram_message(request, chat_id, text):
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
                    funding = info.get('funding_rate', 0.0)
                    formatted = format_price(price)
                    emoji = "🟢" if change >= 0 else "🔴"
                    sign = "+" if change >= 0 else ""
                    funding_str = f" [FR: {funding * 100:+.4f}%]" if abs(funding) >= 0.005 else ""
                    response_lines.append(f"{coin_name.upper()}: {formatted} ({emoji} {sign}{change:.2f}%){funding_str}")
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
            "💀 `/liq` - Xem các vị thế đang mở kèm giá thanh lý chi tiết.\n"
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
            "⏱ `/auto` - Bật/Tắt tự động gửi vị thế mỗi 1 phút.\n"
            "📈 `/analyze [coin]` (hoặc `/a`) - Quét cơ hội giao dịch hoặc phân tích kỹ thuật chi tiết của coin (RSI, EMA, Bollinger, MACD). Chỉ hiển thị tín hiệu 4-5 sao đã qua lọc MTF 1h+4h+1d, xu hướng BTC và win-rate thực tế. Có AI đối chiếu realtime nếu cấu hình DASH_TOKEN.\n"
            "🔔 `/tracking <coin>` (hoặc `/t`) - Theo dõi biến động giá coin tự động mỗi 5%.\n"
            "🔕 `/canceltracking [coin]` (hoặc `/ct`) - Hủy theo dõi một hoặc toàn bộ coin.\n"
            "📜 `/history [coin]` (hoặc `/lichsu`) - Xem lịch sử 10 vị thế đã đóng (Realized PnL) gần nhất.\n\n"
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
                "❌ Sai cú pháp hủy lệnh!\nSử dụng: `/cancel <coin> <order_id>`\nVí dụ: `/cancel btc 1234567`"
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
                "❌ Sai cú pháp đóng vị thế!\nSử dụng: `/close <coin> [long|short]`\nVí dụ: `/close btc` hoặc `/close btc long`"
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
        
    elif command_base in ('/tracking', '/t'):
        parts = text.split()
        if len(parts) < 2:
            await send_telegram_message(
                request.app['session'],
                chat_id,
                "❌ Sai cú pháp!\nSử dụng: `/t <coin>`\nVí dụ: `/t btc`"
            )
        else:
            coin_name = parts[1]
            await handle_tracking_command(request.app['session'], chat_id, coin_name)
    
    elif command_base in ('/ct', '/canceltracking'):
        parts = text.split()
        coin_name = parts[1] if len(parts) > 1 else None
        await handle_cancel_tracking_command(request.app['session'], chat_id, coin_name)
        
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


# Request giả lập cho chế độ polling (process_telegram_message cần request.app['session'])
class FakeRequest:
    def __init__(self, app, data):
        self.app = app
        self._data = data

    async def json(self):
        return self._data

# Long polling Telegram: chủ động lấy update từ Telegram, không cần webhook/IP public
async def telegram_polling_loop(app):
    session = app['session']
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    base_url = f"https://api.telegram.org/bot{token}"

    # Gỡ webhook cũ (nếu có) vì getUpdates xung đột với webhook
    try:
        async with session.post(f"{base_url}/deleteWebhook") as resp:
            data = await resp.json()
            if data.get('ok'):
                logger.info("Đã gỡ webhook cũ, chuyển sang chế độ long polling.")
    except Exception as e:
        logger.warning(f"Không gỡ được webhook cũ: {e}")

    offset = 0
    logger.info("Bắt đầu long polling Telegram updates...")
    while True:
        try:
            params = {"offset": offset, "timeout": 50}
            async with session.post(f"{base_url}/getUpdates", json=params) as resp:
                data = await resp.json()

            if not data.get('ok'):
                logger.error(f"getUpdates trả về lỗi: {data}")
                await asyncio.sleep(5)
                continue

            for update in data.get('result', []):
                offset = update['update_id'] + 1
                if 'message' not in update:
                    continue
                await telegram_webhook_handler(FakeRequest(app, update))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Lỗi long polling Telegram: {e}")
            await asyncio.sleep(5)

# Lifecycle hooks của aiohttp
async def on_startup(app):
    load_active_chats()
    load_auto_chats()
    load_tracking_coins()
    load_signal_history()
    app['session'] = aiohttp.ClientSession()
    
    # 0. Tự động lấy và log IP của server để cấu hình Binance
    await log_server_ip(app['session'])
    
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    
    # 1. Chạy long polling Telegram (không cần webhook/IP public)
    app['polling_task'] = asyncio.create_task(
        telegram_polling_loop(app)
    )
    
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
    app['tracking_price_task'] = asyncio.create_task(
        tracking_price_loop(app)
    )
    app['signal_track_task'] = asyncio.create_task(
        signal_tracking_loop(app)
    )

async def on_cleanup(app):
    logger.info("Đang giải phóng tài nguyên...")
    if 'polling_task' in app:
        app['polling_task'].cancel()
    if 'user_data_task' in app:
        app['user_data_task'].cancel()
    if 'mark_price_task' in app:
        app['mark_price_task'].cancel()
    if 'auto_pos_task' in app:
        app['auto_pos_task'].cancel()
    if 'tracking_price_task' in app:
        app['tracking_price_task'].cancel()
    if 'signal_track_task' in app:
        app['signal_track_task'].cancel()
        
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
