import asyncio
import math
import hmac
import base64
import hashlib
import time
import os
import random
import re
import json
import csv
import io
import gzip
import zlib
import zipfile
import xml.etree.ElementTree as ET
import socket
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
import logging
import aiohttp
import yarl
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
auto_pnl_chats = set()  # Danh sách chat_id nhận cập nhật TỔNG PNL tự động mỗi phút
last_auto_pnl_messages = {}  # Lưu message_id tin nhắn TỔNG PNL auto cuối cùng (key: chat_id, value: message_id)
auto_price_chats = {}  # chat_id -> danh sách symbol được tự động cập nhật giá mỗi phút
last_auto_price_messages = {}  # Tin nhắn giá tự động cuối cùng theo chat
has_new_activity = {}   # Đánh dấu có hoạt động mới trong chat (key: chat_id, value: bool)
hedge_mode = False      # Chế độ Position Mode (True: Hedge Mode, False: One-way Mode)
symbol_precisions = {}  # Lưu độ chính xác số lượng coin (quantityPrecision) của từng symbol
symbol_price_precisions = {}  # Lưu độ chính xác giá (pricePrecision) của từng symbol
symbol_tick_sizes = {}  # Lưu tickSize của từng symbol
order_realized_pnl = {} # Lưu realized PnL cộng dồn cho từng order_id (tránh lỗi fragmented trades PnL)
ai_active_until = {}    # chat_id -> timestamp: /auto tạm im lặng tới thời điểm này để không chen ngang chat AI
AI_QUIET_SECONDS = 120  # Thời gian im lặng của /auto sau mỗi lượt tương tác AI

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
AUTO_CHATS_FILE = "auto_chats_trading.json"
AUTO_PNL_CHATS_FILE = "auto_pnl_chats_trading.json"
AUTO_PRICE_MAX_SYMBOLS = 10
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
    global auto_chats, last_auto_messages, auto_price_chats, last_auto_price_messages
    try:
        if os.path.exists(AUTO_CHATS_FILE):
            with open(AUTO_CHATS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                auto_chats = set(int(cid) for cid in data.get('chats', []))
                last_auto_messages = {int(cid): mid for cid, mid in data.get('last_messages', {}).items()}
                auto_price_chats = {
                    int(cid): list(symbols)
                    for cid, symbols in data.get('price_symbols', {}).items()
                }
                last_auto_price_messages = {
                    int(cid): mid for cid, mid in data.get('price_last_messages', {}).items()
                }
                # File runtime cũ có thể lưu đồng thời hai chế độ; ưu tiên theo dõi giá.
                auto_chats.difference_update(auto_price_chats)
                for cid in auto_price_chats:
                    last_auto_messages.pop(cid, None)
                logger.info(
                    f"Đã tải {len(auto_chats)} chat tự động cập nhật vị thế và "
                    f"{len(auto_price_chats)} chat tự động cập nhật giá."
                )
    except Exception as e:
        logger.error(f"Lỗi khi tải auto_chats: {e}")

def save_auto_chats():
    try:
        with open(AUTO_CHATS_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "chats": list(auto_chats),
                "last_messages": last_auto_messages,
                "price_symbols": auto_price_chats,
                "price_last_messages": last_auto_price_messages,
            }, f)
    except Exception as e:
        logger.error(f"Lỗi khi lưu auto_chats: {e}")

def load_auto_pnl_chats():
    global auto_pnl_chats, last_auto_pnl_messages
    try:
        if os.path.exists(AUTO_PNL_CHATS_FILE):
            with open(AUTO_PNL_CHATS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            auto_pnl_chats = set(int(cid) for cid in data.get('chats', []))
            last_auto_pnl_messages = {int(cid): mid for cid, mid in data.get('last_messages', {}).items()}
            logger.info(f"Đã tải {len(auto_pnl_chats)} chat auto PnL từ file.")
    except Exception as e:
        logger.error(f"Lỗi khi tải auto_pnl_chats: {e}")

def save_auto_pnl_chats():
    try:
        with open(AUTO_PNL_CHATS_FILE, "w", encoding="utf-8") as f:
            json.dump({"chats": list(auto_pnl_chats), "last_messages": last_auto_pnl_messages}, f)
    except Exception as e:
        logger.error(f"Lỗi khi lưu auto_pnl_chats: {e}")


SIGNAL_HISTORY_FILE = "signal_history_trading.json"
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
        temporary = SIGNAL_HISTORY_FILE + '.tmp'
        with open(temporary, 'w', encoding='utf-8') as f:
            json.dump(signal_history, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, SIGNAL_HISTORY_FILE)
    except Exception as e:
        logger.error(f"Lỗi khi lưu signal_history: {e}")

def prune_signal_history(max_keep=500):
    global signal_history
    cutoff = time.time() - SIGNAL_MAX_AGE_DAYS * 86400
    pending = [s for s in signal_history if s.get('status') == 'open']
    resolved = [s for s in signal_history if s.get('status') != 'open' and s.get('ts', 0) >= cutoff]
    signal_history = sorted(pending + resolved[-max_keep:], key=lambda s: s.get('ts', 0))

def record_signal(res, ai_verdict=None, origin='manual', execution=None):
    """Tách tín hiệu giả lập và giao dịch thật; lệnh thật chống trùng bằng order ID."""
    if not isinstance(res, dict) or res.get('signal') not in ('LONG', 'SHORT'):
        return None
    if not res.get('sl') or (not execution and not res.get('tp')):
        return None
    now = time.time()
    for s in signal_history:
        if execution:
            old = s.get('execution') or {}
            if s.get('symbol') == res['symbol'] and str(old.get('order_id')) == str(execution['order_id']):
                return s['id']
        elif (not s.get('execution') and s.get('origin') == origin
              and s.get('status') == 'open' and s.get('symbol') == res['symbol']
              and s.get('side') == res['signal'] and now - s.get('ts', 0) < 4 * 3600):
            return s['id']
    verdict = ai_verdict or {}
    signal_id = f"{res['symbol']}_{origin}_{execution['order_id'] if execution else time.time_ns()}"
    sig = {
        'id': signal_id, 'ts': now, 'symbol': res['symbol'], 'side': res['signal'],
        'entry': float(res['close']), 'tp': res.get('tp'), 'sl': float(res['sl']),
        'score': res.get('long_score' if res['signal'] == 'LONG' else 'short_score', 0),
        'confidence': res.get('confidence', 'Thấp'), 'ai': verdict.get('direction'),
        'ai_score': verdict.get('long_score' if res['signal'] == 'LONG' else 'short_score'),
        'origin': origin, 'status': 'open', 'tracking_version': 2,
        'model': os.getenv('DASH_MODEL', 'claude-sonnet-5'), 'analysis_version': 2,
        'snapshot': {k: res.get(k) for k in ('rsi', 'adx', 'atr', 'vol_ratio', 'bb_pct',
                                           'ema9', 'ema21', 'ema50', 'ema200', 'funding_rate')},
        'ai_verdict': verdict,
        'ai_input': res.get('ai_input'), 'ai_gate_passed': res.get('ai_gate_passed'),
    }
    if execution:
        sig['execution'] = dict(execution)
        sig['ts'] = float(execution.get('entry_time', now))
    else:
        # Không dùng OHLC của phút vào tín hiệu: high/low có thể xảy ra trước entry.
        sig['next_open_ms'] = (int(now * 1000) // 60000 + 1) * 60000
    signal_history.append(sig)
    prune_signal_history()
    save_signal_history()
    return signal_id


def _verified_execution(s):
    """Chỉ kết quả lệnh đã đối soát mới được điều khiển gate và bài học AI."""
    return (bool(s.get('execution')) and s.get('reconciled') is True
            and s.get('status') in ('win', 'loss', 'be')
            and isinstance(s.get('net_pnl'), (int, float)) and math.isfinite(s['net_pnl']))

SCAN_HISTORY_FILE = "scan_history_trading.json"
SCAN_HISTORY_MAX = 200
scan_history = []

def load_scan_history():
    global scan_history
    try:
        if os.path.exists(SCAN_HISTORY_FILE):
            with open(SCAN_HISTORY_FILE, "r", encoding="utf-8") as f:
                scan_history = json.load(f)
            logger.info(f"Đã tải {len(scan_history)} lượt quét từ lịch sử.")
    except Exception as e:
        logger.error(f"Lỗi khi tải scan_history: {e}")

def save_scan_history():
    try:
        with open(SCAN_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(scan_history, f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Lỗi khi lưu scan_history: {e}")

def record_scan(kind, coins):
    """Ghi lại một lượt quét thị trường định kỳ (kind: '30m' | '5h').
    coins: danh sách symbol phù hợp tìm được. KHÔNG ghi lượt nào không có coin."""
    global scan_history
    coins = [c for c in (coins or []) if c]
    if not coins:
        return
    scan_history.append({
        'ts': time.time(),
        'kind': kind,
        'coins': coins
    })
    if len(scan_history) > SCAN_HISTORY_MAX:
        scan_history = scan_history[-SCAN_HISTORY_MAX:]
    save_scan_history()

def get_signal_stats(days=SIGNAL_MAX_AGE_DAYS):
    """Win-rate lệnh thật đã đối soát, không trộn với cảnh báo/giả lập."""
    cutoff = time.time() - days * 86400
    stats = {}
    for s in signal_history:
        if not _verified_execution(s) or s.get('ts', 0) < cutoff:
            continue
        band = '5⭐' if s.get('confidence') == 'Rất mạnh' else '4⭐'
        if s.get('confidence') not in ('Mạnh', 'Rất mạnh'):
            continue
        st = stats.setdefault(band, {'win': 0, 'loss': 0, 'be': 0, 'net_pnl': 0.0})
        st[s['status']] += 1
        st['net_pnl'] += s['net_pnl']
    return stats

def format_signal_stats(days=SIGNAL_MAX_AGE_DAYS):
    """Win-rate sau chi phí của lệnh thật; hòa vốn được tính trong mẫu."""
    stats = get_signal_stats(days)
    parts = []
    for band, st in stats.items():
        total = st['win'] + st['loss'] + st['be']
        parts.append(f"{band} {st['win']}W/{st['loss']}L/{st['be']}BE "
                     f"({st['win'] / total * 100:.0f}%), net {st['net_pnl']:+.2f} USDT")
    return f"📈 *Lệnh thật đã đối soát {days} ngày:* " + ' | '.join(parts) if parts else ''

def band_winrate_ok(confidence, min_samples=10, min_wr=0.5):
    """Adaptive gate: chặn nhóm tín hiệu có win-rate thực tế dưới 50% (tối thiểu min_samples mẫu)."""
    cutoff = time.time() - SIGNAL_MAX_AGE_DAYS * 86400
    wins = losses = 0
    for s in signal_history:
        if not _verified_execution(s) or s.get('origin') != 'auto':
            continue
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

def side_winrate_ok(side, min_samples=None, min_wr=0.5):
    """Adaptive gate theo chiều, chỉ dùng lệnh auto đã đối soát sau chi phí."""
    if min_samples is None:
        min_samples = AI_AUTO_SIDE_MIN_SAMPLES
    cutoff = time.time() - SIGNAL_MAX_AGE_DAYS * 86400
    wins = losses = 0
    for s in signal_history:
        if not _verified_execution(s) or s.get('origin') != 'auto':
            continue
        if (s.get('side') != side or s.get('status') not in ('win', 'loss')
                or s.get('ts', 0) < cutoff or s.get('confidence') not in ('Mạnh', 'Rất mạnh')):
            continue
        if s['status'] == 'win':
            wins += 1
        else:
            losses += 1
    total = wins + losses
    if total < min_samples:
        return True
    return (wins / total) >= min_wr

async def _track_paper_signal(session, sig, now):
    """Đọc bù nến ĐÃ ĐÓNG; chỉ tiến cursor sau khi nhận đủ dữ liệu liên tục."""
    start = int(sig.get('next_open_ms', (int(sig['ts'] * 1000) // 60000 + 1) * 60000))
    deadline = (int(sig['ts'] * 1000 + SIGNAL_TIMEOUT_HOURS * 3600000) // 60000) * 60000
    end = min(int(now * 1000) // 60000 * 60000, deadline)
    changed = False
    for _ in range(8):
        if start >= end:
            if end >= deadline and sig.get('status') == 'open':
                sig.update(status='expired', closed_ts=deadline / 1000)
                changed = True
            break
        params = {'symbol': sig['symbol'], 'interval': '1m', 'startTime': start,
                  'endTime': end - 1, 'limit': 1000}
        async with session.get('https://fapi.binance.com/fapi/v1/klines', params=params) as resp:
            if resp.status != 200:
                return changed
            candles = await resp.json()
        if not isinstance(candles, list) or not candles:
            return changed
        for candle in candles:
            open_ms = int(candle[0])
            if open_ms < start:
                continue
            if open_ms != start or open_ms + 60000 > end:
                return changed
            high, low = float(candle[2]), float(candle[3])
            hit_sl = low <= sig['sl'] if sig['side'] == 'LONG' else high >= sig['sl']
            hit_tp = high >= sig['tp'] if sig['side'] == 'LONG' else low <= sig['tp']
            start = open_ms + 60000
            sig['next_open_ms'] = start
            changed = True
            if hit_sl or hit_tp:
                sig.update(status='loss' if hit_sl else 'win', closed_ts=start / 1000,
                           outcome_basis='paper_closed_candle')
                return True
    return changed


def _execution_trade_result(sig, trades):
    """Đối soát toàn bộ fills tới khi flat; không gán PnL khi có lệnh mở xen vào."""
    execution = sig['execution']
    entry_id = str(execution['order_id'])
    side = 'BUY' if sig['side'] == 'LONG' else 'SELL'
    pos_side = execution.get('position_side', 'BOTH')
    rows = sorted({int(t['id']): t for t in trades
                   if t.get('positionSide', 'BOTH') == pos_side}.values(), key=lambda t: int(t['id']))
    entries = [t for t in rows if str(t['orderId']) == entry_id]
    if not entries:
        return None
    first_id = min(int(t['id']) for t in entries)
    balance = gross = fees = 0.0
    tolerance = max(float(execution['quantity']) * 1e-8, 1e-12)
    expected = sum(float(t['qty']) for t in entries)
    if abs(expected - float(execution['quantity'])) > tolerance:
        return None
    used = []
    for trade in rows:
        if int(trade['id']) < first_id:
            continue
        qty = float(trade['qty'])
        if trade['side'] == side:
            if str(trade['orderId']) != entry_id:
                return None
            balance += qty
        else:
            if qty > balance + tolerance:
                return None
            balance -= qty
        commission = float(trade.get('commission', 0))
        if commission and trade.get('commissionAsset') != 'USDT':
            return None  # Không giả định phí BNB hoặc tài sản khác bằng USDT.
        gross += float(trade.get('realizedPnl', 0))
        fees += commission
        used.append(trade)
        if balance <= tolerance:
            return {'gross_pnl': gross, 'commission': fees,
                    'closed_ts': int(trade['time']) / 1000,
                    'entry_time': min(int(t['time']) for t in entries) / 1000,
                    'trade_ids': [int(t['id']) for t in used]}
    return None


async def reconcile_execution_outcomes(session):
    """REST fills + funding là nguồn thật; không suy diễn lệnh đã đóng từ cache WS."""
    changed = False
    now_ms = int(time.time() * 1000)
    for sig in list(signal_history):
        if not sig.get('execution') or sig.get('status') != 'open':
            continue
        execution = sig['execution']
        start = int(float(execution.get('entry_time', sig['ts'])) * 1000)
        # userTrades chỉ tra được 3 tháng gần nhất — quá hạn thì giữ 'open', không gán PnL suy diễn.
        if now_ms - start > 85 * 86400000:
            sig['reconciliation_note'] = 'Quá hạn 3 tháng của API userTrades — không suy diễn PnL.'
            continue
        trades = []
        complete = True
        cursor = start
        # UserTrades chỉ nhận khoảng thời gian tối đa 7 ngày mỗi request.
        while cursor <= now_ms:
            end = min(cursor + 7 * 86400000 - 1, now_ms)
            page, err = await binance_signed_request(session, 'GET', '/fapi/v1/userTrades',
                {'symbol': sig['symbol'], 'startTime': cursor, 'endTime': end, 'limit': 1000})
            if err or not isinstance(page, list):
                complete = False
                break
            trades.extend(page)
            if len(page) == 1000:
                # Phân trang bằng ID để không mất các fills cùng millisecond.
                from_id = int(page[-1]['id']) + 1
                while True:
                    page, err = await binance_signed_request(session, 'GET', '/fapi/v1/userTrades',
                        {'symbol': sig['symbol'], 'fromId': from_id, 'limit': 1000})
                    if err or not isinstance(page, list):
                        complete = False
                        break
                    trades.extend(t for t in page if int(t['time']) <= now_ms)
                    if len(page) < 1000 or int(page[-1]['time']) > now_ms:
                        break
                    next_id = int(page[-1]['id']) + 1
                    if next_id <= from_id:
                        complete = False
                        break
                    from_id = next_id
                break
            cursor = end + 1
        if not complete:
            continue
        result = _execution_trade_result(sig, trades)
        if result is None:
            sig['reconciliation_note'] = 'Chờ fills đầy đủ/phân bổ phí; không suy diễn PnL từ nến.'
            continue
        closed_ms = int(result['closed_ts'] * 1000)
        # Đợi income có thời gian cập nhật; không finalize ngay sau fill cuối.
        if now_ms - closed_ms < 60000:
            continue
        funding, err = await fetch_income_paginated(session, income_type='FUNDING_FEE',
            start_ms=int(result['entry_time'] * 1000), end_ms=closed_ms)
        if err or funding is None:
            continue
        funding = [f for f in funding if f.get('symbol') == sig['symbol']]
        # Income funding không có positionSide. Nếu hai chiều overlap thì chưa thể phân bổ chính xác.
        if funding and execution.get('position_side', 'BOTH') != 'BOTH':
            opposite = any(t.get('positionSide', 'BOTH') != execution['position_side']
                           and start <= int(t['time']) <= closed_ms for t in trades)
            if opposite or not execution.get('symbol_flat_at_entry'):
                sig['reconciliation_note'] = 'Funding hedge không đủ bằng chứng để phân bổ theo chiều.'
                continue
        if any(f.get('asset') != 'USDT' for f in funding):
            continue
        funding_net = sum(float(f['income']) for f in funding)
        net = result['gross_pnl'] - result['commission'] + funding_net
        initial_risk = abs(sig['entry'] - sig['sl']) * float(execution['quantity'])
        sig.update(result, funding=funding_net, net_pnl=net,
                   realized_r=net / initial_risk if initial_risk > 0 else None,
                   reconciled=True, outcome_basis='exchange_fills',
                   status='win' if net > 1e-8 else ('loss' if net < -1e-8 else 'be'))
        changed = True
    return changed


async def signal_tracking_loop(app):
    """Tín hiệu: nến đóng; lệnh thật: đối soát fills, commission và funding."""
    await asyncio.sleep(10)
    while True:
        try:
            session = app['session']
            sem = asyncio.Semaphore(8)
            now = time.time()
            async def track(sig):
                async with sem:
                    return await _track_paper_signal(session, sig, now)
            pending = [s for s in signal_history if s.get('status') == 'open'
                       and not s.get('execution')]
            await asyncio.gather(*(track(s) for s in pending), return_exceptions=True)
            # Cursor có thể tiến trước khi một request sau bị lỗi: vẫn persist tiến độ đã đọc.
            if pending:
                save_signal_history()
            if await reconcile_execution_outcomes(session):
                prune_signal_history()
                save_signal_history()
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.error('Lỗi theo dõi kết quả; giữ nguyên trạng thái để đối soát lại.')
            await asyncio.sleep(30)


def format_detailed_stats_text(days=SIGNAL_MAX_AGE_DAYS):
    """Tách rõ lệnh thật đã đối soát và kết quả tín hiệu giả lập theo nguồn."""
    cutoff = time.time() - days * 86400
    recent = [s for s in signal_history if s.get('ts', 0) >= cutoff]
    if not recent:
        return ''
    lines = [f"📊 *STATS {days} NGÀY*", 'Lệnh thật: PnL ròng sau commission/funding.']
    for origin in sorted({s.get('origin', 'manual') for s in recent}):
        group = [s for s in recent if s.get('origin', 'manual') == origin]
        real = [s for s in group if _verified_execution(s)]
        pending = sum(bool(s.get('execution')) and not _verified_execution(s) for s in group)
        if real:
            wins = sum(s['net_pnl'] > 1e-8 for s in real)
            pnl = sum(s['net_pnl'] for s in real)
            gains = sum(max(0, s['net_pnl']) for s in real)
            losses = sum(max(0, -s['net_pnl']) for s in real)
            pf = f'{gains / losses:.2f}' if losses else '—'
            rs = [s['realized_r'] for s in real if s.get('realized_r') is not None]
            ev = f'{sum(rs) / len(rs):+.3f}R' if rs else '—'
            lines.append(f"• {origin}: {len(real)} lệnh, WR {wins / len(real) * 100:.1f}%, "
                         f"net {pnl:+.2f} USDT, EV {ev}, PF {pf}")
        if pending:
            lines.append(f'• {origin}: {pending} lệnh đang mở/chờ đối soát, chưa tính WR.')
        paper = [s for s in group if not s.get('execution')]
        if paper:
            wins = sum(s.get('status') == 'win' for s in paper)
            losses = sum(s.get('status') == 'loss' for s in paper)
            expired = sum(s.get('status') == 'expired' for s in paper)
            lines.append(f'• {origin} [GIẢ LẬP, không phải PnL]: {wins}W/{losses}L, {expired} hết hạn.')
    candidates = [s for s in recent if s.get('origin') == 'candidate'
                  and s.get('status') in ('win', 'loss')]
    if candidates:
        accepted = [s for s in candidates if s.get('ai_gate_passed') is True]
        lines.append(f"So sánh GIẢ LẬP cùng tập ứng viên: rule {sum(s['status'] == 'win' for s in candidates)}/{len(candidates)} thắng; "
                     f"AI giữ {sum(s['status'] == 'win' for s in accepted)}/{len(accepted)} thắng. "
                     'Chưa trừ phí; không phải bằng chứng EV hoặc xác suất điểm AI.')
    return '\n'.join(lines)


async def handle_stats_command(session, chat_id):
    """Lệnh /stats: thống kê chi tiết win-rate thực tế 30 ngày."""
    text = format_detailed_stats_text()
    if not text:
        await send_telegram_message(session, chat_id, "ℹ️ Chưa có dữ liệu tín hiệu hoặc lệnh thật trong 30 ngày.")
        return
    await send_telegram_message(session, chat_id, text)


async def handle_trail_command(session, chat_id, coin_name, action_str=None):
    """Lệnh /trail: bật/tắt quản lý trailing stop cho VỊ THẾ NGƯỜI DÙNG đặt tay.
    /trail btc        → bật (đặt SL 1.5×ATR, đạt +0.8R trailing + hủy TP, +1.5R chốt 50%)
    /trail btc off    → tắt (giữ nguyên SL hiện tại)"""
    if not coin_name:
        await send_telegram_message(session, chat_id,
            "❌ Sai cú pháp!\n`/trail <coin>` để BẬT (vd `/trail btc`)\n`/trail <coin> off` để TẮT")
        return
    coin_name = coin_name.upper()
    symbol = coin_name if coin_name.endswith("USDT") else f"{coin_name}USDT"
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    want_off = (action_str or '').strip().lower() == 'off'

    pos = positions.get(f"{symbol}_LONG") or positions.get(f"{symbol}_SHORT")
    key = next((k for k in positions if k.startswith(symbol)), None)
    if key:
        pos = positions[key]

    if want_off or not pos or float(pos.get('positionAmt', 0) or 0) == 0:
        removed = [k for k in auto_managed if k.startswith(symbol)]
        for k in removed:
            auto_managed.pop(k, None)
        _save_auto_managed()
        if removed:
            await send_telegram_message(session, chat_id,
                f"🛑 Đã tắt trailing cho {display_symbol(symbol)} (giữ nguyên SL hiện tại).")
        else:
            await send_telegram_message(session, chat_id,
                f"ℹ️ Không có vị thế {display_symbol(symbol)} đang mở để quản lý.")
        return

    amount = float(pos.get('positionAmt', 0) or 0)
    side = 'LONG' if amount > 0 else 'SHORT'
    entry = float(pos.get('entryPrice', 0) or 0)
    mark = float(pos.get('markPrice', 0) or 0)
    pos_side = pos.get('positionSide', 'BOTH')
    if entry <= 0 or mark <= 0:
        await send_telegram_message(session, chat_id, "⚠️ Chưa lấy được giá entry/mark — thử lại sau.")
        return
    res = await analyze_market(session, symbol, interval='1h', fetch_extras=False)
    if not res or not res.get('atr') or res['atr'] <= 0:
        await send_telegram_message(session, chat_id, "⚠️ Không tính được ATR — thử lại sau.")
        return
    atr = float(res['atr'])
    risk = atr * 1.5
    sl_price = entry - risk if side == 'LONG' else entry + risk

    qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)
    sl_price = round_price_step(sl_price, tick_size, price_p)
    real_qty = abs(amount)
    close_side = 'SELL' if side == 'LONG' else 'BUY'

    # Đặt bảo vệ mới trước; không hủy SL đang bảo vệ nếu API đặt mới thất bại.
    old_orders, old_err = await binance_signed_request(session, 'GET', '/fapi/v1/openAlgoOrders', {'symbol': symbol})
    if old_err or not isinstance(old_orders, list):
        await send_telegram_message(session, chat_id, '⚠️ Không đối soát được SL hiện tại; giữ nguyên bảo vệ.')
        return
    old_stops = [o for o in old_orders
                 if (o.get('orderType') or o.get('type')) == 'STOP_MARKET'
                 and o.get('positionSide', 'BOTH') == pos_side and o.get('side') == close_side]
    for stop in old_stops:
        trigger = float(stop.get('triggerPrice', 0) or 0)
        if trigger > 0:
            sl_price = max(sl_price, trigger) if side == 'LONG' else min(sl_price, trigger)
    if (side == 'LONG' and sl_price >= mark) or (side == 'SHORT' and sl_price <= mark):
        await send_telegram_message(session, chat_id, '⚠️ SL đã vượt giá mark; không thay bảo vệ hiện tại.')
        return
    ok, info = await _place_conditional_tpsl(session, symbol, close_side, 'STOP_MARKET',
                                             f"{sl_price:.{price_p}f}",
                                             f"{real_qty:.{qty_p}f}",
                                             None if pos_side == 'BOTH' else pos_side)
    if not ok:
        await send_telegram_message(session, chat_id, f"❌ Không đặt được SL: {info}")
        return

    algo_id = str(info) if info is not None and str(info).isdigit() else None
    if not algo_id:
        await send_telegram_message(session, chat_id, '⚠️ Chưa xác nhận được ID SL mới; giữ SL cũ.')
        return
    for stop in old_stops:
        if str(stop.get('algoId')) != algo_id:
            await _cancel_algo_sl(session, api_key, api_secret, symbol, stop.get('algoId'))
    pos_key = f"{symbol}_{pos_side}"
    auto_managed[pos_key] = {
        'symbol': symbol, 'side': side, 'origin': 'manual',
        'entry': entry, 'sl_initial': sl_price, 'risk': risk, 'atr': atr,
        'qty': real_qty, 'pos_side': pos_side,
        'sl_algo_id': algo_id, 'tp_algo_id': None,
        'last_sl': sl_price, 'ts': time.time(),
    }
    _save_auto_managed()
    cur_r = ((mark - entry) / risk if side == 'LONG' else (entry - mark) / risk)
    await send_telegram_message(
        session, chat_id,
        f"🛡️ *Đã bật trailing cho {display_symbol(symbol)} {side}*\n"
        f"SL: `{format_price(sl_price)}` (entry ± 1.5×ATR)\n"
        f"Đang {cur_r:+.1f}R. Đạt +0.8R → trailing + hủy TP để lời chạy; +1.5R → chốt 50%.\n"
        f"Tắt: `/trail {coin_name[:-4] if coin_name.endswith('USDT') else coin_name} off`"
    )


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

MINTROUTER_BASE_URL = "https://api.mintrouter.ai/v1"

def _ai_headers(api_key, session_id=None):
    """Headers cho API MintRouter.ai (OpenAI-compatible, xác thực qua Authorization: Bearer)."""
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": "pnlbot/1.0",
    }

# ─── Thống kê token LLM ở local (MintRouter đã gỡ API key-usage — endpoint trả HTML) ───
LLM_USAGE_FILE = "llm_usage_trading.json"
llm_usage = {}   # day 'YYYY-MM-DD' (UTC) -> {model: {'calls': n, 'in': tokens, 'out': tokens}}
_llm_usage_last_ts = 0.0  # ts của cuộc gọi LLM gần nhất (để bypass cache /usage khi có call mới)


def _load_llm_usage():
    global llm_usage
    try:
        if os.path.exists(LLM_USAGE_FILE):
            with open(LLM_USAGE_FILE, "r", encoding="utf-8") as f:
                llm_usage = json.load(f) or {}
    except Exception as e:
        logger.error(f"Lỗi nạp llm_usage: {e}")


def _save_llm_usage():
    try:
        # chỉ giữ 35 ngày gần nhất
        keep = sorted(llm_usage.keys())[-35:]
        trimmed = {k: llm_usage[k] for k in keep}
        llm_usage.clear()
        llm_usage.update(trimmed)
        with open(LLM_USAGE_FILE, "w", encoding="utf-8") as f:
            json.dump(llm_usage, f)
    except Exception as e:
        logger.error(f"Lỗi lưu llm_usage: {e}")


def record_llm_usage(model, usage):
    """Ghi nhận tokens in/out từ trường `usage` của mỗi response chat completions."""
    global _llm_usage_last_ts
    try:
        usage = usage or {}
        p = int(usage.get('prompt_tokens') or 0)
        c = int(usage.get('completion_tokens') or 0)
        if p <= 0 and c <= 0:
            return
        day = time.strftime('%Y-%m-%d', time.gmtime())
        day_stats = llm_usage.setdefault(day, {})
        m = day_stats.setdefault(model or 'unknown', {'calls': 0, 'in': 0, 'out': 0})
        m['calls'] += 1
        m['in'] += p
        m['out'] += c
        _llm_usage_last_ts = time.time()
        _save_llm_usage()
    except Exception as e:
        logger.warning(f"Lỗi ghi usage: {e}")


def _fmt_llm_usage_days(days):
    """Tổng hợp usage N ngày gần nhất. Trả về dòng text hoặc None."""
    cutoff = time.time() - days * 86400
    agg = {}
    for day, models in llm_usage.items():
        try:
            ts = time.mktime(time.strptime(day, '%Y-%m-%d'))
        except ValueError:
            continue
        if ts < cutoff:
            continue
        for model, st in models.items():
            a = agg.setdefault(model, {'calls': 0, 'in': 0, 'out': 0})
            a['calls'] += st.get('calls', 0)
            a['in'] += st.get('in', 0)
            a['out'] += st.get('out', 0)
    if not agg:
        return None
    lines = []
    for model, st in sorted(agg.items(), key=lambda kv: -(kv[1]['in'] + kv[1]['out'])):
        lines.append(f"  · {model}: {st['calls']} calls | in {st['in']:,} | out {st['out']:,} tokens")
    return "\n".join(lines)


async def get_ai_analysis(session, digest, lessons=None):
    """Gọi LLM phân tích digest chỉ báo. Trả về {direction, confidence, reason, analysis} hoặc None."""
    api_key = os.getenv("DASH_TOKEN")
    if not api_key or not digest:
        return None
    model = os.getenv("DASH_MODEL", "claude-sonnet-5")
    url = f"{MINTROUTER_BASE_URL}/chat/completions"
    headers = _ai_headers(api_key)
    system_prompt = (
        "Bạn là một phân tích viên giao dịch crypto futures chuyên nghiệp, kỷ luật và thận trọng, sống bằng kết quả giao dịch thực tế. "
        "Dữ liệu được cung cấp là số liệu chỉ báo đa khung (nến đã đóng) KHÔNG kèm hướng hay điểm số của hệ thống. "
        "Bạn phải TỰ phân tích và TỰ chấm điểm bằng logic riêng, không bám theo bất kỳ hệ thống nào. "
        "Quy trình bắt buộc:\n"
        "1. Đánh giá riêng từng khung: xu hướng (giá so với EMA9/21/50/200), động lượng (RSI, Stoch, MACD, ADX/DI), "
        "vị trí giá trong băng Bollinger, volume, vùng S/R.\n"
        "2. Tổng hợp đa khung: nhiều khung đồng thuận → điểm cao hơn; mâu thuẫn giữa khung nhỏ và khung lớn → hạ điểm, thận trọng.\n"
        "3. Rủi ro ngược chiều phải HẠ điểm hoặc chọn NEUTRAL: giá quá mua/quá bán cực đoan (RSI>75/<25, BB chạm biên), "
        "giá kéo giãn xa EMA50/EMA200 (dễ hồi về trung bình), funding cực đoan (đám đông quá một chiều), "
        "ADX thấp <20 (sideway, không nên giao dịch), volume yếu (tín hiệu khó xác nhận).\n"
        "4. Không bắt dao rơi / cản tàu: không mua đáy đang rơi mạnh, không bán đỉnh đang tăng nóng.\n"
        "5. Điểm 0-10 là mức tự tin ĐỘC LẬP của bạn cho từng chiều. Thang chuẩn: 0-3 rất yếu, 3-5 trung bình, "
        "5-7 khá, 7-10 rất tự tin (hiếm khi đạt — đừng phóng tay). Chỉ khi một chiều thật sự vượt trội và đủ xác nhận "
        "mới cho điểm ≥ 6. Thị trường mơ hồ → cả 2 chiều đều thấp.\n"
        "6. Ưu tiên bảo toàn vốn: khi mâu thuẫn, xu hướng chưa rõ, biến động quá mạnh (ATR cao) hoặc thiếu xác nhận → "
        "chọn NEUTRAL.\n"
        "Phần analysis viết như trader thực dụng: cấu trúc xu hướng từng khung, động lượng, vùng giá quan trọng "
        "(dùng số giá cụ thể có trong dữ liệu), kịch bản phù hợp + điều kiện xác nhận, rủi ro cần tránh. "
        "Tránh nói chung chung kiểu sách vở, tránh liệt kê lại nguyên văn chỉ báo, không khuyên về đòn bẩy hay khối lượng. "
        "Chỉ trả lời bằng MỘT JSON hợp lệ duy nhất, không thêm bất kỳ chữ nào, đúng định dạng: "
        '{"direction": "LONG" | "SHORT" | "NEUTRAL", '
        '"long_score": điểm LONG 0-10 (số thực, ví dụ 8.7), "short_score": điểm SHORT 0-10 (số thực), '
        '"confidence": "cao" | "trung bình" | "thấp", '
        '"reason": "tóm tắt một câu bằng tiếng Việt, dưới 200 ký tự", '
        '"analysis": ["3-5 gạch đầu dòng tiếng Việt tự nhiên, mỗi dòng dưới 200 ký tự, nêu số giá cụ thể khi cần"]}'
    )
    if lessons:
        system_prompt += (
            "\n\nBài học rút ra từ kết quả tín hiệu thực tế gần đây của hệ thống (ưu tiên áp dụng khi đánh giá):\n"
            f"{lessons}"
        )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": digest}
        ],
        "temperature": 0.2,
        # Model reasoning (Claude/GLM...) tiêu tốn token cho phần suy luận nên
        # cần budget đủ lớn để phần JSON cuối cùng không bị cắt (finish_reason=length)
        "max_tokens": 4000
    }
    try:
        timeout = aiohttp.ClientTimeout(total=90)
        # Retry 1 lần với max_tokens lớn hơn nếu content rỗng (reasoning ăn hết budget)
        for attempt, tok_budget in ((1, 4000), (2, 8000)):
            payload['max_tokens'] = tok_budget
            async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"AI API trả lỗi HTTP {resp.status}: {body[:200]}")
                    return None
                data = await resp.json()
            record_llm_usage(model, data.get('usage'))
            content = data.get('choices', [{}])[0].get('message', {}).get('content', '')
            verdict = _extract_json(content)
            if verdict and verdict.get('direction') in ('LONG', 'SHORT', 'NEUTRAL'):
                verdict['confidence'] = verdict.get('confidence', 'trung bình')
                verdict['reason'] = str(verdict.get('reason', ''))[:250]
                for k in ('long_score', 'short_score'):
                    try:
                        v = float(verdict.get(k))
                        verdict[k] = v if 0.0 <= v <= 10.0 else None
                    except (TypeError, ValueError):
                        verdict[k] = None
                analysis = verdict.get('analysis')
                if isinstance(analysis, list):
                    verdict['analysis'] = [str(x).strip()[:200] for x in analysis if str(x).strip()][:6]
                else:
                    verdict['analysis'] = []
                return verdict
            # Model reasoning: nếu max_tokens quá nhỏ, phần suy luận
            # (reasoning_content) ăn hết budget và content trả về rỗng → tăng budget thử lại
            if attempt == 1:
                logger.warning(f"AI trả lời không parse được JSON (content rỗng/sai định dạng), thử lại với max_tokens lớn hơn: {str(content)[:150]}")
            else:
                logger.warning(f"AI vẫn không trả JSON sau retry: {str(content)[:150]}")
        return None
    except Exception as e:
        logger.warning(f"Lỗi gọi AI analysis: {e}")
        return None

def get_symbol_history_text(symbol, limit=6):
    """Lịch sử kết quả các lần hệ thống từng đề xuất symbol — để AI 'nhớ' coin đó khi phân tích lại."""
    past = [s for s in signal_history
            if s.get('symbol') == symbol and _verified_execution(s)]
    if not past:
        return None
    wins = sum(1 for s in past if s['status'] == 'win')
    lines = [f"📌 *{symbol}: {len(past)} lệnh thật đã đối soát, {wins} thắng; net {sum(s['net_pnl'] for s in past):+.2f} USDT:*"]
    for s in past[-limit:]:
        age_h = int((time.time() - s.get('ts', 0)) // 3600)
        age_txt = f"{age_h}h" if age_h < 24 else f"{age_h // 24}d"
        ai_sc = f" (AI chấm {s['ai_score']:.1f})" if s.get('ai_score') is not None else ""
        lines.append(
            f"  · {s.get('side')} entry {format_price(s.get('entry'))} TP {format_price(s.get('tp'))} "
            f"SL {format_price(s.get('sl'))} → *{s['status']}*, net {s['net_pnl']:+.2f} USDT{ai_sc} ({age_txt} trước)"
        )
    lines.append("  → Rút kinh nghiệm từ kết quả này khi đánh giá lần này.")
    return "\n".join(lines)


def build_ai_digest(symbol, timeframe_results, oi_change=None, taker_ratio=None, funding_rate=None, orderbook=None, btc_dominance=None):
    """Dựng text digest chỉ báo đa khung (số liệu thô, KHÔNG kèm hướng/điểm hệ thống) để AI tự đánh giá độc lập."""
    lines = [
        f"Phân tích kỹ thuật {symbol} — Binance Futures, nến ĐÃ ĐÓNG. Dữ liệu thô, không kèm hướng/điểm hệ thống.",
        "Bạn phải TỰ phân tích từng khung rồi tổng hợp bằng logic riêng."
    ]
    for tf_name, res in timeframe_results:
        if not res:
            lines.append(f"- Khung {tf_name}: thiếu dữ liệu")
            continue
        c = res['close']
        e9, e21, e50, e200 = res['ema9'], res['ema21'], res['ema50'], res['ema200']
        rsi = res['rsi']
        rsi_zone = "quá bán" if rsi <= 30 else ("quá mua" if rsi >= 70 else "trung tính")
        macd_txt = "dương (bullish)" if res['hist'] > 0 else ("âm (bearish)" if res['hist'] < 0 else "bằng 0")
        if res['plus_di'] > res['minus_di']:
            di_txt = f"+DI {res['plus_di']:.1f} > -DI {res['minus_di']:.1f} (bên long chiếm ưu thế)"
        else:
            di_txt = f"+DI {res['plus_di']:.1f} < -DI {res['minus_di']:.1f} (bên short chiếm ưu thế)"
        if c > e9 > e21 > e50:
            trend = "UP mạnh (giá > EMA9>21>50)"
        elif c < e9 < e21 < e50:
            trend = "DOWN mạnh (giá < EMA9<21<50)"
        elif c > e21 > e50:
            trend = "UP nhẹ"
        elif c < e21 < e50:
            trend = "DOWN nhẹ"
        elif c > e21:
            trend = "nghiêng lên (giá > EMA21)"
        elif c < e21:
            trend = "nghiêng xuống (giá < EMA21)"
        else:
            trend = "sideway"
        stretch = []
        if not math.isnan(e50) and e50 > 0:
            d50 = (c - e50) / e50 * 100
            stretch.append(f"cách EMA50 {d50:+.1f}%")
        if not math.isnan(e200) and e200 > 0:
            d200 = (c - e200) / e200 * 100
            stretch.append(f"cách EMA200 {d200:+.1f}%")
            if d200 > 15:
                lines.append(f"  · ⚠️ {tf_name}: giá kéo giãn {d200:+.1f}% trên EMA200 — rủi ro hồi về trung bình cao")
            elif d200 < -15:
                lines.append(f"  · ⚠️ {tf_name}: giá kéo giãn {d200:+.1f}% dưới EMA200 — rủi ro hồi về trung bình cao")
        ext = f" · {', '.join(stretch)}" if stretch else ""
        # Dữ liệu bổ sung (nếu có): vwap, MACD line, BB bands tuyệt đối, biến động 24h
        extra_ta = []
        if 'vwap' in res and not math.isnan(res['vwap']):
            vwap_dev = (c - res['vwap']) / c * 100
            extra_ta.append(f"VWAP {'trên' if vwap_dev > 0 else 'dưới'} {abs(vwap_dev):.1f}%")
        if 'macd' in res and 'signal_line' in res:
            extra_ta.append(f"MACD {res['macd']:+.5g}/sig {res['signal_line']:+.5g}")
        if 'upper_band' in res and 'lower_band' in res and res['upper_band'] > 0:
            extra_ta.append(f"BB {format_price(res['lower_band'])}-{format_price(res['upper_band'])}")
        if res.get('price_change_24') is not None:
            extra_ta.append(f"24h {res['price_change_24']:+.2f}%")
        ext_ta_txt = f" | {', '.join(extra_ta)}" if extra_ta else ""
        lines.append(
            f"- Khung {tf_name}: Giá {format_price(c)} | RSI {rsi:.1f} ({rsi_zone}) | "
            f"StochK {res['stoch_k']:.1f}/D {res['stoch_d']:.1f} | MACD hist {res['hist']:+.5g} ({macd_txt}) | "
            f"ADX {res['adx']:.1f} ({di_txt}) | ATR {((res['atr'] / c) * 100):.2f}% | Vol x{res['vol_ratio']:.2f} | "
            f"BB {res['bb_pct'] * 100:.0f}% | S/R {format_price(res['support'])}-{format_price(res['resistance'])}"
            f"{ext_ta_txt}\n"
            f"  Xu hướng: {trend}{ext}"
        )
        if res.get('rsi_div'):
            lines.append(f"  · Divergence RSI: {res['rsi_div']}")
        if res.get('macd_div'):
            lines.append(f"  · Divergence MACD hist: {res['macd_div']}")
        if res.get('pattern'):
            lines.append(f"  · Pattern nến: {res['pattern']}")
    if oi_change is not None:
        lines.append(f"- Open Interest 24h: {oi_change:+.1f}% (tăng = tiền mới vào, giảm = chốt lời/thanh lý)")
    if taker_ratio is not None:
        lines.append(f"- Taker buy/sell: {taker_ratio:.2f} (>=1.15 mua mạnh, <=0.87 bán mạnh)")
    if funding_rate is not None:
        lines.append(f"- Funding rate: {funding_rate * 100:+.4f}% (dương cao = đám đông long quá đông, rủi ro ép giá ngược; âm sâu = đám đông short)")
    if orderbook:
        imb = orderbook['imbalance'] * 100
        ob_desc = "đa số bid 🟢" if imb > 5 else ("đa số ask 🔴" if imb < -5 else "cân bằng")
        lines.append(f"- Order book (top 20 mức): imbalance {imb:+.1f}% ({ob_desc}), spread {orderbook['spread_pct']:.3f}%")
    if btc_dominance is not None:
        lines.append(f"- BTC dominance: {btc_dominance:.1f}%")
    sym_hist = get_symbol_history_text(symbol)
    if sym_hist:
        lines.append(sym_hist)
    lines.append("Hãy kết luận hướng đi ngắn hạn theo đúng JSON yêu cầu.")
    return "\n".join(lines)

async def get_ai_verdict_cached(session, cache_key, digest):
    """Gọi AI có cache TTL 10 phút để tiết kiệm usage.
    Key cache gắn thêm hash digest: digest đổi (giá/điểm mới) → verdict mới, không dùng kết luận cũ."""
    now = time.time()
    lessons = await get_ai_lessons(session)
    model = os.getenv('DASH_MODEL', 'claude-sonnet-5')
    digest_hash = hashlib.sha256(f'{model}\n{lessons}\n{digest}'.encode('utf-8')).hexdigest()
    full_key = f"{cache_key}_{digest_hash}"
    cached = ai_verdict_cache.get(full_key)
    if cached and now - cached['ts'] < AI_CACHE_TTL:
        return cached['verdict']
    verdict = await get_ai_analysis(session, digest, lessons=lessons)
    if verdict:
        ai_verdict_cache[full_key] = {'verdict': verdict, 'ts': now}
        # Dọn entry đã hết hạn (digest chứa giá live → key mới liên tục, không dọn là leak)
        stale = [k for k, v in ai_verdict_cache.items() if now - v.get('ts', 0) > AI_CACHE_TTL]
        for k in stale:
            ai_verdict_cache.pop(k, None)
    return verdict


# ─── AI tự học từ kết quả tín hiệu thực tế (feedback loop) ───
AI_LESSONS_TTL = 12 * 3600
ai_lessons_state = {'text': None, 'ts': 0, 'resolved_count': -1}
ai_lessons_lock = asyncio.Lock()

def build_signal_lessons_digest():
    """Bài học theo lệnh auto đã đối soát của cùng model/phiên bản phân tích."""
    cutoff = time.time() - SIGNAL_MAX_AGE_DAYS * 86400
    resolved = [s for s in signal_history if _verified_execution(s)
                and s.get('origin') == 'auto' and s.get('ts', 0) >= cutoff
                and s.get('model') == os.getenv('DASH_MODEL', 'claude-sonnet-5')
                and s.get('analysis_version') == 2]
    if len(resolved) < 5:
        return None
    wins = sum(1 for s in resolved if s['status'] == 'win')
    lines = [
        f"Thống kê {len(resolved)} lệnh auto đã đối soát; win/loss theo PnL ròng sau phí/funding:",
        f"- {wins} win / {len(resolved)} lệnh (bao gồm hòa vốn), net {sum(s['net_pnl'] for s in resolved):+.2f} USDT",
        "- Đây là mẫu lệnh đã chọn, không chứng minh điểm AI là xác suất thắng hay AI tốt hơn rule-only."
    ]
    for side in ('LONG', 'SHORT'):
        sub = [s for s in resolved if s.get('side') == side]
        if sub:
            w = sum(1 for s in sub if s['status'] == 'win')
            lines.append(f"- {side}: {w}/{len(sub)} win ({w / len(sub) * 100:.0f}%)")
    for band in ('Mạnh', 'Rất mạnh'):
        sub = [s for s in resolved if s.get('confidence') == band]
        if sub:
            w = sum(1 for s in sub if s['status'] == 'win')
            lines.append(f"- Độ tin cậy '{band}': {w}/{len(sub)} win ({w / len(sub) * 100:.0f}%)")
    # Hiệu chuẩn thang điểm AI tự chấm: bucket nào thắng thật, bucket nào là "ảo"
    lines.append("- Win-rate theo mức AI tự chấm lúc đó (kiểm tra AI có phóng tay điểm cao không):")
    for bucket, lo, hi in (('AI < 5', 0, 5), ('AI 5-6', 5, 6), ('AI 6-7', 6, 7), ('AI ≥ 7', 7, 11)):
        sub = [s for s in resolved if s.get('ai_score') is not None and lo <= s['ai_score'] < hi]
        if sub:
            w = sum(1 for s in sub if s['status'] == 'win')
            lines.append(f"  - {bucket}: {w}/{len(sub)} win ({w / len(sub) * 100:.0f}%)")
    lines.append("- 15 tín hiệu gần nhất:")
    for s in resolved[-15:]:
        ai_note = f", AI nhận định lúc đó: {s.get('ai')}" if s.get('ai') else ", AI lúc đó: không có"
        ai_sc = f" (AI chấm {s['ai_score']:.1f})" if s.get('ai_score') is not None else ""
        lines.append(
            f"  · {s['symbol']} {s['side']} ({s.get('confidence')}, điểm {s.get('score', 0):.1f}{ai_sc}) -> {s['status']}, net {s['net_pnl']:+.2f} USDT{ai_note}"
        )
    lines.append(
        "Hãy tóm tắt 3-5 quan sát về rủi ro, nêu cỡ mẫu và độ bất định. Không suy ra quan hệ nhân quả "
        "hoặc xác suất thắng từ điểm tự chấm; không tự đề xuất đổi ngưỡng từ mẫu nhỏ. Trả lời bằng tiếng Việt, "
        "không dùng ký tự markdown (*, _, `). Chỉ trả lời bằng MỘT JSON hợp lệ: {\"lessons\": [\"gạch đầu dòng, mỗi dòng dưới 200 ký tự\"]}"
    )
    return "\n".join(lines)


async def get_ai_lessons(session, force=False):
    """Bài học AI rút từ lịch sử tín hiệu. Cache 12h, chỉ refresh khi có tín hiệu mới kết thúc.
    force=True → bỏ qua cache, ép AI đánh giá lại ngay (dùng cho lệnh thủ công)."""
    st = ai_lessons_state
    async with ai_lessons_lock:
        digest = build_signal_lessons_digest()
        if not digest or not os.getenv('DASH_TOKEN'):
            st.update(text=None, ts=0, resolved_count=-1)
            return None
        model = os.getenv('DASH_MODEL', 'claude-sonnet-5')
        evidence_key = hashlib.sha256(f'{model}\n{digest}'.encode('utf-8')).hexdigest()
        if (not force and st['text'] and st.get('evidence_key') == evidence_key
                and time.time() - st['ts'] < AI_LESSONS_TTL):
            return st['text']
        st['text'] = None
        url = f"{MINTROUTER_BASE_URL}/chat/completions"
        headers = _ai_headers(os.getenv('DASH_TOKEN'))
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "Bạn là quản trị rủi ro giao dịch crypto, rút kinh nghiệm khách quan từ kết quả tín hiệu thực tế."},
                {"role": "user", "content": digest}
            ],
            "temperature": 0.2,
            "max_tokens": 2000
        }
        try:
            timeout = aiohttp.ClientTimeout(total=60)
            async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = data.get('choices', [{}])[0].get('message', {}).get('content', '')
                    lessons = _extract_json(content)
                    if lessons and isinstance(lessons.get('lessons'), list) and lessons['lessons']:
                        st['text'] = "\n".join(f"- {str(x).strip()[:200]}" for x in lessons['lessons'][:5])
                        st['ts'] = time.time()
                        st['evidence_key'] = evidence_key
                        logger.info("Đã cập nhật bài học AI từ lịch sử tín hiệu.")
        except Exception as e:
            logger.warning(f"Lỗi gọi AI lessons: {e}")
        return st['text']


# ─── Dữ liệu bổ trợ cho AI: order book + BTC dominance ───
orderbook_cache = {}
btc_dominance_cache = {'value': None, 'ts': 0}

async def get_orderbook_summary(session, symbol, ttl=120):
    """Tóm tắt order book futures (top 20 mức): mất cân bằng bid/ask + spread. Cache TTL ngắn."""
    cache = orderbook_cache.setdefault(symbol, {'data': None, 'ts': 0})
    now = time.time()
    if cache['data'] is not None and now - cache['ts'] < ttl:
        return cache['data']
    try:
        async with session.get(f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=20") as resp:
            if resp.status == 200:
                data = await resp.json()
                bid_vol = sum(float(b[1]) for b in data.get('bids', []))
                ask_vol = sum(float(a[1]) for a in data.get('asks', []))
                total = bid_vol + ask_vol
                if total > 0:
                    best_bid = float(data['bids'][0][0])
                    best_ask = float(data['asks'][0][0])
                    summary = {
                        'imbalance': (bid_vol - ask_vol) / total,
                        'spread_pct': (best_ask - best_bid) / best_bid * 100 if best_bid > 0 else 0.0
                    }
                    cache.update({'data': summary, 'ts': now})
                    return summary
    except Exception as e:
        logger.warning(f"Lỗi lấy order book {symbol}: {e}")
    return cache['data']


async def get_btc_dominance(session, ttl=3600):
    """BTC dominance từ CoinGecko (free). Cache 1h. Trả về float hoặc None."""
    now = time.time()
    if btc_dominance_cache['value'] is not None and now - btc_dominance_cache['ts'] < ttl:
        return btc_dominance_cache['value']
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        async with session.get("https://api.coingecko.com/api/v3/global", headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                dom = data.get('data', {}).get('market_cap_percentage', {}).get('btc')
                if dom is not None:
                    btc_dominance_cache.update({'value': float(dom), 'ts': now})
                    return float(dom)
    except Exception as e:
        logger.warning(f"Lỗi lấy BTC dominance: {e}")
    return btc_dominance_cache['value']


async def get_ai_review(session, digest):
    """Gọi AI tổng quan rủi ro các vị thế đang mở. Trả về text hoặc None."""
    api_key = os.getenv("DASH_TOKEN")
    if not api_key or not digest:
        return None
    model = os.getenv("DASH_MODEL", "claude-sonnet-5")
    url = f"{MINTROUTER_BASE_URL}/chat/completions"
    headers = _ai_headers(api_key)
    system_prompt = (
        "Bạn là quản trị rủi ro giao dịch crypto futures. Dựa trên danh sách vị thế đang mở của khách hàng "
        "(entry, giá mark, PnL, đòn bẩy, khoảng cách tới giá thanh lý, funding), hãy đánh giá tổng quan rủi ro danh mục "
        "và đưa khuyến nghị hành động cụ thể cho từng vị thế (GIỮ / CHỐT MỘT PHẦN / DCA / CẮT LỖ). "
        "Ưu tiên bảo toàn vốn, cảnh báo rõ vị thế gần thanh lý, đòn bẩy cao hoặc đi ngược động lượng. "
        "Trả lời bằng tiếng Việt, tối đa 12 dòng, mỗi dòng một nhận định hoặc khuyến nghị, "
        "bắt đầu mỗi khuyến nghị bằng tên coin. Không dùng các ký tự markdown (*, _, `). Không trả JSON."
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": digest}
        ],
        "temperature": 0.3,
        "max_tokens": 2000
    }
    try:
        timeout = aiohttp.ClientTimeout(total=90)
        async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.warning(f"AI review trả lỗi HTTP {resp.status}: {body[:200]}")
                return None
            data = await resp.json()
            content = data.get('choices', [{}])[0].get('message', {}).get('content', '')
            content = (content or '').strip()
            return content[:3000] if content else None
    except Exception as e:
        logger.warning(f"Lỗi gọi AI review: {e}")
        return None


_MD_HEADER_RE = re.compile(r'^(\s*(?:🤖\s*)?)#{1,6}\s+(.*)$')
_MD_TABLE_ROW_RE = re.compile(r'^\s*\|(.+)\|\s*$')
_MD_TABLE_SEP_RE = re.compile(r'^\s*\|?[\s:\-]+\|[\s:\-|]*\|?\s*$')
_MD_HRULE_RE = re.compile(r'^\s*([-_*])\1{2,}\s*$')


def sanitize_ai_markdown(text):
    """Chuẩn hoá Markdown kiểu GFM mà LLM hay trả về (dù đã dặn không dùng) sang dạng
    Telegram legacy Markdown (parse_mode='Markdown') hiểu được — tránh hiển thị ký tự
    thô như '###', '|---|---|', '**bold**' ra người dùng.
    - Heading '#'..'######' -> bold một dòng.
    - Bảng '| a | b |' + dòng phân cách -> mỗi hàng thành 1 dòng 'header: value · ...'.
    - Đường kẻ ngang '---'/'___'/'***' -> bỏ.
    - Bold GFM '**x**'/'__x__' -> bold Telegram '*x*'."""
    if not text:
        return text
    lines = text.split('\n')
    out = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        m = _MD_HEADER_RE.match(line)
        if m:
            prefix, content = m.groups()
            content = content.strip()
            out.append(f"{prefix}*{content}*" if content else prefix.rstrip())
            i += 1
            continue
        if _MD_HRULE_RE.match(line):
            i += 1
            continue
        m_row = _MD_TABLE_ROW_RE.match(line)
        if m_row and i + 1 < n and _MD_TABLE_SEP_RE.match(lines[i + 1]):
            header_cells = [c.strip() for c in m_row.group(1).split('|')]
            i += 2  # bỏ dòng phân cách '|---|---|'
            while i < n:
                m_data = _MD_TABLE_ROW_RE.match(lines[i])
                if not m_data:
                    break
                data_cells = [c.strip() for c in m_data.group(1).split('|')]
                parts = []
                for h, c in zip(header_cells, data_cells):
                    if not c:
                        continue
                    parts.append(f"{h}: {c}" if h else c)
                if parts:
                    out.append("• " + " · ".join(parts))
                i += 1
            continue
        out.append(line)
        i += 1
    result = "\n".join(out)
    result = re.sub(r'\*\*(.+?)\*\*', r'*\1*', result)
    result = re.sub(r'__(.+?)__', r'*\1*', result)
    return result


# Hàm tạo chữ ký HMAC-SHA256 cho Binance API
def get_binance_signature(query_string, secret_key):
    return hmac.new(
        secret_key.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()


def _signed_url(path, params, api_secret, base="https://fapi.binance.com", method_extra=None):
    """URL Binance đã ký ĐÚNG chuẩn: params percent-encode trước rồi mới ký (Binance verify
    trên chuỗi đã encode — ký chuỗi thô sẽ vỡ với symbol chữ TQ như 我踏马来了USDT → -1022).
    Trả về yarl.URL(encoded=True) để aiohttp KHÔNG encode lại lần nữa (lệch chữ ký).
    params: dict, thứ tự chèn giữ nguyên khi tạo dict."""
    p = dict(params)
    p['timestamp'] = int(time.time() * 1000)
    p.setdefault('recvWindow', 10000)
    query = urlencode(p)
    sig = get_binance_signature(query, api_secret)
    return yarl.URL(f"{base}{path}?{query}&signature={sig}", encoded=True)

# Gửi tin nhắn Telegram
# Mốc thời gian (epoch) đến khi hết cửa sổ flood 429 của Telegram — mọi send chia sẻ chung,
# tránh việc đè thêm request khi đang bị khóa và không làm mất tin nhắn.
_telegram_flood_until = 0.0
_sent_msg_ids: dict[int, list[int]] = {}  # chat_id → [message_id, ...] cho /clear
SENT_MSG_IDS_FILE = "sent_msg_ids_trading.json"


def _load_sent_msg_ids():
    global _sent_msg_ids
    try:
        if os.path.exists(SENT_MSG_IDS_FILE):
            with open(SENT_MSG_IDS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            _sent_msg_ids = {int(k): [int(x) for x in v] for k, v in data.items()}
            total = sum(len(v) for v in _sent_msg_ids.values())
            logger.info(f"Đã nạp {total} message_id từ {SENT_MSG_IDS_FILE}.")
    except Exception as e:
        logger.error(f"Lỗi nạp {SENT_MSG_IDS_FILE}: {e}")


def _save_sent_msg_ids():
    try:
        data = {str(k): v for k, v in _sent_msg_ids.items()}
        with open(SENT_MSG_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Lỗi lưu {SENT_MSG_IDS_FILE}: {e}")


def _remember_msg_id(chat_id, mid):
    if not mid:
        return
    ids = _sent_msg_ids.setdefault(chat_id, [])
    if ids and ids[-1] == mid:
        return
    ids.append(mid)
    if len(ids) > 5000:
        del ids[:-5000]
    if len(ids) % 25 == 0:
        _save_sent_msg_ids()

# ─── Lệnh /model: list model + giá MintRouter, bấm chọn → đổi .env + restart ───
MINT_MODEL_PRICES = {
    'claude-fable-5-1': (2.90, 14.50), 'claude-fable-5': (2.90, 14.50),
    'gpt-6-astra': (2.90, 14.50),
    'claude-opus-5': (1.45, 7.25), 'claude-opus-4-8': (0.69, 3.47),
    'claude-opus-4-7': (0.69, 3.47), 'claude-opus-4-6': (0.69, 3.47),
    'claude-sonnet-5': (0.28, 1.39), 'claude-sonnet-5-500k': (0.28, 1.39),
    'claude-sonnet-4-6': (0.42, 2.08), 'claude-sonnet-4-6-500k': (0.42, 2.08),
    'claude-haiku-4-5': (0.14, 0.69),
    'gpt-5.6-luna': (0.028, 0.17), 'gpt-5.6-sol': (0.56, 2.78), 'gpt-5.6-terra': (0.28, 1.67),
    'gpt-5.5': (0.69, 4.17),
    'glm-5.3': (0.41, 1.28), 'glm-5.2': (0.41, 1.28),
    'gemini-3.8-flash': (0.16, 0.81), 'gemini-3.7-flash': (0.16, 0.81),
    'gemini-3.1-pro-preview': (0.28, 1.67),
    'grok4.6': (0.43, 1.30), 'grok4.5': (0.43, 1.30),
    'kimi-k3': (0.87, 4.35), 'kimi-k2.7': (0.28, 1.16),
}
MINT_MODEL_LABELS = {
    'claude-fable-5-1': 'Fable 5.1', 'claude-fable-5': 'Fable 5', 'gpt-6-astra': 'GPT-6 Astra',
    'claude-opus-5': 'Opus 5', 'claude-opus-4-8': 'Opus 4.8', 'claude-opus-4-7': 'Opus 4.7',
    'claude-sonnet-5': 'Sonnet 5', 'claude-sonnet-5-500k': 'Sonnet 5 (500K)',
    'claude-sonnet-4-6': 'Sonnet 4.6', 'claude-sonnet-4-6-500k': 'Sonnet 4.6 (500K)',
    'claude-haiku-4-5': 'Haiku 4.5',
    'gpt-5.6-luna': 'GPT-5.6 Luna', 'gpt-5.6-sol': 'GPT-5.6 Sol', 'gpt-5.6-terra': 'GPT-5.6 Terra',
    'gpt-5.5': 'GPT-5.5', 'glm-5.3': 'GLM 5.3', 'glm-5.2': 'GLM 5.2',
    'gemini-3.8-flash': 'Gemini 3.8 Flash', 'gemini-3.7-flash': 'Gemini 3.7 Flash',
    'gemini-3.1-pro-preview': 'Gemini 3.1 Pro',
    'grok4.6': 'Grok 4.6', 'grok4.5': 'Grok 4.5', 'kimi-k3': 'Kimi K3', 'kimi-k2.7': 'Kimi K2.7',
}
# Model ưu tiên hiển thị trước (phổ biến + giá tốt)
MINT_MODEL_ORDER = [
    'glm-5.3', 'claude-sonnet-5', 'claude-fable-5-1', 'gpt-5.6-luna', 'gpt-6-astra',
    'claude-opus-5', 'gpt-5.6-sol', 'gpt-5.6-terra', 'gemini-3.8-flash', 'grok4.6',
    'kimi-k3', 'claude-haiku-4-5', 'gpt-5.5', 'glm-5.2', 'gpt-5.6-terra',
    'gemini-3.7-flash', 'gemini-3.1-pro-preview', 'grok4.5', 'kimi-k2.7',
    'claude-fable-5', 'claude-opus-4-8', 'claude-opus-4-7', 'claude-opus-4-6',
    'claude-sonnet-4-6', 'claude-sonnet-5-500k', 'claude-sonnet-4-6-500k',
    'claude-opus-4-7-500k', 'claude-opus-4-6-500k',
]

MODEL_PAGE_SIZE = 8
model_page_state = {}  # chat_id -> page hiện tại


async def fetch_available_models(session):
    """Danh sách model khả dụng từ /v1/models. Trả về list id, fallback về MINT_MODEL_ORDER."""
    api_key = os.getenv("DASH_TOKEN")
    if not api_key:
        return list(MINT_MODEL_ORDER)
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with session.get("https://api.mintrouter.ai/v1/models",
                               headers={"Authorization": f"Bearer {api_key}"}, timeout=timeout) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                ids = [m.get('id') for m in (data.get('data') or []) if m.get('id')]
                if ids:
                    return ids
    except Exception:
        pass
    return list(MINT_MODEL_ORDER)


def _set_dash_model_env(model_id):
    """Sửa DASH_MODEL trong .env (gitignored). Trả về True nếu thành công."""
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
        with open(env_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        found = False
        for i, line in enumerate(lines):
            if line.startswith('DASH_MODEL='):
                lines[i] = f"DASH_MODEL={model_id}\n"
                found = True
                break
        if not found:
            lines.append(f"DASH_MODEL={model_id}\n")
        with open(env_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        return True
    except Exception as e:
        logger.error(f"Lỗi ghi .env DASH_MODEL: {e}")
        return False


def _restart_bot_service():
    """Restart service pnlbot (bot chạy user ubuntu, sudo NOPASSWD)."""
    import subprocess
    for cmd in (['sudo', 'systemctl', 'restart', 'pnlbot'],
                ['systemctl', 'restart', 'pnlbot']):
        try:
            r = subprocess.run(cmd, capture_output=True, timeout=30)
            if r.returncode == 0:
                return True
            logger.warning(f"Restart bot thất bại: {' '.join(cmd)} → rc={r.returncode} {r.stderr[:120]}")
        except Exception as e:
            logger.warning(f"Lỗi restart bot: {e}")
    return False


async def get_front_model_pricing(session):
    """Giá model MỚI NHẤT từ /v0/front/models/pricing (cần session cookie + UA trình duyệt).
    Trả về dict {model_id: (price_in_1M, price_out_1M, display_name)} hoặc None khi fail.
    Cache 30 phút — giá ít khi đổi, không spam API."""
    now = time.time()
    if (MODEL_PRICING_CACHE['data'] is not None
            and now - MODEL_PRICING_CACHE['ts'] < 1800):
        return MODEL_PRICING_CACHE['data']
    cookies = _load_front_session()
    url = "https://api.mintrouter.ai/v0/front/models/pricing"
    for attempt in (1, 2):
        if not cookies:
            cookies = await _front_login(session)
            if not cookies:
                return None
        cookie_hdr = "; ".join(f"{k}={v}" for k, v in cookies.items())
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with session.get(url, headers={
                "Cookie": cookie_hdr,
                "Origin": "https://mintrouter.ai",
                "Referer": "https://mintrouter.ai/models",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
            }, timeout=timeout) as resp:
                if resp.status == 401 and attempt == 1:
                    cookies = None  # session hết hạn → login lại
                    continue
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                items = (data.get('per_token') if isinstance(data, dict) else None) or []
                prices = {}
                for it in items:
                    mid = it.get('model')
                    pi, po = it.get('price_input_token'), it.get('price_output_token')
                    if mid and pi is not None and po is not None:
                        prices[mid] = (float(pi), float(po), it.get('display_name') or mid)
                if prices:
                    MODEL_PRICING_CACHE['data'] = prices
                    MODEL_PRICING_CACHE['groups'] = {it.get('model'): it.get('group_name') or 'Khác'
                                                     for it in items if it.get('model')}
                    MODEL_PRICING_CACHE['ts'] = now
                    return prices
                return None
        except Exception:
            if attempt == 1:
                cookies = None
                continue
            return None
    return None


def _model_provider(mid, live_prices):
    """Provider của model: lấy group_name từ pricing API, fallback theo prefix id."""
    if live_prices and mid in live_prices:
        return None  # provider thật sẽ lấy từ API bên dưới
    low = mid.lower()
    if low.startswith('claude'):
        return 'Claude'
    if low.startswith(('gpt', 'o1', 'o3')):
        return 'OpenAI'
    if low.startswith('gemini'):
        return 'Gemini'
    if low.startswith('glm'):
        return 'GLM'
    if low.startswith(('grok', 'xai')):
        return 'xAI'
    if low.startswith('kimi'):
        return 'Kimi'
    return 'Khác'


def _build_model_sections(available, live_prices):
    """Gom model theo provider, trả về list [(provider, [model_id...])] theo thứ tự đẹp."""
    # provider theo API (per_token có 'group'), fallback theo prefix
    prov_map = {}
    if live_prices:
        for mid, (_pi, _po, _dn) in live_prices.items():
            prov_map[mid] = None  # điền bên dưới từ dữ liệu API gốc
    # Lấy group từ cache raw nếu có
    raw_groups = MODEL_PRICING_CACHE.get('groups') or {}
    sections = {}
    for mid in available:
        if 'free' in mid:
            continue
        prov = raw_groups.get(mid) or _model_provider(mid, live_prices) or 'Khác'
        sections.setdefault(prov, []).append(mid)
    order = ['Claude', 'OpenAI', 'Gemini', 'GLM', 'MintRouter', 'xAI', 'Kimi', 'Nemotron', 'MiMo', 'OpenCode', 'Khác']
    def sort_key(prov):
        return (order.index(prov) if prov in order else len(order), prov)
    result = []
    for prov in sorted(sections.keys(), key=sort_key):
        mids = sections[prov]
        # Trong cùng provider: model ưu tiên theo MINT_MODEL_ORDER trước
        mids.sort(key=lambda m: (MINT_MODEL_ORDER.index(m) if m in MINT_MODEL_ORDER else 999, m))
        result.append((prov, mids))
    return result


def _fmt_price(p):
    """$0.14 → 2 số lẻ; giá < 0.1 (Luna 0.028) → 3 số lẻ cho chính xác."""
    return f"${p:.3f}" if p < 0.1 else f"${p:.2f}"


def _model_button_label(mid, current, live_prices):
    """Nhãn nút chọn model: CHỈ TÊN (giá đã hiện trong text bên trên).
    Dấu ✅ (model đang dùng) đặt cuối tên."""
    if live_prices and mid in live_prices:
        name = live_prices[mid][2] or mid
    else:
        name = MINT_MODEL_LABELS.get(mid, mid)
    if mid == current:
        name = f"{name} ✅"
    return name


async def handle_model_command(session, chat_id):
    """Lệnh /model: list model theo provider + giá MintRouter, inline keyboard chọn → đổi DASH_MODEL + restart bot."""
    available = await fetch_available_models(session)
    current = os.getenv("DASH_MODEL", "claude-sonnet-5")
    live_prices = await get_front_model_pricing(session)
    sections = _build_model_sections(available, live_prices)
    # Flat list theo thứ tự section để phân trang
    flat = [(prov, m) for prov, mids in sections for m in mids]
    model_page_state[chat_id] = 0
    pages = max(1, -(-len(flat) // MODEL_PAGE_SIZE))
    price_note = "giá mới nhất từ MintRouter" if live_prices else "giá tham khảo"
    page_models = flat[:MODEL_PAGE_SIZE]
    text = _render_model_page(current, pages, 0, price_note, page_models, live_prices)
    kb_rows = [[{"text": _model_button_label(m, current, live_prices), "callback_data": f"setmodel:{m}"}]
               for _prov, m in page_models]
    kb_rows.append([{"text": "➡️ Trang sau", "callback_data": "modelpage:1"}])
    await send_telegram_message(
        session, chat_id,
        text,
        reply_markup={"inline_keyboard": kb_rows}
    )


def _render_model_page(current, pages, page, price_note, page_models, live_prices):
    """Text trang /model: nhóm model cùng provider vào 1 mục có tiêu đề."""
    lines = [f"🤖 *MODEL AI HIỆN TẠI: {current}*", f"→ Trang {page + 1}/{pages} — {price_note} ($/1M in/out):", ""]
    last_prov = None
    for prov, m in page_models:
        if prov != last_prov:
            lines.append(f"━━ {prov} ━━")
            last_prov = prov
        pi = po = None
        if live_prices and m in live_prices:
            pi, po, _dn = live_prices[m]
        elif m in MINT_MODEL_PRICES:
            pi, po = MINT_MODEL_PRICES[m]
        name = (live_prices.get(m, (0, 0, m))[2] if live_prices and m in live_prices
                else MINT_MODEL_LABELS.get(m, m))
        mark = "✅" if m == current else "•"
        if pi is not None:
            lines.append(f"{mark} {name} — ${pi:g}/${po:g}")
        else:
            lines.append(f"{mark} {name}")
    return "\n".join(lines)


async def handle_model_callback(session, chat_id, cb_data, message_id=None, answer_cb=None):
    """Callback cho /model: đổi trang hoặc chọn model → ghi .env → restart bot."""
    action, _, arg = cb_data.partition(':')
    if action == 'modelpage':
        try:
            page = int(arg)
        except ValueError:
            return
        available = await fetch_available_models(session)
        current = os.getenv("DASH_MODEL", "claude-sonnet-5")
        live_prices = await get_front_model_pricing(session)
        sections = _build_model_sections(available, live_prices)
        flat = [(prov, m) for prov, mids in sections for m in mids]
        pages = max(1, -(-len(flat) // MODEL_PAGE_SIZE))
        page = max(0, min(page, pages - 1))
        model_page_state[chat_id] = page
        chunk = flat[page * MODEL_PAGE_SIZE:(page + 1) * MODEL_PAGE_SIZE]
        kb_rows = [[{"text": _model_button_label(m, current, live_prices), "callback_data": f"setmodel:{m}"}]
                   for _prov, m in chunk]
        nav = []
        if page > 0:
            nav.append({"text": "⬅️ Trước", "callback_data": f"modelpage:{page - 1}"})
        if page < pages - 1:
            nav.append({"text": "➡️ Sau", "callback_data": f"modelpage:{page + 1}"})
        if nav:
            kb_rows.append(nav)
        price_note = "giá mới nhất từ MintRouter" if live_prices else "giá tham khảo"
        text = _render_model_page(current, pages, page, price_note, chunk, live_prices)
        if message_id:
            await edit_telegram_message(session, chat_id, message_id, text, reply_markup={"inline_keyboard": kb_rows})
        else:
            await send_telegram_message(session, chat_id, text, reply_markup={"inline_keyboard": kb_rows})
        return
    if action == 'setmodel':
        model_id = arg.strip()
        if not model_id or 'free' in model_id:
            if answer_cb:
                await answer_cb("Model free không dùng được làm main")
            return
        if answer_cb:
            await answer_cb(f"Đổi sang {model_id} — đang restart bot...")
        ok = _set_dash_model_env(model_id)
        if not ok:
            await send_telegram_message(session, chat_id, f"❌ Ghi .env thất bại — không đổi được model.")
            return
        await send_telegram_message(session, chat_id,
            f"🔄 *Đã đổi DASH_MODEL → {model_id}*\nĐang restart bot... (bot sẽ tự mở lại sau ~10 giây)")
        def _do_restart():
            _restart_bot_service()
        await asyncio.get_running_loop().run_in_executor(None, _do_restart)


def _strip_md_chars(text):
    """Bỏ sạch ký tự Markdown thô (*, `) khi phải gửi plain (fallback parse fail)."""
    if not text:
        return text
    text = text.replace('**', '')
    text = text.replace('*', '')
    text = text.replace('`', '')
    text = re.sub(r'^#{1,6}\s*', '', text, flags=re.MULTILINE)
    return text


async def send_telegram_message(session, chat_id, text, is_auto=False, reply_to=None, reply_markup=None):
    global _telegram_flood_until
    if not is_auto:
        has_new_activity[chat_id] = True
    # Telegram giới hạn 4096 ký tự/tin: chia nhỏ gửi tiếp các phần (tránh tin dài bị rơi im lặng)
    if len(text) > 4000:
        chunks = []
        cur = ""
        for line in text.split("\n"):
            if len(cur) + len(line) + 1 > 3900:
                if cur:
                    chunks.append(cur)
                cur = line[:3900]
            else:
                cur = f"{cur}\n{line}" if cur else line
        if cur:
            chunks.append(cur)
        sent_id = None
        for i, chunk in enumerate(chunks[:4]):
            kb = reply_markup if i == 0 else None
            sent_id = await send_telegram_message(session, chat_id, chunk,
                                                  is_auto=is_auto, reply_to=reply_to, reply_markup=kb)
            if len(chunks) > 4 and i == 3:
                await send_telegram_message(session, chat_id, "... (tin quá dài, còn lại bị lược bớt)", is_auto=True)
        return sent_id
    token = os.getenv("TELEGRAM_BOT_TOKEN_TRADING")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "allow_sending_without_reply": True
    }
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    if reply_markup:
        payload["reply_markup"] = reply_markup
    max_attempts = 3
    for attempt in range(max_attempts):
        # Nếu Telegram đang flood (429 trước đó bảo chờ), chờ hết cửa sổ mới gửi —
        # tránh đè thêm request làm khóa lâu hơn và không mất tin nhắn.
        wait = _telegram_flood_until - time.time()
        if wait > 0:
            logger.warning(f"Telegram đang flood: chờ {wait:.0f}s (tin nhắn giữ trong hàng đợi, không mất)")
            await asyncio.sleep(min(wait, 30))
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    mid = data.get('result', {}).get('message_id')
                    if mid:
                        _remember_msg_id(chat_id, mid)
                    return mid
                # Telegram trả 429 (rate limit): ghi nhớ cửa sổ flood, chờ retry_after rồi thử lại
                if resp.status == 429 and attempt < max_attempts - 1:
                    try:
                        err = await resp.json()
                        retry_after = int(err.get('parameters', {}).get('retry_after', 1))
                    except Exception:
                        retry_after = 1
                    _telegram_flood_until = max(_telegram_flood_until, time.time() + retry_after)
                    logger.warning(f"Telegram 429 rate limit: thử lại sau {retry_after}s (lần {attempt + 1}/{max_attempts})")
                    await asyncio.sleep(min(retry_after, 30))
                    continue
                # Lỗi parse markdown (400): bỏ parse_mode + strip ký tự md thô để tin nhắn không bị mất
                if resp.status == 400 and 'parse_mode' in payload:
                    payload.pop('parse_mode')
                    payload['text'] = _strip_md_chars(payload['text'])
                    continue
                body = await resp.text()
                # Chat block bot / không tồn tại → gỡ khỏi mọi danh sách auto để không retry spam mỗi phút
                if resp.status == 403 or 'bot was blocked' in body.lower() or 'chat not found' in body.lower():
                    logger.warning(f"Chat {chat_id} block bot/không tồn tại — gỡ khỏi auto chats.")
                    auto_chats.discard(chat_id)
                    auto_pnl_chats.discard(chat_id)
                    active_chats.discard(chat_id)
                    last_auto_messages.pop(chat_id, None)
                    last_auto_pnl_messages.pop(chat_id, None)
                    save_auto_chats()
                    save_auto_pnl_chats()
                    save_active_chats()
                logger.error(f"Lỗi gửi tin nhắn Telegram: HTTP {resp.status} - {body}")
                return None
        except Exception as e:
            logger.error(f"Lỗi khi gửi tin nhắn Telegram: {e}")
            return None
    return None

# Xóa tin nhắn Telegram
async def delete_telegram_message(session, chat_id, message_id):
    token = os.getenv("TELEGRAM_BOT_TOKEN_TRADING")
    url = f"https://api.telegram.org/bot{token}/deleteMessage"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id
    }
    for attempt in range(4):
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    return True
                if resp.status == 429:
                    wait = 3 * (attempt + 1)
                    try:
                        body = await resp.json()
                        retry_after = (body.get('parameters') or {}).get('retry_after', 0)
                        if retry_after:
                            wait = int(retry_after) + 1
                    except Exception:
                        pass
                    logger.warning(f"deleteMessage 429 rate-limit: chờ {wait}s rồi thử lại tin {message_id}")
                    await asyncio.sleep(wait)
                    continue
                body = await resp.text()
                logger.warning(f"Không thể xóa tin nhắn Telegram {message_id}: HTTP {resp.status} - {body}")
                return False
        except Exception as e:
            logger.error(f"Lỗi khi xóa tin nhắn Telegram: {e}")
            await asyncio.sleep(2)
    return False

# Sửa tin nhắn Telegram
async def edit_telegram_message(session, chat_id, message_id, text, reply_markup=None, parse_mode="Markdown"):
    token = os.getenv("TELEGRAM_BOT_TOKEN_TRADING")
    url = f"https://api.telegram.org/bot{token}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
    }
    # parse_mode=None khi text chứa ký tự Markdown không cân bằng (ID, *, emoji...)
    # để tránh lỗi "can't parse entities".
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
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


# Xoá/đổi nút inline trên tin nhắn (editMessageReplyMarkup) — không parse text nên an toàn
async def edit_telegram_reply_markup(session, chat_id, message_id, reply_markup):
    token = os.getenv("TELEGRAM_BOT_TOKEN_TRADING")
    url = f"https://api.telegram.org/bot{token}/editMessageReplyMarkup"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "reply_markup": reply_markup,
    }
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                return True
            body = await resp.text()
            if "message is not modified" not in body:
                logger.warning(f"Không xoá được nút tin nhắn {message_id}: HTTP {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi xoá nút tin nhắn: {e}")
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

# Hàm hủy DCA dùng chung
async def cancel_dca_orders(session, api_key, api_secret, symbol):
    """
    Hủy tất cả các lệnh DCA đang mở của một symbol.
    """
    headers = {"X-MBX-APIKEY": api_key}
    try:
        url = _signed_url('/fapi/v1/openOrders', {'symbol': symbol}, api_secret)

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
                                del_url = _signed_url('/fapi/v1/order', {'symbol': symbol, 'orderId': order_id}, api_secret)

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
                    # Hủy nốt TP/SL điều kiện CÒN LẠI của side vừa đóng (tránh lệnh mồ côi):
                    # TP chạm rồi thì SL vẫn treo và ngược lại.
                    await cancel_existing_tpsl(session, api_key, api_secret, symbol,
                                               position_side=position_side, cancel_tp=True, cancel_sl=True)
                except Exception as e:
                    logger.error(f"Lỗi khi tự động hủy DCA/TP-SL cho {symbol}: {e}")
            else:
                logger.warning(f"Không có session để hủy DCA cho {symbol}")
            
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
    query_string = f"timestamp={timestamp}&recvWindow=10000"
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


async def position_reconcile_loop(app):
    """Mỗi 5 phút: đối chiếu cache `positions` với REST /fapi/v2/positionRisk và sửa lại.
    WS user-data chỉ đẩy DELTA — 1 sự kiện rớt khi reconnect → cache sai/vmissing VĨNH VIỄN,
    làm sai mọi số liệu /pos /pnl /risk, auto-trader guard và quantity khi đóng lệnh."""
    await asyncio.sleep(90)
    while True:
        try:
            session = app['session']
            data, err = await get_position_risk(session)
            if not err and isinstance(data, list):
                fresh_keys = set()
                for p in data:
                    try:
                        amt = float(p.get('positionAmt', 0) or 0)
                    except (TypeError, ValueError):
                        continue
                    if amt == 0.0:
                        continue
                    symbol = p.get('symbol')
                    pside = p.get('positionSide', 'BOTH')
                    key = f"{symbol}_{pside}"
                    fresh_keys.add(key)
                    entry = float(p.get('entryPrice', 0) or 0)
                    mark = float(p.get('markPrice', 0) or 0)
                    lev = int(float(p.get('leverage', 1) or 1))
                    cached = positions.get(key)
                    if cached:
                        try:
                            cached_amt = float(cached.get('positionAmt', 0) or 0)
                        except (TypeError, ValueError):
                            cached_amt = None
                        if cached_amt == amt and float(cached.get('entryPrice', 0) or 0) == entry:
                            if mark > 0:
                                cached['markPrice'] = mark
                            continue
                    side_sign = -1 if (pside == 'SHORT' or amt < 0) else 1
                    unrealized = (mark - entry) * abs(amt) * side_sign if (mark > 0 and entry > 0) else 0.0
                    old = positions.get(key, {})
                    positions[key] = {
                        'symbol': symbol, 'positionSide': pside, 'positionAmt': amt,
                        'entryPrice': entry,
                        'markPrice': mark or old.get('markPrice', entry),
                        'unrealizedPnL': unrealized if unrealized else old.get('unrealizedPnL', 0.0),
                        'leverage': lev,
                        'fundingRate': old.get('fundingRate', 0.0),
                    }
                    logger.info(f"[RECONCILE] Sửa/nạp vị thế {key} (size {amt}, entry {entry})")
                    await subscribe_mark_price(symbol)
                # Vị thế stale trong cache nhưng Binance không còn → xóa + dọn TP/SL mồ côi
                for key in list(positions.keys()):
                    if key not in fresh_keys:
                        p = positions[key]
                        symbol = p['symbol']
                        await update_position_cache(symbol, p.get('positionSide', 'BOTH'), 0.0,
                                                    float(p.get('entryPrice', 0) or 0),
                                                    int(p.get('leverage', 1) or 1), session=session)
                        logger.info(f"[RECONCILE] Dọn vị thế stale {key}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Lỗi trong position_reconcile_loop: {e}")
        await asyncio.sleep(300)

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

                            # Dọn dẹp cache nếu lệnh kết thúc bằng cách khác (CANCELED/EXPIRED/REJECTED)
                            if status in ('CANCELED', 'EXPIRED', 'REJECTED', 'EXPIRED_IN_MATCHING_ENGINE'):
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
async def _update_auto_chat_message(session, chat_id, message, last_messages):
    """Gửi hoặc sửa tin nhắn auto cho 1 chat (dùng chung cho vị thế và tổng PnL)."""
    # Đang chat với AI (hoặc mới chat xong): tạm im lặng, không chen ngang
    if ai_active_until.get(chat_id, 0) > time.time():
        return
    old_msg_id = last_messages.get(chat_id)

    # Nếu có hoạt động mới trong chat, xóa tin nhắn cũ và gửi tin mới xuống dưới cùng
    if has_new_activity.get(chat_id, True):
        if old_msg_id:
            await delete_telegram_message(session, chat_id, old_msg_id)
        new_msg_id = await send_telegram_message(session, chat_id, message, is_auto=True)
        if new_msg_id:
            last_messages[chat_id] = new_msg_id
            has_new_activity[chat_id] = False
    else:
        # Nếu không có hoạt động mới, chỉnh sửa trực tiếp tin nhắn cũ
        if old_msg_id:
            edited_msg_id = await edit_telegram_message(session, chat_id, old_msg_id, message)
            if edited_msg_id:
                last_messages[chat_id] = edited_msg_id
            else:
                new_msg_id = await send_telegram_message(session, chat_id, message, is_auto=True)
                if new_msg_id:
                    last_messages[chat_id] = new_msg_id
                    has_new_activity[chat_id] = False
        else:
            new_msg_id = await send_telegram_message(session, chat_id, message, is_auto=True)
            if new_msg_id:
                last_messages[chat_id] = new_msg_id
                has_new_activity[chat_id] = False


async def auto_pos_sender_loop(app):
    while True:
        try:
            await asyncio.sleep(60)
            session = app['session']

            # 1. Cập nhật bảng vị thế cho các chat đã bật /auto không tham số
            position_chat_ids = auto_chats.difference(auto_price_chats)
            if position_chat_ids and positions:
                message = build_positions_text()
                await asyncio.gather(*(_update_auto_chat_message(session, cid, message, last_auto_messages)
                                       for cid in list(position_chat_ids)), return_exceptions=True)

            # 2. Cập nhật giá cho từng danh sách coin đã bật bằng /auto <coin...>
            if auto_price_chats:
                price_targets = list(auto_price_chats.items())
                price_updates = await asyncio.gather(*(
                    build_auto_prices_text(session, symbols)
                    for _, symbols in price_targets
                ), return_exceptions=True)
                tasks = []
                for (cid, _), message in zip(price_targets, price_updates):
                    if not isinstance(message, Exception):
                        tasks.append(_update_auto_chat_message(
                            session, cid, message, last_auto_price_messages
                        ))
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            # 3. Cập nhật TỔNG PNL cho các chat đã bật /autopnl
            if auto_pnl_chats:
                pnl_msg = build_pnl_summary_text()
                await asyncio.gather(*(_update_auto_chat_message(session, cid, pnl_msg, last_auto_pnl_messages)
                                       for cid in list(auto_pnl_chats)), return_exceptions=True)
            save_auto_chats()
            save_auto_pnl_chats()
        except asyncio.CancelledError:
            logger.info("Task tự động gửi vị thế/giá đã bị hủy.")
            raise
        except Exception as e:
            logger.error(f"Lỗi trong auto_pos_sender_loop (loop tiếp tục): {e}")
            await asyncio.sleep(5)


async def build_auto_prices_text(session, symbols):
    results = await get_coin_prices(session, symbols)
    now_str = datetime.now(timezone(timedelta(hours=7))).strftime("%d/%m/%Y %H:%M:%S")
    lines = [f"🕒 *GIÁ TỰ ĐỘNG* — `{now_str}`"]
    for coin_name, info in results:
        if info is None:
            lines.append(f"{display_symbol(coin_name)}: Không tìm thấy")
            continue
        change = info['change']
        emoji = "🟢" if change >= 0 else "🔴"
        lines.append(
            f"{display_symbol(coin_name)}: *{format_price(info['price'])}* "
            f"({emoji} {change:+.2f}%){funding_str(info.get('funding_rate', 0.0))}"
        )
    return "\n".join(lines)


# /auto không tham số theo dõi vị thế; /auto <coin...> theo dõi giá
async def handle_auto_command(session, chat_id, coin_names=None):
    coin_names = coin_names or []
    if coin_names:
        if len(coin_names) == 1 and coin_names[0].lower() == 'off':
            was_enabled = chat_id in auto_price_chats or chat_id in auto_chats
            auto_price_chats.pop(chat_id, None)
            auto_chats.discard(chat_id)
            old_message_ids = (
                last_auto_price_messages.pop(chat_id, None),
                last_auto_messages.pop(chat_id, None),
            )
            for old_msg_id in old_message_ids:
                if old_msg_id:
                    await delete_telegram_message(session, chat_id, old_msg_id)
            save_auto_chats()
            message = "❌ Đã tắt mọi cập nhật tự động." if was_enabled else "ℹ️ Tự động cập nhật chưa được bật."
            await send_telegram_message(session, chat_id, message)
            return

        symbols = []
        for coin in coin_names[:AUTO_PRICE_MAX_SYMBOLS]:
            cleaned = coin.strip(',.;:!?()[]').upper()
            if not re.fullmatch(r'[A-Z0-9]{2,16}', cleaned):
                await send_telegram_message(session, chat_id, f"❌ Tên coin không hợp lệ: `{coin}`")
                return
            symbol = cleaned if cleaned.endswith('USDT') else f"{cleaned}USDT"
            if symbol not in symbols:
                symbols.append(symbol)
        results = await get_coin_prices(session, symbols)
        missing = [display_symbol(symbol) for symbol, info in results if info is None]
        if missing:
            await send_telegram_message(
                session, chat_id, f"❌ Không tìm thấy trên Binance Futures: `{', '.join(missing)}`"
            )
            return
        # Theo dõi giá và vị thế là hai chế độ loại trừ nhau.
        auto_chats.discard(chat_id)
        old_position_msg_id = last_auto_messages.pop(chat_id, None)
        if old_position_msg_id:
            await delete_telegram_message(session, chat_id, old_position_msg_id)
        auto_price_chats[chat_id] = symbols
        save_auto_chats()
        await send_telegram_message(
            session, chat_id,
            f"✅ Đã bật tự động cập nhật giá mỗi 1 phút: *{', '.join(map(display_symbol, symbols))}*.\n"
            "Tắt bằng `/auto off`."
        )
        message = await build_auto_prices_text(session, symbols)
        new_msg_id = await send_telegram_message(session, chat_id, message, is_auto=True)
        if new_msg_id:
            last_auto_price_messages[chat_id] = new_msg_id
            has_new_activity[chat_id] = False
            save_auto_chats()
        return

    if chat_id in auto_chats:
        auto_chats.remove(chat_id)
        old_msg_id = last_auto_messages.pop(chat_id, None)
        if old_msg_id:
            await delete_telegram_message(session, chat_id, old_msg_id)
        save_auto_chats()
        await send_telegram_message(session, chat_id, "❌ Đã tắt tự động cập nhật vị thế mỗi 1 phút.")
    else:
        auto_price_chats.pop(chat_id, None)
        old_price_msg_id = last_auto_price_messages.pop(chat_id, None)
        if old_price_msg_id:
            await delete_telegram_message(session, chat_id, old_price_msg_id)
        auto_chats.add(chat_id)
        save_auto_chats()
        await send_telegram_message(session, chat_id, "✅ Đã bật tự động cập nhật vị thế mỗi 1 phút.")
        if positions:
            message = build_positions_text("🔍 *TỰ ĐỘNG CẬP NHẬT VỊ THẾ ĐANG MỞ (1P)*\n----------------------------------")
            new_msg_id = await send_telegram_message(session, chat_id, message, is_auto=True)
            if new_msg_id:
                last_auto_messages[chat_id] = new_msg_id
                has_new_activity[chat_id] = False
                save_auto_chats()
        else:
            await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures nào đang mở.")

# Xử lý lệnh /autopnl
async def handle_auto_pnl_command(session, chat_id):
    if chat_id in auto_pnl_chats:
        auto_pnl_chats.remove(chat_id)

        # Xóa tin nhắn auto PnL cuối cùng nếu có khi tắt chế độ
        old_msg_id = last_auto_pnl_messages.pop(chat_id, None)
        if old_msg_id:
            await delete_telegram_message(session, chat_id, old_msg_id)
        save_auto_pnl_chats()

        await send_telegram_message(session, chat_id, "❌ Đã tắt tự động gửi TỔNG PNL mỗi 1 phút.")
    else:
        auto_pnl_chats.add(chat_id)
        save_auto_pnl_chats()
        await send_telegram_message(session, chat_id, "✅ Đã bật tự động gửi TỔNG PNL mỗi 1 phút.")

        # Gửi luôn tổng PNL hiện tại và lưu message_id làm tin nhắn auto đầu tiên
        new_msg_id = await send_telegram_message(session, chat_id, build_pnl_summary_text(), is_auto=True)
        if new_msg_id:
            last_auto_pnl_messages[chat_id] = new_msg_id
            has_new_activity[chat_id] = False
            save_auto_pnl_chats()

# Đăng ký Webhook với Telegram
async def setup_telegram_webhook(session):
    token = os.getenv("TELEGRAM_BOT_TOKEN_TRADING")
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

def build_pnl_summary_text():
    """Tổng PNL (unrealized) của tất cả vị thế đang mở + số vị thế."""
    total_pnl = sum(pos.get('unrealizedPnL', 0.0) for pos in positions.values())
    return (
        f"📊 *TỔNG PNL VỊ THẾ HIỆN TẠI*\n"
        f"----------------------------------\n"
        f"💰 Trạng thái: {pnl_emoji(total_pnl)} *{fmt_signed(total_pnl)} USDT*\n"
        f"🔥 Vị thế đang mở: *{len(positions)}*"
    )


# Xử lý lệnh /pnl
async def handle_pnl_command(session, chat_id):
    if not positions:
        await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures nào đang mở.")
        return

    await send_telegram_message(session, chat_id, build_pnl_summary_text())

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
    try:
        price = float(price)
    except (TypeError, ValueError):
        return str(price)
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
    query_string = f"timestamp={timestamp}&recvWindow=10000"
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


ORIGIN_LABEL = {
    'auto': '🤖 AI tự vào',
    'ai': '🤖 theo AI chấm',
    'alert': '🔔 theo alert 30p',
}
STATUS_LABEL = {
    'win': '✅ THẮNG',
    'loss': '❌ THUA',
    'expired': '⏰ HẾT HẠN',
    'open': '🔄 ĐANG MỞ',
}


async def handle_kq_command(session, chat_id, filter_arg=None):
    """Lệnh /kq: chỉ lệnh thật có order ID; cảnh báo/giả lập xem riêng qua /stats."""
    fl = (filter_arg or '').strip().lower()
    want_status = None
    want_origin = None
    if fl in ('win', 'thang', 'thắng'):
        want_status = 'win'
    elif fl in ('loss', 'thua', 'thua'):
        want_status = 'loss'
    elif fl in ('open', 'dangmo', 'đang mở'):
        want_status = 'open'
    elif fl in ('auto'):
        want_origin = 'auto'
    elif fl in ('ai'):
        want_origin = 'ai'
    elif fl in ('alert'):
        want_origin = 'alert'

    pool = [s for s in signal_history
            if s.get('execution') and s.get('origin') in ORIGIN_LABEL
            and (want_status is None or s.get('status') == want_status)
            and (want_origin is None or s.get('origin') == want_origin)]
    pool.sort(key=lambda s: s.get('ts', 0), reverse=True)
    if not pool:
        await send_telegram_message(session, chat_id, "ℹ️ Chưa có lệnh nào khớp điều kiện lọc.")
        return

    shown = pool[:20]
    tz_vn = timezone(timedelta(hours=7))
    lines = ["🎯 *KẾT QUẢ LỆNH ĐÃ VÀO THEO AI*", "----------------------------------"]
    for s in shown:
        t_str = datetime.fromtimestamp(s.get('ts', 0), tz=tz_vn).strftime("%d/%m %H:%M")
        disp = s['symbol'][:-4] if s['symbol'].endswith('USDT') else s['symbol']
        ai_sc = f", AI {s['ai_score']:.1f}" if s.get('ai_score') is not None else ""
        lines.append(
            f"{t_str} {disp} {s['side']} @{format_price(s.get('entry', 0))} "
            f"{ORIGIN_LABEL.get(s.get('origin'), '')} → {STATUS_LABEL.get(s.get('status'), s.get('status'))} "
            f"(điểm {s.get('score', 0):.1f}{ai_sc})"
            + (f", net {s['net_pnl']:+.2f} USDT" if _verified_execution(s) else ', chờ đối soát PnL')
        )
    # Tổng kết cả pool (không chỉ 20 lệnh hiển thị)
    cnt = {'win': 0, 'loss': 0, 'be': 0, 'expired': 0, 'open': 0}
    for s in pool:
        st = s.get('status')
        if st in cnt:
            cnt[st] += 1
    decided = cnt['win'] + cnt['loss'] + cnt['be']
    total_n = len(pool)
    wr_txt = f"{cnt['win']}/{decided} ({cnt['win'] / decided * 100:.0f}%)" if decided else "—"
    lines.append("----------------------------------")
    lines.append(f"📈 Tổng: {total_n} lệnh | Thắng {wr_txt} | Hết hạn {cnt['expired']} | Đang mở {cnt['open']}")
    lines.append("💵 Tiền lời/lỗ thực tế từng lệnh: /history")
    await send_telegram_message(session, chat_id, "\n".join(lines))


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
    params = {
        'incomeType': 'REALIZED_PNL',
        'limit': 100,
        'timestamp': timestamp,
        'recvWindow': 10000,
    }
    if symbol:
        params['symbol'] = symbol

    url = _signed_url('/fapi/v1/income', params, api_secret)
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
    query_string = f"timestamp={timestamp}&recvWindow=10000"
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
    """Quy đổi điểm số thành nhãn độ tin cậy (dùng khi điểm bị điều chỉnh sau analyze).
    Ngưỡng 'Mạnh' = 5.0 theo backtest (bucket 4.5-5.0 chỉ win ~51%, ngang tung đồng xu)."""
    if score >= 6.0:
        return 'Rất mạnh'
    if score >= 5.0:
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
    up_move = df['high'].diff()
    down_move = -df['low'].diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
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
    
    # ── RSI (max 1.5đ) — MOMENTUM, thay vì mean-reversion ──
    # Dữ liệu 246 tín hiệu 15 coin: RSI quá bán/quá mua (mean-reversion) thua
    # (30-40: 25%, >=70: 33%), RSI đà vừa phải thắng (50-70: 67-73%).
    # => ĐẢO DẤU: cực đoan = phạt, đà vừa phải = thưởng nhẹ.
    if rsi_val <= 25:
        long_score -= 1.5          # cực oversold = đang rơi, bắt dao
    elif rsi_val <= 35:
        long_score -= 0.8
    elif rsi_val <= 45:
        long_score += 0.3
    elif rsi_val >= 75:
        short_score -= 1.5         # cực overbought = cản tàu
    elif rsi_val >= 65:
        short_score -= 0.8
    elif rsi_val >= 55:
        short_score += 0.3
        
    # ── Stochastic RSI (max 1.0đ) — MOMENTUM ──
    # Dữ liệu: 0-20 (42%), 60-80 (60%), >=80 (56%), 20-40 (64%). Cực oversold yếu.
    if stoch_k <= 20 and stoch_d <= 20:
        long_score -= 1.0
    elif stoch_k <= 30:
        long_score -= 0.4
    elif stoch_k >= 80 and stoch_d >= 80:
        short_score -= 1.0
    elif stoch_k >= 70:
        short_score -= 0.4
    # Crossover bonus (momentum vùng trung bình)
    prev_stoch_k = prev['stoch_k']
    prev_stoch_d = prev['stoch_d']
    if stoch_k > stoch_d and prev_stoch_k <= prev_stoch_d and 20 <= stoch_k <= 60:
        long_score += 0.5  # Bullish cross vùng trung bình (không phải oversold)
    elif stoch_k < stoch_d and prev_stoch_k >= prev_stoch_d and 40 <= stoch_k <= 80:
        short_score += 0.5  # Bearish cross vùng trung bình
        
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
        
    # ── Bollinger Bands (max 1.5đ) — MOMENTUM ──
    bb_width = upper_b - lower_b
    bb_pct = (close_price - lower_b) / (bb_width + 1e-10)  # 0 = lower band, 1 = upper band
    # Dữ liệu: chạm biên dưới (<0.2) thua 36.5%, giữa-đỉnh (0.6-0.8) thắng 70.6%.
    if bb_pct <= 0.0:
        long_score -= 1.5  # Chạm/phá biên dưới = bắt dao rơi
    elif bb_pct <= 0.15:
        long_score -= 0.8
    elif bb_pct >= 1.0:
        short_score -= 1.5  # Chạm/phá biên trên = cản tàu
    elif bb_pct >= 0.85:
        short_score -= 0.8

    # ── Confluence Penalty (không thưởng bắt đảo chiều cực đoan) ──
    if bb_pct <= 0.05 and rsi_val <= 30:
        long_score -= 0.8  # Quá bán + Chạm biên dưới = bắt dao rơi
    if bb_pct >= 0.95 and rsi_val >= 70:
        short_score -= 0.8  # Quá mua + Chạm biên trên = cản tàu
        
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
        long_score *= 0.85
        short_score *= 0.85
        
    # ═══ Divergence (tín hiệu đảo chiều) ═══
    # Dữ liệu: bearish divergence thắng 61-65%, bullish chỉ 46-50%.
    # => chỉ giữ bonus cho bearish, bullish không cộng (yếu, không có edge).
    if rsi_div == 'bearish':
        short_score += 1.0
    if macd_div == 'bearish':
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
        
    # ═══ PENALTY: Tín hiệu mâu thuẫn (logic MOMENTUM: RSI cao = động lượng tăng) ═══
    # MACD đang tăng nhưng RSI yếu (hoặc ngược lại) → động lượng chưa đồng thuận, hạ điểm
    macd_bullish = hist_val > 0
    rsi_bullish = rsi_val > 55
    macd_bearish = hist_val < 0
    rsi_bearish = rsi_val < 45
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
        elif long_score >= 5.0:
            confidence = 'Mạnh'
        elif long_score >= 3.0:
            confidence = 'Trung bình'
        else:
            confidence = 'Yếu'
    elif short_score > long_score and short_score >= min_signal_score:
        signal = 'SHORT'
        if short_score >= 6.0:
            confidence = 'Rất mạnh'
        elif short_score >= 5.0:
            confidence = 'Mạnh'
        elif short_score >= 3.0:
            confidence = 'Trung bình'
        else:
            confidence = 'Yếu'
            
    # ═══ TÍNH TP/SL DỰA TRÊN ATR ═══
    tp_price = 0.0
    sl_price = 0.0
    # Giữ RR hiện có; hiệu quả phải đo lại với simulator có phí và expiry MTM.
    rr_ratio = 1.0
    
    # Lấy thông tin làm tròn
    qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)
    
    if signal == 'LONG':
        sl_price = close_price - (atr_val * 1.5)
        if sl_price <= 0 or sl_price >= close_price:
            sl_price = close_price * 0.97
        # Chỉ siết SL về dưới support khi support GẦN (trên ATR SL):
        # support là đáy 50 nến, kéo SL xuống đáy xa làm TP 1:1 viển vông → 27% tín hiệu hết hạn 72h.
        if support > 0 and support < close_price:
            sl_from_support = support - (atr_val * 0.3)
            if sl_from_support > sl_price:
                sl_price = sl_from_support
        # SL không vượt quá 20% giá — TP 1:1 xa hơn thế gần như không bao giờ chạm
        if sl_price < close_price * 0.80:
            sl_price = close_price * 0.80
        risk = close_price - sl_price
        tp_price = close_price + (risk * rr_ratio)
        
    elif signal == 'SHORT':
        sl_price = close_price + (atr_val * 1.5)
        if sl_price <= close_price:
            sl_price = close_price * 1.03
        # Chỉ siết SL về trên resistance khi resistance GẦN (trong 1.5×ATR)
        if resistance > 0 and resistance > close_price:
            sl_from_resistance = resistance + (atr_val * 0.3)
            if sl_from_resistance < sl_price:
                sl_price = sl_from_resistance
        if sl_price > close_price * 1.20:
            sl_price = close_price * 1.20
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
        'price_change_24': price_change_24,
        'signal': signal,
        'confidence': confidence,
        'long_score': long_score,
        'short_score': short_score,
        'tp': tp_price,
        'sl': sl_price
    }


async def get_scan_signals_fresh(session, max_age=300):
    """Lấy tín hiệu quét thị trường: dùng cache nếu còn mới, quét ngoài lock (single-flight).
    Trước đây toàn bộ scan 75 coin × 3 khung chạy TRONG lock → /a và các loop phải chờ cả phút."""
    now = time.time()
    if market_scan_cache["signals"] is not None and now - market_scan_cache["timestamp"] < max_age:
        return market_scan_cache["signals"]
    async with market_scan_cache["lock"]:
        now = time.time()
        if market_scan_cache["signals"] is not None and now - market_scan_cache["timestamp"] < max_age:
            return market_scan_cache["signals"]
        if market_scan_cache.get("scanning"):
            waiter = True
        else:
            market_scan_cache["scanning"] = True
            waiter = False
    if waiter:
        # Có scan đang chạy: chờ tối đa ~3 phút thay vì quét lại tốn request
        start_ts = market_scan_cache.get("timestamp", 0.0)
        for _ in range(90):
            await asyncio.sleep(2)
            if (not market_scan_cache.get("scanning")
                    and market_scan_cache["signals"] is not None
                    and market_scan_cache.get("timestamp", 0.0) > start_ts):
                return market_scan_cache["signals"]
        sigs = market_scan_cache.get("signals")
        return sigs or ([], [])
    try:
        long_signals, short_signals = await scan_market_signals(session)
        async with market_scan_cache["lock"]:
            market_scan_cache["signals"] = (long_signals, short_signals)
            market_scan_cache["timestamp"] = time.time()
            market_scan_cache.pop("scanning", None)
        return long_signals, short_signals
    except Exception:
        async with market_scan_cache["lock"]:
            market_scan_cache.pop("scanning", None)
        raise


async def scan_market_signals(session):
    """
    Quét qua top 75 coin theo volume 24h để tìm cơ hội giao dịch có tỉ lệ thắng cao.
    Sử dụng Semaphore để giới hạn request song song và lọc xu hướng khung 4h (MTF Confluence) để tăng win rate.
    Chỉ trả về các tín hiệu có độ tin cậy từ 4 sao trở lên (Mạnh và Rất mạnh).
    """
    url_ticker = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    # Loại cổ phiếu/hàng hóa giao dịch theo phiên (không 24/7): EMA stack + volume 24h
    # của các symbol này gây tín hiệu rác trong quét (AAPL/MSTR/SOXL/XAU... xuất hiện trong live history)
    SCAN_BLACKLIST = {
        'AAPLUSDT', 'NVDAUSDT', 'MSTRUSDT', 'TSLAUSDT', 'GOOGLUSDT', 'AMZNUSDT', 'METAUSDT',
        'MSFTUSDT', 'COINUSDT', 'NFLXUSDT', 'AVGOUSDT', 'ORCLUSDT', 'PLTRUSDT', 'HOODUSDT',
        'CRCLUSDT', 'SBUXUSDT', 'MCDUSDT', 'DISUSDT', 'BAUSDT', 'INTCUSDT', 'AMDUSDT',
        'SPXUSDT', 'SPYUSDT', 'QQQUSDT', 'DIAUSDT', 'IWMUSDT', 'SOXLUSDT', 'SOXSUSDT',
        'TSLLUSDT', 'TSLQUSDT', 'GOOGUSDT', 'SHOPUSDT', 'ABNBUSDT', 'UBERUSDT',
        'XAUUSDT', 'XAGUSDT', 'BZUSDT', 'CLUSDT', 'NGUSDT', 'GCSIUSDT', 'WTIUSDT', 'XPTUSDT',
    }

    coins_to_scan = []
    try:
        async with session.get(url_ticker, headers=headers) as resp:
            if resp.status == 200:
                tickers = await resp.json()
                usdt_tickers = [t for t in tickers if t['symbol'].endswith('USDT') and t['symbol'] not in SCAN_BLACKLIST]
                # Sắp xếp theo quoteVolume 24h giảm dần
                usdt_tickers.sort(key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
                coins_to_scan = [t['symbol'] for t in usdt_tickers[:75]]
            else:
                body = await resp.text()
                logger.error(f"Lỗi gọi API ticker 24h: {resp.status} - {body}")
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách top 75 volume: {e}")
        
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
            # Ngưỡng 5.0 khớp ngưỡng confidence 'Mạnh' (tín hiệu < 5.0 không qua được cổng)
            if score >= 5.0:
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
        mtf_pass = res_4h is not None and res_1d is not None
        
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
                # Digest phải GIỐNG HỆT với nhánh /a <coin> (đủ 4 khung + funding + orderbook + BTC dominance)
                # để AI cho ra kết luận nhất quán giữa quét và phân tích chi tiết
                res_15m = await analyze_with_sem(res['symbol'], '15m', fetch_extras=False)
                ob = await get_orderbook_summary(session, res['symbol'])
                dom = await get_btc_dominance(session)
                digest = build_ai_digest(res['symbol'],
                                          [("15m", res_15m), ("1h", res), ("4h", res.get('res_4h')), ("1d", res.get('res_1d'))],
                                          oi_change=res.get('oi_change'), taker_ratio=res.get('taker_ratio'),
                                          funding_rate=res.get('funding_rate'), orderbook=ob, btc_dominance=dom)
                res['ai_input'] = digest
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
            ai_score = (verdict or {}).get('long_score' if r['signal'] == 'LONG' else 'short_score')
            opp_ai = (verdict or {}).get('short_score' if r['signal'] == 'LONG' else 'long_score')
            margin = (ai_score - opp_ai) if (ai_score is not None and opp_ai is not None) else None
            # Fallback rule-only nếu AI lỗi/không trả lời; gate chặt khi AI có kết luận độc lập:
            # cùng chiều + AI chấm ≥ 4.5 + cách biệt với chiều ngược ≥ 1.0 (tránh tín hiệu lưng chừng)
            passed = (verdict is not None and verdict.get('direction') == r['signal']
                      and ai_score is not None and ai_score >= 4.5
                      and margin is not None and margin >= 1.0)
            r['ai_gate_passed'] = passed
            record_signal(r, verdict, origin='candidate')
            if verdict is None or passed:
                if r['signal'] == 'LONG':
                    long_signals.append(r)
                else:
                    short_signals.append(r)
            elif verdict is not None:
                if verdict.get('direction') != r['signal']:
                    logger.info(f"AI gate loại bỏ tín hiệu {r['signal']} của {r['symbol']} (AI: {verdict.get('direction')})")
                elif margin is not None and margin < 1.0:
                    logger.info(f"AI gate loại bỏ tín hiệu {r['signal']} của {r['symbol']} "
                                f"(AI chấm {ai_score:.1f} vs ngược {opp_ai:.1f}, cách biệt < 1.0)")
                else:
                    logger.info(f"AI gate loại bỏ tín hiệu {r['signal']} của {r['symbol']} (điểm AI thiếu hoặc dưới ngưỡng)")
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
            rsi_desc = "Quá bán ⚠️ (rủi ro rơi tiếp)" if res['rsi'] <= 30 else ("Quá mua ⚠️ (rủi ro bật lại)" if res['rsi'] >= 70 else "Trung tính")
            
            stoch_str = f"K:{res['stoch_k']:.1f} D:{res['stoch_d']:.1f}"
            stoch_desc = "Quá bán ⚠️" if res['stoch_k'] <= 20 else ("Quá mua ⚠️" if res['stoch_k'] >= 80 else "Trung tính")
            
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
            bb_desc = "Chạm biên dưới ⚠️ (rủi ro rơi tiếp)" if res['bb_pct'] <= 0.05 else ("Chạm biên trên ⚠️ (rủi ro bật lại)" if res['bb_pct'] >= 0.95 else "Trung tính")
            
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
                ob_task = asyncio.create_task(get_orderbook_summary(session, symbol))
                dom_task = asyncio.create_task(get_btc_dominance(session))
                orderbook = await ob_task
                btc_dominance = await dom_task
                digest = build_ai_digest(symbol, [("15m", res_15m), ("1h", res), ("4h", res_4h), ("1d", res_1d)],
                                          oi_change=oi_change, taker_ratio=taker_ratio, funding_rate=funding_rate,
                                          orderbook=orderbook, btc_dominance=btc_dominance)
                ai_verdict = await get_ai_verdict_cached(session, f"ai_{symbol}", digest)
                if ai_verdict:
                    ai_dir = ai_verdict.get('direction', 'NEUTRAL')
                    ai_emoji = "🟩 LONG" if ai_dir == 'LONG' else ("🟥 SHORT" if ai_dir == 'SHORT' else "⬜ NEUTRAL")
                    ai_l = ai_verdict.get('long_score')
                    ai_s = ai_verdict.get('short_score')
                    ai_sc = ""
                    if ai_l is not None and ai_s is not None:
                        ai_sc = f" | AI tự chấm L:`{ai_l:.1f}` S:`{ai_s:.1f}`"
                    msg += (
                        f"\n🤖 *AI phân tích (tự chấm độc lập):*\n"
                        f"👉 AI khuyến nghị: *{ai_emoji}* _({ai_verdict.get('confidence', 'trung bình')})_{ai_sc}\n"
                        f"💬 _{ai_verdict.get('reason', '')}_\n"
                    )
                    for bullet in ai_verdict.get('analysis', [])[:4]:
                        msg += f"• _{bullet}_\n"
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
            
            if res['signal'] != 'NEUTRAL' and res.get('tp') and res.get('sl'):
                record_signal(res, ai_verdict, origin='query')
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
            # Scan ngoài lock (single-flight) — /a không còn chặn các loop và ngược lại
            long_signals, short_signals = await get_scan_signals_fresh(session, max_age=300)
            now_check = time.time()
            if market_scan_cache["signals"] is not None:
                cache_age = int(now_check - market_scan_cache["timestamp"])
            else:
                cache_age = 0
            # Lưu tín hiệu quét mới vào lịch sử để theo dõi win-rate
            if cache_age <= 5:
                for sig_res in list(long_signals) + list(short_signals):
                    record_signal(sig_res, sig_res.get('ai'), origin='scan')
            
            if loading_msg_id:
                await delete_telegram_message(session, chat_id, loading_msg_id)
                
            await _send_scan_results(session, chat_id, long_signals, short_signals, cache_age)
            
        except Exception as e:
            logger.error(f"Lỗi khi quét tín hiệu: {e}")
            if loading_msg_id:
                await delete_telegram_message(session, chat_id, loading_msg_id)
            await send_telegram_message(session, chat_id, f"❌ Lỗi khi quét tín hiệu thị trường: {e}")


async def handle_review_command(session, chat_id):
    """Lệnh /review: AI soi tổng thể các vị thế đang mở, khuyến nghị giữ/chốt/DCA/cắt lỗ."""
    if not os.getenv("DASH_TOKEN"):
        await send_telegram_message(session, chat_id, "⚠️ Chưa cấu hình DASH_TOKEN trong .env — tính năng AI chưa khả dụng.")
        return
    if not positions:
        await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures nào đang mở.")
        return

    loading_msg_id = await send_telegram_message(session, chat_id, "⏳ *[1/2]* Đang lấy dữ liệu vị thế từ Binance...")
    try:
        api_key = os.getenv("BINANCE_API_KEY")
        api_secret = os.getenv("BINANCE_API_SECRET")
        timestamp = int(time.time() * 1000)
        query_string = f"timestamp={timestamp}&recvWindow=10000"
        signature = get_binance_signature(query_string, api_secret)
        url = f"https://fapi.binance.com/fapi/v2/positionRisk?{query_string}&signature={signature}"
        headers = {"X-MBX-APIKEY": api_key}
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.error(f"Lỗi lấy positionRisk cho /review: HTTP {resp.status} - {body}")
                if loading_msg_id:
                    await delete_telegram_message(session, chat_id, loading_msg_id)
                await send_telegram_message(session, chat_id, "❌ Lỗi khi lấy dữ liệu vị thế từ Binance.")
                return
            data = await resp.json()

        open_positions = [p for p in data if float(p.get('positionAmt', 0)) != 0.0]
        if not open_positions:
            if loading_msg_id:
                await delete_telegram_message(session, chat_id, loading_msg_id)
            await send_telegram_message(session, chat_id, "ℹ️ Hiện tại không có vị thế Futures nào đang mở.")
            return

        lines = ["Danh sách vị thế đang mở của tài khoản (Futures):"]
        for p in open_positions:
            amount = float(p.get('positionAmt', 0))
            entry = float(p.get('entryPrice', 0))
            mark = float(p.get('markPrice', 0))
            pnl_pct = ((mark - entry) / entry * 100) if entry > 0 else 0.0
            if amount < 0:
                pnl_pct = -pnl_pct
            liq = float(p.get('liquidationPrice', 0))
            liq_dist = abs(mark - liq) / mark * 100 if mark > 0 and liq > 0 else None
            funding_rate = positions.get(f"{p.get('symbol')}_{p.get('positionSide')}", {}).get('fundingRate', 0.0)

            line = (
                f"- {p.get('symbol')} {pos_side_display(p.get('positionSide'), amount)}: "
                f"entry {format_price(entry)}, mark {format_price(mark)}, PnL {pnl_pct:+.2f}%, "
                f"đòn bẩy {p.get('leverage')}x"
            )
            if liq_dist is not None:
                line += f", cách giá thanh lý {liq_dist:.1f}% (liq {format_price(liq)})"
            if abs(funding_rate) >= 0.005:
                line += f", funding {funding_rate * 100:+.4f}%"
            lines.append(line)
        lines.append("Hãy đánh giá tổng quan rủi ro danh mục và khuyến nghị hành động cho từng vị thế.")

        if loading_msg_id:
            loading_msg_id = await edit_telegram_message(
                session, chat_id, loading_msg_id, f"🤖 *[2/2]* Đã có dữ liệu {len(open_positions)} vị thế. Đang nhờ AI đánh giá..."
            )

        review = await get_ai_review(session, "\n".join(lines))

        if loading_msg_id:
            await delete_telegram_message(session, chat_id, loading_msg_id)
        if review:
            await send_telegram_message(
                session, chat_id,
                f"🤖 *AI REVIEW VỊ THẾ ĐANG MỞ*\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{sanitize_ai_markdown(review)}"
            )
        else:
            await send_telegram_message(session, chat_id, "🤖 AI không phản hồi hoặc lỗi. Vui lòng thử lại sau.")
    except Exception as e:
        logger.error(f"Lỗi khi xử lý lệnh review: {e}")
        if loading_msg_id:
            await delete_telegram_message(session, chat_id, loading_msg_id)
        await send_telegram_message(session, chat_id, f"❌ Đã xảy ra lỗi khi review vị thế: {e}")


async def handle_recalib_command(session, chat_id):
    """Ép AI tự đánh giá lại chiến lược từ lịch sử tín hiệu (bỏ qua cache), rồi báo bài học đã rút ra."""
    loading = await send_telegram_message(session, chat_id, "🤖 Đang để AI rà soát toàn bộ lịch sử tín hiệu và tự đánh giá lại chiến lược...")
    try:
        lessons = await get_ai_lessons(session, force=True)
        resolved = [s for s in signal_history if s.get('status') in ('win', 'loss', 'expired')]
        wins = sum(1 for s in resolved if s['status'] == 'win')
        stats_line = format_signal_stats()
        msg = (
            "🤖 *AI TỰ ĐÁNH GIÁ LẠI CHIẾN LƯỢC*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Bộ nhớ: {len(resolved)} tín hiệu đã kết thúc ({wins} win)."
            + (f"\n{stats_line}" if stats_line else "")
            + ("\n\n📚 *Bài học AI vừa rút ra (tự áp dụng cho các lần chấm sau):*\n" + sanitize_ai_markdown(lessons) if lessons else "\n\n⚠️ Chưa đủ dữ liệu (cần ≥ 5 tín hiệu kết thúc) hoặc AI lỗi — chưa thể đánh giá.")
        )
        if loading:
            await delete_telegram_message(session, chat_id, loading)
        await send_telegram_message(session, chat_id, msg)
    except Exception as e:
        logger.error(f"Lỗi khi ép AI đánh giá lại: {e}")
        if loading:
            await delete_telegram_message(session, chat_id, loading)
        await send_telegram_message(session, chat_id, f"❌ Lỗi khi đánh giá lại: {e}")


async def build_account_context(session):
    """Lấy snapshot read-only tài khoản Futures (số dư + vị thế đang mở) làm ngữ cảnh cho /ai."""
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        return None
    headers = {"X-MBX-APIKEY": api_key}
    lines = []
    try:
        ts = int(time.time() * 1000)
        qs = f"timestamp={ts}&recvWindow=10000"
        sig = get_binance_signature(qs, api_secret)
        async with session.get(f"https://fapi.binance.com/fapi/v2/account?{qs}&signature={sig}", headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                lines.append(
                    f"- Số dư ví: {float(data.get('totalWalletBalance', 0)):,.2f} USDT, "
                    f"PnL chưa thực hiện: {float(data.get('totalUnrealizedProfit', 0)):+,.2f} USDT, "
                    f"Khả dụng: {float(data.get('availableBalance', 0)):,.2f} USDT"
                )
        pos_data, pos_err = await get_position_risk(session)
        if not pos_err and pos_data:
            open_positions = [p for p in pos_data if float(p.get('positionAmt', 0)) != 0.0]
            # Gộp TP/SL điều kiện (Algo Service) theo symbol — bot đặt TP/SL là lệnh riêng
            algo_map = {}
            try:
                adata, aerr = await binance_signed_request(session, 'GET', '/fapi/v1/openAlgoOrders')
                if not aerr and isinstance(adata, list):
                    for o in adata:
                        algo_map.setdefault(o.get('symbol'), []).append(
                            ((o.get('orderType') or '').upper(), o.get('triggerPrice')))
            except Exception:
                pass
            if open_positions:
                lines.append("- Vị thế đang mở:")
                for p in open_positions[:15]:
                    amount = float(p.get('positionAmt', 0))
                    entry = float(p.get('entryPrice', 0))
                    mark = float(p.get('markPrice', 0))
                    pnl = float(p.get('unRealizedProfit', p.get('unrealizedProfit', 0)))
                    liq = float(p.get('liquidationPrice', 0))
                    lev = p.get('leverage')
                    if not lev:
                        cached_pos = positions.get(f"{p.get('symbol')}_{p.get('positionSide')}")
                        lev = (cached_pos or {}).get('leverage') if cached_pos else None
                        if not lev and p.get('positionSide') == 'BOTH':
                            for cp in positions.values():
                                if cp.get('symbol') == p.get('symbol'):
                                    lev = cp.get('leverage')
                                    break
                    line = (
                        f"  · {p.get('symbol')} {pos_side_display(p.get('positionSide'), amount)}: "
                        f"entry {format_price(entry)}, mark {format_price(mark)}, "
                        f"PnL {fmt_signed(pnl)} USDT, đòn bẩy {lev or '?'}x"
                    )
                    if liq > 0:
                        line += f", giá thanh lý {format_price(liq)}"
                    line += format_position_tpsl(p)
                    # Gắn TP/SL algo vào NGAY dòng vị thế để AI không bỏ sót
                    sym_algos = algo_map.get(p.get('symbol'), [])
                    tp_txt = [f"TP {format_price(float(trig))}" for typ, trig in sym_algos
                              if 'TAKE_PROFIT' in typ and _safe_float(trig)]
                    sl_txt = [f"SL {format_price(float(trig))}" for typ, trig in sym_algos
                              if 'STOP' in typ and _safe_float(trig)]
                    if tp_txt or sl_txt:
                        line += " | " + ", ".join(tp_txt + sl_txt)
                    lines.append(line)
        # Lệnh đang chờ: lệnh thường + lệnh TP/SL điều kiện (algo service, gồm cả lệnh đặt từ app)
        oo_data, oo_err = await binance_signed_request(session, 'GET', '/fapi/v1/openOrders')
        if not oo_err and oo_data:
            for o in oo_data[:10]:
                lines.append(f"- Lệnh chờ thường: {o.get('symbol')} {o.get('side')} {o.get('type')} "
                             f"qty {o.get('origQty')} @ {o.get('price')} (orderId {o.get('orderId')})")
        algo_data, algo_err = await binance_signed_request(session, 'GET', '/fapi/v1/openAlgoOrders')
        if not algo_err and algo_data:
            for o in algo_data[:10]:
                lines.append(
                    f"- TP/SL điều kiện: {o.get('symbol')} {o.get('side')} {o.get('orderType')} "
                    f"qty {o.get('quantity')} kích hoạt khi chạm {format_price(o.get('triggerPrice'))} "
                    f"(algoId {o.get('algoId')}, status {o.get('algoStatus')}, "
                    f"reduceOnly={'có' if o.get('reduceOnly') else 'không'})"
                )
    except Exception as e:
        logger.warning(f"Lỗi lấy ngữ cảnh tài khoản cho /ai: {e}")
        return None
    return "\n".join(lines) if lines else None


async def download_telegram_photo(session, photo_sizes, max_bytes=4 * 1024 * 1024):
    """Tải ảnh từ Telegram (chọn size lớn nhất <= max_bytes), trả về (data_url_base64, None) hoặc (None, err)."""
    token = os.getenv("TELEGRAM_BOT_TOKEN_TRADING")
    if not token:
        return None, "Chưa cấu hình TELEGRAM_BOT_TOKEN."
    chosen = None
    for p in reversed(photo_sizes or []):
        if (p.get('file_size') or 0) <= max_bytes:
            chosen = p
            break
    if not chosen:
        return None, "Ảnh quá lớn (giới hạn 4MB cho tính năng AI)."
    try:
        async with session.get(f"https://api.telegram.org/bot{token}/getFile?file_id={chosen['file_id']}") as resp:
            data = await resp.json()
        file_path = (data.get('result') or {}).get('file_path')
        if not file_path:
            return None, "Không lấy được thông tin ảnh từ Telegram."
        async with session.get(f"https://api.telegram.org/file/bot{token}/{file_path}") as resp:
            if resp.status != 200:
                return None, "Không tải được nội dung ảnh từ Telegram."
            raw = await resp.read()
        return f"data:image/jpeg;base64,{base64.b64encode(raw).decode()}", None
    except Exception as e:
        logger.warning(f"Lỗi tải ảnh Telegram: {e}")
        return None, "Lỗi khi tải ảnh."


async def handle_photo_message(session, chat_id, photo_sizes, caption, reply_to=None):
    """Xử lý tin nhắn ảnh (kèm/không kèm caption): tải ảnh rồi đưa cho AI agent phân tích."""
    question = (caption or '').strip()[:1000] or "Phân tích hình ảnh này trong bối cảnh giao dịch crypto của tôi."
    image_url, err = await download_telegram_photo(session, photo_sizes)
    if err:
        await send_telegram_message(session, chat_id, f"❌ {err}", reply_to=reply_to)
        return
    await handle_ai_command(session, chat_id, question, reply_to=reply_to, image_data_url=image_url)


# ─── Usage API của MintRouter.ai (số dư + usage 24h/7 ngày/30 ngày) ───
async def get_go_usage(session):
    """Gọi usage API của MintRouter.ai (key-usage). Trả về (data_dict, None) hoặc (None, err)."""
    api_key = os.getenv("DASH_TOKEN")
    if not api_key:
        return None, "Chưa cấu hình DASH_TOKEN."
    url = "https://api.mintrouter.ai/v0/front/public/key-usage"
    headers = _ai_headers(api_key)
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with session.get(url, headers=headers, timeout=timeout) as resp:
            if resp.status != 200:
                body = await resp.text()
                return None, f"HTTP {resp.status}: {body[:150]}"
            # Endpoint có thể trả HTML — không crash, trả None
            try:
                data = await resp.json(content_type=None)
            except Exception:
                return None, "API trả nội dung không phải JSON (endpoint có thể đã bị gỡ)"
            if not isinstance(data, dict):
                return None, "API trả định dạng không mong đợi"
            return data, None
    except Exception as e:
        return None, str(e)


# ─── Plan usage (dashboard) — cần session cookie từ /v0/front/login ───
FRONT_SESSION_FILE = "mint_session_trading.json"
FRONT_OVERVIEW_CACHE = {'data': None, 'ts': 0.0, 'plan': None}
_front_last_login = {'ts': 0.0}
MODEL_PRICING_CACHE = {'data': None, 'ts': 0.0}


def _load_front_session():
    try:
        if os.path.exists(FRONT_SESSION_FILE):
            with open(FRONT_SESSION_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and data.get('cookies'):
                FRONT_OVERVIEW_CACHE['plan'] = data.get('plan')
                return data['cookies']
    except Exception:
        pass
    return None


def _save_front_session(cookies, plan=None):
    try:
        with open(FRONT_SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump({'cookies': cookies, 'plan': plan or FRONT_OVERVIEW_CACHE.get('plan'), 'ts': time.time()}, f)
        os.chmod(FRONT_SESSION_FILE, 0o600)
    except Exception as e:
        logger.warning(f"Lỗi lưu mint_session: {e}")


async def _front_login(session):
    """Login dashboard MintRouter (email/password trong .env) → session cookies.
    Rate-limit: không login quá 1 lần/60s để không bị khóa account."""
    email = os.getenv("MINTROUTER_EMAIL")
    pwd = os.getenv("MINTROUTER_PASSWORD")
    if not email or not pwd:
        return None
    now = time.time()
    if now - _front_last_login['ts'] < 60:
        return None
    _front_last_login['ts'] = now
    url = "https://api.mintrouter.ai/v0/front/login"
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with session.post(url, json={"username": email, "password": pwd},
                                headers={"Origin": "https://mintrouter.ai",
                                         "Referer": "https://mintrouter.ai/login",
                                         "Content-Type": "application/json"},
                                timeout=timeout) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.warning(f"MintRouter login thất bại: HTTP {resp.status} {body[:120]}")
                return None
            data = await resp.json(content_type=None)
            cookies = {}
            for k, v in resp.cookies.items():
                cookies[k] = v.value
            if not cookies:
                return None
            plan = data.get('plan') if isinstance(data, dict) else None
            _front_last_login['ts'] = time.time()
            _save_front_session(cookies, plan)
            FRONT_OVERVIEW_CACHE['plan'] = plan or FRONT_OVERVIEW_CACHE.get('plan')
            logger.info(f"[MINTROUTER] Đã login dashboard (plan: {plan})")
            return cookies
    except Exception as e:
        logger.warning(f"Lỗi login MintRouter: {e}")
        return None


async def get_front_overview(session, force=False):
    """Lấy usage PLAN từ dashboard MintRouter (/v0/front/dashboard/overview) qua session cookie.
    Cache 5 phút để không spam API. Trả về (data|None, plan_name|None, err|None)."""
    now = time.time()
    if (not force and FRONT_OVERVIEW_CACHE['data'] is not None
            and now - FRONT_OVERVIEW_CACHE['ts'] < 300):
        return FRONT_OVERVIEW_CACHE['data'], FRONT_OVERVIEW_CACHE.get('plan'), None
    cookies = _load_front_session()
    plan = FRONT_OVERVIEW_CACHE.get('plan')
    url = "https://api.mintrouter.ai/v0/front/dashboard/overview"
    for attempt in (1, 2):
        if not cookies:
            cookies = await _front_login(session)
            if not cookies:
                return None, plan, "chưa có session (thiếu MINTROUTER_EMAIL/PASSWORD trong .env)"
        cookie_hdr = "; ".join(f"{k}={v}" for k, v in cookies.items())
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with session.get(url, headers={
                "Cookie": cookie_hdr,
                "Origin": "https://mintrouter.ai",
                "Referer": "https://mintrouter.ai/dashboard",
                "Accept": "application/json",
            }, timeout=timeout) as resp:
                if resp.status == 401 and attempt == 1:
                    cookies = None  # session hết hạn → login lại
                    continue
                if resp.status != 200:
                    return None, plan, f"HTTP {resp.status}"
                data = await resp.json(content_type=None)
                if isinstance(data, dict):
                    FRONT_OVERVIEW_CACHE['data'] = data
                    FRONT_OVERVIEW_CACHE['ts'] = time.time()
                    if plan:
                        FRONT_OVERVIEW_CACHE['plan'] = plan
                    return data, FRONT_OVERVIEW_CACHE.get('plan'), None
                return None, plan, "Định dạng không mong đợi"
        except Exception as e:
            return None, plan, str(e)
    return None, plan, "session hết hạn và không login lại được"


def _fmt_micros(v):
    try:
        return f"${float(v) / 1_000_000:,.2f}"
    except (TypeError, ValueError):
        return "?"


async def get_front_pass(session, force=False):
    """Quota PLAN thật từ /v0/front/pass (daily/weekly used-limit + reset_at). Cache 5 phút.
    Quota đếm theo GIÁ TRỊ OFFICIAL/pass-covered — KHÁC với spend_limits (metered $)."""
    now = time.time()
    cached = getattr(get_front_pass, '_cache', None)
    # Có cuộc gọi AI MỚI kể từ lần đọc cache cuối → bỏ cache lấy số mới
    if (not force and cached and now - cached[0] < 300
            and _llm_usage_last_ts <= cached[0]):
        return cached[1], None, None
    if cached:
        get_front_pass._cache = None
    cookies = _load_front_session()
    if not cookies:
        cookies = await _front_login(session)
        if not cookies:
            return None, None, "chưa có session (thiếu MINTROUTER_EMAIL/PASSWORD trong .env)"
    cookie_hdr = "; ".join(f"{k}={v}" for k, v in cookies.items())
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with session.get("https://api.mintrouter.ai/v0/front/pass", headers={
            "Cookie": cookie_hdr,
            "Origin": "https://mintrouter.ai",
            "Referer": "https://mintrouter.ai/dashboard",
            "Accept": "application/json",
        }, timeout=timeout) as resp:
            if resp.status == 401:
                # session hết hạn → login lại 1 lần
                cookies = await _front_login(session)
                if not cookies:
                    return None, None, "session hết hạn, không login lại được"
                cookie_hdr = "; ".join(f"{k}={v}" for k, v in cookies.items())
                async with session.get("https://api.mintrouter.ai/v0/front/pass", headers={
                    "Cookie": cookie_hdr,
                    "Origin": "https://mintrouter.ai",
                    "Accept": "application/json",
                }, timeout=timeout) as resp2:
                    if resp2.status != 200:
                        return None, None, f"HTTP {resp2.status}"
                    data = await resp2.json(content_type=None)
            elif resp.status != 200:
                return None, None, f"HTTP {resp.status}"
            else:
                data = await resp.json(content_type=None)
            if isinstance(data, dict):
                get_front_pass._cache = (time.time(), data)
                if data.get('group_name'):
                    FRONT_OVERVIEW_CACHE['plan'] = data.get('group_name')
                return data, None, None
            return None, None, "Định dạng không mong đợi"
    except Exception as e:
        return None, None, str(e)


def _fmt_reset_vn(iso_str):
    """ISO datetime UTC → 'dd/MM HH:mm (giờ VN)'."""
    try:
        ts = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        vn = ts.astimezone(timezone(timedelta(hours=7)))
        return vn.strftime("%d/%m %H:%M")
    except Exception:
        return "?"


def _fmt_remaining(iso_str):
    """ISO datetime UTC → thời gian còn lại: '2 ngày 10h' / '15h20p'."""
    try:
        ts = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        delta = ts - datetime.now(timezone.utc)
        secs = max(int(delta.total_seconds()), 0)
        days, rem = divmod(secs, 86400)
        hours, rem2 = divmod(rem, 3600)
        mins = rem2 // 60
        if days > 0:
            return f"{days} ngày {hours}h"
        if hours > 0:
            return f"{hours}h{mins}p"
        return f"{mins}p"
    except Exception:
        return "?"


async def get_front_analysis(session):
    """Token composition 30 ngày từ dashboard MintRouter (/v0/front/dashboard/analysis). Cache 5 phút."""
    now = time.time()
    if getattr(get_front_analysis, '_cache', None) and now - get_front_analysis._cache[0] < 300:
        return get_front_analysis._cache[1], None, None
    cookies = _load_front_session()
    if not cookies:
        cookies = await _front_login(session)
        if not cookies:
            return None, None, "chưa có session"
    cookie_hdr = "; ".join(f"{k}={v}" for k, v in cookies.items())
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with session.get("https://api.mintrouter.ai/v0/front/dashboard/analysis", headers={
            "Cookie": cookie_hdr,
            "Origin": "https://mintrouter.ai",
            "Referer": "https://mintrouter.ai/dashboard",
            "Accept": "application/json",
        }, timeout=timeout) as resp:
            if resp.status != 200:
                return None, None, f"HTTP {resp.status}"
            data = await resp.json(content_type=None)
            if isinstance(data, dict):
                get_front_analysis._cache = (time.time(), data)
                return data, None, None
            return None, None, "Định dạng không mong đợi"
    except Exception as e:
        return None, None, str(e)


async def handle_usage_command(session, chat_id):
    """Lệnh /usage: QUOTA PLAN MintRouter — ngắn gọn, reset hiển thị theo thời gian còn lại."""
    pass_data, _, perr = await get_front_pass(session)
    plan = (pass_data or {}).get('group_name') or FRONT_OVERVIEW_CACHE.get('plan')
    if pass_data:
        lines = [f"📊 *QUOTA PLAN* — {plan or 'MintRouter'} (còn {_fmt_remaining(pass_data.get('expires_at', ''))})"]

        def _row(label, blk):
            blk = blk or {}
            limit = float(blk.get('limit', 0) or 0)
            used = float(blk.get('used', 0) or 0)
            tail = f" — reset sau {_fmt_remaining(blk.get('reset_at', ''))}" if blk.get('reset_at') else ""
            if limit <= 0:
                return f"🟢 {label}: ${used:,.2f}"
            pct = used / limit * 100
            emoji = "🔴" if pct >= 100 else ("🟧" if pct >= 90 else ("🟨" if pct >= 70 else "🟩"))
            return f"{emoji} {label}: ${used:,.2f}/${limit:,.0f} ({pct:.0f}%)" + tail

        lines.append(_row("Ngày", pass_data.get('daily')))
        lines.append(_row("Tuần", pass_data.get('weekly')))
        try:
            data, _, _ = await get_front_overview(session)
            kpi = (data or {}).get('kpi') or {}
            if kpi:
                lines.append(f"📈 Hôm nay: {kpi.get('total_requests', 0)} request, "
                             f"{int(kpi.get('total_tokens', 0)):,} token, "
                             f"thành công {kpi.get('success_rate', 0):.0f}%")
        except Exception:
            pass
        await send_telegram_message(session, chat_id, "\n".join(lines))
        return
    key_data, kerr = await get_go_usage(session)
    lines = ["📊 *Usage MintRouter (key)*"]
    if perr:
        lines.append(f"⚠️ Không lấy được quota plan: {perr}")
    if key_data:
        usage = key_data.get('usage') or {}
        for label, key in (("Hôm nay", 'today'), ("7 ngày", 'rolling_7d'), ("30 ngày", 'rolling_30d')):
            w = usage.get(key) or {}
            if isinstance(w, dict):
                spend = float(w.get('spend_micros', 0)) / 1_000_000
                lines.append(f"• {label}: {w.get('requests', 0)} request, {int(w.get('total_tokens', 0)):,} token, ${spend:,.4f}")
    elif kerr:
        lines.append(f"⚠️ Không lấy được key usage: {kerr}")
    await send_telegram_message(session, chat_id, "\n".join(lines))





async def handle_scan_history_command(session, chat_id):
    """Lịch sử các lượt quét định kỳ (30 phút & 5 giờ): thời gian + coin phù hợp tìm được."""
    if not scan_history:
        await send_telegram_message(session, chat_id, "📋 Chưa có lượt quét nào được ghi lại.")
        return
    lines = []
    tz_vn = timezone(timedelta(hours=7))
    for rec in reversed(scan_history[-30:]):
        t_str = datetime.fromtimestamp(rec.get('ts', 0), tz_vn).strftime('%d/%m %H:%M')
        kind = '⏱ 30 phút' if rec.get('kind') == '30m' else '🕐 5 giờ'
        coins = rec.get('coins') or []
        coin_txt = ', '.join(display_symbol(c) for c in coins) if coins else '—'
        lines.append(f"{kind} · {t_str}\n  → {coin_txt}")
    await send_telegram_message(
        session, chat_id,
        "📋 *Lịch sử quét thị trường*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines)
    )


def _safe_leverage_for_sl(entry_price, sl_price, max_lev):
    """Chọn đòn bẩy vừa đủ để SL gốc nằm trong vùng an toàn trước thanh lý (KHÔNG kéo SL).
    Hàm clamp dùng `safe_dist = 0.5/lev × entry`, nên lev cao → safe_dist nhỏ → SL bị kéo sát entry.
    Trả về đòn bẩy phù hợp (≥ 1, ≤ max_lev) để khoảng cách SL gốc không bị clamp."""
    try:
        entry_price = float(entry_price)
        sl_price = float(sl_price)
        max_lev = float(max_lev or 0)
    except (TypeError, ValueError):
        return 1
    if max_lev <= 0:
        return 1
    sl_dist = abs(entry_price - sl_price)
    if entry_price <= 0 or sl_dist <= 0 or max_lev <= 1:
        return max(1, int(max_lev))
    # Cần: 0.5/lev × entry >= sl_dist  →  lev <= 0.5 × entry / sl_dist
    lev = int(0.5 * entry_price / sl_dist)
    return max(1, min(int(max_lev), lev))


# ─── AI tự động vào lệnh mỗi 5 giờ ───
AI_AUTO_TRADER_INTERVAL = 5 * 3600
# LƯU Ý: các ngưỡng dưới đây là GIÁ TRỊ LEGACY — hiệu chỉnh bằng simulator cũ (đã phát hiện
# thiên lệch look-ahead/exit), CHƯA được chứng minh lại bằng backtest đúng. Giữ nguyên giá trị,
# chỉ dùng như rào chắn thận trọng cho tới khi có kết quả backtest corrected/full-strategy.
AI_AUTO_MIN_SCORE = 5.0  # Điểm hệ thống tối thiểu mới tự vào lệnh
AI_AUTO_AI_MIN_SCORE = 5.0  # AI tự chấm chiều tín hiệu phải ≥ ngưỡng này (điểm AI độc lập, không thể thiếu)
# SHORT bị siết chặt hơn LONG theo số liệu LEGACY (simulator cũ, chưa tái kiểm chứng) — giữ để thận trọng.
AI_AUTO_SHORT_MIN_SCORE = 5.5      # SHORT hệ thống phải ≥ ngưỡng này
AI_AUTO_SHORT_AI_MIN_SCORE = 6.0   # SHORT phải có AI tự chấm ≥ ngưỡng này
AI_AUTO_SIDE_MIN_SAMPLES = 12      # Số lệnh kết thúc tối thiểu để bộ lọc side có hiệu lực (mẫu nhỏ dễ chặn nhầm)
AI_AUTO_SIDE_MIN_WR = 0.5          # Win-rate tối thiểu của 1 side; dưới mức này → chặn side đó tự vào lệnh

# ─── Rào chắn an toàn cho tự động hoá (giúp AI tự trade nhiều mà không liều) ───
AUTO_MAX_CONSEC_LOSSES = 3        # Thua N lệnh auto liên tiếp → nghỉ (chống revenge trading)
AUTO_CIRCUIT_BREAK_HOURS = 12     # Thời gian nghỉ khi chạm circuit breaker
AUTO_DAILY_MAX_LOSS = 50.0        # Lỗ trong ngày (theo UTC) vượt mức này → dừng tới hết ngày (USDT)
AUTO_MAX_OPEN_POSITIONS = 3       # Tối đa vị thế AI tự mở cùng lúc
AUTO_STATE = {'circuit_break_until': 0.0, 'last_notify': 0.0}

# ─── Rào chắn thêm cho lệnh AI tự vào ───
AUTO_MAX_FUNDING = 0.001          # Funding cực đoan (≥ 0.1%/h) theo hướng đám đông → bỏ qua (nguy cơ đảo chiều)
AUTO_MAX_SLIP_PCT = 0.005         # Giá hiện tại cách close lúc quét > 0.5% → tín hiệu đã cũ, bỏ qua

# ─── Ngân sách rủi ro dùng chung cho MỌI lệnh thật (auto 5h + AI /ai) ───
# Trước đây mỗi lệnh auto được phép lỗ tới 20% số dư khả dụng — lớn hơn cả hạn lỗ ngày
# (AUTO_DAILY_MAX_LOSS) nên chỉ 1 lệnh thua là đã vượt trần ngày. Nay size tính từ RỦI RO:
AUTO_RISK_PER_TRADE_PCT = 0.005   # rủi ro tối đa 1 lệnh = 0.5% equity (equity = totalMarginBalance)
AUTO_RISK_PORTFOLIO_PCT = 0.015   # tổng rủi ro đang mở của cả danh mục ≤ 1.5% equity
AUTO_MARGIN_MAX_PCT = 0.25        # margin 1 lệnh ≤ 25% số dư KHẢ DỤNG (available)
AUTO_TRADE_COST_PCT = 0.0015      # đệm phí+trượt giá khi tính size (0.05% phí ×2 chiều + đệm trượt)
_ENTRY_LOCK = None                # khoá tuần tự hoá lệnh MỞ thật (xem _entry_lock())
_SYMBOL_FILTERS = {}              # symbol -> {'step', 'min_qty', 'min_notional'} từ exchangeInfo
_SYMBOL_FILTERS_TS = 0.0

# ─── Cổng rollout cho lệnh auto THẬT ───
# Backtest corrected (6 coin × 2000 nến 1h, có phí/trượt/funding, SL-first, expiry MTM):
#   TRAIN n=116: EV -0.208R / PF 0.66 ; TEST n=57: EV -0.054R / PF 0.90 → EV ÂM ở cả hai tập.
# Vì vậy auto vào lệnh thật MẶC ĐỊNH TẮT; chỉ bật khi người vận hành đặt AUTO_TRADE_ENABLED=true
# sau khi có bằng chứng EV dương. KHÔNG tự bật lại trong code.
AUTO_TRADE_ENABLED = os.getenv('AUTO_TRADE_ENABLED', 'false').strip().lower() == 'true'
AUTO_TRADE_OFF_REASON = (
    "auto vào lệnh thật đang TẮT (AUTO_TRADE_ENABLED != true): backtest corrected cho EV âm ở cả "
    "TRAIN (-0.208R, PF 0.66) và TEST (-0.054R, PF 0.90) — chỉ bật khi có bằng chứng EV dương."
)


def _auto_trade_enabled():
    """Cổng bật lệnh auto thật. Đọc env tại thời điểm kiểm tra vì .env chỉ được nạp trong main()
    (sau khi module đã import), mặc định lấy hằng số AUTO_TRADE_ENABLED."""
    raw = os.getenv('AUTO_TRADE_ENABLED')
    if raw is None:
        return AUTO_TRADE_ENABLED
    return raw.strip().lower() == 'true'

# ─── Trailing stop + breakeven cho vị thế AI tự mở ───
# Ngưỡng dưới đây là GIÁ TRỊ LEGACY (hiệu chỉnh bằng simulator cũ đã phát hiện thiên lệch),
# CHƯA được tái kiểm chứng bằng backtest corrected/full-strategy — giữ nguyên, không suy diễn thêm.
AUTO_BE_RR = 0.0                  # Tắt breakeven (legacy: bật BE bị cho là cắt lệnh thắng — chưa tái kiểm chứng)
AUTO_TRAIL_START_RR = 0.8         # Đạt +0.8R → bắt đầu trailing stop
AUTO_TRAIL_ATR_MULT = 1.0         # SL trailing cách giá hiện tại 1.0×ATR
AUTO_CANCEL_TP_ON_TRAIL = True    # Khi trailing kích hoạt → bỏ TP cố định, để lời chạy theo trailing
AUTO_TRAIL_MIN_RR = 0.25          # Chỉ cập nhật SL khi cải thiện ≥ 0.25R (tránh spam API)
AUTO_TRAIL_CHECK_SEC = 30         # Chu kỳ kiểm tra (giây)

# ─── Chốt lời một phần (partial TP) cho vị thế auto ───
# Giá trị LEGACY (sweep bằng simulator cũ, chưa tái kiểm chứng) — giữ nguyên ngưỡng.
AUTO_PARTIAL_TP_RR = 1.5          # Đạt +1.5R → chốt 50% khối lượng, phần còn lại chạy tiếp
AUTO_PARTIAL_TP_PCT = 0.5         # Tỷ lệ chốt sớm
AUTO_MAX_HOLD_HOURS = 72          # Vị thế auto mở quá 72h → đóng thị trường (khớp MAX_HOLD_BARS của backtest cũ)

AUTO_MANAGED_FILE = "auto_managed_trading.json"

# position_key -> meta lệnh đã vào thật: symbol, side, entry, sl_initial, risk, atr, qty, pos_side,
# sl_algo_id, tp_algo_id, last_sl, ts, signal_id, managed (False = vị thế /ai, không trailing),
# close_pending (đang chờ đóng lại cho tới khi hết vị thế), stale_sl_ids, be_arm, partial_done.
auto_managed = {}

# ─── Setup mặc định cho lệnh thủ công (/long, /short) ───
# Người dùng yêu cầu TẮT tự động đặt TP/SL khi vào lệnh thủ công: /l và /s chỉ đặt
# đúng 1 lệnh vào, TP/SL chỉ được đặt khi người dùng truyền tp=/sl= hoặc dùng /tp /sl /tpsl.
DEFAULT_SETUP_ENABLED = False
DEFAULT_SETUP = {
    'sl_pct': 2.0,    # SL cách entry 2% giá (chỉ dùng khi bật lại)
    'tp_rr': 2.0,     # TP cách entry 2x khoảng cách SL (chỉ dùng khi bật lại)
}


def _save_auto_managed():
    """Lưu trạng thái trailing xuống đĩa để sống sót qua restart bot."""
    try:
        with open(AUTO_MANAGED_FILE, "w", encoding="utf-8") as f:
            json.dump(auto_managed, f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Lỗi lưu auto_managed: {e}")


def _load_auto_managed():
    """Nạp lại trạng thái trailing sau restart (vị thế đã đóng sẽ được loop dọn dẹp)."""
    global auto_managed
    try:
        if os.path.exists(AUTO_MANAGED_FILE):
            with open(AUTO_MANAGED_FILE, "r", encoding="utf-8") as f:
                auto_managed = json.load(f)
            logger.info(f"Đã nạp {len(auto_managed)} vị thế auto cần trailing từ file.")
    except Exception as e:
        logger.error(f"Lỗi nạp auto_managed: {e}")


def _count_auto_open_positions():
    """Số vị thế AI TỰ vào đang mở (đếm theo auto_managed — chỉ lệnh loop 5h tự mở,
    KHÔNG tính lệnh người dùng đặt tay hay nhờ AI /ai đặt)."""
    cnt = 0
    for key, meta in auto_managed.items():
        if not meta.get('managed', True):
            continue  # vị thế do /ai đặt: không tính vào hạn mức tự động
        pos = positions.get(key)
        if pos and float(pos.get('positionAmt', 0) or 0) != 0:
            cnt += 1
    return cnt


# ═══ Bảng rủi ro / kill-switch / funding / cảnh báo tiến độ TP-SL ═══
AUTO_STATE_FILE = "auto_state_trading.json"


def _load_auto_state():
    try:
        if os.path.exists(AUTO_STATE_FILE):
            with open(AUTO_STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                AUTO_STATE.update(data)
                logger.info(f"Đã nạp AUTO_STATE: circuit_break_until={AUTO_STATE.get('circuit_break_until', 0)}")
    except Exception as e:
        logger.error(f"Lỗi nạp auto_state: {e}")


def _save_auto_state():
    try:
        with open(AUTO_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(AUTO_STATE, f)
    except Exception as e:
        logger.error(f"Lỗi lưu auto_state: {e}")


def _ai_features_paused():
    """True khi người dùng đã tắt TẤT CẢ auto AI (/stopai).
    Chỉ tắt các loop AI (auto-trader, alert 30p, pos guard, review 6h) —
    /auto, /autopnl, cảnh báo tiến độ TP/SL vẫn chạy bình thường."""
    return time.time() < AUTO_STATE.get('features_off_until', 0)


async def _fetch_algo_tpsl_map(session):
    """Lấy TP/SL điều kiện đang treo của mọi symbol: {symbol: [('TP'|'SL', trigger_price, qty)]}."""
    tpsl_map = {}
    adata, aerr = await binance_signed_request(session, 'GET', '/fapi/v1/openAlgoOrders')
    if aerr or not isinstance(adata, list):
        return tpsl_map
    for o in adata:
        otype = (o.get('orderType') or '').upper()
        try:
            trig = float(o.get('triggerPrice') or 0)
        except (TypeError, ValueError):
            trig = 0.0
        if trig <= 0:
            continue
        kind = 'TP' if otype in ('TAKE_PROFIT_MARKET', 'TAKE_PROFIT') else 'SL'
        try:
            qty = float(o.get('quantity') or 0)
        except (TypeError, ValueError):
            qty = 0.0
        tpsl_map.setdefault(o.get('symbol'), []).append((kind, trig, qty))
    return tpsl_map


def build_risk_text(session, pos_risk, avail, funding_24h, tpsl_map):
    """Bảng rủi ro danh mục: margin dùng, đòn bẩy, vị thế gần thanh lý nhất, funding 24h."""
    open_pos = [p for p in (pos_risk or []) if float(p.get('positionAmt', 0) or 0) != 0.0]
    avail_line = f"\n💰 Số dư khả dụng: {avail:,.2f} USDT" if avail is not None else ""
    if not open_pos:
        return ("🩺 *BẢNG RỦI RO DANH MỤC*\n----------------------------------\n"
                f"✅ Không có vị thế nào đang mở — rủi ro 0.{avail_line}")
    total_notional = 0.0
    total_margin = 0.0
    total_pnl = 0.0
    lev_weighted = 0.0
    rows = []
    for p in open_pos:
        amount = float(p.get('positionAmt', 0) or 0)
        entry = float(p.get('entryPrice', 0) or 0)
        mark = float(p.get('markPrice', 0) or 0)
        lev = float(p.get('leverage', 1) or 1)
        pnl = float(p.get('unrealizedProfit', p.get('unRealizedProfit', 0)) or 0)
        liq = float(p.get('liquidationPrice', 0) or 0)
        side = 'LONG' if amount > 0 else 'SHORT'
        disp = p['symbol'][:-4] if p['symbol'].endswith('USDT') else p['symbol']
        notional = abs(amount) * mark if mark > 0 else abs(amount) * entry
        margin = notional / lev if lev > 0 else notional
        total_notional += notional
        total_margin += margin
        total_pnl += pnl
        lev_weighted += margin * lev
        liq_pct = abs(mark - liq) / mark * 100 if (liq > 0 and mark > 0) else None
        # TP/SL gần nhất đang treo cho vị thế này
        tp_line = sl_line = ""
        for kind, trig, _q in tpsl_map.get(p['symbol'], []):
            if side == 'LONG':
                if kind == 'TP' and trig > mark:
                    tp_line = f"TP {format_price(trig)}"
                elif kind == 'SL' and trig < mark:
                    sl_line = f"SL {format_price(trig)}"
            else:
                if kind == 'TP' and trig < mark:
                    tp_line = f"TP {format_price(trig)}"
                elif kind == 'SL' and trig > mark:
                    sl_line = f"SL {format_price(trig)}"
        tpsl_txt = " | ".join(x for x in (tp_line, sl_line) if x) or "không có TP/SL ⚠️"
        rows.append((liq_pct if liq_pct is not None else 999, (
            f"• {disp} {side} {abs(amount):g} @ {format_price(entry)} — PnL {fmt_signed(pnl)}, "
            f"lev {lev:g}x, cách thanh lý {'%.1f' % liq_pct + '%' if liq_pct is not None else '?'}\n"
            f"  {tpsl_txt}"
        )))
    avg_lev = (lev_weighted / total_margin) if total_margin > 0 else 0.0
    rows.sort(key=lambda x: x[0])
    msg = (
        f"🩺 *BẢNG RỦI RO DANH MỤC*\n"
        f"----------------------------------\n"
        f"💵 Margin đang dùng: *{total_margin:,.2f} USDT* (giá trị vị thế {total_notional:,.0f} USDT)\n"
        f"⚙️ Đòn bẩy trung bình: *{avg_lev:.1f}x* | PnL đang mở: {fmt_signed(total_pnl)} USDT\n"
        f"⏳ Funding 24h qua: {fmt_signed(funding_24h)} USDT\n"
        f"⚠️ Gần thanh lý nhất đứng đầu:\n" + "\n".join(r[1] for r in rows[:8])
        + avail_line
    )
    return msg


async def handle_risk_command(session, chat_id):
    """Lệnh /risk: bảng rủi ro danh mục futures hiện tại."""
    pos_risk, err = await get_position_risk(session)
    if err:
        await send_telegram_message(session, chat_id, f"❌ Không lấy được vị thế: {err}")
        return
    avail = await get_available_balance(session)
    now_ms = int(time.time() * 1000)
    recs, ferr = await fetch_income_paginated(session, income_type='FUNDING_FEE',
                                              start_ms=now_ms - 24 * 3600 * 1000)
    funding_24h = sum(float(r.get('income', 0)) for r in (recs or []))
    tpsl_map = await _fetch_algo_tpsl_map(session)
    msg = build_risk_text(session, pos_risk, avail, funding_24h, tpsl_map)
    await send_telegram_message(session, chat_id, msg)


async def handle_stopauto_command(session, chat_id, arg=None):
    """Lệnh /stopauto: kill switch AI.
    /stopauto        → dừng AI TỰ VÀO LỆNH 24h
    /stopauto all    → TẮT TẤT CẢ auto AI: auto-trade + alert 30p + pos-guard + review 6h
                       (lệnh tay, /auto, /autopnl, cảnh báo TP/SL vẫn chạy bình thường)
    /stopauto close  → dừng AI + ĐÓNG luôn các vị thế AI tự mở
    /stopauto off    → bật lại mọi auto AI (bỏ kill switch + pause toàn bộ)"""
    arg = (arg or '').strip().lower()
    if arg == 'off':
        AUTO_STATE['circuit_break_until'] = 0.0
        AUTO_STATE['features_off_until'] = 0.0
        _save_auto_state()
        await send_telegram_message(session, chat_id, "▶️ Đã bật lại TOÀN BỘ AI: auto-trade + alert + pos-guard + review.")
        return
    if arg == 'all':
        AUTO_STATE['features_off_until'] = time.time() + 365 * 24 * 3600
        _save_auto_state()
        await send_telegram_message(
            session, chat_id,
            "🛑 *Đã TẮT TẤT CẢ auto AI:*\n"
            "• AI tự đặt lệnh 5h ❌\n"
            "• Báo coin ngon 30p ❌\n"
            "• Rà vị thế (pos guard) ❌\n"
            "• AI review 6h ❌\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "✅ Vẫn chạy bình thường: /auto, /autopnl, cảnh báo tiến độ TP/SL, lệnh tay, /trail.\n"
            "Bật lại toàn bộ: `/stopauto off`"
        )
        return
    AUTO_STATE['circuit_break_until'] = time.time() + 24 * 3600
    _save_auto_state()
    if arg == 'close':
        api_key = os.getenv("BINANCE_API_KEY")
        api_secret = os.getenv("BINANCE_API_SECRET")
        closed, failed = 0, 0
        for key, meta in list(auto_managed.items()):
            pos = positions.get(key)
            if not pos or float(pos.get('positionAmt', 0) or 0) == 0:
                auto_managed.pop(key, None)
                continue
            qty_p, price_p, _ = await get_symbol_precisions(session, meta['symbol'])
            real_qty = abs(float(pos.get('positionAmt', 0) or 0))
            close_side = 'SELL' if meta['side'] == 'LONG' else 'BUY'
            params = {'symbol': meta['symbol'], 'side': close_side, 'type': 'MARKET',
                      'quantity': f"{real_qty:.{qty_p}f}", 'reduceOnly': 'true'}
            if meta.get('pos_side') and meta['pos_side'] != 'BOTH':
                params['positionSide'] = meta['pos_side']
            for aid in (meta.get('tp_algo_id'), meta.get('sl_algo_id')):
                await _cancel_algo_sl(session, api_key, api_secret, meta['symbol'], aid)
            _, err = await binance_signed_request(session, 'POST', '/fapi/v1/order', params)
            if err:
                failed += 1
                logger.warning(f"[STOP-AUTO] Đóng {meta['symbol']} thất bại: {err}")
            else:
                closed += 1
            auto_managed.pop(key, None)
        _save_auto_managed()
        await send_telegram_message(
            session, chat_id,
            f"🛑 *KILL SWITCH: đã dừng AI tự trade 24h + đóng {closed} vị thế auto*"
            + (f" (❌ {failed} lệnh đóng thất bại — kiểm tra /pos)" if failed else "")
        )
    else:
        await send_telegram_message(
            session, chat_id,
            "🛑 *Đã dừng AI tự trade trong 24h.*\n"
            "Lệnh tay không bị ảnh hưởng. Bật lại: `/stopauto off`.\n"
            "Đóng luôn vị thế AI đang mở: `/stopauto close`"
        )


async def handle_fund_command(session, chat_id):
    """Lệnh /fund: tổng funding đã trả/thu 7 ngày theo coin + cảnh báo vị thế đang cháy funding."""
    now_ms = int(time.time() * 1000)
    recs, err = await fetch_income_paginated(session, income_type='FUNDING_FEE',
                                             start_ms=now_ms - 7 * 24 * 3600 * 1000)
    if err:
        await send_telegram_message(session, chat_id, f"❌ Không lấy được lịch sử funding: {err}")
        return
    per_sym = {}
    for r in (recs or []):
        sym = r.get('symbol')
        if not sym:
            continue
        per_sym[sym] = per_sym.get(sym, 0.0) + float(r.get('income', 0))
    if not per_sym:
        await send_telegram_message(session, chat_id, "ℹ️ 7 ngày qua không có giao dịch funding nào.")
        return
    total = sum(per_sym.values())
    lines = [
        "⏳ *FUNDING 7 NGÀY QUA THEO COIN*",
        "----------------------------------",
        f"💰 Tổng: {fmt_signed(total)} USDT",
    ]
    ranked = sorted(per_sym.items(), key=lambda kv: kv[1])[:10]
    for sym, val in ranked:
        disp = sym[:-4] if sym.endswith('USDT') else sym
        note = ""
        pos = positions.get(f"{sym}_LONG") or positions.get(f"{sym}_SHORT")
        pos = pos or next((p for k, p in positions.items() if k.startswith(sym)), None)
        if pos and float(pos.get('positionAmt', 0) or 0) != 0:
            pnl = float(pos.get('unrealizedPnL', 0) or 0)
            if val < 0 and pnl > 0 and pnl < abs(val):
                note = " ⚠️ lỗ funding > lời — cân nhắc đóng"
            elif val <= -1.0 and pnl <= 0:
                note = f" (đang lỗ {fmt_signed(pnl)})"
        lines.append(f"• {disp}: {fmt_signed(val)}{note}")
    await send_telegram_message(session, chat_id, "\n".join(lines))


# ─── Cảnh báo tiến độ TP/SL cho mọi vị thế đang mở ───
TPSL_PROGRESS_INTERVAL = 120          # Chu kỳ quét (giây)
TPSL_ALERT_SL_PROGRESS = 0.70         # Giá đi được ≥70% quãng tới SL → cảnh báo
TPSL_ALERT_TP_PROGRESS = 0.80         # Giá đi được ≥80% quãng tới TP → nhắc chốt
TPSL_ALERT_COOLDOWN = 4 * 3600        # Không nhắc lặp lại cùng symbol+loại trong 4h
tpsl_progress_last = {}               # (symbol, kind) -> ts lần cảnh báo gần nhất


async def tpsl_progress_loop(app):
    """Mỗi 2 phút: so giá mark hiện tại với TP/SL điều kiện đang treo.
    Đi gần SL (≥70%) → cảnh báo cắt lỗ sớm; gần TP (≥80%) → nhắc chủ động chốt."""
    await asyncio.sleep(180)
    while True:
        try:
            session = app['session']
            open_pos = [p for p in positions.values() if float(p.get('positionAmt', 0) or 0) != 0.0]
            if not open_pos:
                await asyncio.sleep(TPSL_PROGRESS_INTERVAL)
                continue
            tpsl_map = await _fetch_algo_tpsl_map(session)
            if not tpsl_map:
                await asyncio.sleep(TPSL_PROGRESS_INTERVAL)
                continue
            now = time.time()
            warns = []
            for p in open_pos:
                symbol = p['symbol']
                amount = float(p.get('positionAmt', 0) or 0)
                entry = float(p.get('entryPrice', 0) or 0)
                mark = float(p.get('markPrice', 0) or 0)
                if entry <= 0 or mark <= 0:
                    continue
                side = 'LONG' if amount > 0 else 'SHORT'
                disp = symbol[:-4] if symbol.endswith('USDT') else symbol
                sl_trig = tp_trig = None
                for kind, trig, _q in tpsl_map.get(symbol, []):
                    if side == 'LONG':
                        if kind == 'SL' and trig < entry:
                            sl_trig = max(trig, sl_trig) if sl_trig else trig
                        elif kind == 'TP' and trig > entry:
                            tp_trig = min(trig, tp_trig) if tp_trig else trig
                    else:
                        if kind == 'SL' and trig > entry:
                            sl_trig = min(trig, sl_trig) if sl_trig else trig
                        elif kind == 'TP' and trig < entry:
                            tp_trig = max(trig, tp_trig) if tp_trig else trig
                # Gần SL: tiến độ = quãng đường lỗ từ entry tới SL
                if sl_trig is not None:
                    risk = abs(entry - sl_trig)
                    cur_loss = (entry - mark) if side == 'LONG' else (mark - entry)
                    if risk > 0 and cur_loss > 0:
                        prog = cur_loss / risk
                        key = (symbol, 'SL')
                        if prog >= TPSL_ALERT_SL_PROGRESS and now - tpsl_progress_last.get(key, 0) > TPSL_ALERT_COOLDOWN:
                            tpsl_progress_last[key] = now
                            warns.append(
                                f"⚠️ {display_symbol(symbol)} {side} đang đi được *{prog * 100:.0f}%* quãng tới SL "
                                f"({format_price(entry)} → SL {format_price(sl_trig)}, giá hiện tại {format_price(mark)}).\n"
                                f"→ Sắp cắt lỗ: chủ động quyết sớm (đóng/DCA) thay vì chờ SL khớp."
                            )
                # Gần TP: tiến độ lời
                if tp_trig is not None:
                    reward = abs(tp_trig - entry)
                    cur_gain = (mark - entry) if side == 'LONG' else (entry - mark)
                    if reward > 0 and cur_gain > 0:
                        prog = cur_gain / reward
                        key = (symbol, 'TP')
                        if prog >= TPSL_ALERT_TP_PROGRESS and now - tpsl_progress_last.get(key, 0) > TPSL_ALERT_COOLDOWN:
                            tpsl_progress_last[key] = now
                            warns.append(
                                f"🎯 {display_symbol(symbol)} {side} đã đi được *{prog * 100:.0f}%* quãng tới TP "
                                f"(TP {format_price(tp_trig)}, giá hiện tại {format_price(mark)}).\n"
                                f"→ Gần chốt lời: cân nhắc chốt một phần hoặc dời SL về entry."
                            )
            if warns:
                await _notify_all_chats(session, "\n\n".join(warns[:5]))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Lỗi trong tpsl_progress_loop: {e}")
        await asyncio.sleep(TPSL_PROGRESS_INTERVAL)


# ─── AI review tự động mỗi 6h (gửi mọi chat) ───
AI_REVIEW_INTERVAL = 6 * 3600


async def ai_review_loop(app):
    """Mỗi 6h: tự tổng kết lệnh vừa đóng + PnL hôm nay + bài học AI (cache), gửi mọi chat."""
    await asyncio.sleep(300)
    while True:
        try:
            if _ai_features_paused():
                await asyncio.sleep(AI_REVIEW_INTERVAL)
                continue
            session = app['session']
            now = time.time()
            window_start = now - AI_REVIEW_INTERVAL
            resolved = [s for s in signal_history
                        if s.get('status') in ('win', 'loss')
                        and s.get('closed_ts', s.get('ts', 0)) >= window_start]
            wins = sum(1 for s in resolved if s['status'] == 'win')
            day_start_ms = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
            recs, err = await fetch_income_paginated(session, start_ms=day_start_ms)
            day_pnl = sum(float(r.get('income', 0)) for r in (recs or [])
                          if r.get('incomeType') in ('REALIZED_PNL', 'FUNDING_FEE', 'COMMISSION'))
            lines = [
                "🧾 *AI REVIEW 6 GIỜ*",
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                f"📊 6h qua: {wins} win / {len(resolved) - wins} loss (lệnh đã chốt TP/SL)",
                f"💵 PnL hôm nay (PnL + funding + phí): {fmt_signed(day_pnl)} USDT",
                f"🔥 Đang mở: {len([p for p in positions.values() if float(p.get('positionAmt', 0) or 0) != 0])} vị thế",
            ]
            for s in resolved[-8:]:
                t_str = datetime.fromtimestamp(s.get('closed_ts', s.get('ts', 0)), tz=timezone(timedelta(hours=7))).strftime("%H:%M")
                disp = s['symbol'][:-4] if s['symbol'].endswith('USDT') else s['symbol']
                icon = '✅' if s['status'] == 'win' else '❌'
                lines.append(f"  {t_str} {disp} {s['side']} @{format_price(s.get('entry', 0))} → {icon} {s['status']}")
            stats_line = format_signal_stats()
            if stats_line:
                lines.append(stats_line)
            lessons_txt = ai_lessons_state.get('text')
            if lessons_txt:
                lines.append("\n📚 Bài học AI đang áp dụng:\n" + sanitize_ai_markdown(lessons_txt))
            await _notify_all_chats(session, "\n".join(lines))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Lỗi trong ai_review_loop: {e}")
        await asyncio.sleep(AI_REVIEW_INTERVAL)


async def _auto_trade_guard(session):
    """Kiểm tra an toàn trước khi tự vào lệnh. Trả về (ok: bool, reason: str).
    Mọi lỗi đọc dữ liệu tài khoản/rủi ro ⇒ chặn (fail closed)."""
    # 0. Cổng rollout: chưa có bằng chứng EV dương ⇒ KHÔNG tự vào lệnh thật
    if not _auto_trade_enabled():
        return False, AUTO_TRADE_OFF_REASON
    now = time.time()
    if now < AUTO_STATE.get('circuit_break_until', 0):
        left_min = int((AUTO_STATE['circuit_break_until'] - now) // 60)
        return False, f"đang tạm nghỉ (thua {AUTO_MAX_CONSEC_LOSSES} lệnh liên tiếp), còn ~{left_min}p."
    # 1. Chuỗi thua liên tiếp — CHỈ tính lệnh THẬT đã đối soát (execution + net_pnl).
    resolved = sorted([s for s in signal_history
                       if s.get('origin') == 'auto' and _verified_execution(s)
                       and s.get('status') in ('win', 'loss')],
                      key=lambda s: s.get('closed_ts', s.get('ts', 0)), reverse=True)
    streak = 0
    for s in resolved:
        if s['status'] == 'loss':
            streak += 1
        else:
            break
    if streak >= AUTO_MAX_CONSEC_LOSSES:
        newest_loss_ts = resolved[0].get('closed_ts', resolved[0].get('ts', 0))
        # KHÔNG tự gia hạn vô hạn trên cùng chuỗi thua cũ: chỉ khoá lại khi có lệnh thua MỚI.
        if newest_loss_ts > AUTO_STATE.get('circuit_break_loss_ts', 0):
            AUTO_STATE['circuit_break_loss_ts'] = newest_loss_ts
            AUTO_STATE['circuit_break_until'] = now + AUTO_CIRCUIT_BREAK_HOURS * 3600
            _save_auto_state()
            return False, f"thua {streak} lệnh auto liên tiếp — nghỉ {AUTO_CIRCUIT_BREAK_HOURS}h."
        logger.info("[AI-AUTO] Chuỗi thua cũ đã nghỉ đủ thời gian — cho phép trade lại "
                    "(chỉ lệnh thua MỚI mới khoá tiếp).")
    # 2. Hạn lỗ trong ngày (income hôm nay theo UTC) — lỗi đọc ⇒ không trade
    remaining, derr = await _daily_loss_budget(session)
    if derr:
        return False, f"không đo được hạn lỗ ngày ({derr}) — tạm không tự vào lệnh (fail closed)."
    if remaining <= 0:
        return False, f"đã dùng hết hạn lỗ ngày {AUTO_DAILY_MAX_LOSS:g} USDT — nghỉ tới hết ngày."
    # 3. Đo rủi ro danh mục: vị thế không có SL ⇒ không đo được ⇒ chặn auto mới (KHÔNG tự đóng lệnh thủ công)
    snap, serr = await _account_risk_snapshot(session)
    if serr:
        return False, f"không đo được rủi ro tài khoản ({serr}) — tạm không tự vào lệnh (fail closed)."
    if snap['unprotected']:
        return False, ("có vị thế chưa đặt SL nên không đo được rủi ro danh mục "
                       f"({', '.join(snap['unprotected'][:5])}) — đặt SL cho các vị thế đó rồi thử lại.")
    # 4. Giới hạn số vị thế AI TỰ vào (không tính lệnh user đặt tay / nhờ /ai đặt)
    open_auto = _count_auto_open_positions()
    if open_auto >= AUTO_MAX_OPEN_POSITIONS:
        return False, f"đã có {open_auto}/{AUTO_MAX_OPEN_POSITIONS} vị thế AI tự vào đang mở."
    return True, ""


async def get_available_balance(session):
    """Số dư khả dụng của tài khoản Futures. Trả về float hoặc None nếu lỗi."""
    data, err = await binance_signed_request(session, 'GET', '/fapi/v2/account')
    if err:
        logger.warning(f"[AI-AUTO] Lỗi lấy số dư khả dụng: {err}")
        return None
    try:
        return float(data.get('availableBalance', 0))
    except (TypeError, ValueError):
        return None


# ═══ Khối rủi ro + thực thi lệnh thật dùng chung (auto 5h + AI /ai) ═══
# Nguyên tắc: đo rủi ro bằng EQUITY, đo margin bằng AVAILABLE; mọi lỗi đọc dữ liệu
# tài khoản/rủi ro đều FAIL CLOSED (không mở lệnh), và KHÔNG bao giờ kéo SL cho vừa
# ngân sách — thiếu chỗ thì giảm size hoặc bỏ lệnh.

def _entry_lock():
    """Khoá tuần tự hoá lệnh MỞ thật: tránh 2 lệnh vào cùng lúc cùng vượt trần danh mục."""
    global _ENTRY_LOCK
    if _ENTRY_LOCK is None:
        _ENTRY_LOCK = asyncio.Lock()
    return _ENTRY_LOCK


def _min_step_qty(qty_p):
    """Bước khối lượng suy từ precision (10^-qty_p) — chỉ dùng khi chưa đọc được LOT_SIZE."""
    return 10 ** (-int(qty_p))


def _round_to_step(value, step):
    """Làm tròn XUỐNG theo bội số stepSize thật của sàn (tránh lỗi -1111 / mất khối lượng)."""
    if step is None or step <= 0:
        return value
    return math.floor(value / step + 1e-9) * step


async def _load_symbol_filters(session, ttl=3600):
    """Nạp LOT_SIZE.stepSize / MIN_NOTIONAL của toàn bộ symbol futures (cache theo tiến trình).
    Trả True nếu có dữ liệu (kể cả dữ liệu cũ trong TTL)."""
    global _SYMBOL_FILTERS_TS
    now = time.time()
    if _SYMBOL_FILTERS and now - _SYMBOL_FILTERS_TS < ttl:
        return True
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo", headers=headers) as resp:
            if resp.status != 200:
                logger.warning(f"Không nạp được filters exchangeInfo: HTTP {resp.status}")
                return bool(_SYMBOL_FILTERS)
            data = await resp.json()
    except Exception as e:
        logger.warning(f"Lỗi nạp filters exchangeInfo: {e}")
        return bool(_SYMBOL_FILTERS)
    filters = {}
    for s in data.get('symbols', []) or []:
        step = min_qty = min_notional = 0.0
        for f in s.get('filters', []) or []:
            ftype = f.get('filterType')
            try:
                if ftype == 'LOT_SIZE':
                    step = float(f.get('stepSize') or 0)
                    min_qty = float(f.get('minQty') or 0)
                elif ftype == 'MARKET_LOT_SIZE' and not step:
                    step = float(f.get('stepSize') or 0)
                    min_qty = float(f.get('minQty') or 0)
                elif ftype == 'MIN_NOTIONAL':
                    min_notional = max(min_notional, float(f.get('notional') or 0))
            except (TypeError, ValueError):
                continue
        filters[s.get('symbol')] = {'step': step, 'min_qty': min_qty, 'min_notional': min_notional}
    if filters:
        _SYMBOL_FILTERS.clear()
        _SYMBOL_FILTERS.update(filters)
        _SYMBOL_FILTERS_TS = now
        logger.info(f"Đã nạp stepSize/minNotional cho {len(filters)} symbol.")
    return bool(_SYMBOL_FILTERS)


async def _symbol_constraints(session, symbol, qty_p):
    """(step, min_qty, min_notional) thật của symbol; thiếu dữ liệu ⇒ suy từ precision (đã log)."""
    await _load_symbol_filters(session)
    info = _SYMBOL_FILTERS.get(symbol) or {}
    step = float(info.get('step') or 0)
    if step <= 0:
        step = _min_step_qty(qty_p)
    return step, float(info.get('min_qty') or 0), float(info.get('min_notional') or 0)


def _normalize_order_id(value):
    """Binance trả id dạng int hoặc str — lưu luôn dạng str, rỗng thì None."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _new_client_order_id(prefix):
    """clientOrderId chống trùng khi kết quả gửi lệnh không chắc chắn (≤ 36 ký tự)."""
    return f"{prefix}_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"[:36]


async def _max_leverage_strict(session, symbol):
    """Đòn bẩy tối đa của symbol. Lỗi đọc bracket → (None, lý do): nơi gọi fail closed."""
    data, err = await binance_signed_request(session, 'GET', '/fapi/v1/leverageBracket', {'symbol': symbol})
    if err:
        return None, f"không đọc được leverageBracket {symbol}: {err}"
    brackets = []
    if isinstance(data, list) and data:
        brackets = (data[0] or {}).get('brackets') or []
    if not isinstance(brackets, list) or not brackets:
        return None, f"leverageBracket {symbol} trả về dữ liệu lạ."
    try:
        lev = int(brackets[0].get('initialLeverage') or 0)
    except (TypeError, ValueError):
        return None, f"leverageBracket {symbol} thiếu initialLeverage."
    if lev <= 0:
        return None, f"leverageBracket {symbol} có initialLeverage ≤ 0."
    return lev, None


async def _fetch_open_algo_orders(session, symbol=None):
    """Lệnh điều kiện đang treo. Truyền `symbol` để tránh weight=40 khi bỏ trống.
    Trả (list|None, err) — dùng để ĐO RỦI RO nên phải fail closed."""
    params = {'symbol': symbol} if symbol else None
    data, err = await binance_signed_request(session, 'GET', '/fapi/v1/openAlgoOrders', params)
    if err:
        return None, err
    if data is None:
        return [], None
    if not isinstance(data, list):
        return None, "openAlgoOrders trả về dữ liệu lạ."
    return data, None


def _position_stop_candidates(orders, symbol, side, pos_side, entry, mark=0.0, pos_qty=0.0):
    """Lệnh STOP hợp lệ bảo vệ 1 vị thế: đúng symbol/positionSide, đúng chiều ĐÓNG,
    và kích hoạt được (nằm phía giá hiện tại: LONG cần trigger < mark, SHORT cần > mark).
    SL hoà vốn/lãi (trigger tốt hơn entry) vẫn tính là bảo vệ (rủi ro 0)."""
    want_close_side = 'SELL' if side == 'LONG' else 'BUY'
    ref = float(mark or entry or 0)
    out = []
    for o in orders or []:
        if o.get('symbol') != symbol:
            continue
        otype = (o.get('orderType') or o.get('type') or '').upper()
        if 'STOP' not in otype or 'TAKE_PROFIT' in otype:
            continue
        if (o.get('positionSide') or 'BOTH').upper() not in ('BOTH', pos_side):
            continue
        o_side = (o.get('side') or '').upper()
        if o_side and o_side != want_close_side:
            continue
        try:
            trig = float(o.get('triggerPrice') or 0)
            qty = float(o.get('quantity') or o.get('origQty') or 0)
        except (TypeError, ValueError):
            continue
        if trig <= 0 or ref <= 0:
            continue
        # Lệnh closePosition=true gửi quantity 0 → coi như bảo vệ toàn bộ vị thế
        if qty <= 0:
            qty = float(pos_qty or 0)
        if qty <= 0:
            continue
        # Trigger sai phía giá hiện tại (LONG: SL ≥ mark) ⇒ không còn là bảo vệ
        if (side == 'LONG' and trig >= ref) or (side == 'SHORT' and trig <= ref):
            continue
        out.append({'trigger': trig, 'qty': qty,
                    'id': _normalize_order_id(o.get('algoId') or o.get('orderId'))})
    return out


def _stop_risk_usdt(side, entry, qty, candidates):
    """(rủi ro xấu nhất USDT, đã bảo vệ đủ khối lượng chưa).
    Dùng SL xa entry nhất (kịch bản xấu nhất); SL hoà vốn/lãi ⇒ rủi ro 0 nhưng vẫn là bảo vệ.
    KHÔNG coi là bảo vệ nếu khối lượng được SL phủ nhỏ hơn khối lượng vị thế."""
    if entry <= 0 or qty <= 0 or not candidates:
        return 0.0, False
    covered = max((c['qty'] for c in candidates if c.get('qty')), default=0.0)
    if covered < qty:
        return 0.0, False
    worst = max(candidates, key=lambda c: abs(entry - c['trigger']))
    risk = (entry - worst['trigger']) if side == 'LONG' else (worst['trigger'] - entry)
    return max(0.0, risk) * qty, True


async def _open_risk_snapshot(session):
    """Rủi ro đang mở của TOÀN tài khoản + vị thế KHÔNG đo được rủi ro (thiếu SL).
    Trả (dict|None, err). Số liệu lạ/không hữu hạn ⇒ lỗi (nơi gọi fail closed).
    Vị thế chưa có SL ⇒ unprotected (chặn auto mới, KHÔNG tự đóng lệnh thủ công)."""
    pos_data, perr = await get_position_risk(session)
    if perr:
        return None, f"không đọc được vị thế: {perr}"
    if not isinstance(pos_data, list):
        return None, "positionRisk trả về dữ liệu lạ."
    # Đọc vị thế trước để biết symbol nào cần hỏi SL (openAlgoOrders bỏ symbol = weight 40)
    symbols = []
    for p in pos_data:
        if not isinstance(p, dict):
            continue
        try:
            amt = float(p.get('positionAmt') or 0)
        except (TypeError, ValueError):
            return None, "positionRisk có positionAmt không đọc được."
        if amt != 0 and p.get('symbol') not in symbols:
            symbols.append(p.get('symbol'))
    orders = []
    for symbol in symbols:
        chunk, oerr = await _fetch_open_algo_orders(session, symbol)
        if oerr:
            return None, f"không đọc được SL đang treo của {symbol}: {oerr}"
        orders.extend(chunk or [])
    open_risk = 0.0
    unprotected = []
    open_symbols = []
    for p in pos_data:
        if not isinstance(p, dict):
            return None, "positionRisk có bản ghi không phải object."
        try:
            amt = float(p.get('positionAmt') or 0)
            entry = float(p.get('entryPrice') or 0)
            mark = float(p.get('markPrice') or 0)
        except (TypeError, ValueError):
            return None, "positionRisk có trường số không đọc được."
        if not all(math.isfinite(v) for v in (amt, entry, mark)):
            return None, "positionRisk có giá trị không hữu hạn."
        if amt == 0:
            continue
        symbol = p.get('symbol')
        if symbol not in open_symbols:
            open_symbols.append(symbol)
        raw_pos_side = (p.get('positionSide') or 'BOTH').upper()
        side = 'LONG' if (amt > 0 if raw_pos_side == 'BOTH' else raw_pos_side == 'LONG') else 'SHORT'
        pos_side = 'BOTH' if raw_pos_side == 'BOTH' else raw_pos_side
        qty = abs(amt)
        cands = _position_stop_candidates(orders, symbol, side, pos_side, entry, mark, qty)
        # TP/SL gắn thẳng trên vị thế (đặt từ UI Binance) cũng là SL hợp lệ
        try:
            attached = float(p.get('slPrice') or 0)
        except (TypeError, ValueError):
            attached = 0.0
        if attached > 0 and (mark <= 0 or (side == 'LONG' and attached < mark) or (side == 'SHORT' and attached > mark)):
            cands.append({'trigger': attached, 'qty': qty, 'id': None})
        risk, protected = _stop_risk_usdt(side, entry, qty, cands)
        open_risk += risk
        if not protected:
            unprotected.append(f"{display_symbol(symbol)} {side} ({qty:g})")
    return {'open_risk': open_risk, 'unprotected': unprotected, 'open_symbols': open_symbols}, None


async def _daily_loss_budget(session):
    """Phần hạn lỗ ngày còn lại (USDT). Lỗi đọc income → (None, lý do) để nơi gọi fail closed."""
    day_start = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    recs, err = await fetch_income_paginated(session, start_ms=day_start)
    if err:
        return None, f"không đọc được income trong ngày: {err}"
    day_pnl = 0.0
    for r in recs or []:
        if r.get('incomeType') in ('REALIZED_PNL', 'FUNDING_FEE', 'COMMISSION'):
            try:
                day_pnl += float(r.get('income', 0) or 0)
            except (TypeError, ValueError):
                return None, "income trong ngày có giá trị không đọc được."
    return max(0.0, AUTO_DAILY_MAX_LOSS + day_pnl), None


async def _account_risk_snapshot(session):
    """Ảnh chụp tài khoản để quyết định size: equity (risk) + available (margin)
    + rủi ro đang mở + hạn lỗ ngày còn lại. Số liệu thiếu/lạ/không hữu hạn → (None, lý do)."""
    data, err = await binance_signed_request(session, 'GET', '/fapi/v2/account')
    if err:
        return None, f"không đọc được tài khoản: {err}"
    if not isinstance(data, dict):
        return None, "tài khoản trả về dữ liệu lạ."
    try:
        wallet = float(data.get('totalWalletBalance') or 0)
        upnl = float(data.get('totalUnrealizedProfit') or 0)
        equity = float(data.get('totalMarginBalance') or 0) or (wallet + upnl)
        available = float(data.get('availableBalance') or 0)
    except (TypeError, ValueError):
        return None, "tài khoản thiếu trường số dư."
    if not all(math.isfinite(v) for v in (wallet, upnl, equity, available)):
        return None, "số dư tài khoản không hữu hạn."
    if available < 0 or wallet < 0:
        return None, "số dư tài khoản âm — không đo được ngân sách."
    if equity <= 0:
        return None, "equity ≤ 0 — không đo được ngân sách rủi ro."
    daily_remaining, derr = await _daily_loss_budget(session)
    if derr:
        return None, derr
    risk, rerr = await _open_risk_snapshot(session)
    if rerr:
        return None, rerr
    snap = {'equity': equity, 'available': available, 'wallet': wallet, 'upnl': upnl,
            'daily_remaining': daily_remaining}
    snap.update(risk)
    return snap, None


def _risk_budget_allowance(equity, open_risk, daily_remaining):
    """Ngân sách rủi ro cho MỘT lệnh mới:
    min(0.5% equity, room 1.5% equity − rủi ro đang mở, hạn lỗ ngày còn lại − rủi ro đang mở).
    Rủi ro đang mở được trừ ở CẢ HAI trần vì các vị thế đó vẫn có thể lỗ."""
    per_trade = AUTO_RISK_PER_TRADE_PCT * equity
    portfolio_room = AUTO_RISK_PORTFOLIO_PCT * equity - open_risk
    return min(per_trade, portfolio_room, daily_remaining - open_risk)


def _plan_entry_size(price, sl_price, side, *, equity, available, open_risk, daily_remaining,
                     leverage, step, min_qty=0.0, min_notional=0.0, notional_cap=None):
    """Kích thước lệnh theo ngân sách rủi ro (đã trừ phí+trượt giá dự phòng).
    KHÔNG nới SL cho vừa ngân sách — thiếu chỗ thì giảm size hoặc bỏ lệnh. Trả (ok, quantity|None, msg)."""
    try:
        price = float(price)
        sl_price = float(sl_price)
        step = float(step or 0)
    except (TypeError, ValueError):
        return False, None, "giá entry/SL/step không hợp lệ."
    if price <= 0 or sl_price <= 0 or step <= 0:
        return False, None, "giá entry/SL/step không hợp lệ."
    is_long = side == 'LONG'
    if (is_long and sl_price >= price) or (not is_long and sl_price <= price):
        return False, None, "SL nằm sai phía entry — không đo được rủi ro."
    # rủi ro biên trên mỗi đơn vị: khoảng cách SL + đệm phí/trượt giá
    risk_per_unit = abs(price - sl_price) + price * AUTO_TRADE_COST_PCT
    budget = _risk_budget_allowance(equity, open_risk, daily_remaining)
    if budget <= 0:
        return False, None, ("hết ngân sách rủi ro (trần/lệnh "
                             f"{AUTO_RISK_PER_TRADE_PCT * 100:g}% equity, trần danh mục "
                             f"{AUTO_RISK_PORTFOLIO_PCT * 100:g}% equity trừ rủi ro đang mở, "
                             "hạn lỗ ngày còn lại).")
    quantity = _round_to_step(budget / risk_per_unit, step)
    if leverage and leverage > 0:
        quantity = min(quantity, _round_to_step(available * AUTO_MARGIN_MAX_PCT * leverage / price, step))
    if notional_cap:
        quantity = min(quantity, _round_to_step(notional_cap / price, step))
    quantity = _round_to_step(quantity, step)
    min_ok = max(step, float(min_qty or 0))
    if quantity < min_ok:
        return False, None, (f"khối lượng tối thiểu {min_ok:g} vượt ngân sách rủi ro "
                             f"({budget:,.2f} USDT cho SL cách entry {abs(price - sl_price):,.8g}).")
    notional = quantity * price
    if min_notional and notional < min_notional:
        return False, None, (f"notional {notional:,.2f} USDT dưới mức tối thiểu của sàn "
                            f"({min_notional:,.2f} USDT).")
    risk = quantity * risk_per_unit
    if risk > budget + 1e-9:
        return False, None, "size vượt ngân sách rủi ro."
    return True, quantity, (f"rủi ro khi SL khớp {quantity * abs(price - sl_price):,.2f} USDT "
                            f"(≈{quantity * abs(price - sl_price) / equity * 100:.2f}% equity, "
                            f"đã tính đệm phí/trượt {AUTO_TRADE_COST_PCT * 100:g}%, ngân sách {budget:,.2f} USDT)")


def _fill_from_order(data):
    """(executed_qty, avg_price) từ response lệnh. None nếu CHƯA xác định được giá khớp thật
    (tuyệt đối không lấy ticker làm giá vào)."""
    if not isinstance(data, dict):
        return None
    try:
        qty = float(data.get('executedQty') or 0)
        avg = float(data.get('avgPrice') or 0)
    except (TypeError, ValueError):
        return None
    if qty <= 0:
        return None
    if avg <= 0:
        try:
            cum = float(data.get('cumQuote') or 0)
        except (TypeError, ValueError):
            cum = 0.0
        if cum > 0:
            avg = cum / qty
    if avg <= 0:
        return None
    return qty, avg


async def _query_order(session, symbol, order_id=None, client_id=None):
    """Tra 1 lệnh theo orderId hoặc clientOrderId (dùng khi kết quả gửi lệnh không chắc chắn)."""
    params = {'symbol': symbol}
    if order_id:
        params['orderId'] = _normalize_order_id(order_id)
    elif client_id:
        params['origClientOrderId'] = client_id
    else:
        return None, "thiếu orderId/origClientOrderId."
    return await binance_signed_request(session, 'GET', '/fapi/v1/order', params)


async def _submit_entry_order(session, symbol, side, qty_str, pos_side, client_id,
                              order_type='MARKET', price_str=None):
    """Gửi lệnh MỞ (MARKET dùng newOrderRespType=RESULT để có giá khớp).
    Khi kết quả không chắc chắn → tra lại theo clientOrderId, KHÔNG gửi lại (chống trùng lệnh)."""
    params = {'symbol': symbol, 'side': side, 'type': order_type, 'quantity': qty_str,
              'newClientOrderId': client_id, 'newOrderRespType': 'RESULT'}
    if order_type == 'LIMIT':
        if not price_str:
            return None, "lệnh LIMIT thiếu giá."
        params['price'] = price_str
        params['timeInForce'] = 'GTC'
    if pos_side and pos_side != 'BOTH':
        params['positionSide'] = pos_side
    data, err = await binance_signed_request(session, 'POST', '/fapi/v1/order', params)
    if err:
        found, qerr = await _query_order(session, symbol, client_id=client_id)
        if not qerr and isinstance(found, dict) and found.get('orderId'):
            logger.warning(f"[EXEC] {symbol}: submit báo lỗi '{err}' nhưng lệnh ĐÃ tồn tại "
                           f"(orderId={found.get('orderId')}) — không gửi lại.")
            return found, None
        return None, err
    return data, None


async def _fill_from_position_risk(session, symbol, pos_side):
    """Giá vào thật lấy từ positionRisk (khi response lệnh thiếu avgPrice). None nếu chưa có vị thế."""
    data, err = await get_position_risk(session, {'symbol': symbol})
    if err or not isinstance(data, list):
        return None
    for p in data:
        try:
            amt = float(p.get('positionAmt', 0) or 0)
            entry = float(p.get('entryPrice', 0) or 0)
        except (TypeError, ValueError):
            continue
        if amt == 0 or entry <= 0:
            continue
        if pos_side and pos_side != 'BOTH' and (p.get('positionSide') or 'BOTH').upper() != pos_side:
            continue
        return abs(amt), entry
    return None


async def _live_position(session, symbol, pos_side=None):
    """Vị thế ĐANG MỞ thật của symbol (theo REST): (positionSide, side, qty) hoặc None nếu đã phẳng.
    Lỗi đọc → None kèm cờ err ở phần tử thứ 2 để nơi gọi fail closed."""
    data, err = await get_position_risk(session, {'symbol': symbol})
    if err:
        return None, err
    if not isinstance(data, list):
        return None, "positionRisk trả về dữ liệu lạ."
    for p in data:
        if not isinstance(p, dict):
            continue
        try:
            amt = float(p.get('positionAmt') or 0)
        except (TypeError, ValueError):
            return None, "positionRisk có positionAmt không đọc được."
        if not math.isfinite(amt) or amt == 0:
            continue
        raw_side = (p.get('positionSide') or 'BOTH').upper()
        if pos_side and pos_side != 'BOTH' and raw_side not in (pos_side, 'BOTH'):
            continue
        side = 'LONG' if (amt > 0 if raw_side == 'BOTH' else raw_side == 'LONG') else 'SHORT'
        return {'pos_side': 'BOTH' if raw_side == 'BOTH' else raw_side, 'side': side, 'qty': abs(amt)}, None
    return None, None


async def _emergency_reduce_close(session, symbol, side, pos_side=None, qty_cap=None):
    """Đóng giảm vị thế bằng MARKET khi KHÔNG đặt được SL (hoặc lệnh bảo vệ hỏng).
    - Khối lượng lấy từ VỊ THẾ THẬT đang mở (đọc lại ngay trước khi gửi) → không bao giờ
      gửi dư khối lượng kiểu hedge (dư 1 chiều = MỞ vị thế ngược, rất nguy hiểm).
    - Kết quả gửi lệnh không chắc chắn ⇒ CHỈ tra theo clientOrderId, KHÔNG gửi lại.
    - Chỉ coi là thành công khi xác nhận ĐÃ KHỚP (executedQty > 0); ngược lại trả False để
      nơi gọi giữ close_pending và thử lại vòng sau.
    Trả (ok, msg)."""
    live, lerr = await _live_position(session, symbol, pos_side)
    if lerr:
        return False, f"không đọc được vị thế để đóng: {lerr}"
    if live is None:
        return True, "vị thế đã phẳng — không cần đóng"
    qty_p, _, _ = await get_symbol_precisions(session, symbol)
    step, _, _ = await _symbol_constraints(session, symbol, qty_p)
    qty = live['qty'] if not qty_cap else min(live['qty'], float(qty_cap))
    qty = _round_to_step(qty, step)
    if qty < max(step, _min_step_qty(qty_p)) or qty <= 0:
        return False, f"khối lượng còn lại {live['qty']:g} dưới bước tối thiểu — cần đóng tay"
    close_side = 'SELL' if live['side'] == 'LONG' else 'BUY'
    client_id = _new_client_order_id('pnlbot_emg')
    params = {'symbol': symbol, 'side': close_side, 'type': 'MARKET',
              'quantity': f"{qty:.{qty_p}f}", 'newOrderRespType': 'RESULT',
              'newClientOrderId': client_id}
    if live['pos_side'] != 'BOTH':
        params['positionSide'] = live['pos_side']
    else:
        params['reduceOnly'] = 'true'
    data, err = await binance_signed_request(session, 'POST', '/fapi/v1/order', params)
    if err:
        # Không rõ lệnh có vào sàn hay không → tra theo clientOrderId, tuyệt đối không gửi lại
        found, qerr = await _query_order(session, symbol, client_id=client_id)
        fill = _fill_from_order(found) if not qerr else None
        if fill:
            return True, f"lệnh đóng đã khớp {fill[0]:g} @ {fill[1]:g} (xác nhận qua tra cứu)"
        if not qerr and isinstance(found, dict) and found.get('orderId'):
            return False, f"lệnh đóng đang treo chưa khớp (orderId={found.get('orderId')}) — sẽ thử lại"
        return False, err
    fill = _fill_from_order(data)
    if fill:
        return True, f"đã đóng {fill[0]:g} @ {fill[1]:g}"
    order_id = _normalize_order_id((data or {}).get('orderId'))
    if order_id:
        found, qerr = await _query_order(session, symbol, order_id=order_id)
        fill = _fill_from_order(found) if not qerr else None
        if fill:
            return True, f"đã đóng {fill[0]:g} @ {fill[1]:g} (xác nhận qua tra cứu)"
    return False, "lệnh đóng chưa xác nhận khớp — sẽ thử lại vòng sau"


async def _execute_protected_entry(session, *, symbol, side, quantity, price, sl_price, tp_price,
                                   qty_p, price_p, step, pos_side, max_lev, open_symbols=(),
                                   risk_budget=None):
    """Mở vị thế MARKET + bảo vệ ĐÚNG THỨ TỰ: fill thật → SL → TP.
    - Không vào lệnh nếu symbol đang có vị thế (chồng lệnh, không đối soát được funding hedge).
    - Rủi ro CHỐT LẠI theo giá khớp thật (đã tính đệm phí/trượt): vượt ngân sách ⇒ giảm size, không đủ thì đóng.
    - Thiếu SL ⇒ đóng khẩn cấp (không để vị thế trần); không báo 'đã bảo vệ' khi SL lỗi.
    Trả dict: ok, stage, order_id, entry_time, entry_qty (khớp gốc), filled_qty (còn được bảo vệ),
    entry_price, sl_id, tp_id, msg, emergency, close_pending."""
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    order_side = 'BUY' if side == 'LONG' else 'SELL'   # API Binance dùng BUY/SELL, không dùng LONG/SHORT
    close_side = 'SELL' if side == 'LONG' else 'BUY'
    if symbol in set(open_symbols or ()):
        return {'ok': False, 'stage': 'flat_check',
                'msg': f"{display_symbol(symbol)} đang có vị thế — không mở chồng (rủi ro/funding không đối soát được)."}
    lev = _safe_leverage_for_sl(price, sl_price, max_lev)
    if not await set_leverage(session, api_key, api_secret, symbol, lev):
        return {'ok': False, 'stage': 'leverage',
                'msg': f"không set được đòn bẩy {lev}x cho {symbol} — không mở lệnh (fail closed)."}
    qty_str = f"{quantity:.{qty_p}f}"
    client_id = _new_client_order_id('pnlbot_entry')
    submit_ts = time.time()   # mốc thời gian TRƯỚC khi gửi: đối soát dùng startTime nên phải sớm hơn fill
    order, oerr = await _submit_entry_order(session, symbol, order_side, qty_str, pos_side, client_id)
    if oerr:
        return {'ok': False, 'stage': 'entry', 'entry_time': submit_ts,
                'msg': f"đặt lệnh thất bại: {oerr}"}
    order_id = _normalize_order_id((order or {}).get('orderId'))
    fill = _fill_from_order(order)
    if fill is None and order_id:
        queried, qerr = await _query_order(session, symbol, order_id=order_id)
        if not qerr:
            fill = _fill_from_order(queried)
    if fill is None:
        fill = await _fill_from_position_risk(session, symbol, pos_side)
    if fill is None:
        emg_ok, emg_msg = await _emergency_reduce_close(session, symbol, side, pos_side, quantity)
        return {'ok': False, 'stage': 'fill', 'order_id': order_id, 'entry_time': submit_ts,
                'emergency': emg_ok, 'close_pending': not emg_ok,
                'msg': f"không xác nhận được giá khớp → {'đóng khẩn cấp: ' if emg_ok else 'CHƯA đóng được: '}{emg_msg}"}
    entry_qty, entry_price = fill   # entry_qty = khối lượng KHỚP GỐC (đối soát PnL), không phải phần còn lại
    remaining_qty = entry_qty
    # Rủi ro THẬT theo giá khớp (không theo ticker): trượt giá làm vượt ngân sách ⇒ giảm size ngay
    sl_dist_fill = abs(entry_price - sl_price) + entry_price * AUTO_TRADE_COST_PCT
    if risk_budget is not None and sl_dist_fill > 0:
        actual_risk = entry_qty * sl_dist_fill
        if actual_risk > risk_budget + 1e-9:
            allowed_qty = _round_to_step(risk_budget / sl_dist_fill, step)
            if allowed_qty < max(step, _min_step_qty(qty_p)):
                emg_ok, emg_msg = await _emergency_reduce_close(session, symbol, side, pos_side, entry_qty)
                return {'ok': False, 'stage': 'risk_trim', 'order_id': order_id, 'entry_time': submit_ts,
                        'entry_price': entry_price, 'entry_qty': entry_qty, 'filled_qty': entry_qty,
                        'emergency': emg_ok, 'close_pending': not emg_ok, 'lev': lev,
                        'msg': (f"trượt giá làm rủi ro {actual_risk:,.2f} > ngân sách {risk_budget:,.2f} USDT "
                                f"và size tối thiểu cũng vượt ngân sách → "
                                f"{'đóng khẩn cấp: ' if emg_ok else 'CHƯA đóng được: '}{emg_msg}")}
            excess_qty = entry_qty - allowed_qty
            red_ok, red_msg = await _emergency_reduce_close(session, symbol, side, pos_side, excess_qty)
            if not red_ok:
                emg_ok, emg_msg = await _emergency_reduce_close(session, symbol, side, pos_side, entry_qty)
                return {'ok': False, 'stage': 'risk_trim', 'order_id': order_id, 'entry_time': submit_ts,
                        'entry_price': entry_price, 'entry_qty': entry_qty, 'filled_qty': entry_qty,
                        'emergency': emg_ok, 'close_pending': not emg_ok, 'lev': lev,
                        'msg': (f"không giảm được size theo rủi ro thật ({red_msg}) → "
                                f"{'đóng khẩn cấp: ' if emg_ok else 'CHƯA đóng được: '}{emg_msg}")}
            logger.warning(f"[EXEC] {symbol}: trượt giá → giảm size {entry_qty:g} → {allowed_qty:g} "
                           f"(rủi ro thật {actual_risk:,.2f} > ngân sách {risk_budget:,.2f} USDT).")
            remaining_qty = allowed_qty
    close_qty_str = f"{round_down(remaining_qty, qty_p):.{qty_p}f}"
    ok_sl, sl_info = await _place_conditional_tpsl(
        session, symbol, close_side, 'STOP_MARKET', f"{sl_price:.{price_p}f}", close_qty_str,
        pos_side, client_id=_new_client_order_id('pnlbot_sl'))
    if not ok_sl:
        emg_ok, emg_msg = await _emergency_reduce_close(session, symbol, side, pos_side, remaining_qty)
        return {'ok': False, 'stage': 'sl', 'order_id': order_id, 'entry_time': submit_ts,
                'entry_price': entry_price, 'entry_qty': entry_qty, 'filled_qty': remaining_qty,
                'emergency': emg_ok, 'close_pending': not emg_ok, 'lev': lev,
                'msg': (f"KHÔNG đặt được SL ({sl_info}) → "
                        f"{'đóng khẩn cấp: ' if emg_ok else 'vẫn CHƯA đóng được: '}{emg_msg}")}
    result = {'ok': True, 'stage': 'done', 'order_id': order_id, 'entry_time': submit_ts,
              'entry_price': entry_price, 'entry_qty': entry_qty, 'filled_qty': remaining_qty,
              'sl_id': _normalize_order_id(sl_info), 'tp_id': None,
              'lev': lev, 'emergency': False, 'close_pending': False, 'msg': ''}
    if tp_price:
        ok_tp, tp_info = await _place_conditional_tpsl(
            session, symbol, close_side, 'TAKE_PROFIT_MARKET', f"{tp_price:.{price_p}f}", close_qty_str,
            pos_side, client_id=_new_client_order_id('pnlbot_tp'))
        if ok_tp:
            result['tp_id'] = _normalize_order_id(tp_info)
        else:
            result['msg'] = f"SL đã đặt nhưng TP lỗi: {tp_info}"
    return result


async def _replace_protective_sl(session, meta, new_sl, close_qty, close_side, pos_side, price_p, qty_p):
    """Đặt SL MỚI trước, chỉ hủy SL cũ SAU khi SL mới đã xác nhận.
    last_sl chỉ đổi khi SL mới thật sự đặt thành công. Trả id SL đang hiệu lực (hoặc None)."""
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    old_id = _normalize_order_id(meta.get('sl_algo_id'))
    ok, info = await _place_conditional_tpsl(
        session, meta['symbol'], close_side, 'STOP_MARKET', f"{new_sl:.{price_p}f}",
        f"{close_qty:.{qty_p}f}", pos_side, client_id=_new_client_order_id('pnlbot_sl'))
    if not ok:
        logger.warning(f"[AI-TRAIL] SL mới {format_price(new_sl)} cho {meta['symbol']} thất bại: {info} "
                       f"— GIỮ SL cũ {format_price(meta.get('last_sl', meta.get('sl_initial')))}.")
        return None
    new_id = _normalize_order_id(info)
    meta['sl_algo_id'] = new_id or old_id
    meta['last_sl'] = new_sl
    _save_auto_managed()
    if old_id and new_id and old_id != new_id:
        if not await _cancel_algo_sl(session, api_key, api_secret, meta['symbol'], old_id):
            stale = list(meta.get('stale_sl_ids') or [])
            if old_id not in stale:
                stale.append(old_id)
            meta['stale_sl_ids'] = stale
            _save_auto_managed()
            logger.warning(f"[AI-TRAIL] Không hủy được SL cũ {old_id} của {meta['symbol']} "
                           "(SL mới đã hoạt động) — sẽ dọn ở vòng sau.")
    return meta['sl_algo_id']


async def _cleanup_stale_sl(session, meta):
    """Dọn các SL cũ chưa hủy được ở vòng trước (chỉ khi SL mới đã hoạt động)."""
    stale = list(meta.get('stale_sl_ids') or [])
    if not stale:
        return
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    remain = []
    for aid in stale:
        if aid == _normalize_order_id(meta.get('sl_algo_id')):
            continue
        if not await _cancel_algo_sl(session, api_key, api_secret, meta['symbol'], aid):
            remain.append(aid)
    if remain:
        meta['stale_sl_ids'] = remain
    else:
        meta.pop('stale_sl_ids', None)
    _save_auto_managed()


def _execution_record(*, order_id, quantity, pos_side, side, entry_time, flat_at_entry=True):
    """Dict `execution` cho record_signal: đối soát độc lập bằng fills REST (không dùng nến).
    entry_time = mốc TRƯỚC khi gửi lệnh (epoch giây) để đối soát startTime không bỏ sót fill vào.
    quantity = khối lượng KHỚP GỐC của lệnh vào (các lần đóng sớm sẽ cộng dồn theo order_id này)."""
    pos = pos_side if pos_side in ('LONG', 'SHORT') else ('LONG' if side == 'LONG' else 'SHORT')
    return {
        'order_id': _normalize_order_id(order_id),
        'quantity': float(quantity),
        'position_side': pos,
        'entry_time': float(entry_time),
        'symbol_flat_at_entry': bool(flat_at_entry),
    }


# ═══ Lệnh MỞ LIMIT chờ khớp: gắn bảo vệ NGAY khi có khối lượng khớp ═══
# Sàn TỪ CHỐI lệnh SL reduce-only khi chưa có vị thế (-2022), nên lệnh LIMIT chưa khớp
# không thể đặt SL trước. Theo dõi lệnh tới khi khớp rồi mới gắn SL (trước) + TP (sau).
PENDING_ENTRIES_FILE = "pending_entries_trading.json"
PENDING_ENTRY_CHECK_SEC = 20
PENDING_ENTRY_MAX_AGE_SEC = 7 * 86400
pending_entries = {}   # client_order_id -> {symbol, side, pos_side, order_id, client_order_id,
                       #   quantity, sl, tp, limit_price, entry_time, ts, signal_id, entry_price,
                       #   sl_algo_id, tp_algo_id, source}


def save_pending_entries():
    """Ghi NGUYÊN TỬ (tmp + fsync + os.replace) để restart giữa chừng không mất bảo vệ."""
    try:
        temporary = PENDING_ENTRIES_FILE + '.tmp'
        with open(temporary, 'w', encoding='utf-8') as f:
            json.dump(pending_entries, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, PENDING_ENTRIES_FILE)
    except Exception as e:
        logger.error(f"Lỗi lưu pending_entries: {e}")


def load_pending_entries():
    """Nạp lại các lệnh MỞ đang chờ khớp sau restart (watcher sẽ gắn bảo vệ tiếp)."""
    global pending_entries
    try:
        if os.path.exists(PENDING_ENTRIES_FILE):
            with open(PENDING_ENTRIES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                pending_entries = data
            logger.info(f"Đã nạp {len(pending_entries)} lệnh MỞ đang chờ khớp.")
    except Exception as e:
        logger.error(f"Lỗi nạp pending_entries: {e}")


def _drop_pending_entry(key, reason):
    """Bỏ theo dõi 1 lệnh MỞ đang chờ (đã khớp + bảo vệ xong, huỷ, hoặc quá hạn)."""
    if pending_entries.pop(key, None) is not None:
        save_pending_entries()
    logger.info(f"[ENTRY-WATCH] Bỏ theo dõi {key}: {reason}")


def _register_ai_position_meta(*, symbol, side, pos_side, entry_price, sl_price, tp_price, qty,
                               sl_id, tp_id, signal_id, close_pending=False):
    """Lưu meta vị thế do /ai mở. managed=False: loop trailing KHÔNG quản lý vị thế của người dùng,
    chỉ dùng để (a) giữ liên kết signal_id và (b) đóng khẩn cấp lại nếu lệnh bảo vệ hỏng."""
    auto_managed[f"{symbol}_{pos_side}"] = {
        'symbol': symbol, 'side': side, 'entry': float(entry_price or 0),
        'sl_initial': float(sl_price or 0),
        'risk': abs(float(entry_price or 0) - float(sl_price or 0)),
        'atr': 0.0, 'tp': tp_price or None, 'qty': float(qty or 0), 'pos_side': pos_side,
        'sl_algo_id': _normalize_order_id(sl_id), 'tp_algo_id': _normalize_order_id(tp_id),
        'last_sl': float(sl_price or 0), 'ts': time.time(), 'signal_id': signal_id,
        'managed': False, 'close_pending': bool(close_pending),
    }
    _save_auto_managed()


async def _protect_entry_fill(session, pend, order_data):
    """Gắn bảo vệ cho 1 lệnh MỞ đã có khối lượng khớp: record_signal (1 lần) → SL → TP,
    khối lượng theo VỊ THẾ THẬT. Thiếu SL ⇒ đóng khẩn cấp + giữ meta close_pending. Trả (ok, msg)."""
    symbol = pend.get('symbol')
    side = str(pend.get('side') or '').upper()
    pos_side = str(pend.get('pos_side') or 'BOTH')
    sl_price = float(pend.get('sl') or 0)
    if sl_price <= 0:
        return False, "thiếu SL — không thể bảo vệ vị thế"
    live, lerr = await _live_position(session, symbol, None if pos_side == 'BOTH' else pos_side)
    if lerr:
        return False, f"không đọc được vị thế {symbol}: {lerr}"
    if live is None:
        return False, "chưa có vị thế (hoặc đã phẳng)"
    qty_p, price_p, _ = await get_symbol_precisions(session, symbol)
    step, min_qty, _ = await _symbol_constraints(session, symbol, qty_p)
    qty = _round_to_step(live['qty'], step)
    if qty < max(step, min_qty):
        return False, "khối lượng vị thế dưới bước tối thiểu của sàn"
    close_side = 'SELL' if live['side'] == 'LONG' else 'BUY'
    prot_pos_side = None if pos_side == 'BOTH' else pos_side

    # 1. Ghi lịch sử tín hiệu theo KHỚP THẬT (chống trùng bằng order_id của lệnh vào)
    if not pend.get('signal_id'):
        fill = _fill_from_order(order_data)
        entry_price = fill[1] if fill and fill[1] > 0 else float(pend.get('limit_price') or 0)
        entry_qty = fill[0] if fill else qty
        if entry_price > 0:
            execution = _execution_record(
                order_id=pend.get('order_id'), quantity=entry_qty, pos_side=pos_side, side=side,
                entry_time=pend.get('entry_time') or pend.get('ts') or time.time())
            pend['signal_id'] = record_signal(
                {'symbol': symbol, 'signal': side, 'close': entry_price, 'tp': pend.get('tp') or None,
                 'sl': sl_price, 'confidence': 'AI chat'}, origin='ai', execution=execution)
            pend['entry_price'] = entry_price
            save_pending_entries()

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    old_id = _normalize_order_id(pend.get('sl_algo_id'))
    # 2. SL TRƯỚC (đặt mới rồi mới hủy cái cũ nếu khối lượng vị thế đã tăng thêm)
    ok_sl, sl_info = await _place_conditional_tpsl(
        session, symbol, close_side, 'STOP_MARKET', f"{sl_price:.{price_p}f}",
        f"{qty:.{qty_p}f}", prot_pos_side, client_id=_new_client_order_id('pnlbot_sl'))
    new_id = _normalize_order_id(sl_info) if ok_sl else None
    if not new_id:
        emg_ok, emg_msg = await _emergency_reduce_close(session, symbol, live['side'],
                                                       prot_pos_side, qty)
        _register_ai_position_meta(
            symbol=symbol, side=side, pos_side=pos_side,
            entry_price=pend.get('entry_price') or pend.get('limit_price'),
            sl_price=sl_price, tp_price=None, qty=qty, sl_id=None, tp_id=None,
            signal_id=pend.get('signal_id'), close_pending=not emg_ok)
        return False, (f"KHÔNG đặt được SL ({sl_info}) → "
                       f"{'đóng khẩn cấp: ' if emg_ok else 'CHƯA đóng được: '}{emg_msg}")
    if old_id and new_id != old_id:
        await _cancel_algo_sl(session, api_key, api_secret, symbol, old_id)
    pend['sl_algo_id'] = new_id
    save_pending_entries()

    # 3. TP SAU khi SL đã xác nhận
    tp_price = float(pend.get('tp') or 0)
    if tp_price > 0 and not pend.get('tp_algo_id'):
        ok_tp, tp_info = await _place_conditional_tpsl(
            session, symbol, close_side, 'TAKE_PROFIT_MARKET', f"{tp_price:.{price_p}f}",
            f"{qty:.{qty_p}f}", prot_pos_side, client_id=_new_client_order_id('pnlbot_tp'))
        if ok_tp:
            pend['tp_algo_id'] = _normalize_order_id(tp_info)
            save_pending_entries()
        else:
            logger.warning(f"[ENTRY-WATCH] {symbol}: SL đã gắn nhưng TP lỗi ({tp_info}).")
    _register_ai_position_meta(
        symbol=symbol, side=side, pos_side=pos_side,
        entry_price=pend.get('entry_price') or pend.get('limit_price'),
        sl_price=sl_price, tp_price=tp_price or None, qty=qty, sl_id=new_id,
        tp_id=pend.get('tp_algo_id'), signal_id=pend.get('signal_id'))
    return True, f"đã gắn SL {format_price(sl_price)} cho {qty:g} {display_symbol(symbol)}"


async def _refresh_pending_entries(session):
    """Một vòng: kiểm tra mọi lệnh MỞ đang chờ khớp, gắn bảo vệ ngay khi có vị thế."""
    now = time.time()
    for key, pend in list(pending_entries.items()):
        try:
            if now - float(pend.get('ts') or now) > PENDING_ENTRY_MAX_AGE_SEC:
                _drop_pending_entry(key, "quá 7 ngày chưa khớp — bỏ theo dõi")
                await _notify_all_chats(
                    session,
                    f"⚠️ *Lệnh MỞ treo quá 7 ngày* của {display_symbol(pend.get('symbol'))} "
                    f"(#{pend.get('order_id')}) đã bị bỏ theo dõi — kiểm tra và hủy tay nếu cần."
                )
                continue
            data, err = await _query_order(session, pend.get('symbol'), order_id=pend.get('order_id'))
            if err:
                logger.warning(f"[ENTRY-WATCH] Không đọc được lệnh {key}: {err}")
                continue
            status = str((data or {}).get('status') or '').upper()
            fill = _fill_from_order(data)
            if status in ('CANCELED', 'EXPIRED', 'REJECTED') and not fill:
                _drop_pending_entry(key, f"lệnh {status} và chưa khớp")
                continue
            if not fill and status == 'NEW':
                continue   # chưa khớp: chưa có gì để bảo vệ
            if fill and status in ('FILLED', 'CANCELED', 'EXPIRED', 'REJECTED'):
                # Lệnh đã kết thúc: nếu vị thế đã phẳng (bị đóng tay/SL) thì không còn gì phải bảo vệ
                live, lerr = await _live_position(session, pend.get('symbol'),
                                                 None if str(pend.get('pos_side') or 'BOTH') == 'BOTH'
                                                 else pend.get('pos_side'))
                if not lerr and live is None:
                    _drop_pending_entry(key, f"lệnh {status} và vị thế đã phẳng")
                    continue
            ok, msg = await _protect_entry_fill(session, pend, data)
            logger.info(f"[ENTRY-WATCH] {key}: {msg}")
            if not ok:
                continue   # giữ pending + meta close_pending để vòng sau/recovery xử lý tiếp
            if status in ('FILLED', 'CANCELED', 'EXPIRED', 'REJECTED'):
                _drop_pending_entry(key, f"lệnh {status} — bảo vệ đã gắn")
                await _notify_all_chats(
                    session,
                    f"🛡️ *Đã gắn bảo vệ lệnh MỞ* {display_symbol(pend.get('symbol'))} "
                    f"({pend.get('side')}) — {msg}"
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[ENTRY-WATCH] Lỗi xử lý {key}: {e}")


async def pending_entry_watch_loop(app):
    """Mỗi PENDING_ENTRY_CHECK_SEC: gắn SL/TP cho lệnh MỞ LIMIT ngay khi khớp (không để vị thế trần)."""
    await asyncio.sleep(30)
    while True:
        try:
            if pending_entries:
                await _refresh_pending_entries(app['session'])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Lỗi trong pending_entry_watch_loop: {e}")
        await asyncio.sleep(PENDING_ENTRY_CHECK_SEC)


async def _notify_all_chats(session, text):
    """Gửi thông báo tới tất cả chat đã biết (auto chats + active chats)."""
    for cid in list(set(auto_chats) | set(active_chats)):
        await send_telegram_message(session, cid, text, is_auto=True)


async def _auto_place_order(session, best, snapshot):
    """Đặt 1 lệnh auto cho tín hiệu `best` theo NGÂN SÁCH RỦI RO. Trả True nếu đã vào được vị thế.
    `snapshot` = _account_risk_snapshot (equity, available, rủi ro đang mở, hạn lỗ ngày còn lại)."""
    symbol = best['symbol']

    # Rào chắn: funding cực đoan theo hướng đám đông → nguy cơ đảo chiều, bỏ qua
    funding_rate = best.get('funding_rate')
    if funding_rate is not None:
        if best['signal'] == 'LONG' and funding_rate >= AUTO_MAX_FUNDING:
            logger.info(f"[AI-AUTO] Bỏ qua {symbol} LONG: funding quá đông long ({funding_rate * 100:.4f}%/h).")
            await _notify_all_chats(
                session,
                f"🤖🚫 *Bỏ qua {display_symbol(symbol)} LONG*: funding quá đông long "
                f"({funding_rate * 100:.4f}%/h) — nguy cơ đảo chiều cao."
            )
            return False
        if best['signal'] == 'SHORT' and funding_rate <= -AUTO_MAX_FUNDING:
            logger.info(f"[AI-AUTO] Bỏ qua {symbol} SHORT: funding quá đông short ({funding_rate * 100:.4f}%/h).")
            await _notify_all_chats(
                session,
                f"🤖🚫 *Bỏ qua {display_symbol(symbol)} SHORT*: funding quá đông short "
                f"({funding_rate * 100:.4f}%/h) — nguy cơ đảo chiều cao."
            )
            return False

    # Không có SL ⇒ không đo được rủi ro ⇒ không vào lệnh
    if not best.get('sl'):
        logger.warning(f"[AI-AUTO] {symbol}: tín hiệu thiếu SL — bỏ qua.")
        return False

    ai_sc = (best.get('ai') or {}).get('long_score' if best['signal'] == 'LONG' else 'short_score')
    ai_sc_txt = f"{ai_sc:.1f}" if isinstance(ai_sc, (int, float)) else "n/a"
    signal_desc = (f"{symbol} {best['signal']} ({best['confidence']}, "
                   f"điểm {best.get('_score', 0):.1f}, AI tự chấm {ai_sc_txt})\n"
                   f"Entry tín hiệu ~{format_price(best.get('close'))}, "
                   f"TP {format_price(best.get('tp'))}, SL {format_price(best['sl'])}")

    async with _entry_lock():
        try:
            price_ref = await get_single_price(session, symbol)
        except Exception as e:
            logger.warning(f"[AI-AUTO] Không lấy được giá {symbol}: {e}")
            price_ref = 0
        if not price_ref or price_ref <= 0:
            logger.warning(f"[AI-AUTO] {symbol}: không có giá tham chiếu — bỏ qua.")
            return False

        # Rào chắn: giá hiện tại cách close lúc quét quá xa → tín hiệu đã cũ, TP/SL lệch thực tế
        if best.get('close'):
            slip_pct = abs(price_ref - best['close']) / best['close']
            if slip_pct > AUTO_MAX_SLIP_PCT:
                logger.info(f"[AI-AUTO] Bỏ qua {symbol}: giá lệch tín hiệu {slip_pct * 100:.2f}%.")
                await _notify_all_chats(
                    session,
                    f"🤖🚫 *Bỏ qua {display_symbol(symbol)} {best['signal']}*: giá hiện tại cách tín hiệu "
                    f"{slip_pct * 100:.2f}% (giới hạn {AUTO_MAX_SLIP_PCT * 100:.2f}%) — tín hiệu đã cũ."
                )
                return False

        max_lev, lerr = await _max_leverage_strict(session, symbol)
        if lerr:
            logger.warning(f"[AI-AUTO] {lerr} — không vào lệnh (fail closed).")
            await _notify_all_chats(
                session,
                f"⚠️ *Bỏ qua {display_symbol(symbol)}*: {lerr} — không mở lệnh khi chưa đọc được "
                f"đòn bẩy tối đa (fail closed)."
            )
            return False

        qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)
        sl_trig = round_price_step(float(best['sl']), tick_size, price_p)
        if (best['signal'] == 'LONG' and sl_trig >= price_ref) or \
                (best['signal'] == 'SHORT' and sl_trig <= price_ref):
            logger.warning(f"[AI-AUTO] {symbol}: SL {format_price(sl_trig)} nằm sai phía entry — bỏ qua.")
            return False
        # Đòn bẩy vừa đủ để SL nằm trong vùng an toàn trước thanh lý (KHÔNG kéo SL về gần entry)
        lev = _safe_leverage_for_sl(price_ref, sl_trig, max_lev)
        step, min_qty, min_notional = await _symbol_constraints(session, symbol, qty_p)
        ok_size, quantity, size_msg = _plan_entry_size(
            price_ref, sl_trig, best['signal'],
            equity=snapshot['equity'], available=snapshot['available'],
            open_risk=snapshot['open_risk'], daily_remaining=snapshot['daily_remaining'],
            leverage=lev, step=step, min_qty=min_qty, min_notional=min_notional)
        if not ok_size:
            logger.info(f"[AI-AUTO] {symbol}: {size_msg}")
            await _notify_all_chats(
                session,
                f"🤖⚡ *AI tìm được lệnh ngon* nhưng không vào được theo ngân sách rủi ro:\n{signal_desc}\n"
                f"→ {size_msg}\n"
                f"Ngân sách: 0.5% equity/lệnh, tổng rủi ro mở ≤ 1.5% equity (đã trừ rủi ro đang mở), "
                f"hạn lỗ ngày còn {snapshot['daily_remaining']:,.2f} USDT."
            )
            return False

        sl_dist = abs(price_ref - sl_trig)
        # TP: giữ nguyên hành vi cũ — cap khoảng cách ≤ 2× khoảng cách SL; không có TP thì chỉ đặt SL
        tp_trig = 0.0
        try:
            tp_raw = float(best.get('tp') or 0)
        except (TypeError, ValueError):
            tp_raw = 0.0
        if tp_raw > 0:
            if abs(tp_raw - price_ref) > 2 * sl_dist:
                tp_trig = price_ref + (2 * sl_dist if best['signal'] == 'LONG' else -2 * sl_dist)
            else:
                tp_trig = tp_raw
            tp_trig = round_price_step(tp_trig, tick_size, price_p)

        pos_side = ('LONG' if best['signal'] == 'LONG' else 'SHORT') if hedge_mode else 'BOTH'
        risk_budget = _risk_budget_allowance(snapshot['equity'], snapshot['open_risk'],
                                             snapshot['daily_remaining'])
        result = await _execute_protected_entry(
            session, symbol=symbol, side=best['signal'], quantity=quantity, price=price_ref,
            sl_price=sl_trig, tp_price=tp_trig, qty_p=qty_p, price_p=price_p, step=step,
            pos_side=pos_side, max_lev=max_lev, open_symbols=snapshot.get('open_symbols') or (),
            risk_budget=risk_budget)

        # Lệnh CÓ THỂ đã khớp dù bảo vệ lỗi → ghi signal theo khớp thật để PnL vẫn được đối soát
        opened_qty = result.get('entry_qty')
        signal_id = None
        if opened_qty:
            execution = _execution_record(order_id=result['order_id'], quantity=opened_qty,
                                          pos_side=pos_side, side=best['signal'],
                                          entry_time=result['entry_time'])
            best['tp'] = tp_trig or None
            best['sl'] = sl_trig
            best['close'] = result['entry_price']
            signal_id = record_signal(best, best.get('ai'), origin='auto', execution=execution)

        if not result['ok']:
            logger.warning(f"[AI-AUTO] {symbol}: {result['msg']}")
            await _notify_all_chats(
                session,
                f"⚠️ *AI định tự vào {display_symbol(symbol)} nhưng KHÔNG an toàn*:\n{signal_desc}\n"
                f"→ {result['msg']}"
            )
            if result.get('close_pending'):
                # Khớp rồi mà chưa đóng lại được → để loop trailing thử tiếp, không bỏ mặc vị thế
                auto_managed[f"{symbol}_{pos_side}"] = {
                    'symbol': symbol, 'side': best['signal'], 'pos_side': pos_side,
                    'managed': True, 'close_pending': True,
                    'entry': result.get('entry_price') or price_ref,
                    'sl_initial': sl_trig, 'risk': sl_dist,
                    'qty': result.get('entry_qty') or quantity,
                    'atr': float(best.get('atr') or 0) or sl_dist * 1.5,
                    'ts': time.time(), 'signal_id': signal_id,
                }
                _save_auto_managed()
            return False

        filled_qty = result['filled_qty']   # phần CÒN LẠI được bảo vệ (đã trừ size cắt do trượt giá)
        entry_fill = result['entry_price']
        logger.info(f"[AI-AUTO] Đã vào {symbol} {best['signal']} khớp {result['entry_qty']:g} "
                    f"(còn {filled_qty:g}) @ {entry_fill:g} "
                    f"(lev {result['lev']}x, orderId={result['order_id']}, signal_id={signal_id})")

        # Đăng ký vị thế để trailing loop quản lý breakeven + trailing stop
        auto_managed[f"{symbol}_{pos_side}"] = {
            'symbol': symbol,
            'side': best['signal'],
            'entry': entry_fill,
            'sl_initial': sl_trig,
            'risk': sl_dist,
            'atr': float(best.get('atr') or 0) or sl_dist * 1.5,
            'tp': tp_trig or None,
            'qty': filled_qty,
            'pos_side': pos_side,
            'sl_algo_id': result['sl_id'],
            'tp_algo_id': result['tp_id'],
            'last_sl': sl_trig,
            'ts': time.time(),
            'signal_id': signal_id,
            'managed': True,
        }
        _save_auto_managed()

        tpsl_lines = [f"{'✅ SL' if result['sl_id'] else '❌ SL'} kích hoạt {format_price(sl_trig)} "
                      f"(algoId `{result['sl_id']}`)"]
        if tp_trig:
            tpsl_lines.append(f"{'✅ TP' if result['tp_id'] else '❌ TP'} kích hoạt {format_price(tp_trig)} "
                              f"(algoId `{result['tp_id']}`)")
        if result['msg']:
            tpsl_lines.append(f"⚠️ {result['msg']}")
        await _notify_all_chats(
            session,
            f"🤖⚡ *AI TỰ ĐỘNG VÀO LỆNH*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 {signal_desc}\n"
            f"📦 Khối lượng: {filled_qty:g} (đòn bẩy {result['lev']}x)\n"
            f"🎯 Giá khớp thật: {format_price(entry_fill)}\n"
            f"💵 {size_msg}\n"
            f"OrderId: `{result['order_id']}`\n"
            f"🛡️ *Bảo vệ:*\n" + "\n".join(tpsl_lines)
        )
        return True


async def _cancel_algo_sl(session, api_key, api_secret, symbol, algo_id):
    """Hủy 1 lệnh SL algo cụ thể (tránh đụng TP/SL của lệnh khác)."""
    if not algo_id:
        return False
    try:
        query = urlencode({'symbol': symbol, 'algoId': algo_id, 'timestamp': int(time.time() * 1000)})
        sig = get_binance_signature(query, api_secret)
        url = f"https://fapi.binance.com/fapi/v1/algoOrder?{query}&signature={sig}"
        headers = {"X-MBX-APIKEY": api_key}
        async with session.delete(yarl.URL(url, encoded=True), headers=headers) as resp:
            data = await resp.json()
            if resp.status == 200:
                logger.info(f"[AI-TRAIL] Đã hủy SL algo cũ algoId={algo_id} của {symbol}")
                return True
            logger.warning(f"[AI-TRAIL] Hủy SL algo {algo_id} thất bại: {data.get('msg')}")
    except Exception as e:
        logger.error(f"[AI-TRAIL] Lỗi hủy SL algo {algo_id}: {e}")
    return False


async def _place_conditional_tpsl(session, symbol, close_side, order_type, trigger_price, quantity,
                                  pos_side=None, client_id=None):
    """Đặt 1 lệnh điều kiện TP/SL qua Algo Service (workingType = MARK_PRICE).
    trigger_price/quantity phải là chuỗi đã làm tròn đúng tick size.
    Hedge Mode: dùng positionSide và KHÔNG gửi reduceOnly; One-way: gửi reduceOnly.
    Trả về (ok: bool, algoId dạng str hoặc thông báo lỗi)."""
    params = {
        'algoType': 'CONDITIONAL',
        'symbol': symbol,
        'side': close_side,
        'type': order_type,
        'triggerPrice': trigger_price,
        'quantity': quantity,
        'workingType': 'MARK_PRICE',
    }
    if pos_side and pos_side != 'BOTH':
        params['positionSide'] = pos_side
    else:
        params['reduceOnly'] = 'true'
    if client_id:
        # Algo Service dùng `clientAlgoId` (KHÔNG phải newClientOrderId): ^[.A-Z:/a-z0-9_-]{1,36}$
        params['clientAlgoId'] = client_id
    data, err = await binance_signed_request(session, 'POST', '/fapi/v1/algoOrder', params)
    if err:
        return False, err
    algo_id = _normalize_order_id((data or {}).get('algoId') or (data or {}).get('orderId'))
    if not algo_id:
        return False, "sàn không trả về algoId — không xác nhận được lệnh bảo vệ."
    return True, algo_id


async def auto_trailing_loop(app):
    """Mỗi AUTO_TRAIL_CHECK_SEC: quản lý breakeven + trailing stop cho vị thế AI tự mở.
    Chỉ SIẾT CHẶT SL (kéo lên cho long, kéo xuống cho short), không bao giờ nới.
    - Đạt +AUTO_TRAIL_START_RR (0.8R) → trailing SL cách giá 1.0×ATR + hủy TP để lời chạy.
    - Mở quá AUTO_MAX_HOLD_HOURS (72h) → đóng thị trường (khớp mô hình expired của backtest,
      tránh kẹt vốn + phí funding trên lệnh không đi đâu)."""
    await asyncio.sleep(120)  # chờ khởi động xong
    while True:
        try:
            session = app['session']
            api_key = os.getenv("BINANCE_API_KEY")
            api_secret = os.getenv("BINANCE_API_SECRET")
            now = time.time()
            for key, meta in list(auto_managed.items()):
                # SL cũ chưa hủy được ở vòng trước → dọn (không ảnh hưởng SL mới đang hiệu lực)
                await _cleanup_stale_sl(session, meta)

                pos = positions.get(key)
                pos_amt = float(pos.get('positionAmt', 0) or 0) if pos else 0.0

                # 0. Đang chờ đóng (không đặt được SL hoặc quá hạn): thử đóng tới khi hết vị thế,
                #    CHỈ hủy bảo vệ SAU khi vị thế đã phẳng (đọc REST, không tin cache WS).
                if meta.get('close_pending'):
                    live, lerr = await _live_position(session, meta['symbol'], meta.get('pos_side'))
                    if lerr:
                        logger.warning(f"[AI-TRAIL] Không kiểm tra được vị thế {meta['symbol']}: {lerr}")
                        continue
                    if live is None:
                        for aid in (meta.get('sl_algo_id'), meta.get('tp_algo_id')):
                            await _cancel_algo_sl(session, api_key, api_secret, meta['symbol'], aid)
                        logger.info(f"[AI-TRAIL] {meta['symbol']}: vị thế đã phẳng, dọn bảo vệ còn lại.")
                        auto_managed.pop(key, None)
                        _save_auto_managed()
                        continue
                    red_ok, red_msg = await _emergency_reduce_close(
                        session, meta['symbol'], meta['side'], meta.get('pos_side'))
                    logger.warning(f"[AI-TRAIL] Đóng lại {meta['symbol']} {meta['side']}: "
                                   f"{'OK — ' if red_ok else 'CHƯA được — '}{red_msg}")
                    if red_ok:
                        await _notify_all_chats(
                            session,
                            f"⚠️ *Đã đóng khẩn cấp {display_symbol(meta['symbol'])} {meta['side']}* "
                            f"(lệnh bảo vệ không đặt được): {red_msg}"
                        )
                    continue

                if pos_amt == 0:
                    auto_managed.pop(key, None)  # vị thế đã đóng → dọn dẹp
                    _save_auto_managed()
                    continue
                if not meta.get('managed', True):
                    continue  # vị thế do /ai mở: không trailing, chỉ giữ bảo vệ

                # Đóng vị thế quá hạn (khớp expired của backtest = đóng ở ~0R, tránh phí funding)
                if now - meta.get('ts', 0) > AUTO_MAX_HOLD_HOURS * 3600:
                    # Đóng XONG mới hủy bảo vệ (đóng lỗi ⇒ vẫn còn SL che)
                    ok_close, close_msg = await _emergency_reduce_close(
                        session, meta['symbol'], meta['side'], meta.get('pos_side'))
                    if ok_close:
                        meta['close_pending'] = True
                        _save_auto_managed()
                        logger.info(f"[AI-TRAIL] Đóng vị thế auto {meta['symbol']} {meta['side']} "
                                    f"sau {AUTO_MAX_HOLD_HOURS}h: {close_msg}")
                        await _notify_all_chats(
                            session,
                            f"⏰ *Đóng vị thế auto {display_symbol(meta['symbol'])} {meta['side']}* "
                            f"vì mở quá {AUTO_MAX_HOLD_HOURS}h chưa chạm TP/SL ({close_msg})."
                        )
                    else:
                        logger.warning(f"[AI-TRAIL] Đóng vị thế quá hạn {meta['symbol']} thất bại: {close_msg}")
                    continue

                mark = float(pos.get('markPrice', 0) or 0)
                if mark <= 0:
                    continue  # chưa có giá mark (WS chưa cập nhật)
                entry = meta['entry']
                risk = meta['risk']
                if risk <= 0:
                    continue
                is_long = meta['side'] == 'LONG'
                r = (mark - entry) / risk if is_long else (entry - mark) / risk

                # Chốt lời một phần: đạt +PARTIAL_RR → chốt 50%; SL về entry chỉ đổi SAU khi đặt được SL mới
                if (AUTO_PARTIAL_TP_RR > 0 and not meta.get('partial_done')
                        and r >= AUTO_PARTIAL_TP_RR):
                    qty_p, price_p, _ = await get_symbol_precisions(session, meta['symbol'])
                    step, _, _ = await _symbol_constraints(session, meta['symbol'], qty_p)
                    real_qty = abs(pos_amt)
                    part_qty = _round_to_step(real_qty * AUTO_PARTIAL_TP_PCT, step)
                    if part_qty < max(step, _min_step_qty(qty_p)) or (real_qty - part_qty) < max(step, _min_step_qty(qty_p)):
                        logger.warning(f"[AI-TRAIL] {meta['symbol']}: khối lượng quá nhỏ để chốt một phần "
                                       "— bỏ qua bước này.")
                        meta['partial_done'] = True
                        _save_auto_managed()
                    else:
                        close_side = 'SELL' if is_long else 'BUY'
                        part_client = _new_client_order_id('pnlbot_part')
                        params = {
                            'symbol': meta['symbol'], 'side': close_side, 'type': 'MARKET',
                            'quantity': f"{part_qty:.{qty_p}f}", 'newOrderRespType': 'RESULT',
                            'newClientOrderId': part_client,
                        }
                        if meta['pos_side'] != 'BOTH':
                            params['positionSide'] = meta['pos_side']
                        else:
                            params['reduceOnly'] = 'true'
                        data, err = await binance_signed_request(session, 'POST', '/fapi/v1/order', params)
                        fill = _fill_from_order(data) if not err else None
                        if err and fill is None:
                            found, qerr = await _query_order(session, meta['symbol'], client_id=part_client)
                            fill = _fill_from_order(found) if not qerr else None
                        if fill:
                            closed_qty = fill[0]
                            remaining = max(real_qty - closed_qty, 0.0)
                            meta['partial_done'] = True
                            meta['qty'] = _round_to_step(remaining, step)
                            # Mức SL mong muốn cho phần còn lại: siết về entry — chỉ ghi last_sl khi đặt được
                            meta['be_arm'] = entry
                            _save_auto_managed()
                            logger.info(f"[AI-TRAIL] {meta['symbol']} {meta['side']} R={r:.2f}: "
                                        f"chốt {AUTO_PARTIAL_TP_PCT * 100:.0f}% lời +{AUTO_PARTIAL_TP_RR:.0f}R "
                                        f"({closed_qty:g}).")
                            await _notify_all_chats(
                                session,
                                f"💰 *Chốt lời một phần* {display_symbol(meta['symbol'])} {meta['side']} "
                                f"(+{AUTO_PARTIAL_TP_RR:.0f}R)\n"
                                f"Đã chốt {AUTO_PARTIAL_TP_PCT * 100:.0f}% khối lượng; phần còn lại "
                                f"đang được siết SL về entry."
                            )
                        else:
                            logger.warning(f"[AI-TRAIL] Chốt một phần {meta['symbol']} thất bại: {err or 'chưa xác nhận khớp'}")

                target_sl = None
                if r >= AUTO_TRAIL_START_RR:
                    if is_long:
                        target_sl = mark - meta['atr'] * AUTO_TRAIL_ATR_MULT
                    else:
                        target_sl = mark + meta['atr'] * AUTO_TRAIL_ATR_MULT
                elif AUTO_BE_RR > 0 and r >= AUTO_BE_RR:
                    target_sl = entry  # breakeven
                arm = meta.get('be_arm')
                if arm is not None:
                    target_sl = arm if target_sl is None else (max(target_sl, arm) if is_long else min(target_sl, arm))
                if target_sl is None:
                    continue

                last_sl = meta.get('last_sl', meta['sl_initial'])
                if is_long:
                    new_sl = max(last_sl, target_sl)
                else:
                    new_sl = min(last_sl, target_sl)
                improvement = ((new_sl - meta['sl_initial']) / risk if is_long
                               else (meta['sl_initial'] - new_sl) / risk)
                forced = arm is not None and new_sl != last_sl
                if new_sl == last_sl or (improvement < AUTO_TRAIL_MIN_RR and not forced):
                    continue

                qty_p, price_p, tick_size = await get_symbol_precisions(session, meta['symbol'])
                step, _, _ = await _symbol_constraints(session, meta['symbol'], qty_p)
                new_sl = round_price_step(new_sl, tick_size, price_p)
                if new_sl == last_sl:
                    if arm is not None:
                        meta.pop('be_arm', None)
                        _save_auto_managed()
                    continue

                # SL mới dùng ĐÚNG khối lượng vị thế hiện tại (vị thế có thể đã giảm do chốt một phần)
                close_qty = _round_to_step(abs(pos_amt), step)
                if close_qty < max(step, _min_step_qty(qty_p)):
                    continue
                close_side = 'SELL' if is_long else 'BUY'
                pos_side = None if meta['pos_side'] == 'BOTH' else meta['pos_side']
                # Đặt SL MỚI trước — chỉ hủy SL cũ sau khi SL mới đã xác nhận
                new_id = await _replace_protective_sl(session, meta, new_sl, close_qty, close_side,
                                                      pos_side, price_p, qty_p)
                if new_id:
                    if arm is not None:
                        meta.pop('be_arm', None)
                        _save_auto_managed()
                    logger.info(f"[AI-TRAIL] {meta['symbol']} {meta['side']} R={r:.2f}: "
                                f"SL {format_price(last_sl)} → {format_price(new_sl)}")
                    await _notify_all_chats(
                        session,
                        f"🛡️ *Trailing Stop* {display_symbol(meta['symbol'])} {meta['side']} (+{r:.1f}R)\n"
                        f"SL: `{format_price(last_sl)}` → `{format_price(new_sl)}`"
                    )

                # Chỉ hủy TP cố định SAU khi đã có SL mới hoạt động (SL không lỗi ⇒ vị thế không trần)
                if (AUTO_CANCEL_TP_ON_TRAIL and not meta.get('tp_cancelled')
                        and r >= AUTO_TRAIL_START_RR and meta.get('sl_algo_id')):
                    if await _cancel_algo_sl(session, api_key, api_secret, meta['symbol'], meta.get('tp_algo_id')):
                        meta['tp_cancelled'] = True
                        _save_auto_managed()
                        logger.info(f"[AI-TRAIL] {meta['symbol']} {meta['side']} R={r:.2f}: "
                                    f"hủy TP cố định, để lời chạy theo trailing.")
            await asyncio.sleep(AUTO_TRAIL_CHECK_SEC)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Lỗi trong auto_trailing_loop: {e}")
            await asyncio.sleep(AUTO_TRAIL_CHECK_SEC)


async def ai_auto_trader_loop(app):
    """Mỗi 5h: quét thị trường, tự vào lệnh các tín hiệu 5⭐ cực mạnh (tối đa AUTO_MAX_OPEN_POSITIONS)
    có AI xác nhận cùng chiều + tự chấm cao, kèm rào chắn an toàn (circuit breaker, giới hạn lỗ ngày,
    giới hạn số vị thế). Tự báo Telegram từng bước."""
    await asyncio.sleep(90)  # chờ khởi động xong (exchangeInfo, positions...)
    while True:
        try:
            if _ai_features_paused():
                await asyncio.sleep(AI_AUTO_TRADER_INTERVAL)
                continue
            session = app['session']
            # 1. Quét thị trường TƯƠI (bỏ qua cache) — scan ngoài lock, single-flight
            long_signals, short_signals = await get_scan_signals_fresh(session, max_age=0)

            # 2. Lọc tín hiệu "CỰC LỚN": CHỈ 4-5 sao (Mạnh/Rất mạnh) + điểm ≥ ngưỡng + nhóm có win-rate OK.
            # Win-rate dùng ở đây giờ chỉ lấy từ LỆNH THẬT đã đối soát (fills + phí + funding),
            # không còn trộn tín hiệu quét/giả lập như trước.
            quasi = [s for s in (list(long_signals) + list(short_signals))
                     if s.get('confidence') in ('Mạnh', 'Rất mạnh') and band_winrate_ok(s.get('confidence'))]
            def _auto_score(s):
                return s.get('long_score') if s.get('signal') == 'LONG' else s.get('short_score')
            def _ai_score(s):
                ai = s.get('ai')
                if not ai or ai.get('direction') != s.get('signal'):
                    return None
                return ai.get('long_score' if s.get('signal') == 'LONG' else 'short_score')
            def _ai_opp(s):
                ai = s.get('ai')
                if not ai:
                    return None
                return ai.get('short_score' if s.get('signal') == 'LONG' else 'long_score')
            # Tự vào lệnh chỉ khi: 4-5 sao + rule cao + AI xác nhận cùng chiều + AI chấm cao + cách biệt chiều ngược đủ lớn.
            # Các ngưỡng dưới đây là LEGACY (hiệu chỉnh bằng simulator cũ đã phát hiện thiên lệch),
            # CHƯA tái kiểm chứng; backtest corrected cho EV âm nên auto đang TẮT mặc định.
            # AI lỗi/không phản hồi (ai=None) → KHÔNG tự vào lệnh (tiền thật, không liều).
            # SHORT siết chặt hơn LONG chỉ là rào chắn thận trọng, không phải bằng chứng EV dương.
            candidates = [s for s in quasi
                          if s.get('confidence') in ('Mạnh', 'Rất mạnh')
                          and _auto_score(s) >= (AI_AUTO_SHORT_MIN_SCORE if s.get('signal') == 'SHORT' else AI_AUTO_MIN_SCORE)
                          and _ai_score(s) is not None
                          and _ai_score(s) >= (AI_AUTO_SHORT_AI_MIN_SCORE if s.get('signal') == 'SHORT' else AI_AUTO_AI_MIN_SCORE)
                          and (_ai_opp(s) is None or (_ai_score(s) - _ai_opp(s)) >= 1.5)
                          and side_winrate_ok(s.get('signal'))]
            if quasi and not candidates:
                logger.info(f"[AI-AUTO] Có {len(quasi)} tín hiệu 4-5 sao nhưng không đạt ngưỡng "
                            f"(phải 4-5⭐ + điểm ≥ {AI_AUTO_MIN_SCORE} + AI tự chấm ≥ {AI_AUTO_AI_MIN_SCORE} "
                            f"+ cách biệt ≥ 1.5; SHORT cần điểm ≥ {AI_AUTO_SHORT_MIN_SCORE} + AI ≥ {AI_AUTO_SHORT_AI_MIN_SCORE}) "
                            f"— bỏ qua tất cả.")
                quasi_lines = []
                for s in sorted(quasi, key=_auto_score, reverse=True)[:5]:
                    sc = _auto_score(s)
                    as_ = _ai_score(s)
                    as_txt = f", AI tự chấm {as_:.1f}" if as_ is not None else ", AI không xác nhận"
                    quasi_lines.append(
                        f"• {s['symbol']} {s['signal']} ({s['confidence']}, điểm {sc:.1f}{as_txt}) "
                        f"— entry ~{format_price(s['close'])}, TP {format_price(s['tp'])}, SL {format_price(s['sl'])}"
                    )
                await _notify_all_chats(
                    session,
                    f"🤖👀 *AI thấy {len(quasi)} tín hiệu 4-5 sao* nhưng chưa đạt ngưỡng tự vào lệnh:\n"
                    + "\n".join(quasi_lines)
                    + "\n→ Bạn muốn vào lệnh nào thì nhắn t nhé."
                )
            for s in candidates:
                s['_score'] = _auto_score(s)
            candidates.sort(key=lambda s: s.get('_score', 0), reverse=True)
            record_scan('5h', [s['symbol'] for s in quasi])

            # 3. Rào chắn an toàn trước khi đặt lệnh
            ok, reason = await _auto_trade_guard(session)
            if not ok:
                logger.info(f"[AI-AUTO] Không vào lệnh: {reason}")
                if candidates and time.time() - AUTO_STATE.get('last_notify', 0) > 6 * 3600:
                    AUTO_STATE['last_notify'] = time.time()
                    await _notify_all_chats(session, f"🤖⏸️ *AI tạm dừng tự trade:* {reason}")
                await asyncio.sleep(AI_AUTO_TRADER_INTERVAL)
                continue

            if not candidates:
                logger.info("[AI-AUTO] Lượt này không có tín hiệu nào đạt ngưỡng.")
                await asyncio.sleep(AI_AUTO_TRADER_INTERVAL)
                continue

            # 4. Mở lần lượt các tín hiệu tốt nhất; mỗi lệnh refresh lại risk/balance (fail closed)
            #    (khi cổng rollout tắt: chỉ báo tín hiệu, KHÔNG gọi _auto_place_order)
            if not _auto_trade_enabled():
                logger.info(f"[AI-AUTO] {AUTO_TRADE_OFF_REASON}")
                gate_lines = []
                for s in candidates[:5]:
                    gate_lines.append(
                        f"• {s['symbol']} {s['signal']} ({s['confidence']}, điểm {s.get('_score', 0):.1f}"
                        f", AI tự chấm {_ai_score(s):.1f}) — entry ~{format_price(s['close'])}, "
                        f"TP {format_price(s['tp'])}, SL {format_price(s['sl'])}"
                    )
                await _notify_all_chats(
                    session,
                    f"🤖✋ *AI KHÔNG tự vào lệnh* (auto đang tắt để bảo toàn vốn):\n"
                    + "\n".join(gate_lines)
                    + f"\n→ {AUTO_TRADE_OFF_REASON}\nMuốn vào lệnh nào thì nhắn t nhé."
                )
                await asyncio.sleep(AI_AUTO_TRADER_INTERVAL)
                continue
            for best in candidates:
                if _count_auto_open_positions() >= AUTO_MAX_OPEN_POSITIONS:
                    break
                if any(p.get('symbol') == best['symbol'] for p in positions.values()):
                    logger.info(f"[AI-AUTO] Đã có vị thế {best['symbol']} — bỏ qua, không chồng lệnh.")
                    continue
                snapshot, serr = await _account_risk_snapshot(session)
                if serr:
                    logger.warning(f"[AI-AUTO] Dừng lượt này: {serr}")
                    if time.time() - AUTO_STATE.get('last_notify', 0) > 6 * 3600:
                        AUTO_STATE['last_notify'] = time.time()
                        await _notify_all_chats(
                            session,
                            f"🤖⏸️ *AI tạm dừng tự trade:* không đo được rủi ro tài khoản ({serr})."
                        )
                    break
                logger.info(f"[AI-AUTO] Tín hiệu: {best['symbol']} {best['signal']} "
                            f"({best['confidence']}, điểm {best.get('_score', 0):.1f}, "
                            f"AI tự chấm {_ai_score(best):.1f})")
                await _auto_place_order(session, best, snapshot)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Lỗi trong ai_auto_trader_loop: {e}")
        await asyncio.sleep(AI_AUTO_TRADER_INTERVAL)


# ─── AI báo tín hiệu ngon mỗi 30 phút (CHỈ BÁO, KHÔNG tự vào lệnh) ───
AI_ALERT_INTERVAL = 30 * 60
AI_ALERT_COOLDOWN_SEC = 4 * 3600  # Không báo lặp lại cùng symbol+hướng trong 4h
AI_ALERT_MAX_ITEMS = 5            # Tối đa số tín hiệu báo mỗi lượt
AI_ALERT_STATE_FILE = "ai_alert_state_trading.json"
ai_alert_last_notified = {}       # (symbol, signal) -> timestamp lần báo gần nhất
last_alert_signals = []           # tín hiệu vừa báo trong tin "AI QUÉT MỖI 30 PHÚT" (để AI hiểu "coin này/2 coin này")
last_alert_ts = 0.0               # timestamp của lần báo gần nhất

def _load_ai_alert_state():
    """Nạp cooldown đã báo từ file để không báo trùng sau khi restart bot."""
    global ai_alert_last_notified
    try:
        if os.path.exists(AI_ALERT_STATE_FILE):
            with open(AI_ALERT_STATE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                # Key dạng "SYMBOL|SIGNAL" trong file
                ai_alert_last_notified = {tuple(k.split('|')): float(v) for k, v in raw.items() if '|' in k}
                logger.info(f"Đã nạp {len(ai_alert_last_notified)} lần báo gần nhất (chống trùng alert).")
    except Exception as e:
        logger.error(f"Lỗi nạp ai_alert_state: {e}")

def _save_ai_alert_state():
    try:
        with open(AI_ALERT_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({f"{k[0]}|{k[1]}": v for k, v in ai_alert_last_notified.items()}, f)
    except Exception as e:
        logger.error(f"Lỗi lưu ai_alert_state: {e}")


# ─── Pump radar: phát hiện coin "bất ngờ bay vút" CÒN nhiên liệu để pump tiếp ───
PUMP_RADAR_INTERVAL = int(float(os.getenv('PUMP_RADAR_INTERVAL', '600')))  # 10 phút
PUMP_SCORE_MIN = float(os.getenv('PUMP_SCORE_MIN', '10'))                 # ngưỡng điểm báo — chỉ báo kèo 10/10
PUMP_CHANGE_MIN = 15.0            # % tăng 24h tối thiểu để coi là "đang bay"
PUMP_COOLDOWN_SEC = 2 * 3600      # không báo lại cùng coin trong 2h
PUMP_MAX_ITEMS = 3
PUMP_STATE_FILE = "pump_radar_state.json"
pump_last_alerted = {}            # symbol -> ts lần báo gần nhất


def _load_pump_state():
    try:
        if os.path.exists(PUMP_STATE_FILE):
            with open(PUMP_STATE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                global pump_last_alerted
                pump_last_alerted = {k: float(v) for k, v in raw.items()}
    except Exception as e:
        logger.error(f"Lỗi nạp pump_radar_state: {e}")


def _save_pump_state():
    try:
        with open(PUMP_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(pump_last_alerted, f)
    except Exception as e:
        logger.error(f"Lỗi lưu pump_radar_state: {e}")


def _pump_score(res_15m, res_1h, change24, funding, oi_change, taker_ratio):
    """Điểm 0-10 đo 'đà tăng còn nguyên hay đã cháy đuồi' cho coin đang bay.

    Trần 10 áp cho phần ĐIỂM CỘNG trước rồi mới trừ điểm phạt: nếu clamp sau khi trừ thì
    coin đã bay quá xa (+150%/24h) hoặc funding quá đông vẫn hiện đúng 10/10 và bị báo
    'FOMO ngay' — đúng loại rủi ro mà điểm phạt sinh ra để loại."""
    base = 0.0
    penalty = 0.0
    reasons = []
    if res_15m.get('close') and res_15m.get('ema9') and res_15m.get('ema21'):
        if res_15m['close'] > res_15m['ema9'] > res_15m['ema21']:
            base += 2
            reasons.append("15m EMA stack chuẩn 🟢")
        if res_15m['close'] > (res_15m.get('vwap') or 0):
            base += 1
            reasons.append("giá trên VWAP 15m")
    if res_1h.get('close') and res_1h.get('ema9') and res_1h.get('ema21'):
        if res_1h['close'] > res_1h['ema9'] > res_1h['ema21']:
            base += 2
            reasons.append("1h uptrend còn nguyên")
    vr = res_15m.get('vol_ratio') or 0
    if vr >= 2.0:
        base += 2
        reasons.append(f"volume x{vr:.1f} so với MA20 (tiền đổ vào thật)")
    elif vr >= 1.3:
        base += 1
    f = funding if funding is not None else 0.0
    if f <= 0:
        base += 2
        reasons.append(f"funding {f * 100:+.3f}% — short bị vắt le, nhiên liệu squeeze")
    elif 0 < f <= 0.0005:
        base += 1
    elif f > 0.0015:
        penalty += 2
        reasons.append(f"funding {f * 100:+.3f}% — long đang đông, dễ cháy đuồi ⚠️")
    oi = oi_change if oi_change is not None else 0.0
    if oi > 2:
        base += 2
        reasons.append(f"OI +{oi:.1f}% — tiền mới vẫn vào vị thế")
    elif oi < -2:
        penalty += 1
        reasons.append(f"OI {oi:+.1f}% — tiền đang rút ❌")
    if taker_ratio and taker_ratio > 1.02:
        base += 1
        reasons.append("taker mua > bán")
    # Quá muộn: đã bay quá xa thì rủi ro đón đầu cao
    if change24 > 120:
        penalty += 2
        reasons.append(f"đã bay +{change24:.0f}%/24h — VERY late ⚠️")
    elif change24 > 60:
        penalty += 1
        reasons.append(f"đã bay +{change24:.0f}%/24h")
    return max(0.0, min(base, 10.0) - penalty), reasons


async def detect_pump_candidates(session, limit=6):
    """Tìm coin đang 'bay vút' còn nhiên liệu pump tiếp.
    Trả về list dict: symbol, change24, score, reasons, signal15m, funding, oi_change, tp/sl/close (1h)."""
    try:
        async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr") as resp:
            if resp.status != 200:
                return []
            tickers = await resp.json()
    except Exception as e:
        logger.warning(f"[PUMP-RADAR] Lỗi ticker 24h: {e}")
        return []
    SCAN_BLACKLIST = {'AAPLUSDT', 'NVDAUSDT', 'MSTRUSDT', 'TSLAUSDT', 'GOOGLUSDT', 'AMZNUSDT', 'METAUSDT',
                      'MSFTUSDT', 'COINUSDT', 'NFLXUSDT', 'AVGOUSDT', 'ORCLUSDT', 'PLTRUSDT', 'HOODUSDT',
                      'SPXUSDT', 'SPYUSDT', 'QQQUSDT', 'IWMUSDT', 'SOXLUSDT', 'SOXSUSDT', 'TSLLUSDT',
                      'XAUUSDT', 'XAGUSDT', 'XPTUSDT', 'XBIUSDT', 'DDOGUSDT', 'CRMUSDT', 'PLTRUSDT',
                      'PUMPBTCUSDT', 'BTCUSDT_260925', 'ETHUSDT_260925', 'BTCUSDT_261225', 'ETHUSDT_261225'}
    cands = []
    for t in tickers:
        sym = t['symbol']
        if not sym.endswith('USDT') or sym in SCAN_BLACKLIST:
            continue
        change = float(t.get('priceChangePercent', 0))
        if change < PUMP_CHANGE_MIN:
            continue
        if float(t.get('quoteVolume', 0)) < 2_000_000:  # thanh khoản tối thiểu
            continue
        cands.append((sym, change))
    cands.sort(key=lambda x: x[1], reverse=True)
    cands = cands[:limit]
    if not cands:
        return []
    sem = asyncio.Semaphore(4)

    async def analyze_one(sym, change):
        async with sem:
            try:
                res_15m, res_1h = await asyncio.gather(
                    analyze_market(session, sym, interval='15m', fetch_extras=False),
                    analyze_market(session, sym, interval='1h'))
                if not res_15m or not res_1h:
                    return None
                funding = res_1h.get('funding_rate')
                if funding is None:
                    funding = await get_single_funding_rate(session, sym)
                oi = res_1h.get('oi_change')
                if oi is None:
                    oi = 0.0
                score, reasons = _pump_score(res_15m, res_1h, change, funding, oi, res_1h.get('taker_ratio'))
                support15 = res_15m.get('support')
                resistance15 = res_15m.get('resistance')
                # Kế hoạch vào lệnh: scalp momentum — SL tối đa 2.5% dưới entry (không lấy support xa),
                # TP = kháng cự 15m gần nhất (≤6%) hoặc +3%; giữ R:R hợp lý cho đuổi giá.
                entry_now = res_1h.get('close')
                if entry_now:
                    fomo_sl = res_15m.get('support')
                    if not fomo_sl or not (entry_now * 0.975 <= fomo_sl < entry_now * 0.995):
                        fomo_sl = entry_now * 0.975
                    tp_c = res_15m.get('resistance')
                    if not tp_c or not (entry_now * 1.005 < tp_c < entry_now * 1.06):
                        tp_c = entry_now * 1.03
                    fomo_tp = tp_c
                else:
                    fomo_sl, fomo_tp = res_1h.get('sl'), res_1h.get('tp')
                return {'symbol': sym, 'change24': change, 'score': score, 'reasons': reasons,
                        'signal15m': res_15m.get('signal'), 'signal1h': res_1h.get('signal'),
                        'confidence': res_1h.get('confidence'), 'funding': funding, 'oi_change': oi,
                        'close': res_1h.get('close'), 'tp': res_1h.get('tp'), 'sl': res_1h.get('sl'),
                        'vwap15m': res_15m.get('vwap'), 'support15m': support15,
                        'resistance15m': resistance15, 'fomo_tp': fomo_tp, 'fomo_sl': fomo_sl,
                        'ema9_15m': res_15m.get('ema9'), 'ema21_15m': res_15m.get('ema21')}
            except Exception as e:
                logger.warning(f"[PUMP-RADAR] Lỗi phân tích {sym}: {e}")
                return None
    results = await asyncio.gather(*[analyze_one(s, c) for s, c in cands])
    results = [r for r in results if r]
    results.sort(key=lambda x: x['score'], reverse=True)
    # GIÁ LIVE THẬT (KHÔNG cache): entry MARKET phải là giá tại thời điểm báo, không chơi
    # snapshot TTL 30s. 1 call /ticker/price lấy giá tươi cho các coin trong danh sách.
    live_map = {}
    try:
        async with session.get("https://fapi.binance.com/fapi/v1/ticker/price") as resp:
            if resp.status == 200:
                for t in await resp.json():
                    live_map[t['symbol']] = float(t['price'])
    except Exception as e:
        logger.warning(f"[PUMP-RADAR] Không lấy được giá live: {e}")
    for r in results:
        _apply_live_entry(r, live_map.get(r['symbol']))
    return results


def _apply_live_entry(r, entry):
    """Gắn giá live vào ứng viên pump: tính lại SL/TP scalp và re-check momentum
    (snapshot nến có thể cũ vài phút — giá gãy dưới EMA9 15m thì tầng 'EMA stack
    chuẩn' không còn đúng lúc báo, trừ 2 điểm như tầng đó từng cộng)."""
    if not entry:
        return
    r['close'] = entry
    sup, res15 = r.get('support15m'), r.get('resistance15m')
    fomo_sl = sup if sup and entry * 0.975 <= sup < entry * 0.995 else entry * 0.975
    fomo_tp = res15 if res15 and entry * 1.005 < res15 < entry * 1.06 else entry * 1.03
    r['fomo_sl'], r['fomo_tp'] = fomo_sl, fomo_tp
    ema9 = r.get('ema9_15m')
    if ema9 and entry < ema9:
        r['momentum_broken'] = True
        r['score'] = max(0.0, r['score'] - 2)


def _fmt_pump_message(cands, mode="auto"):
    """Format tin báo pump radar — kèm KẾ HOẠCH vào lệnh với SL/TP và rủi ro tính từ dữ liệu."""
    if not cands:
        return None
    lines = ["🚀 *PUMP RADAR — coin đang bay vút, còn nhiên liệu pump tiếp*"]
    for c in cands[:PUMP_MAX_ITEMS]:
        sym_disp = display_symbol(c['symbol'])
        r_txt = " · ".join(c['reasons'][:3])
        if c.get('momentum_broken'):
            r_txt += " · ⚠️ giá live vừa gãy dưới EMA9 15m (−2 điểm)"
        hot = c['score'] >= PUMP_SCORE_MIN
        head = "🔥🔥 FOMO NGAY ĐƯỢC" if hot else "🔥 chưa đạt ngưỡng"
        lines.append(
            f"\n• *{sym_disp}* — điểm {c['score']:.1f}/10 {head}\n"
            f"  Giá {format_price(c['close'])} (+{c['change24']:.1f}%/24h) | 15m {c['signal15m']} / 1h {c['signal1h']} ({c['confidence']})\n"
            f"  {r_txt}\n"
        )
        close = c['close'] or 0
        sl = c.get('fomo_sl') or close
        tp = c.get('fomo_tp') or close
        if close > 0:
            sl_pct = abs(close - sl) / close * 100
            tp_pct = abs(tp - close) / close * 100
            rr = tp_pct / sl_pct if sl_pct > 0 else 0.0
            risk_pct = AUTO_RISK_PER_TRADE_PCT * 100
            if hot:
                lines.append(
                    f"  ⚡ *Vào được* (điểm {c['score']:.1f} ≥ ngưỡng {PUMP_SCORE_MIN:g}): "
                    f"entry MARKET {format_price(close)}, TP {format_price(tp)} (+{tp_pct:.1f}%), "
                    f"SL {format_price(sl)} (−{sl_pct:.1f}%), R:R 1:{rr:.1f}. "
                    f"Volume tính sao cho lỗ khi SL khớp ≤ {risk_pct:g}% equity, chốt một phần khi +1R."
                )
            else:
                lines.append(
                    f"  ⏳ *Chờ pullback* về VWAP 15m {format_price(c['vwap15m'])} rồi long — "
                    f"TP {format_price(c['tp'])}, SL {format_price(c['sl'])}"
                )
    downside = "SL cách entry > 2.5% thì coi như bỏ, đừng nới SL cho vừa size."
    lines.append(
        f"\n⚠️ Radar chỉ báo khi điểm ≥ {PUMP_SCORE_MIN:g}/10: mọi tầng momentum còn nguyên + nhiên liệu squeeze, "
        f"và coin đã bay quá xa/đuồi yếu bị trừ điểm TRƯỚC khi so ngưỡng. Coin bay >60%/24h chia nhỏ vào nhiều lần, "
        f"không all-in. {downside}"
    )
    return "\n".join(lines)


async def pump_radar_loop(app):
    """Mỗi 10 phút: quét coin đang bay vút còn nhiên liệu pump tiếp → báo Telegram.
    Chỉ BÁO, không tự vào lệnh. /stopauto all sẽ tắt cả radar này; /fomo off tắt riêng."""
    await asyncio.sleep(120)  # chờ khởi động
    while True:
        try:
            if _ai_features_paused() or AUTO_STATE.get('pump_radar_off'):
                await asyncio.sleep(PUMP_RADAR_INTERVAL)
                continue
            session = app['session']
            cands = await detect_pump_candidates(session)
            now = time.time()
            alertable = []
            for c in cands:
                if c['score'] < PUMP_SCORE_MIN:
                    continue
                if now - pump_last_alerted.get(c['symbol'], 0) < PUMP_COOLDOWN_SEC:
                    continue
                alertable.append(c)
            if alertable:
                for c in alertable:
                    pump_last_alerted[c['symbol']] = now
                    record_signal({
                        'symbol': c['symbol'],
                        'signal': 'LONG',
                        'close': c['close'],
                        'tp': c['fomo_tp'],
                        'sl': c['fomo_sl'],
                        'long_score': c['score'],
                        'short_score': c['score'],
                        'confidence': c['confidence'],
                    }, origin='pump')
                _save_pump_state()
                msg = _fmt_pump_message(alertable)
                if msg:
                    await _notify_all_chats(session, msg)
                    logger.info(f"[PUMP-RADAR] Báo {len(alertable)} coin bay vút: {[c['symbol'] for c in alertable]}")
            else:
                logger.info("[PUMP-RADAR] Không có coin bay vút nào đạt ngưỡng báo.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Lỗi trong pump_radar_loop: {e}")
        await asyncio.sleep(PUMP_RADAR_INTERVAL)


async def handle_fomo_command(session, chat_id, arg=None):
    """Lệnh /fomo:
    /fomo        → quét NGAY coin đang bay vút FOMO được
    /fomo on     → bật auto radar (quét tự động mỗi 10 phút)
    /fomo off    → tắt auto radar (vẫn quét tay được)"""
    arg = (arg or '').strip().lower()
    if arg == 'on':
        AUTO_STATE['pump_radar_off'] = False
        _save_auto_state()
        await send_telegram_message(session, chat_id,
            f"🚀 *Đã BẬT auto PUMP RADAR* — quét tự động mỗi 10 phút.\n"
            f"Chỉ báo coin đạt ≥ {PUMP_SCORE_MIN:g}/10 (đã trừ điểm coin bay quá xa) kèm entry/TP/SL. Tắt: `/fomo off`")
        return
    if arg == 'off':
        AUTO_STATE['pump_radar_off'] = True
        _save_auto_state()
        await send_telegram_message(session, chat_id,
            "🔇 *Đã TẮT auto PUMP RADAR.* Quét tay vẫn dùng được: gõ `/fomo` bất kỳ lúc nào.")
        return
    await send_telegram_message(session, chat_id, "🚀 Đang quét coin bay vút... (chờ ~1 phút)")
    cands = await detect_pump_candidates(session, limit=8)
    hot = [c for c in cands if c['score'] >= PUMP_SCORE_MIN]
    if hot:
        msg = _fmt_pump_message(hot)
    else:
        # Không có kèo đạt ngưỡng: vẫn cho xem các coin sát ngưỡng để chờ pullback,
        # tránh việc /fomo chỉ trả về "không có gì" rồi người dùng tự đoán.
        near = [c for c in cands if c['score'] >= max(0.0, PUMP_SCORE_MIN - 2)][:PUMP_MAX_ITEMS]
        msg = _fmt_pump_message(near) if near else \
            (f"Không có coin nào đạt {PUMP_SCORE_MIN:g}/10 — thị trường đang quá muộn hoặc thiếu nhiên liệu, "
             f"bỏ qua lượt này.")
    for c in hot[:PUMP_MAX_ITEMS]:
        record_signal({
            'symbol': c['symbol'], 'signal': 'LONG', 'close': c['close'],
            'tp': c['fomo_tp'], 'sl': c['fomo_sl'],
            'long_score': c['score'], 'short_score': c['score'],
            'confidence': c['confidence'],
        }, origin='pump')
    await send_telegram_message(session, chat_id, msg)


async def ai_signal_alert_loop(app):
    """Mỗi 30 phút: quét thị trường, BÁO qua Telegram mọi tín hiệu 4-5 sao tìm được.
    Khác ai_auto_trader_loop ở chỗ KHÔNG tự vào lệnh, không đặt TP/SL —
    để người dùng tự quyết định."""
    await asyncio.sleep(60)
    while True:
        try:
            if _ai_features_paused():
                await asyncio.sleep(AI_ALERT_INTERVAL)
                continue
            session = app['session']
            # 1. Quét thị trường (dùng cache còn mới < 5 phút để đỡ tốn request) — ngoài lock
            long_signals, short_signals = await get_scan_signals_fresh(session, max_age=300)

            now = time.time()
            candidates = []
            for s in (list(long_signals) + list(short_signals)):
                if s.get('confidence') not in ('Mạnh', 'Rất mạnh'):
                    continue
                key = (s['symbol'], s['signal'])
                if now - ai_alert_last_notified.get(key, 0) < AI_ALERT_COOLDOWN_SEC:
                    continue
                s['_score'] = s.get('long_score') if s.get('signal') == 'LONG' else s.get('short_score')
                candidates.append(s)
            candidates.sort(key=lambda s: s.get('_score', 0) or 0, reverse=True)
            # Ghi tín hiệu 4-5⭐ thực tế tìm được để history phản ánh đúng
            scan_30m_coins = [s['symbol'] for s in (list(long_signals) + list(short_signals))
                              if s.get('confidence') in ('Mạnh', 'Rất mạnh')]
            record_scan('30m', scan_30m_coins)

            if not candidates:
                logger.info("[AI-ALERT] Không có tín hiệu ngon ở lượt này.")
            else:
                lines = []
                announced = []
                for s in candidates[:AI_ALERT_MAX_ITEMS]:
                    ai = s.get('ai') or {}
                    ai_txt = ""
                    if ai and ai.get('direction') == s.get('signal'):
                        ai_sc = ai.get('long_score' if s['signal'] == 'LONG' else 'short_score')
                        if ai_sc is not None:
                            ai_txt = f", AI {ai_sc:.1f}"
                    conf_stars = CONF_MAP.get(s['confidence'], s['confidence'])
                    lines.append(
                        f"• *{display_symbol(s['symbol'])}* {s['signal']} {conf_stars} "
                        f"(điểm {s['_score']:.1f}{ai_txt})\n"
                        f"  Entry `{format_price(s['close'])}` → TP `{format_price(s['tp'])}` / "
                        f"SL `{format_price(s['sl'])}`"
                    )
                    announced.append({
                        'symbol': s['symbol'], 'signal': s['signal'],
                        'confidence': s['confidence'], 'score': s['_score'],
                        'entry': s['close'], 'tp': s['tp'], 'sl': s['sl'],
                    })
                    ai_alert_last_notified[(s['symbol'], s['signal'])] = now
                    # Lưu vào lịch sử (persist qua signal_history.json) để AI rút kinh nghiệm
                    # dù có restart bot — origin='alert' để không tính vào chuỗi thua của auto trade.
                    record_signal(s, s.get('ai'), origin='alert')
                _save_ai_alert_state()  # persist cooldown để không báo trùng sau restart
                global last_alert_signals, last_alert_ts
                last_alert_signals = announced
                last_alert_ts = now
                await _notify_all_chats(
                    session,
                    "🔔 *AI QUÉT MỖI 30 PHÚT — TÍN HIỆU NGON:*\n"
                    + "\n".join(lines)
                    + "\n→ Bot chỉ báo, KHÔNG tự vào lệnh. Muốn vào lệnh nào thì nhắn t nhé."
                )
                logger.info(f"[AI-ALERT] Đã báo {len(candidates[:AI_ALERT_MAX_ITEMS])} tín hiệu ngon.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Lỗi trong ai_signal_alert_loop: {e}")
        await asyncio.sleep(AI_ALERT_INTERVAL)


# ─── Theo dõi vị thế đang mở mỗi 30 phút: cảnh báo rủi ro + funding cực đoan ───
AI_POS_GUARD_INTERVAL = 30 * 60
AI_POS_GUARD_COOLDOWN_SEC = 2 * 3600  # Không cảnh báo lặp cùng vị thế trong 2h
ai_pos_guard_last = {}                # symbol -> timestamp lần cảnh báo gần nhất
AI_POS_MAX_FUNDING = 0.001            # Funding cực đoan (≥ 0.1%/h) theo hướng đám đông → cảnh báo
AI_POS_ALERT_MIN_SCORE = 4.5          # Điểm tín hiệu ngược chiều đủ mạnh mới cảnh báo


async def ai_position_guard_loop(app):
    """Mỗi 30 phút: với mỗi vị thế ĐANG MỞ (mọi nguồn, không chỉ auto), phân tích MTF + AI:
    - Tín hiệu ngược chiều mạnh → cảnh báo "nên cân nhắc chốt/DCA".
    - Funding cực đoan theo hướng đám đông → cảnh báo hạ đòn bẩy.
    Chỉ CẢNH BÁO, không tự đóng lệnh."""
    await asyncio.sleep(60)
    while True:
        try:
            if _ai_features_paused():
                await asyncio.sleep(AI_POS_GUARD_INTERVAL)
                continue
            session = app['session']
            open_positions = [p for p in positions.values() if float(p.get('positionAmt', 0) or 0) != 0.0]
            if not open_positions:
                await asyncio.sleep(AI_POS_GUARD_INTERVAL)
                continue

            now = time.time()
            warns = []
            for p in open_positions:
                symbol = p['symbol']
                amount = float(p.get('positionAmt', 0) or 0)
                p_side = pos_side_display(p.get('positionSide'), amount)
                entry = float(p.get('entryPrice', 0) or 0)
                mark = float(p.get('markPrice', 0) or 0)
                lev = p.get('leverage', 1) or 1
                if now - ai_pos_guard_last.get((symbol, p_side), 0) < AI_POS_GUARD_COOLDOWN_SEC:
                    continue

                try:
                    # 1. Funding rate cực đoan theo hướng đám đông → rủi ro ép giá
                    funding = await get_single_funding_rate(session, symbol)
                    funding_warn = None
                    if p_side == 'LONG' and funding >= AI_POS_MAX_FUNDING:
                        funding_warn = (f"📊 Funding {display_symbol(symbol)} đang quá đông LONG "
                                        f"({funding * 100:+.4f}%/h) — rủi ro ép giá xuống, cân nhắc hạ đòn bẩy.")
                    elif p_side == 'SHORT' and funding <= -AI_POS_MAX_FUNDING:
                        funding_warn = (f"📊 Funding {display_symbol(symbol)} đang quá đông SHORT "
                                        f"({funding * 100:+.4f}%/h) — rủi ro ép giá lên, cân nhắc hạ đòn bẩy.")

                    # 2. Phân tích MTF: tín hiệu ngược chiều vị thế đang mở
                    res_15m_task = asyncio.create_task(analyze_market(session, symbol, interval='15m', fetch_extras=False))
                    res_1h_task = asyncio.create_task(analyze_market(session, symbol, interval='1h'))
                    res_4h_task = asyncio.create_task(analyze_market(session, symbol, interval='4h', fetch_extras=False))
                    res_1d_task = asyncio.create_task(analyze_market(session, symbol, interval='1d', fetch_extras=False))
                    btc_task = asyncio.create_task(get_btc_filter(session, '4h'))

                    res_15m = await res_15m_task
                    res = await res_1h_task
                    res_4h = await res_4h_task
                    res_1d = await res_1d_task
                    btc_res = await btc_task
                    if not res:
                        continue
                    apply_btc_penalty(res, btc_res)

                    opposite = 'SHORT' if p_side == 'LONG' else 'LONG'
                    opp_score = res['short_score'] if opposite == 'SHORT' else res['long_score']
                    trend_warn = None
                    if res.get('signal') == opposite and opp_score >= AI_POS_ALERT_MIN_SCORE:
                        r = (mark - entry) / (entry + 1e-10) * 100 if p_side == 'LONG' else (entry - mark) / (entry + 1e-10) * 100
                        conf_stars = CONF_MAP.get(res['confidence'], res['confidence'])
                        trend_warn = (
                            f"⚠️ *Cảnh báo đảo chiều* {display_symbol(symbol)} ({p_side} @ {format_price(entry)}, "
                            f"lev {lev}x, đang {r:+.1f}%)\n"
                            f"AI thấy tín hiệu *{opposite}* {conf_stars} (điểm {opp_score:.1f}) — ngược chiều lệnh bạn giữ.\n"
                            f"→ Cân nhắc chốt lời/cắt lỗ hoặc DCA, không nên ôm tiếp."
                        )
                        # AI xác nhận cùng chiều tín hiệu đảo (nếu có cấu hình)
                        if os.getenv("DASH_TOKEN"):
                            ob = await get_orderbook_summary(session, symbol)
                            dom = await get_btc_dominance(session)
                            digest = build_ai_digest(symbol,
                                                     [("15m", res_15m), ("1h", res), ("4h", res_4h), ("1d", res_1d)],
                                                     oi_change=res.get('oi_change'), taker_ratio=res.get('taker_ratio'),
                                                     funding_rate=res.get('funding_rate'), orderbook=ob, btc_dominance=dom)
                            ai_v = await get_ai_verdict_cached(session, f"guard_{symbol}", digest)
                            if ai_v and ai_v.get('direction') not in (opposite, None):
                                trend_warn = None  # AI mâu thuẫn → bỏ qua cảnh báo

                    # 3. Điểm tổn thất hiện tại lớn (đang lỗ sâu) → nhắc quản lý rủi ro
                    loss_warn = None
                    if mark > 0 and entry > 0:
                        pnl_pct = ((mark - entry) / entry * 100) if p_side == 'LONG' else ((entry - mark) / entry * 100)
                        if pnl_pct <= -5.0:
                            loss_warn = (f"📉 {display_symbol(symbol)} đang lỗ *{pnl_pct:.1f}%* "
                                         f"(lev {lev}x, tương đương ~{abs(pnl_pct) * lev:.0f}% ký quỹ). "
                                         f"Cân nhắc cắt lỗ đúng kỷ luật hoặc DCA nếu vẫn còn xu hướng.")

                    parts = [w for w in (trend_warn, funding_warn, loss_warn) if w]
                    if parts:
                        ai_pos_guard_last[(symbol, p_side)] = now
                        warns.append("\n\n".join(parts))
                except Exception as e:
                    logger.warning(f"[AI-POS-GUARD] Lỗi phân tích {symbol}: {e}")

            if warns:
                await _notify_all_chats(
                    session,
                    "🛡️ *AI RÀ SOÁT VỊ THẾ ĐANG MỞ:*\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" + "\n\n".join(warns)
                    + "\n\n→ Bot chỉ CẢNH BÁO, không tự đóng lệnh. Muốn xử lý thì nhắn t nhé."
                )
                logger.info(f"[AI-POS-GUARD] Đã cảnh báo {len(warns)} vị thế.")
            else:
                logger.info("[AI-POS-GUARD] Không có vị thế nào cần cảnh báo.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Lỗi trong ai_position_guard_loop: {e}")
        await asyncio.sleep(AI_POS_GUARD_INTERVAL)


# ─── Agent tools cho /ai: đọc dữ liệu tài khoản + soạn lệnh (có bước xác nhận) ───


# ─── Agent tools cho /ai: đọc dữ liệu tài khoản + soạn lệnh (có bước xác nhận) ───
pending_orders = {}  # chat_id -> {'type': 'place_order'|'cancel_order', 'params': {...}, 'desc': str, 'ts': float}
PENDING_ORDER_TTL = 600
ai_chat_history = {}  # chat_id -> list message (user/assistant) gần nhất: bộ nhớ hội thoại của agent
AI_HISTORY_MAX_MSGS = 8  # Giữ ngắn để mỗi lượt gọi LLM không chở context khổng lồ (AI trả lời nhanh hơn)
# Nhận diện xác nhận/hủy linh hoạt: tin nhắn ngắn (< 40 ký tự) có chứa từ khóa
CONFIRM_RE = re.compile(
    r'\b(xac nhan|xác nhận|xacnhan|ok|oke|okê|yes|y|confirm|dong y|đồng ý|dat di|đặt đi|'
    r'chot di|chốt đi|lam di|làm đi|ap dung|áp dụng|do it|dzo|zo)\b', re.IGNORECASE)
CANCEL_RE = re.compile(
    r'\b(hủy|huy|hủy bỏ|hủy đi|huy di|huy bo|cancel|bỏ qua|bo qua|dừng lại|dung lai|đừng|no)\b',
    re.IGNORECASE)


def _is_confirmation(text):
    t = text.strip().lower()
    if len(t) > 40:
        return False
    # Có dấu hiệu câu hỏi (dấu ?, hỏi giá, hỏi thế nào...) -> không phải xác nhận, để AI xử lý
    if re.search(r'\?|giá|gia |bao nhiêu|bao nhieu|thế nào|the nao|sao|vì sao|vi sao|không\b|ko\b|nhỉ|chứ', t):
        return False
    return bool(CONFIRM_RE.search(t))


def _is_cancellation(text):
    t = text.strip().lower()
    return len(t) <= 40 and bool(CANCEL_RE.search(t))


async def binance_signed_request(session, method, path, params=None, base_url="https://fapi.binance.com"):
    """Gọi API Binance (futures hoặc portfolio-margin) có ký HMAC. Trả về (data, None) hoặc (None, error_msg)."""
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        return None, "Chưa cấu hình BINANCE_API_KEY/BINANCE_API_SECRET."
    params = dict(params or {})
    params['timestamp'] = int(time.time() * 1000)
    params['recvWindow'] = 10000
    query = urlencode(params)
    signature = get_binance_signature(query, api_secret)
    url = f"{base_url}{path}?{query}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    try:
        # encoded=True: yarl KHÔNG decode lại URL. Nếu không, symbol chữ TQ (我踏马来了USDT...)
        # bị decode %XX → ký tự thô trên dây ≠ chuỗi đã ký → Binance trả lỗi -1022.
        async with session.request(method, yarl.URL(url, encoded=True), headers=headers) as resp:
            data = await resp.json()
            if resp.status != 200:
                msg = data.get('msg', f"HTTP {resp.status}") if isinstance(data, dict) else f"HTTP {resp.status}"
                return None, msg
            return data, None
    except Exception as e:
        return None, str(e)


def _norm_symbol(args):
    symbol = (args.get('symbol') or '').upper().strip()
    if not symbol.endswith('USDT'):
        symbol += 'USDT'
    return symbol


async def get_position_risk(session, params=None):
    """Lấy positionRisk: ưu tiên v3 (có tpPrice/slPrice - TP/SL gắn trên vị thế đặt từ app), fallback v2."""
    data, err = await binance_signed_request(session, 'GET', '/fapi/v3/positionRisk', params)
    if not err:
        return data, None
    logger.warning(f"positionRisk v3 lỗi ({err}) — fallback v2, sẽ KHÔNG thấy TP/SL gắn trên vị thế.")
    data2, err2 = await binance_signed_request(session, 'GET', '/fapi/v2/positionRisk', params)
    return data2, err2


def format_position_tpsl(p):
    """Dựng mô tả TP/SL gắn trên vị thế (nếu có) từ record positionRisk."""
    try:
        tp_price = float(p.get('tpPrice') or 0)
        sl_price = float(p.get('slPrice') or 0)
    except (TypeError, ValueError):
        tp_price = sl_price = 0
    parts = []
    if tp_price > 0:
        parts.append(f"TP {format_price(tp_price)}")
    if sl_price > 0:
        parts.append(f"SL {format_price(sl_price)}")
    if parts:
        return f" | TP/SL gắn trên vị thế: {', '.join(parts)}"
    return " | chưa có TP/SL"


async def tool_get_account_summary(session, chat_id, args):
    data, err = await binance_signed_request(session, 'GET', '/fapi/v2/account')
    if err:
        return f"LỖI: {err}"
    return (f"Số dư ví: {float(data.get('totalWalletBalance', 0)):,.2f} USDT | "
            f"PnL chưa thực hiện: {float(data.get('totalUnrealizedProfit', 0)):+,.2f} USDT | "
            f"Margin balance: {float(data.get('totalMarginBalance', 0)):,.2f} USDT | "
            f"Khả dụng: {float(data.get('availableBalance', 0)):,.2f} USDT")


def _safe_float(v):
    """Trả về True nếu v có thể convert sang float hợp lệ (>0)."""
    try:
        return float(v) > 0
    except (TypeError, ValueError):
        return False


async def tool_get_positions(session, chat_id, args):
    data, err = await get_position_risk(session)
    if err:
        return f"LỖI: {err}"
    open_positions = [p for p in data if float(p.get('positionAmt', 0)) != 0.0]
    try:
        logger.info(f"[DIAG] positionRisk: {len(open_positions)} vị thế | "
                    + " | ".join(f"{p.get('symbol')} tp={p.get('tpPrice')} sl={p.get('slPrice')}" for p in open_positions[:10]))
    except Exception:
        pass
    if not open_positions:
        return "Không có vị thế nào đang mở."
    # Gộp TP/SL từ Algo Service (bot đặt TP/SL là lệnh điều kiện RIÊNG, không gắn trên vị thế)
    algo_map = {}
    try:
        algo_data, algo_err = await binance_signed_request(session, 'GET', '/fapi/v1/openAlgoOrders')
        if not algo_err and isinstance(algo_data, list):
            for o in algo_data:
                sym = o.get('symbol')
                otype = (o.get('orderType') or '').upper()
                trig = o.get('triggerPrice')
                oid = o.get('algoId')
                algo_map.setdefault(sym, []).append((otype, trig, oid))
    except Exception as e:
        logger.warning(f"[POSITIONS] Lỗi lấy algo orders: {e}")
    lines = []
    for p in open_positions[:20]:
        amount = float(p.get('positionAmt', 0))
        entry = float(p.get('entryPrice', 0))
        mark = float(p.get('markPrice', 0))
        pnl = float(p.get('unRealizedProfit', p.get('unrealizedProfit', 0)))
        liq = float(p.get('liquidationPrice', 0))
        lev = p.get('leverage')
        if not lev:
            # v3 positionRisk không trả leverage → lấy từ cache vị thế (WS)
            cached_pos = positions.get(f"{p.get('symbol')}_{p.get('positionSide')}")
            lev = (cached_pos or {}).get('leverage') if cached_pos else None
            if not lev and p.get('positionSide') == 'BOTH':
                for cp in positions.values():
                    if cp.get('symbol') == p.get('symbol'):
                        lev = cp.get('leverage')
                        break
        line = (f"{p.get('symbol')} {pos_side_display(p.get('positionSide'), amount)}: "
                f"entry {format_price(entry)}, mark {format_price(mark)}, "
                f"PnL {fmt_signed(pnl)} USDT, đòn bẩy {lev or '?'}x")
        if liq > 0:
            line += f", giá thanh lý {format_price(liq)}"
        # TP/SL gắn trên vị thế (nếu có)
        line += format_position_tpsl(p)
        # TP/SL điều kiện riêng từ Algo Service
        algo_orders = algo_map.get(p.get('symbol'), [])
        tp_parts = [f"TP {format_price(float(trig))}" for otype, trig, _ in algo_orders
                    if 'TAKE_PROFIT' in otype and _safe_float(trig)]
        sl_parts = [f"SL {format_price(float(trig))}" for otype, trig, _ in algo_orders
                    if 'STOP' in otype and _safe_float(trig)]
        if tp_parts or sl_parts:
            # Đã có TP/SL algo → thay dòng "chưa có TP/SL" (nếu có) bằng thông tin thật
            if "chưa có TP/SL" in line:
                line = line.replace(" | chưa có TP/SL", "")
            line = line.rstrip(" | ") + " | " + ", ".join(tp_parts + sl_parts)
        lines.append(line)
    return "\n".join(lines)


async def tool_get_open_orders(session, chat_id, args):
    """Lệnh đang chờ: gồm cả LIMIT lẫn lệnh điều kiện (STOP_MARKET, TAKE_PROFIT_MARKET...)."""
    params = {}
    if args.get('symbol'):
        params['symbol'] = _norm_symbol(args)
    data, err = await binance_signed_request(session, 'GET', '/fapi/v1/openOrders', params)
    if err:
        return f"LỖI: {err}"
    orders = list(data or [])
    # Lệnh điều kiện TP/SL (đặt từ app Binance hoặc từ bot) đã migrated sang Algo Service (12/2025):
    # nằm ở /fapi/v1/openAlgoOrders, KHÔNG nằm trong openOrders thường
    algo, algo_err = await binance_signed_request(session, 'GET', '/fapi/v1/openAlgoOrders', params or None)
    if algo_err:
        logger.info(f"[DIAG] openAlgoOrders lỗi: {algo_err}")
    else:
        logger.info(f"[DIAG] openAlgoOrders: {len(algo or [])} lệnh")
        orders = orders + list(algo or [])
    try:
        logger.info(f"[DIAG] openOrders ({params}): {len(data or [])} lệnh thường + {len(orders) - len(data or [])} lệnh algo")
    except Exception:
        pass
    if not orders:
        return "Không có lệnh nào đang chờ (kể cả TP/SL điều kiện)."
    lines = []
    for o in orders[:20]:
        is_algo = 'algoId' in o
        otype = o.get('orderType') if is_algo else o.get('type', '')
        oid = o.get('algoId') if is_algo else o.get('orderId')
        line = (f"{o.get('symbol')} #{oid} {o.get('side')} {otype} qty {o.get('quantity') or o.get('origQty')}")
        if is_algo and o.get('algoStatus') and o.get('algoStatus') != 'NEW':
            line += f" (algoStatus={o.get('algoStatus')}, đã thành lệnh thường #{o.get('actualOrderId')})"
        try:
            trigger = float(o.get('triggerPrice') or o.get('stopPrice') or 0)
        except (TypeError, ValueError):
            trigger = 0
        if trigger > 0:
            line += f" | kích hoạt khi chạm {format_price(trigger)}"
            wt = o.get('workingType')
            if wt:
                line += f" (theo {wt})"
        else:
            line += f" @ {o.get('price')}"
        if o.get('reduceOnly') or o.get('closePosition'):
            line += " (reduceOnly - chỉ đóng vị thế)"
        if is_algo:
            line += " [lệnh TP/SL điều kiện — hủy bằng algoId với conditional=true]"
        lines.append(line)
    return "\n".join(lines)


async def tool_get_order_history(session, chat_id, args):
    if not args.get('symbol'):
        return "LỖI: cần tham số symbol (ví dụ BTCUSDT)."
    limit = min(int(args.get('limit', 10) or 10), 20)
    data, err = await binance_signed_request(session, 'GET', '/fapi/v1/allOrders',
                                             {'symbol': _norm_symbol(args), 'limit': limit})
    if err:
        return f"LỖI: {err}"
    if not data:
        return "Không có lệnh nào cho symbol này."
    lines = []
    for o in data[-limit:]:
        lines.append(f"#{o.get('orderId')} {o.get('side')} {o.get('type')} "
                     f"qty {o.get('executedQty')}/{o.get('origQty')} @ {o.get('price')} status: {o.get('status')}")
    return "\n".join(lines)


# Cache kết quả PnL "lifetime" (async download) — tránh đốt giới hạn 5 lần/tháng của Binance
LIFETIME_PNL_CACHE = {'data': None, 'ts': 0.0}
LIFETIME_PNL_TTL = 24 * 3600
LIFETIME_PNL_FILE = "lifetime_pnl_cache_trading.json"


def _load_lifetime_cache():
    """Nạp cache PnL lifetime từ file (giữ qua restart, không phải đốt quota lại)."""
    try:
        if os.path.exists(LIFETIME_PNL_FILE):
            with open(LIFETIME_PNL_FILE, encoding='utf-8') as f:
                d = json.load(f)
            if isinstance(d, dict) and isinstance(d.get('text'), str):
                return d['text'], float(d.get('ts', 0))
    except Exception as e:
        logger.warning(f"Lỗi nạp lifetime_pnl_cache: {e}")
    return None, 0.0


async def fetch_income_paginated(session, income_type=None, start_ms=None, end_ms=None, max_records=50000):
    """Phân trang /fapi/v1/income theo cửa sổ thời gian (limit 1000/lần).
    REST income chỉ giữ ~3 tháng; trả về (list_records, None) hoặc (None, err)."""
    records = []
    cur = start_ms
    end = end_ms or int(time.time() * 1000)
    while True:
        params = {'limit': 1000}
        if cur:
            params['startTime'] = int(cur)
        params['endTime'] = int(end)
        if income_type:
            params['incomeType'] = income_type
        data, err = await binance_signed_request(session, 'GET', '/fapi/v1/income', params)
        if err:
            return None, err
        if not isinstance(data, list) or not data:
            break
        records.extend(data)
        if len(data) < 1000:
            break
        last_time = int(data[-1].get('time', 0))
        if not last_time or last_time + 1 <= (cur or 0):
            break
        cur = last_time + 1
        if len(records) >= max_records:
            break
    return records, None


def _decompress_download(raw):
    """File download của Binance thường bị nén (gzip/zlib/zip). Giải nén về dữ liệu CSV thô."""
    if raw[:4] == b'PK\x03\x04':
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                name = z.namelist()[0]
                return z.read(name)
        except Exception:
            pass
    try:
        return gzip.decompress(raw)
    except Exception:
        pass
    try:
        return zlib.decompress(raw)
    except Exception:
        pass
    return raw


def _decode_csv_bytes(raw):
    """Giải mã CSV Binance (thường UTF-8 BOM hoặc UTF-16LE BOM). Tránh lỗi double-decode ra NUL."""
    if raw[:2] in (b'\xff\xfe', b'\xfe\xff'):
        return raw.decode('utf-16')
    for enc in ('utf-8-sig', 'utf-8', 'utf-16-le', 'utf-16'):
        try:
            text = raw.decode(enc)
            if '\x00' in text:
                continue  # decode nhầm → còn NUL, thử encoding khác
            return text
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode('utf-16-le', errors='replace').replace('\x00', '')


async def fetch_income_async_download(session, start_ms, end_ms):
    """Lấy toàn bộ lịch sử thu nhập qua endpoint async download (/fapi/v1/income/asyn).
    Giới hạn: 1 lần tối đa 1 năm, 5 lần/tháng (chung với app Binance).
    Trả về (csv_text, None) hoặc (None, err)."""
    data, err = await binance_signed_request(session, 'GET', '/fapi/v1/income/asyn',
                                             {'startTime': int(start_ms), 'endTime': int(end_ms)})
    if err:
        return None, f"Lỗi tạo yêu cầu download lịch sử: {err}"
    download_id = data.get('downloadId')
    if not download_id:
        return None, "Không nhận được downloadId từ Binance."
    for _ in range(30):
        await asyncio.sleep(5)
        d, e = await binance_signed_request(session, 'GET', '/fapi/v1/income/asyn/id', {'downloadId': download_id})
        if e:
            return None, f"Lỗi kiểm tra trạng thái download: {e}"
        status = d.get('status')
        if status == 'completed' and d.get('url'):
            try:
                async with session.get(d['url']) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        raw = _decompress_download(raw)
                        return _decode_csv_bytes(raw), None
                    return None, f"Lỗi tải file CSV lịch sử (HTTP {resp.status})."
            except Exception as exc:
                return None, f"Lỗi tải file CSV lịch sử: {exc}"
        if status != 'processing':
            return None, f"Trạng thái download bất thường: {status}."
    return None, "Tạo file download lịch sử quá lâu (>2.5 phút)."


def parse_income_csv(text):
    """Parse CSV thu nhập futures → (totals_by_type, per_symbol_net, num_records)."""
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if r and any(c.strip() for c in r)]
    if len(rows) < 2:
        return {}, {}, 0
    header = [h.strip().lower() for h in rows[0]]
    idx_income = next((i for i, h in enumerate(header) if h in ('income', 'amount')), None)
    idx_type = next((i for i, h in enumerate(header) if 'income type' in h or h == 'type'), None)
    idx_symbol = next((i for i, h in enumerate(header) if h == 'symbol'), None)
    totals = {}
    per_symbol = {}
    count = 0
    for r in rows[1:]:
        if idx_income is None or idx_income >= len(r):
            continue
        try:
            val = float(r[idx_income])
        except (ValueError, TypeError):
            continue
        t = (r[idx_type].strip().upper() if idx_type is not None and idx_type < len(r) else '?')
        totals[t] = totals.get(t, 0) + val
        if idx_symbol is not None and idx_symbol < len(r):
            sym = r[idx_symbol].strip()
            if sym:
                per_symbol[sym] = per_symbol.get(sym, 0) + val
        count += 1
    return totals, per_symbol, count


# Các loại thu nhập là CHUYỂN VỐN (không phải PnL) — loại khỏi "Tổng ví" để khớp app Binance PNL Analysis
CAPITAL_MOVE_TYPES = {'TRANSFER', 'INTERNAL_TRANSFER', 'CROSS_COLLATERAL_TRANSFER', 'COIN_SWAP_DEPOSIT',
                      'COIN_SWAP_WITHDRAW', 'AUTO_EXCHANGE', 'DELIVERED_SETTELMENT'}


def _wallet_total(totals):
    """Tổng biến động ví = tổng mọi loại thu nhập trừ chuyển vốn (giống PNL Analysis của Binance)."""
    return sum(v for k, v in totals.items() if k not in CAPITAL_MOVE_TYPES)


def _fmt_pnl(v):
    return f"{v:+.2f} USDT"


async def tool_get_income_history(session, chat_id, args):
    """Lịch sử thu nhập (REALIZED_PNL/FUNDING_FEE/COMMISSION...) theo thời gian, phân trang đầy đủ trong cửa sổ chọn."""
    days = max(1, min(int(args.get('days', 7) or 7), 90))
    income_type = str(args.get('income_type') or '').upper().strip() or None
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 86400 * 1000
    records, err = await fetch_income_paginated(session, income_type=income_type,
                                                start_ms=start_ms, end_ms=end_ms)
    if err:
        return f"LỖI: {err}"
    if not records:
        return f"Không có bản ghi thu nhập nào trong {days} ngày qua."
    lines = []
    for rec in records[-20:]:
        lines.append(f"{rec.get('symbol', '')} {rec.get('incomeType')}: {_fmt_pnl(float(rec.get('income', 0)))} ({datetime.fromtimestamp(rec.get('time', 0) / 1000).strftime('%d/%m %H:%M')})")
    return f"📜 *Lịch sử thu nhập ({days} ngày, {len(records)} bản ghi)* — 20 gần nhất:\n" + "\n".join(lines)


async def tool_get_pnl_summary(session, chat_id, args):
    """Tổng kết PnL: mode='summary' (tổng theo loại) | 'by_coin' (PnL thực tế từng coin) | 'lifetime' (tổng 12 tháng qua toàn bộ lịch sử)."""
    mode = str(args.get('mode', 'summary')).lower()
    days = max(1, min(int(args.get('days', 30) or 30), 90))

    if mode == 'lifetime':
        now = time.time()
        cache = LIFETIME_PNL_CACHE
        if cache['data'] and now - cache['ts'] < LIFETIME_PNL_TTL:
            return cache['data']
        cached_text, cached_ts = _load_lifetime_cache()
        if cached_text and now - cached_ts < LIFETIME_PNL_TTL:
            cache.update({'data': cached_text, 'ts': cached_ts})
            return cached_text

        # Xích các cửa sổ 1 năm về quá khứ tới khi hết lịch sử.
        # Binance giới hạn 5 lần download/tháng, mỗi lần tối đa 1 năm → lâu nhất ~5 năm.
        MAX_WINDOWS = 5
        totals = {}
        per_symbol = {}
        total_records = 0
        windows = 0
        cur_end = int(now * 1000)
        first_err = None
        while windows < MAX_WINDOWS:
            cur_start = cur_end - 365 * 86400 * 1000
            csv_text, err = await fetch_income_async_download(session, cur_start, cur_end)
            windows += 1
            if err:
                if not total_records:
                    return f"❌ {err}\nℹ️ Giới hạn download lịch sử của Binance: 5 lần/tháng, tối đa 1 năm mỗi lần."
                if first_err is None:
                    first_err = err
                logger.warning(f"[LIFETIME PNL] cửa sổ {cur_start} lỗi: {err} — dùng dữ liệu đã tải.")
                break
            t, ps, cnt = parse_income_csv(csv_text)
            total_records += cnt
            for k, v in t.items():
                totals[k] = totals.get(k, 0) + v
            for k, v in ps.items():
                per_symbol[k] = per_symbol.get(k, 0) + v
            if cnt == 0:
                break  # cửa sổ trống → đã quét hết lịch sử, dừng để không phí quota
            cur_end = cur_start - 1

        realized = totals.get('REALIZED_PNL', 0)
        funding = totals.get('FUNDING_FEE', 0)
        commission = totals.get('COMMISSION', 0)
        net = realized + funding + commission
        wallet_total = _wallet_total(totals)
        years = windows
        text = (
            f"📊 *TỔNG KẾT PNL TRỌN ĐỜI* — {total_records:,} bản ghi, quét {years} năm\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 *Realized PnL:* {_fmt_pnl(realized)}\n"
            f"💸 *Funding:* {_fmt_pnl(funding)}\n"
            f"🧾 *Phí giao dịch:* {_fmt_pnl(commission)}\n"
            f"🟢 *NET (giao dịch):* {_fmt_pnl(net)}\n"
            f"📉 *Tổng ví (gồm mọi loại, trừ chuyển vốn):* {_fmt_pnl(wallet_total)}\n"
            f"\n🏆 *Top coin lãi / lỗ:*\n"
        )
        sorted_syms = sorted(per_symbol.items(), key=lambda kv: kv[1], reverse=True)[:12]
        if not sorted_syms:
            text += "· (không có giao dịch)\n"
        for sym, v in sorted_syms:
            text += f"• {sym}: {_fmt_pnl(v)}\n"
        if windows >= MAX_WINDOWS:
            text += "\n⚠️ Mới quét được ~5 năm (giới hạn 5 download/tháng). Tài khoản có thể lâu đời hơn — tháng sau hỏi lại để lấy thêm."
        elif first_err:
            text += f"\nℹ️ Dừng sớm do 1 cửa sổ lỗi: {first_err[:100]}"
        text += "\nℹ️ Cache 24h trong file, không tốn quota khi hỏi lại."
        cache.update({'data': text, 'ts': now})
        try:
            with open(LIFETIME_PNL_FILE, 'w', encoding='utf-8') as f:
                json.dump({'text': text, 'ts': now}, f)
        except Exception as e:
            logger.warning(f"Lỗi lưu lifetime_pnl_cache: {e}")
        return text

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - days * 86400 * 1000
    records, err = await fetch_income_paginated(session, start_ms=start_ms, end_ms=end_ms)
    if err:
        return f"LỖI: {err}"
    if not records:
        return f"Không có dữ liệu thu nhập trong {days} ngày qua."

    if mode == 'by_coin':
        per_symbol = {}
        for rec in records:
            sym = rec.get('symbol') or ''
            if not sym:
                continue
            per_symbol[sym] = per_symbol.get(sym, 0) + float(rec.get('income', 0))
        lines = sorted(per_symbol.items(), key=lambda kv: kv[1], reverse=True)
        top = [f"• {sym}: {_fmt_pnl(v)}" for sym, v in lines[:15]]
        return f"📈 *PnL thực tế theo coin ({days} ngày, NET gồm realized+funding+phí):*\n" + "\n".join(top)

    totals = {}
    for rec in records:
        t = rec.get('incomeType', '?')
        totals[t] = totals.get(t, 0) + float(rec.get('income', 0))
    realized = totals.get('REALIZED_PNL', 0)
    funding = totals.get('FUNDING_FEE', 0)
    commission = totals.get('COMMISSION', 0)
    net = realized + funding + commission
    return (
        f"📊 *Tổng kết thu nhập ({days} ngày, {len(records)} bản ghi):*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 *Realized PnL:* {_fmt_pnl(realized)}\n"
        f"💸 *Funding:* {_fmt_pnl(funding)}\n"
        f"🧾 *Phí giao dịch:* {_fmt_pnl(commission)}\n"
        f"🟢 *NET (giao dịch):* {_fmt_pnl(net)}\n"
        f"📉 *Tổng ví (mọi loại, trừ chuyển vốn):* {_fmt_pnl(_wallet_total(totals))}\n"
    )


async def _position_leverage_actual(session, symbol, pos_side=None):
    """Đòn bẩy THẬT đang áp cho vị thế (v2 positionRisk có trường leverage). Trả (lev, err).
    Lỗi đọc ⇒ (None, lý do): nơi gọi fail closed vì không xác nhận được vùng thanh lý."""
    data, err = await binance_signed_request(session, 'GET', '/fapi/v2/positionRisk', {'symbol': symbol})
    if err:
        return None, f"không đọc được positionRisk {symbol}: {err}"
    if not isinstance(data, list):
        return None, f"positionRisk {symbol} trả về dữ liệu lạ."
    fallback = None
    wanted = (pos_side or '').upper()
    for p in data:
        if not isinstance(p, dict):
            continue
        try:
            lev = int(float(p.get('leverage') or 0))
        except (TypeError, ValueError):
            lev = 0
        if lev <= 0:
            continue
        try:
            amt = float(p.get('positionAmt') or 0)
        except (TypeError, ValueError):
            amt = 0.0
        raw_side = (p.get('positionSide') or 'BOTH').upper()
        if amt != 0 and (not wanted or raw_side in (wanted, 'BOTH')):
            return lev, None
        if fallback is None:
            fallback = lev
    if fallback:
        return fallback, None
    return None, f"positionRisk {symbol} không có trường leverage."


async def _stage_order(session, chat_id, order_type, params, desc, origin='ai', is_exit=False):
    """Soạn lệnh ghi vào hàng chờ xác nhận (hỗ trợ NHIỀU lệnh/cùng coin), không thực thi ngay.
    `is_exit=True`: lệnh chỉ đóng/chốt vị thế có sẵn (không mở rủi ro mới, không đụng đòn bẩy)."""
    entry = pending_orders.get(chat_id)
    if not entry or time.time() - entry.get('ts', 0) > PENDING_ORDER_TTL:
        entry = {'items': [], 'ts': time.time()}
    if len(entry['items']) >= 8:
        return ("LỖI: đã có 8 lệnh chờ xác nhận — hãy chờ người dùng bấm Xác nhận/Hủy trước "
                "khi soạn thêm (đừng gộp thêm lệnh vào hàng chờ).")
    entry['items'].append({'type': order_type, 'params': params, 'desc': desc,
                           'coin': params.get('symbol', ''), 'origin': origin, 'is_exit': bool(is_exit)})
    entry['ts'] = time.time()
    pending_orders[chat_id] = entry
    return (f"NEEDS_CONFIRMATION: Lệnh đã được soạn (hiện có {len(entry['items'])} lệnh chờ xác nhận):\n" + desc +
            "\nHãy trình bày lại chi tiết lệnh cho người dùng. Hệ thống sẽ TỰ ĐỘNG đính kèm nút '✅ Xác nhận / ❌ Hủy' vào tin nhắn sau — "
            "bạn KHÔNG cần nhắc người dùng gõ 'xác nhận', chỉ nói rõ lệnh nào làm gì. KHÔNG gọi công cụ này lần nữa cho đến khi người dùng phản hồi.")


def build_pending_keyboard(items):
    """Dựng bàn phím xác nhận: 1 coin -> nút chung; nhiều coin -> từng nút theo coin."""
    coins = []
    for it in items:
        coin = it.get('coin') or 'KHÁC'
        if coin not in coins:
            coins.append(coin)
    if len(coins) <= 1:
        return CONFIRM_KEYBOARD
    rows = []
    for coin in coins:
        disp = coin[:-4] if coin.endswith('USDT') else coin
        rows.append([
            {"text": f"✅ Xác nhận {disp}", "callback_data": f"ai_confirm:{coin}"},
            {"text": f"❌ Hủy {disp}", "callback_data": f"ai_cancel:{coin}"}
        ])
    return {"inline_keyboard": rows}


AI_VOLUME_TIERS = (200, 400, 800)   # TRẦN notional (USDT) cho lệnh MỞ do AI /ai đặt — KHÔNG còn là mức bắt buộc
AI_TP_SL_MIN_DIST = 0.01            # TP/SL không sát entry quá ~1%
AI_TP_SL_MAX_DIST = 0.20            # TP/SL không xa entry quá ~20%
AI_VOLUME_MAX = max(AI_VOLUME_TIERS)


async def _ai_entry_risk_plan(session, symbol, side, otype, quantity, sl_price, limit_price=None):
    """Kiểm tra + chốt size lệnh MỞ của AI /ai bằng CÙNG helper ngân sách rủi ro với auto.
    Trả về (ok: bool, msg: str, quantity: float|None, size_msg: str).
    - SL là BẮT BUỘC (không có SL ⇒ không đo được rủi ro ⇒ từ chối).
    - 200/400/800 chỉ còn là TRẦN notional; size thật do ngân sách 0.5%/lệnh + 1.5% danh mục quyết định.
    - Mọi lỗi đọc tài khoản/rủi ro ⇒ fail closed."""
    if not sl_price or sl_price <= 0:
        return False, ("❌ TỪ CHỐI LỆNH MỞ: phải kèm stop_loss — hệ thống không mở vị thế trần "
                       "(không có SL thì không đo được rủi ro)."), None, ""
    try:
        if otype == 'LIMIT' and limit_price:
            ref_price = float(limit_price)
        else:
            ref_price = await get_single_price(session, symbol)
    except Exception as e:
        return False, f"LỖI lấy giá {symbol}: {e}", None, ""
    if not ref_price or ref_price <= 0:
        return False, f"LỖI: không lấy được giá {symbol} để kiểm tra rủi ro.", None, ""
    qty_p, _, _ = await get_symbol_precisions(session, symbol)
    step, min_qty, min_notional = await _symbol_constraints(session, symbol, qty_p)
    max_lev, lerr = await _max_leverage_strict(session, symbol)
    if lerr:
        return False, f"❌ TỪ CHỐI LỆNH: {lerr} (fail closed).", None, ""
    snap, serr = await _account_risk_snapshot(session)
    if serr:
        return False, f"❌ TỪ CHỐI LỆNH: không đo được rủi ro tài khoản ({serr}).", None, ""
    if snap['unprotected']:
        return False, ("❌ TỪ CHỐI LỆNH: đang có vị thế CHƯA đặt SL nên không đo được rủi ro danh mục "
                       f"({', '.join(snap['unprotected'][:5])}). Đặt SL cho các vị thế đó rồi thử lại."), None, ""
    lev = _safe_leverage_for_sl(ref_price, float(sl_price), max_lev)
    ok, allowed, size_msg = _plan_entry_size(
        ref_price, float(sl_price), side, equity=snap['equity'], available=snap['available'],
        open_risk=snap['open_risk'], daily_remaining=snap['daily_remaining'], leverage=lev,
        step=step, min_qty=min_qty, min_notional=min_notional, notional_cap=AI_VOLUME_MAX)
    if not ok:
        return False, (f"❌ TỪ CHỐI LỆNH MỞ: {size_msg} "
                       f"(trần notional {AI_VOLUME_MAX} USDT, equity {snap['equity']:,.2f})."), None, ""
    final_qty = min(float(quantity or 0), allowed) if quantity else allowed
    final_qty = _round_to_step(final_qty, step)
    min_ok = max(step, min_qty)
    if final_qty < min_ok:
        return False, (f"❌ TỪ CHỐI LỆNH MỞ: size theo ngân sách rủi ro ({allowed:g}) nhỏ hơn "
                       f"khối lượng tối thiểu của sàn ({min_ok:g})."), None, ""
    note = ""
    if quantity and final_qty < float(quantity):
        note = f"size giảm {float(quantity):g} → {final_qty:g}: {size_msg}"
    return True, "OK", final_qty, note or size_msg


async def _ai_validate_tpsl_distance(session, symbol, trigger_price):
    """Khoảng cách TP/SL tính từ giá hiện tại phải trong khoảng ~1% đến ~20%.
    Trả về (ok: bool, msg: str)."""
    try:
        cur = await get_single_price(session, symbol)
    except Exception as e:
        return False, f"LỖI lấy giá {symbol}: {e}"
    if not cur or cur <= 0 or trigger_price <= 0:
        return False, f"LỖI: không lấy được giá {symbol} để kiểm tra TP/SL."
    dist = abs(trigger_price - cur) / cur
    if dist < AI_TP_SL_MIN_DIST:
        return False, (
            f"❌ TỪ CHỐI TP/SL: khoảng cách {dist * 100:.2f}% quá gần entry (< ~1%). "
            f"Đặt TP/SL cách giá ít nhất ~1%."
        )
    if dist > AI_TP_SL_MAX_DIST:
        return False, (
            f"❌ TỪ CHỐI TP/SL: khoảng cách {dist * 100:.2f}% quá xa (> ~20%). "
            f"Đặt TP/SL trong khoảng ~1% đến ~20%."
        )
    return True, f"{dist * 100:.2f}%"


async def tool_place_order(session, chat_id, args):
    symbol = _norm_symbol(args)
    side = str(args.get('side') or '').upper()
    otype = str(args.get('type') or 'MARKET').upper()
    try:
        quantity = float(args.get('quantity', 0))
    except (TypeError, ValueError):
        return "LỖI: quantity không hợp lệ."
    if side not in ('BUY', 'SELL'):
        return "LỖI: side phải là BUY hoặc SELL."
    if otype not in ('MARKET', 'LIMIT', 'STOP_MARKET', 'TAKE_PROFIT_MARKET'):
        return "LỖI: chỉ hỗ trợ MARKET, LIMIT, STOP_MARKET, TAKE_PROFIT_MARKET."
    if quantity <= 0:
        return "LỖI: quantity phải > 0."
    if otype == 'LIMIT' and not args.get('price'):
        return "LỖI: lệnh LIMIT cần price."
    if otype in ('STOP_MARKET', 'TAKE_PROFIT_MARKET') and not args.get('stop_price'):
        return "LỖI: lệnh điều kiện cần stop_price (giá kích hoạt)."
    qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)
    quantity = round_down(quantity, qty_p)
    desc = f"{display_symbol(symbol)} {side} {otype} {quantity}"

    if otype in ('STOP_MARKET', 'TAKE_PROFIT_MARKET'):
        # Conditional orders đã migrated sang Algo Service: /fapi/v1/order trả -4120
        stop_price = round_price_step(float(args['stop_price']), tick_size, price_p)
        if otype == 'STOP_MARKET':
            # KHÔNG kéo SL cho vừa số dư (SL là ý định của người dùng). Chỉ kiểm tra SL còn
            # kích hoạt được TRƯỚC vùng thanh lý theo đòn bẩy THẬT của vị thế đó; nếu không,
            # từ chối để không tạo cảm giác "đã được bảo vệ" trong khi thanh lý tới trước.
            try:
                entry_ref = await get_single_price(session, symbol)
                lev_now, lerr = await _position_leverage_actual(session, symbol,
                                                                args.get('position_side'))
                if lerr:
                    return (f"❌ TỪ CHỐI TP/SL: không đọc được đòn bẩy/vị thế của {symbol} "
                            f"({lerr}) — không xác nhận được vùng thanh lý (fail closed).")
                if entry_ref and entry_ref > 0 and lev_now > 1 and stop_price > 0:
                    safe_dist = (0.5 / lev_now) * entry_ref
                    beyond = (side == 'SELL' and stop_price < entry_ref - safe_dist) or \
                             (side == 'BUY' and stop_price > entry_ref + safe_dist)
                    if beyond:
                        return (f"❌ TỪ CHỐI TP/SL: SL {format_price(stop_price)} nằm quá xa so với đòn bẩy "
                                f"hiện tại ({lev_now}x) — vị thế sẽ bị thanh lý trước khi SL khớp. "
                                f"Hãy đặt SL gần hơn hoặc giảm khối lượng vị thế.")
            except Exception as lev_e:
                logger.warning(f"Lỗi kiểm tra vùng thanh lý cho SL {symbol}: {lev_e}")
        ok_dist, dist_msg = await _ai_validate_tpsl_distance(session, symbol, stop_price)
        if not ok_dist:
            return dist_msg
        algo_params = {
            'algoType': 'CONDITIONAL',
            'symbol': symbol, 'side': side, 'type': otype,
            'triggerPrice': f"{stop_price:.{price_p}f}",
            'quantity': f"{quantity:.{qty_p}f}",
        }
        if args.get('working_type'):
            wt = str(args['working_type']).upper()
            if wt in ('MARK_PRICE', 'CONTRACT_PRICE'):
                algo_params['workingType'] = wt
        if hedge_mode:
            algo_params['positionSide'] = str(args.get('position_side') or ('LONG' if side == 'BUY' else 'SHORT')).upper()
        else:
            algo_params['reduceOnly'] = 'true'
        desc += f" @ {stop_price}"
        # Chỉ sửa bảo vệ cho vị thế đang có ⇒ KHÔNG đụng đòn bẩy (đổi lev sẽ đổi giá thanh lý của vị thế)
        return await _stage_order(session, chat_id, 'place_algo_order', algo_params, desc,
                                  origin='ai', is_exit=True)

    params = {'symbol': symbol, 'side': side, 'type': otype, 'quantity': f"{quantity:.{qty_p}f}"}
    if otype == 'LIMIT':
        price = round_price_step(float(args['price']), tick_size, price_p)
        params.update({'price': f"{price:.{price_p}f}", 'timeInForce': 'GTC'})
        desc += f" @ {price}"
    if hedge_mode:
        params['positionSide'] = str(args.get('position_side') or ('LONG' if side == 'BUY' else 'SHORT')).upper()
    elif args.get('reduce_only'):
        params['reduceOnly'] = 'true'
        desc += " (RO)"
    if args.get('reduce_only'):
        # Đóng/chốt vị thế hiện có: không mở rủi ro mới ⇒ không kiểm tra ngân sách, không đụng đòn bẩy
        return await _stage_order(session, chat_id, 'place_order', params, desc, origin='ai', is_exit=True)

    # ─── Lệnh MỞ vị thế mới: BẮT BUỘC đi kèm SL ngay trong cùng lượt soạn ───
    try:
        sl_price = float(args.get('stop_loss') or 0)
    except (TypeError, ValueError):
        return "LỖI: stop_loss không hợp lệ."
    if sl_price <= 0:
        return ("❌ TỪ CHỐI LỆNH MỞ: thiếu stop_loss. Mọi lệnh mở vị thế PHẢI kèm SL "
                "(hệ thống không mở vị thế trần) — soạn lại với tham số stop_loss, "
                "và take_profit nếu muốn chốt lời.")
    ok_risk, risk_msg, final_qty, size_note = await _ai_entry_risk_plan(
        session, symbol, ('LONG' if side == 'BUY' else 'SHORT'), otype, quantity, sl_price,
        limit_price=(price if otype == 'LIMIT' else None))
    if not ok_risk:
        return risk_msg
    if final_qty < quantity:
        quantity = final_qty
        params['quantity'] = f"{quantity:.{qty_p}f}"
    desc = f"{display_symbol(symbol)} {side} {otype} {quantity}"
    if otype == 'LIMIT':
        desc += f" @ {price}"
    if size_note:
        desc += f" ({size_note})"

    # 1) Lệnh vào, 2) SL điều kiện, 3) TP điều kiện (nếu có) — thứ tự này được giữ khi thực thi
    staged = await _stage_order(session, chat_id, 'place_order', params, desc, origin='ai')
    sl_params = {
        'algoType': 'CONDITIONAL', 'symbol': symbol,
        'side': 'SELL' if side == 'BUY' else 'BUY', 'type': 'STOP_MARKET',
        'triggerPrice': f"{round_price_step(sl_price, tick_size, price_p):.{price_p}f}",
        'quantity': f"{quantity:.{qty_p}f}", 'workingType': 'MARK_PRICE',
    }
    if hedge_mode:
        sl_params['positionSide'] = str(args.get('position_side') or ('LONG' if side == 'BUY' else 'SHORT')).upper()
    else:
        sl_params['reduceOnly'] = 'true'
    await _stage_order(session, chat_id, 'place_algo_order', sl_params,
                       f"SL {display_symbol(symbol)} kích hoạt {format_price(float(sl_params['triggerPrice']))}",
                       origin='ai')
    try:
        tp_price = float(args.get('take_profit') or 0)
    except (TypeError, ValueError):
        tp_price = 0.0
    if tp_price > 0:
        ok_dist, dist_msg = await _ai_validate_tpsl_distance(session, symbol, tp_price)
        if ok_dist:
            tp_params = {
                'algoType': 'CONDITIONAL', 'symbol': symbol,
                'side': 'SELL' if side == 'BUY' else 'BUY', 'type': 'TAKE_PROFIT_MARKET',
                'triggerPrice': f"{round_price_step(tp_price, tick_size, price_p):.{price_p}f}",
                'quantity': f"{quantity:.{qty_p}f}", 'workingType': 'MARK_PRICE',
            }
            if hedge_mode:
                tp_params['positionSide'] = sl_params['positionSide']
            else:
                tp_params['reduceOnly'] = 'true'
            await _stage_order(session, chat_id, 'place_algo_order', tp_params,
                               f"TP {display_symbol(symbol)} kích hoạt {format_price(float(tp_params['triggerPrice']))}",
                               origin='ai')
        else:
            staged += f"\n⚠️ Bỏ qua TP: {dist_msg}"
    return staged


async def tool_cancel_order(session, chat_id, args):
    if not args.get('order_id'):
        return "LỖI: cần order_id."
    symbol = _norm_symbol(args)
    if args.get('conditional'):
        # Lệnh TP/SL điều kiện (algo service): hủy bằng algoId
        params = {'symbol': symbol, 'algoId': str(args['order_id'])}
        desc = f"Hủy TP/SL #{args['order_id']} {display_symbol(symbol)}"
        return await _stage_order(session, chat_id, 'cancel_algo', params, desc)
    params = {'symbol': symbol, 'orderId': str(args['order_id'])}
    desc = f"Hủy #{args['order_id']} {display_symbol(symbol)}"
    return await _stage_order(session, chat_id, 'cancel_order', params, desc)


async def tool_close_position(session, chat_id, args):
    symbol = _norm_symbol(args)
    data, err = await get_position_risk(session, {'symbol': symbol})
    if err:
        return f"LỖI: {err}"
    open_positions = [p for p in data if float(p.get('positionAmt', 0)) != 0.0]
    if not open_positions:
        return f"Không có vị thế nào đang mở cho {symbol}."
    qty_p, _, _ = await get_symbol_precisions(session, symbol)
    descs = []
    for p in open_positions:
        amount = float(p.get('positionAmt', 0))
        p_side = pos_side_display(p.get('positionSide'), amount)
        close_side = 'SELL' if p_side == 'LONG' else 'BUY'
        params = {'symbol': symbol, 'side': close_side, 'type': 'MARKET',
                  'quantity': f"{round(abs(amount), qty_p):.{qty_p}f}"}
        if hedge_mode:
            params['positionSide'] = 'LONG' if p_side == 'LONG' else 'SHORT'
        else:
            params['reduceOnly'] = 'true'
        desc = f"ĐÓNG {display_symbol(symbol)} {p_side} {round(abs(amount), qty_p)}"
        descs.append(desc)
        await _stage_order(session, chat_id, 'place_order', params, desc, origin='ai', is_exit=True)
        pending_orders[chat_id]['items'][-1]['is_close'] = True
    return ("NEEDS_CONFIRMATION: Lệnh đã được soạn:\n" + "\n".join(descs) +
            "\nHãy trình bày lại chi tiết cho người dùng và nhắc họ trả lời 'xác nhận' hoặc 'hủy'. "
            "KHÔNG gọi công cụ này lần nữa cho đến khi người dùng phản hồi.")


async def tool_scan_market(session, chat_id, args):
    """Quét toàn thị trường tìm tín hiệu LONG/SHORT mạnh nhất (dùng lại cache quét của /analyze nếu còn hạn)."""
    long_signals, short_signals = await get_scan_signals_fresh(session, max_age=300)

    # Ghi vào bộ nhớ: các tín hiệu AI quét ra cũng được nhớ (dedup 4h tránh trùng với nguồn scan)
    for sig_res in list(long_signals) + list(short_signals):
        record_signal(sig_res, sig_res.get('ai'), origin='ai')

    lines = []
    for signals in (long_signals, short_signals):
        for res in signals[:5]:
            score = res.get('long_score') if res['signal'] == 'LONG' else res.get('short_score')
            rr = abs(res['tp'] - res['close']) / (abs(res['close'] - res['sl']) + 1e-10)
            lines.append(
                f"{res['symbol']} {res['signal']} ({res['confidence']}, điểm {score:.1f}): "
                f"entry {format_price(res['close'])}, TP {format_price(res['tp'])}, "
                f"SL {format_price(res['sl'])}, R:R 1:{rr:.1f}"
            )
    if not lines:
        return "Hiện không có tín hiệu LONG/SHORT nào đạt chuẩn 4-5 sao. Thị trường chưa có cơ hội rõ ràng."
    return ("Các tín hiệu mạnh nhất hiện tại (đã qua lọc MTF 1h+4h+1d, xu hướng BTC và win-rate thực tế):\n"
            + "\n".join(lines))


async def tool_search_news(session, chat_id, args):
    """Tìm tin tức/mạng xã hội mới nhất về một coin từ Google News RSS (miễn phí, không cần API key).
    Trả về tối đa 8 tiêu đề bài báo kèm link. Dùng khi người dùng hỏi tin tức, sự kiện,
    lý do coin tăng/giảm, tin cộng đồng về coin. Lưu ý: tin tức chỉ để THAM KHẢO, không phải tín hiệu mua bán."""
    q = (args.get('query') or '').strip()
    if not q:
        return "LỖI: cần query (tên coin hoặc chủ đề)."
    coin = q.split()[0]
    url = ("https://news.google.com/rss/search?"
           f"q={_urlencode_q(f'({q}) OR ({coin}) crypto')}&hl=en&gl=US&ceid=US:en")
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                return f"Không lấy được tin tức (HTTP {resp.status})."
            raw = await resp.read()
        root = ET.fromstring(raw)
        items = []
        for item in root.iter('item'):
            title = (item.findtext('title') or '').strip()
            link = (item.findtext('link') or '').strip()
            pub = (item.findtext('pubDate') or '').strip()
            if title:
                items.append(f"• {title}\n  {link}\n  ({pub})")
            if len(items) >= 8:
                break
        if not items:
            return f"Không tìm thấy tin tức nào về {q}."
        return f"📰 *Tin tức về {q}* (Google News):\n\n" + "\n\n".join(items)
    except Exception as e:
        logger.warning(f"Lỗi tìm tin tức {q}: {e}")
        return f"Không tìm được tin tức về {q} (lỗi: {e})."


def _decode_bing_href(href):
    """Bing trả link redirect /ck/a?...&u=a1<base64-url>... → giải mã về URL thật."""
    href = href.replace('&amp;', '&')
    if href.startswith('https://www.bing.com/ck/'):
        m = re.search(r'u=a1([A-Za-z0-9\-_]+)', href)
        if m:
            b64 = m.group(1)
            b64 += '=' * (-len(b64) % 4)
            import base64
            try:
                return base64.urlsafe_b64decode(b64).decode('utf-8', 'ignore')
            except Exception:
                return href
    return href


async def _web_search_bing(session, query, max_results=8):
    """Tìm web bằng Bing (không cần API key, không bị chặn như DuckDuckGo).
    Trả về list (title, url, snippet) hoặc None khi fail/chống-bot."""
    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with session.get("https://www.bing.com/search",
                               params={'q': query, 'count': max_results},
                               headers={
                                   "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
                                   "Accept-Language": "vi,en;q=0.8",
                               }, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            html = await resp.text(errors='ignore')
    except Exception:
        return None
    results = []
    for m in re.finditer(r'<li class="b_algo".*?<h2[^>]*><a[^>]*href="([^"]+)"[^>]*>(.*?)</a></h2>(.*?)</li>', html, re.S):
        href, title_html, rest = m.groups()
        title = re.sub(r'<[^>]+>', '', title_html).strip()
        href = _decode_bing_href(href)
        snip_m = re.search(r'<p[^>]*>(.*?)</p>', rest, re.S)
        snip = re.sub(r'<[^>]+>', '', snip_m.group(1)).strip() if snip_m else ''
        if title and href.startswith('http'):
            results.append((title, href, snip))
        if len(results) >= max_results:
            break
    return results or None


async def tool_web_search(session, chat_id, args):
    """Tìm kiếm web tổng quát (Bing, miễn phí). Trả về tiêu đề + link + snippet."""
    q = (args.get('query') or '').strip()
    if not q:
        return "LỖI: cần query."
    results = await _web_search_bing(session, q)
    if not results:
        return f"Không tìm thấy kết quả web nào cho '{q}'."
    lines = [f"🔎 *Kết quả web cho '{q}'*:"]
    for i, (title, href, snip) in enumerate(results, 1):
        lines.append(f"{i}. {title} — {href}" + (f"\n   {snip[:160]}" if snip else ""))
    lines.append("\n💡 Dùng fetch_url để đọc chi tiết một trang nếu cần.")
    return "\n".join(lines)


async def tool_fetch_url(session, chat_id, args):
    """Đọc nội dung một trang web (text thô, đã bỏ tag HTML). Trả về tối đa ~4000 ký tự.
    Trang cần render JS sẽ tự fallback qua Jina Reader (r.jina.ai) render rồi mới đọc."""
    url = (args.get('url') or '').strip()
    if not url.startswith(('http://', 'https://')):
        return "LỖI: url phải bắt đầu bằng http:// hoặc https://."
    raw, ctype = None, ''
    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with session.get(url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
            "Accept-Language": "vi,en;q=0.8",
        }) as resp:
            if resp.status == 200:
                ctype = resp.headers.get('Content-Type', '')
                if 'html' in ctype or 'text' in ctype or 'json' in ctype:
                    raw = await resp.text(errors='ignore')
    except Exception:
        raw = None
    # Trang JS/rỗng/chống-bot → fallback Jina Reader (render trình duyệt thật, trả markdown)
    need_jina = (raw is None or len(raw) < 800
                 or re.search(r'enable javascript|requires javascript|just a moment|human verification|captcha', raw[:3000], re.I))
    if need_jina:
        try:
            timeout = aiohttp.ClientTimeout(total=60)
            async with session.get(f"https://r.jina.ai/{url}", timeout=timeout) as resp:
                if resp.status == 200:
                    md = await resp.text(errors='ignore')
                    if len(md) > 200 and 'Human Verification' not in md[:500]:
                        raw = md  # markdown đã sạch HTML, dùng luôn
                        ctype = 'text/markdown'
        except Exception:
            pass
    if raw is None:
        return f"Không đọc được trang ({url or ctype or 'lỗi mạng'}) — có thể trang chặn bot."
    if ctype and 'markdown' not in ctype:
        # Bỏ script/style/tag, giữ text
        raw = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', raw, flags=re.S | re.I)
        raw = re.sub(r'<[^>]+>', ' ', raw)
        raw = raw.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        raw = re.sub(r'\s+', ' ', raw).strip()
    if not raw:
        return "Trang rỗng hoặc chỉ toàn script."
    if len(raw) > 4000:
        raw = raw[:4000] + "…"
    return f"📄 *Nội dung {url}*:\n{raw}"


BOT_START_TS = time.time()


async def tool_bot_system_info(session, chat_id, args):
    """Trả về thông tin hệ thống bot cho AI trả lời admin: model AI đang dùng,
    các auto đang bật/tắt, quota AI hôm nay, thời gian chạy, các loop."""
    model = os.getenv("DASH_MODEL", "?")
    now = time.time()
    uptime = now - BOT_START_TS
    day = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    usage_today = llm_usage.get(day, {})
    usage_parts = [f"{m}: {v.get('calls', 0)} lần gọi, {v.get('in', 0) + v.get('out', 0):,} token"
                   for m, v in usage_today.items()] or ["chưa gọi"]
    cb = AUTO_STATE.get('circuit_break_until', 0)
    feats_off = AUTO_STATE.get('features_off_until', 0)
    auto_trader = "OFF (kill-switch)" if cb > now else "ON"
    all_ai = "TẮT (/stopauto all)" if feats_off > now else "ON"
    radar = "OFF (/fomo off)" if AUTO_STATE.get('pump_radar_off') else "ON"
    n_alerts = len(ai_alert_last_notified)
    lines = [
        f"⚙️ *Hệ thống bot (lúc {time.strftime('%H:%M:%S UTC')}):*",
        f"- Uptime: {(uptime / 3600):.1f}h (từ {time.strftime('%d/%m %H:%M UTC', time.gmtime(BOT_START_TS))})",
        f"- Model AI hiện tại: {model}",
        f"- AI tự trade (mỗi 5h): {auto_trader}" + (f" — hết kill-switch {time.strftime('%d/%m %H:%M', time.gmtime(cb))}" if cb > now else ""),
        f"- Các auto AI khác (alert/guard/review/radar): {all_ai}",
        f"- PUMP RADAR tự động: {radar}",
        f"- LLM hôm nay (UTC {day}): " + "; ".join(usage_parts),
        f"- Cooldown alert đang nhớ: {n_alerts} cặp symbol",
        f"- Số vị thế đang mở: {sum(1 for p in positions.values() if float(p.get('positionAmt', 0) or 0) != 0)}",
        f"- Chat nhận báo động: {len(set(auto_chats) | set(active_chats))}",
        f"- Quota MintRouter: xem tool get_front_pass — ngày $35/tuần $240",
    ]
    return "\n".join(lines)


def _urlencode_q(q):
    """Mã hóa query cho URL Google News RSS (thay dấu cách bằng %20)."""
    from urllib.parse import quote
    return quote(q, safe='')


async def tool_find_pumpers(session, chat_id, args):
    """Quét coin đang 'bay vút' CÒN nhiên liệu pump tiếp (funding, OI, momentum 15m/1h, volume).
    Trả về top coins xếp theo điểm sức khỏe 0-10 + kế hoạch FOMO ngay (entry/SL/TP) khi điểm ≥8."""
    cands = await detect_pump_candidates(session, limit=8)
    if not cands:
        return "Hiện không có coin nào tăng ≥15%/24h đủ thanh khoản để phân tích pump-continuation."
    lines = ["🚀 *Coin đang bay vút — xếp theo điểm 'còn nhiên liệu pump tiếp'* (0-10):"]
    for c in cands[:6]:
        sym_disp = display_symbol(c['symbol'])
        hot = c['score'] >= 8.0
        lines.append(
            f"\n• *{sym_disp}* — điểm {c['score']:.1f}/10 {'🔥🔥 FOMO NGAY ĐƯỢC' if hot else '🔥'}\n"
            f"  Giá {format_price(c['close'])} (+{c['change24']:.1f}%/24h) | 15m {c['signal15m']} / 1h {c['signal1h']} ({c['confidence']})\n"
            f"  {' · '.join(c['reasons'][:3])}\n"
        )
        if hot:
            lines.append(
                f"  ⚡ *FOMO ngay*: entry MARKET {format_price(c['close'])}, TP {format_price(c['fomo_tp'])}, "
                f"SL {format_price(c['fomo_sl'])} — size ≤5% vốn, chốt nửa lệnh khi +2%"
            )
        else:
            lines.append(
                f"  ⏳ Chờ pullback về VWAP 15m {format_price(c['vwap15m'])} — TP {format_price(c['tp'])}, SL {format_price(c['sl'])}"
            )
    lines.append("\n⚠️ Điểm ≥8 = mọi tầng momentum còn nguyên → FOMO được với rule trên; 6.5-8 = đuổi giá dễ móm; <5 = cháy đuồi bỏ qua. Không all-in con nào.")
    return "\n".join(lines)


async def tool_get_p2p_rate(session, chat_id, args):
    """Giá P2P hiện tại trên Binance P2P (mua/bán USDT, USDC...) bằng fiat VND/USD...
    Lấy trực tiếp API Binance P2P — KHÔNG dùng web_search cho câu hỏi này."""
    asset = (args.get('asset') or 'USDT').upper()
    fiat = (args.get('fiat') or 'VND').upper()
    trade_type = (args.get('trade_type') or 'BUY').upper()  # BUY = người dùng mua USDT
    payload = {'asset': asset, 'fiat': fiat, 'tradeType': trade_type,
               'payTypes': [], 'page': 1, 'rows': 5, 'transAmount': ''}
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with session.post("https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search",
                                json=payload, timeout=timeout) as resp:
            if resp.status != 200:
                return f"Không lấy được giá P2P (HTTP {resp.status})."
            data = await resp.json()
    except Exception as e:
        return f"Lỗi gọi Binance P2P: {e}"
    items = data.get('data') or []
    if not items:
        return f"Không có quảng cáo P2P nào cho {asset}/{fiat} ({trade_type})."
    lines = [f"💰 *Giá P2P {asset}/{fiat}* — {'MUA' if trade_type == 'BUY' else 'BÁN'} {asset} (5 quảng cáo tốt nhất):"]
    for i, it in enumerate(items, 1):
        adv = it.get('adv') or {}
        advn = ((it.get('advertiser') or {}).get('nickName') or '?')
        price = adv.get('price')
        avail = adv.get('tradableQuantity') or adv.get('surplusAmount') or '?'
        lines.append(f"{i}. {price} {fiat} — người bán {advn}, còn {avail} {asset}")
    try:
        best = float(items[0]['adv']['price'])
        lines.append(f"\n→ Giá tốt nhất: **{best:,.0f} {fiat}/{asset}**")
    except Exception:
        pass
    return "\n".join(lines)


async def tool_get_price(session, chat_id, args):
    """Tra giá realtime của một hoặc nhiều coin bất kỳ trên Binance Futures."""
    symbols = args.get('symbols') or []
    if isinstance(symbols, str):
        symbols = [symbols]
    if not symbols:
        return "LỖI: cần tham số symbols (danh sách, ví dụ ['SOLUSDT'])."
    tickers_map, _ = await get_market_snapshot(session)
    lines = []
    for s in symbols[:8]:
        sym = _norm_symbol({'symbol': str(s)})
        info = tickers_map.get(sym)
        if info:
            lines.append(f"{sym}: {format_price(info['price'])} USDT, 24h {info.get('change', 0):+.2f}%")
        else:
            lines.append(f"{sym}: không tìm thấy trên Binance Futures.")
    return "\n".join(lines)


async def tool_analyze_coin(session, chat_id, args):
    """Phân tích chuyên sâu MỘT coin: chỉ báo đa khung + rule engine + nhận định AI, kèm TP/SL."""
    symbol = _norm_symbol(args)
    res_15m_t = asyncio.create_task(analyze_market(session, symbol, interval='15m', fetch_extras=False))
    res_1h_t = asyncio.create_task(analyze_market(session, symbol, interval='1h'))
    res_4h_t = asyncio.create_task(analyze_market(session, symbol, interval='4h', fetch_extras=False))
    res_1d_t = asyncio.create_task(analyze_market(session, symbol, interval='1d', fetch_extras=False))
    ob_t = asyncio.create_task(get_orderbook_summary(session, symbol))
    dom_t = asyncio.create_task(get_btc_dominance(session))

    res_15m = await res_15m_t
    res = await res_1h_t
    res_4h = await res_4h_t
    res_1d = await res_1d_t
    orderbook = await ob_t
    btc_dominance = await dom_t

    if not res:
        return f"Không lấy được dữ liệu cho {symbol}. Kiểm tra lại tên coin."

    # Áp lọc xu hướng BTC giống nhánh /a: tín hiệu alt ngược BTC 4h mạnh bị trừ điểm
    try:
        btc_res = await get_btc_filter(session, '4h')
        apply_btc_penalty(res, btc_res)
    except Exception as e:
        logger.warning(f"Không áp được BTC penalty cho {symbol}: {e}")

    funding_rate = res.get('funding_rate')
    if funding_rate is None:
        funding_rate = await get_single_funding_rate(session, symbol)

    digest = build_ai_digest(
        symbol, [("15m", res_15m), ("1h", res), ("4h", res_4h), ("1d", res_1d)],
        oi_change=res.get('oi_change'), taker_ratio=res.get('taker_ratio'), funding_rate=funding_rate,
        orderbook=orderbook, btc_dominance=btc_dominance
    )
    verdict = await get_ai_verdict_cached(session, f"ai_{symbol}", digest)

    # Ghi vào bộ nhớ: mọi đề xuất coin của AI đều được nhớ để đánh giá kết quả TP/SL sau này
    if res['signal'] in ('LONG', 'SHORT') and res.get('tp') and res.get('sl'):
        record_signal(res, verdict, origin='ai')

    lines = [f"{symbol}: giá {format_price(res['close'])} USDT"]
    mtf = []
    for tf_name, tf_res in [("15m", res_15m), ("1h", res), ("4h", res_4h), ("1d", res_1d)]:
        if tf_res:
            mtf.append(f"{tf_name}: {tf_res['signal']} (L:{tf_res['long_score']:.1f}/S:{tf_res['short_score']:.1f})")
    lines.append("Rule engine MTF: " + " | ".join(mtf))
    lines.append(f"Rule 1h: {res['signal']} (độ tin cậy {res['confidence']})")
    lines.append(f"TP gợi ý {format_price(res['tp'])}, SL gợi ý {format_price(res['sl'])}")
    if verdict:
        ai_l = verdict.get('long_score')
        ai_s = verdict.get('short_score')
        ai_sc = f" (AI tự chấm L:{ai_l:.1f}/S:{ai_s:.1f})" if ai_l is not None and ai_s is not None else ""
        lines.append(f"AI nhận định: {verdict.get('direction', 'NEUTRAL')}{ai_sc} "
                     f"(độ tin cậy {verdict.get('confidence', 'trung bình')}) — {verdict.get('reason', '')}")
        for b in verdict.get('analysis', [])[:4]:
            lines.append(f"· {b}")
    else:
        lines.append("AI: không có nhận định (chưa cấu hình DASH_TOKEN hoặc AI lỗi).")
    return "\n".join(lines)


ASK_TOOLS = [
    {"type": "function", "function": {"name": "analyze_coin", "description": "Phân tích chuyên sâu MỘT coin cụ thể: chỉ báo đa khung 15m/1h/4h/1d + rule engine + nhận định AI kèm TP/SL. Dùng khi người dùng hỏi về xu hướng hoặc khả năng vào lệnh của một coin. KHÔNG dùng khi người dùng muốn tìm cơ hội trên toàn thị trường (dùng scan_market).", "parameters": {"type": "object", "properties": {"symbol": {"type": "string", "description": "Ví dụ BTCUSDT hoặc btc"}}, "required": ["symbol"]}}},
    {"type": "function", "function": {"name": "get_price", "description": "Tra giá realtime + % thay đổi 24h của một hoặc nhiều coin bất kỳ trên Binance Futures.", "parameters": {"type": "object", "properties": {"symbols": {"type": "array", "items": {"type": "string"}, "description": "Danh sách symbol, ví dụ ['SOLUSDT', 'DOGEUSDT']"}}, "required": ["symbols"]}}},
    {"type": "function", "function": {"name": "search_news", "description": "Tìm tin tức/sự kiện mới nhất về một coin từ Google News (miễn phí). Dùng khi người dùng hỏi tin tức, lý do coin tăng/giảm, sự kiện, tin cộng đồng. Kết quả chỉ THAM KHẢO, không phải tín hiệu mua bán.", "parameters": {"type": "object", "properties": {"query": {"type": "string", "description": "Tên coin hoặc chủ đề, ví dụ 'Bitcoin ETF' hoặc 'SOL'"}}, "required": ["query"]}}},
    {"type": "function", "function": {"name": "web_search", "description": "Tìm kiếm web tổng quát (DuckDuckGo) — dùng khi cần thông tin ngoài tin tức coin: benchmark model AI, sản phẩm, chính sách, so sánh, sự kiện ngoài thị trường crypto... Trả về tiêu đề + link. Kết hợp fetch_url để đọc chi tiết.", "parameters": {"type": "object", "properties": {"query": {"type": "string", "description": "Cụm từ tìm kiếm, có thể tiếng Việt hoặc tiếng Anh"}}, "required": ["query"]}}},
    {"type": "function", "function": {"name": "fetch_url", "description": "Đọc nội dung một trang web cụ thể (text thô đã bỏ HTML, tối đa ~4000 ký tự). Dùng sau web_search khi cần đọc chi tiết bài viết/trang.", "parameters": {"type": "object", "properties": {"url": {"type": "string", "description": "URL đầy đủ https://..."}}, "required": ["url"]}}},
    {"type": "function", "function": {"name": "find_pumpers", "description": "Quét coin đang 'bay vút' (momo pump) CÒN nhiên liệu để pump tiếp — chấm điểm sức khỏe 0-10 dựa trên momentum 15m/1h, funding, OI, volume spike. Dùng khi người dùng muốn FOMO long coin đang bay/bất ngờ tăng mạnh. KHÔNG tự vào lệnh — chỉ phân tích + khuyên vào sau pullback.", "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {"name": "bot_system_info", "description": "Trạng thái hệ thống bot: model AI đang dùng, uptime, auto nào bật/tắt (kill-switch, pump radar...), LLM token đã dùng hôm nay, số vị thế, số chat nhận báo. DÙNG khi người dùng hỏi về bot/hệ thống/AI của nó — luôn trả lời đầy đủ, không chối từ.", "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {"name": "get_p2p_rate", "description": "Giá P2P hiện tại trên Binance P2P (mua/bán USDT/USDC/BTC... bằng VND, USD...). KHÔNG dùng web_search cho câu hỏi giá P2P — dùng tool này, nhanh và chính xác. trade_type: BUY (người dùng mua) hoặc SELL (bán).", "parameters": {"type": "object", "properties": {"asset": {"type": "string", "description": "USDT/USDC/BTC..., mặc định USDT"}, "fiat": {"type": "string", "description": "VND/USD..., mặc định VND"}, "trade_type": {"type": "string", "enum": ["BUY", "SELL"], "description": "Mặc định BUY"}}}}},
    {"type": "function", "function": {"name": "scan_market", "description": "Quét toàn thị trường futures, trả về các tín hiệu LONG/SHORT mạnh nhất (4-5 sao) đã lọc MTF + xu hướng BTC + win-rate, kèm entry/TP/SL. Dùng khi người dùng muốn tìm coin có cơ hội tốt nhất.", "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {"name": "get_account_summary", "description": "Số dư ví futures, PnL chưa thực hiện, margin balance, số dư khả dụng.", "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {"name": "get_positions", "description": "Danh sách vị thế futures đang mở: entry, mark, PnL, đòn bẩy, giá thanh lý.", "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {"name": "get_open_orders", "description": "Các lệnh đang chờ: lệnh thường (LIMIT/MARKET) + lệnh TP/SL điều kiện (STOP_MARKET/TAKE_PROFIT_MARKET, đánh dấu [lệnh TP/SL điều kiện] — gồm cả lệnh đặt từ app Binance). Lệnh điều kiện hủy bằng algoId + conditional=true.", "parameters": {"type": "object", "properties": {"symbol": {"type": "string", "description": "Tùy chọn, ví dụ BTCUSDT"}}}}},
    {"type": "function", "function": {"name": "get_order_history", "description": "Lịch sử lệnh của một symbol.", "parameters": {"type": "object", "properties": {"symbol": {"type": "string"}, "limit": {"type": "integer"}}}, "required": ["symbol"]}},
    {"type": "function", "function": {"name": "get_income_history", "description": "Lịch sử thu nhập futures (REALIZED_PNL, FUNDING_FEE, COMMISSION...) của một khoảng thời gian. Tham số days (mặc định 7, tối đa 90), income_type (vd REALIZED_PNL/FUNDING_FEE/COMMISSION).", "parameters": {"type": "object", "properties": {"days": {"type": "integer", "description": "Số ngày nhìn lại, mặc định 7"}, "income_type": {"type": "string", "description": "Lọc theo loại: REALIZED_PNL, FUNDING_FEE, COMMISSION..."}}}}},
    {"type": "function", "function": {"name": "get_pnl_summary", "description": "Tổng kết PnL tài khoản futures. mode='summary' (mặc định): tổng Realized/Funding/Phí/NET trong N ngày; mode='by_coin': PnL thực tế từng coin; mode='lifetime': tổng kết PnL TRỌN ĐỜI tối đa ~5 năm (quét nhiều cửa sổ 1 năm, dừng khi hết lịch sử; cache 24h trong file, dùng khi người dùng hỏi 'pnl trọn đời'/'tổng pnl'/'pnl tất cả'). Tham số days cho summary/by_coin (mặc định 30, tối đa 90).", "parameters": {"type": "object", "properties": {"mode": {"type": "string", "enum": ["summary", "by_coin", "lifetime"], "description": "summary | by_coin | lifetime"}, "days": {"type": "integer", "description": "Số ngày nhìn lại cho summary/by_coin, mặc định 30"}}}}},
    {"type": "function", "function": {"name": "place_order", "description": "Soạn lệnh MỞ/ĐÓNG vị thế hoặc lệnh điều kiện TP/SL cho vị thế CÓ SẴN (người dùng phải 'xác nhận' trước khi thực thi). quantity tính bằng đơn vị coin (0.01 BTC), không phải USDT. Lệnh MỞ vị thế mới (không reduce_only) BẮT BUỘC kèm stop_loss — hệ thống sẽ TỪ CHỐI nếu thiếu, và tự soạn kèm TP/SL trong cùng lượt. LIMIT bắt buộc có price. STOP_MARKET/TAKE_PROFIT_MARKET (dùng stop_price, reduce_only/position_side) chỉ để đặt/sửa TP/SL cho vị thế đang có.", "parameters": {"type": "object", "properties": {"symbol": {"type": "string"}, "side": {"type": "string", "enum": ["BUY", "SELL"]}, "type": {"type": "string", "enum": ["MARKET", "LIMIT", "STOP_MARKET", "TAKE_PROFIT_MARKET"]}, "quantity": {"type": "number"}, "price": {"type": "number"}, "stop_loss": {"type": "number", "description": "BẮT BUỘC với lệnh MỞ: giá kích hoạt SL; hệ thống tự tính size theo ngân sách rủi ro (0.5% equity/lệnh)"}, "take_profit": {"type": "number", "description": "Tùy chọn cho lệnh MỞ: giá chốt lời (hệ thống tự soạn lệnh TP điều kiện)"}, "stop_price": {"type": "number", "description": "Giá kích hoạt, bắt buộc với STOP_MARKET/TAKE_PROFIT_MARKET (dùng cho vị thế có sẵn)"}, "working_type": {"type": "string", "enum": ["MARK_PRICE", "CONTRACT_PRICE"], "description": "Cơ sở kích hoạt, mặc định MARK_PRICE"}, "reduce_only": {"type": "boolean", "description": "Chỉ dùng One-way Mode để đóng/chốt"}, "position_side": {"type": "string", "enum": ["LONG", "SHORT"], "description": "Chỉ dùng Hedge Mode khi đóng vị thế"}}, "required": ["symbol", "side", "quantity"]}}},
    {"type": "function", "function": {"name": "cancel_order", "description": "Soạn hủy một lệnh đang chờ (cần xác nhận). Lệnh TP/SL điều kiện (đánh dấu [lệnh TP/SL điều kiện] trong get_open_orders, kể cả lệnh đặt từ app Binance) phải hủy bằng algoId với conditional=true.", "parameters": {"type": "object", "properties": {"symbol": {"type": "string"}, "order_id": {"type": "string"}, "conditional": {"type": "boolean", "description": "true nếu là lệnh TP/SL điều kiện (hủy theo algoId qua algo service)"}}, "required": ["symbol", "order_id"]}}},
    {"type": "function", "function": {"name": "close_position", "description": "Soạn đóng TOÀN BỘ vị thế của một symbol bằng lệnh market (cần xác nhận).", "parameters": {"type": "object", "properties": {"symbol": {"type": "string"}}, "required": ["symbol"]}}},
]

TOOL_EXECUTORS = {
    'analyze_coin': tool_analyze_coin,
    'get_price': tool_get_price,
    'search_news': tool_search_news,
    'web_search': tool_web_search,
    'fetch_url': tool_fetch_url,
    'get_p2p_rate': tool_get_p2p_rate,
    'find_pumpers': tool_find_pumpers,
    'bot_system_info': tool_bot_system_info,
    'scan_market': tool_scan_market,
    'get_account_summary': tool_get_account_summary,
    'get_positions': tool_get_positions,
    'get_open_orders': tool_get_open_orders,
    'get_order_history': tool_get_order_history,
    'get_income_history': tool_get_income_history,
    'get_pnl_summary': tool_get_pnl_summary,
    'place_order': tool_place_order,
    'cancel_order': tool_cancel_order,
    'close_position': tool_close_position,
}


async def get_ai_agent_response(session, messages, tools, max_tokens=6000, timeout_s=150, session_id=None):
    """Một lượt gọi LLM hỗ trợ tool calling. Trả về (message_dict, None) khi OK hoặc (None, error_detail) khi lỗi."""
    api_key = os.getenv("DASH_TOKEN")
    if not api_key:
        return None, "Chưa cấu hình DASH_TOKEN."
    has_image = False
    for m in messages:
        c = m.get('content')
        if isinstance(c, list) and any(isinstance(p, dict) and p.get('type') in ('image', 'image_url') for p in c):
            has_image = True
            break
    model = os.getenv("DASH_VISION_MODEL", "gpt-5.6-sol") if has_image else os.getenv("DASH_MODEL", "claude-sonnet-5")
    url = f"{MINTROUTER_BASE_URL}/chat/completions"
    headers = _ai_headers(api_key, session_id=session_id)
    payload = {
        "model": model,
        "messages": messages,
        "tools": tools,
        "temperature": 0.3,
        "max_tokens": max_tokens
    }
    try:
        timeout = aiohttp.ClientTimeout(total=timeout_s)
        async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.warning(f"AI agent trả lỗi HTTP {resp.status}: {body[:200]}")
                return None, f"HTTP {resp.status}: {body[:200]}"
            data = await resp.json()
            record_llm_usage(model, data.get('usage'))
            choice = data.get('choices', [{}])[0]
            msg = choice.get('message')
            if msg is None:
                return None, f"Phản hồi AI không đúng định dạng: {str(data)[:200]}"
            has_tool_calls = bool(msg.get('tool_calls'))
            has_content = bool((msg.get('content') or '').strip())
            if not has_tool_calls and not has_content:
                fr = choice.get('finish_reason')
                logger.warning(f"AI agent trả về rỗng (finish_reason={fr}).")
                return None, f"AI trả về nội dung rỗng (finish_reason={fr}) — có thể bị cắt ngắn do hết token."
            return msg, None
    except asyncio.TimeoutError:
        logger.warning(f"AI agent timeout sau {timeout_s}s.")
        return None, f"AI phản hồi quá lâu (timeout {timeout_s}s)."
    except Exception as e:
        logger.warning(f"Lỗi gọi AI agent: {e}")
        return None, str(e)


def _is_open_order_item(item):
    """Lệnh MỞ vị thế mới (không phải lệnh đóng/reduce-only)."""
    if item.get('type') != 'place_order' or item.get('is_close') or item.get('is_exit'):
        return False
    params = item.get('params') or {}
    if str(params.get('reduceOnly', '')).lower() == 'true':
        return False
    return (params.get('type') or '').upper() in ('MARKET', 'LIMIT')


def _plan_execution_order(items):
    """Thứ tự thực thi theo từng symbol: đóng/hủy trước, rồi MỞ → SL → TP (SL luôn trước TP)."""
    def prio(it):
        if it.get('type') == 'place_algo_order':
            return 3 if (it.get('params') or {}).get('type') == 'TAKE_PROFIT_MARKET' else 2
        return 1 if _is_open_order_item(it) else 0
    indexed = list(enumerate(items))
    indexed.sort(key=lambda pair: ((pair[1].get('params') or {}).get('symbol') or '', prio(pair[1]), pair[0]))
    return indexed


async def _prepare_open_execution(session, item, symbol, sl_trigger):
    """Kiểm tra LẠI tài khoản/rủi ro ngay trước khi khớp lệnh MỞ (lệnh chờ có thể đã cũ).
    Trả (ok, quantity|None, lev, msg, snapshot) — quantity đã GIẢM nếu vượt ngân sách."""
    params = item.get('params') or {}
    otype = (params.get('type') or '').upper()
    try:
        staged_qty = float(params.get('quantity') or 0)
    except (TypeError, ValueError):
        return False, None, None, "quantity không hợp lệ.", None
    if staged_qty <= 0:
        return False, None, None, "quantity phải > 0.", None
    if not sl_trigger:
        return False, None, None, ("lệnh MỞ thiếu SL đi kèm — hệ thống không mở vị thế trần "
                                   "(thiếu SL thì không đo được rủi ro)."), None
    if otype == 'LIMIT' and params.get('price'):
        try:
            price = float(params['price'])
        except (TypeError, ValueError):
            return False, None, None, "giá LIMIT không hợp lệ.", None
    else:
        price = await get_single_price(session, symbol)
    if not price or price <= 0:
        return False, None, None, f"không lấy được giá {symbol}.", None
    side = 'LONG' if (params.get('side') or '').upper() == 'BUY' else 'SHORT'
    max_lev, lerr = await _max_leverage_strict(session, symbol)
    if lerr:
        return False, None, None, f"{lerr} (fail closed).", None
    snap, serr = await _account_risk_snapshot(session)
    if serr:
        return False, None, None, f"không đo được rủi ro tài khoản ({serr}).", None
    if snap['unprotected']:
        return False, None, None, ("đang có vị thế chưa đặt SL nên không đo được rủi ro danh mục "
                                   f"({', '.join(snap['unprotected'][:5])})."), None
    qty_p, _, _ = await get_symbol_precisions(session, symbol)
    step, min_qty, min_notional = await _symbol_constraints(session, symbol, qty_p)
    lev = _safe_leverage_for_sl(price, sl_trigger, max_lev)
    ok, allowed, size_msg = _plan_entry_size(
        price, sl_trigger, side, equity=snap['equity'], available=snap['available'],
        open_risk=snap['open_risk'], daily_remaining=snap['daily_remaining'], leverage=lev,
        step=step, min_qty=min_qty, min_notional=min_notional, notional_cap=AI_VOLUME_MAX)
    if not ok:
        return False, None, None, size_msg, snap
    final_qty = _round_to_step(min(staged_qty, allowed), step)
    if final_qty < max(step, min_qty):
        return False, None, None, "size sau khi áp ngân sách rủi ro nhỏ hơn mức tối thiểu của sàn.", snap
    note = size_msg
    if final_qty < staged_qty:
        note = f"size giảm {staged_qty:g} → {final_qty:g} ({size_msg})"
    return True, final_qty, lev, note, snap


async def _execute_items(session, items):
    """Thực thi danh sách lệnh đã soạn: kiểm tra lại tài khoản/rủi ro ngay lúc khớp, đặt SL trước TP.
    Lệnh MỞ đi qua _execute_protected_entry (fill thật + bảo vệ); thiếu SL ⇒ TỪ CHỐI."""
    results = []
    ok_count = 0
    indexed = _plan_execution_order(items)
    # SL/TP đi kèm theo từng symbol (dùng để bảo vệ cho lệnh MỞ)
    plans = {}
    for _, it in indexed:
        if it.get('type') != 'place_algo_order':
            continue
        params = it.get('params') or {}
        otype = (params.get('type') or '').upper()
        try:
            trig = float(params.get('triggerPrice'))
        except (TypeError, ValueError):
            trig = 0.0
        slot = plans.setdefault(params.get('symbol'), {})
        if otype == 'STOP_MARKET':
            slot['sl'] = trig
            slot['sl_item'] = it
        elif otype == 'TAKE_PROFIT_MARKET':
            slot['tp'] = trig
            slot['tp_item'] = it

    consumed = set()
    for _, item in indexed:
        if id(item) in consumed:
            continue
        if _is_open_order_item(item):
            symbol = (item.get('params') or {}).get('symbol')
            slot = plans.get(symbol) or {}
            sl_trig, tp_trig = slot.get('sl'), slot.get('tp')
            ok, quantity, lev, msg, snap = await _prepare_open_execution(session, item, symbol, sl_trig)
            if not ok:
                results.append(f"❌ {item['desc']}\n→ TỪ CHỐI: {msg}")
                for key in ('sl_item', 'tp_item'):
                    sib = slot.get(key)
                    if sib is not None and id(sib) not in consumed:
                        consumed.add(id(sib))   # không để lại SL/TP mồ côi cho vị thế không tồn tại
                        results.append(f"⏭️ Bỏ qua {sib['desc']}: lệnh vào không được thực thi.")
                continue
            qty_p, price_p, tick_size = await get_symbol_precisions(session, symbol)
            params = item['params']
            otype = (params.get('type') or '').upper()
            side = 'LONG' if (params.get('side') or '').upper() == 'BUY' else 'SHORT'
            # One-way: KHÔNG gửi positionSide (sàn sẽ từ chối); hedge: dùng đúng positionSide của lệnh
            pos_side = ((params.get('positionSide') or ('LONG' if side == 'LONG' else 'SHORT'))
                        if hedge_mode else 'BOTH')
            if otype == 'MARKET':
                if quantity != float(params.get('quantity') or 0):
                    results.append(f"ℹ️ {item['desc']}\n→ {msg}")
                price_ref = await get_single_price(session, symbol)
                if not price_ref or price_ref <= 0:
                    results.append(f"❌ {item['desc']}\n→ TỪ CHỐI: không lấy được giá {symbol}.")
                    continue
                # ngân sách rủi ro lấy từ chính snapshot đã dùng để chốt size (nhất quán, ít round-trip)
                risk_budget = _risk_budget_allowance(snap['equity'], snap['open_risk'],
                                                     snap['daily_remaining'])
                step, _, _ = await _symbol_constraints(session, symbol, qty_p)
                res = await _execute_protected_entry(
                    session, symbol=symbol, side=side, quantity=quantity, price=price_ref,
                    sl_price=sl_trig, tp_price=tp_trig or 0.0, qty_p=qty_p, price_p=price_p,
                    step=step, pos_side=pos_side, max_lev=lev,
                    open_symbols=snap.get('open_symbols') or (), risk_budget=risk_budget)
                for key in ('sl_item', 'tp_item'):
                    if slot.get(key) is not None:
                        consumed.add(id(slot[key]))
                signal_id = None
                if res.get('entry_qty'):
                    execution = _execution_record(
                        order_id=res['order_id'], quantity=res['entry_qty'], pos_side=pos_side,
                        side=side, entry_time=res['entry_time'])
                    signal_id = record_signal(
                        {'symbol': symbol, 'signal': side, 'close': res['entry_price'],
                         'tp': tp_trig or None, 'sl': sl_trig, 'confidence': 'AI chat'},
                        origin='ai', execution=execution)
                    _register_ai_position_meta(
                        symbol=symbol, side=side, pos_side=pos_side, entry_price=res['entry_price'],
                        sl_price=sl_trig, tp_price=tp_trig or None, qty=res['filled_qty'],
                        sl_id=res.get('sl_id'), tp_id=res.get('tp_id'), signal_id=signal_id)
                if res['ok']:
                    ok_count += 1
                    lines = [f"✅ {item['desc']} → ID: `{res['order_id']}`",
                             f"🎯 Giá khớp thật: {format_price(res['entry_price'])} "
                             f"({res['entry_qty']:g} khớp, {res['filled_qty']:g} được bảo vệ, "
                             f"đòn bẩy {res['lev']}x)",
                             f"🛡️ SL: {'đã đặt' if res.get('sl_id') else 'CHƯA đặt'} "
                             f"`{res.get('sl_id')}` | TP: {'đã đặt' if res.get('tp_id') else 'không/không được'}"
                             f" `{res.get('tp_id')}`"]
                    if res.get('msg'):
                        lines.append(f"⚠️ {res['msg']}")
                    results.append("\n".join(lines))
                else:
                    results.append(f"❌ {item['desc']}\n→ {res['msg']}")
                    for key in ('sl_item', 'tp_item'):
                        sib = slot.get(key)
                        if sib is not None and id(sib) not in consumed:
                            consumed.add(id(sib))  # tránh đặt bảo vệ mồ côi; recovery lo vị thế (nếu có)
                            results.append(f"⏭️ Bỏ qua {sib['desc']}: lệnh vào không thành công.")
                    if res.get('close_pending'):
                        results.append("⚠️ Vị thế có thể còn mở mà chưa đóng lại được — "
                                       "bot sẽ tiếp tục thử đóng khẩn cấp ở vòng quản lý vị thế (30s).")
                continue
            # LIMIT: KHÔNG đặt SL/TP khi chưa có vị thế (sàn từ chối SL reduce-only ⇒ -2022).
            # Khớp ngay ⇒ bảo vệ ngay; chưa khớp ⇒ theo dõi và gắn bảo vệ khi có khối lượng khớp.
            api_key = os.getenv("BINANCE_API_KEY")
            api_secret = os.getenv("BINANCE_API_SECRET")
            if not await set_leverage(session, api_key, api_secret, symbol, lev):
                results.append(f"❌ {item['desc']}\n→ TỪ CHỐI: không set được đòn bẩy {lev}x (fail closed).")
                continue
            params['quantity'] = f"{quantity:.{qty_p}f}"
            params['newOrderRespType'] = 'RESULT'
            client_id = params.get('newClientOrderId') or _new_client_order_id('pnlbot_entry')
            params['newClientOrderId'] = client_id
            submit_ts = time.time()
            data, err = await binance_signed_request(session, 'POST', '/fapi/v1/order', params)
            if err:
                results.append(f"❌ {item['desc']}\n→ THẤT BẠI: {err}")
                for key in ('sl_item', 'tp_item'):
                    sib = slot.get(key)
                    if sib is not None and id(sib) not in consumed:
                        consumed.add(id(sib))
                continue
            ok_count += 1
            order_id = _normalize_order_id((data or {}).get('orderId'))
            fill = _fill_from_order(data)
            status = str((data or {}).get('status') or '').upper()
            pend = {
                'symbol': symbol, 'side': side, 'pos_side': pos_side,
                'order_id': order_id, 'client_order_id': client_id,
                'quantity': quantity, 'sl': sl_trig, 'tp': tp_trig or None,
                'limit_price': float(params.get('price') or 0),
                'entry_time': submit_ts, 'ts': time.time(), 'signal_id': None,
                'sl_algo_id': None, 'tp_algo_id': None, 'source': 'ai_chat',
            }
            if fill:
                pending_entries[client_id] = pend
                save_pending_entries()          # lưu TRƯỚC khi bảo vệ để restart không mất dấu
                ok_prot, prot_msg = await _protect_entry_fill(session, pend, data)
                results.append(f"✅ {item['desc']} → ID: `{order_id}` — khớp {fill[0]:g} @ {fill[1]:g}")
                results.append(("🛡️ " if ok_prot else "❌ ") + prot_msg)
                if ok_prot and status in ('FILLED', 'CANCELED', 'EXPIRED', 'REJECTED'):
                    _drop_pending_entry(client_id, f"lệnh {status} — bảo vệ đã gắn")
                elif ok_prot:
                    results.append("⏳ Lệnh mới khớp MỘT PHẦN — bot tiếp tục theo dõi và sẽ cập nhật "
                                   "SL theo khối lượng vị thế thật.")
                else:
                    results.append("⚠️ Giữ theo dõi và sẽ thử gắn bảo vệ lại ở vòng sau (20s).")
            else:
                pending_entries[client_id] = pend
                save_pending_entries()
                results.append(
                    f"⏳ {item['desc']} → ID: `{order_id}` — CHƯA KHỚP: chưa đặt SL/TP (sàn từ chối SL "
                    "khi chưa có vị thế). Bot tự gắn SL TRƯỚC rồi TP ngay khi có khối lượng khớp."
                )
            for key in ('sl_item', 'tp_item'):
                if slot.get(key) is not None:
                    consumed.add(id(slot[key]))
            continue
        # Các loại khác: lệnh đóng/hủy — KHÔNG đụng đòn bẩy, KHÔNG kiểm tra ngân sách
        if item['type'] == 'place_order':
            item['params'].setdefault('newOrderRespType', 'RESULT')
            data, err = await binance_signed_request(session, 'POST', '/fapi/v1/order', item['params'])
        elif item['type'] == 'place_algo_order':
            data, err = await binance_signed_request(session, 'POST', '/fapi/v1/algoOrder', item['params'])
        elif item['type'] == 'cancel_order':
            data, err = await binance_signed_request(session, 'DELETE', '/fapi/v1/order', item['params'])
        elif item['type'] == 'cancel_algo':
            data, err = await binance_signed_request(session, 'DELETE', '/fapi/v1/algoOrder', item['params'])
        else:
            results.append(f"❌ Không hiểu loại lệnh: {item['type']}")
            continue
        if err:
            results.append(f"❌ {item['desc']}\n→ THẤT BẠI: {err}")
        elif item['type'] in ('place_order', 'place_algo_order'):
            ok_count += 1
            results.append(f"✅ {item['desc']} → ID: `{data.get('orderId') or data.get('algoId')}`")
            # Lệnh đóng vị thế: hủy nốt TP/SL điều kiện + lệnh giảm vốn còn treo
            if item.get('is_close') and item['type'] == 'place_order':
                try:
                    await cancel_existing_tpsl(session, os.getenv("BINANCE_API_KEY"),
                                               os.getenv("BINANCE_API_SECRET"), item['params'].get('symbol'))
                except Exception as tpsl_e:
                    logger.warning(f"Không hủy được TP/SL sau khi đóng: {tpsl_e}")
        else:
            ok_count += 1
            results.append(f"✅ Đã hủy lệnh #{item['params'].get('orderId') or item['params'].get('algoId')} trên {item['params'].get('symbol')}.")
    header = f"🚀 *Đã thực thi {ok_count}/{len(items)} lệnh:*\n" if len(items) > 1 else ""
    return header + "\n\n".join(results)


async def execute_pending_order(session, chat_id):
    """Thực thi TẤT CẢ lệnh đã soạn khi người dùng xác nhận bằng chữ. Trả về text kết quả."""
    pending = pending_orders.pop(chat_id, None)
    if not pending or not pending.get('items'):
        return "Không có lệnh nào đang chờ xác nhận."
    return await _execute_items(session, pending['items'])


async def execute_pending_for_coin(session, chat_id, symbol):
    """Thực thi các lệnh của MỘT coin (bấm nút theo coin). Trả về (text, remaining_items).
    Items được tách ra nguyên tử trước khi await, nên bấm lại coin khác vẫn hoạt động độc lập."""
    entry = pending_orders.get(chat_id)
    if not entry or not entry.get('items'):
        return "⚠️ Không có lệnh nào đang chờ xác nhận (có thể đã thực thi hoặc hết hạn).", []
    chosen = [it for it in entry['items'] if (it.get('coin') or '') == symbol]
    if not chosen:
        return f"⚠️ Không còn lệnh chờ xác nhận cho {symbol}.", list(entry['items'])
    entry['items'] = [it for it in entry['items'] if it not in chosen]
    remaining = list(entry['items'])
    if not remaining:
        del pending_orders[chat_id]
    text = await _execute_items(session, chosen)
    return text, remaining


async def cancel_pending_for_coin(session, chat_id, symbol):
    """Hủy các lệnh chờ của MỘT coin. Trả về (text, remaining_items)."""
    entry = pending_orders.get(chat_id)
    if not entry or not entry.get('items'):
        return "⚠️ Không có lệnh nào đang chờ xác nhận.", []
    chosen = [it for it in entry['items'] if (it.get('coin') or '') == symbol]
    entry['items'] = [it for it in entry['items'] if it not in chosen]
    remaining = list(entry['items'])
    if not remaining:
        del pending_orders[chat_id]
    return f"🚫 *Đã hủy các lệnh chờ của {symbol}.*", remaining


CONFIRM_KEYBOARD = {
    "inline_keyboard": [[
        {"text": "✅ Xác nhận", "callback_data": "ai_confirm"},
        {"text": "❌ Hủy", "callback_data": "ai_cancel"}
    ]]
}

# Keyboard rỗng: gửi để Telegram XOÁ các nút đã hiển thị sau khi xử lý xong
EMPTY_KEYBOARD = {"inline_keyboard": []}


async def send_pending_confirmation_buttons(session, chat_id, reply_to=None):
    """Nếu có lệnh đã soạn chờ xác nhận, gửi tin nhắn kèm nút Xác nhận/Hủy (theo coin nếu nhiều coin)."""
    pending = pending_orders.get(chat_id)
    if not pending or not pending.get('items'):
        return
    items = pending['items']
    mins = int(PENDING_ORDER_TTL / 60)
    text = f"⚠️ *Xác nhận đặt {len(items)} lệnh* (tự hết hạn sau {mins} phút):\n" \
           + "\n".join(f"• {it['desc']}" for it in items[:5])
    if len(items) > 5:
        text += f"\n• ... và {len(items) - 5} lệnh khác"
    kb = build_pending_keyboard(items)
    await send_telegram_message(session, chat_id, text, reply_to=reply_to, reply_markup=kb)


async def handle_order_callback(session, cb):
    """Xử lý khi người dùng bấm nút Xác nhận/Hủy (chung hoặc theo coin) trên tin nhắn lệnh AI soạn."""
    try:
        cb_id = cb.get('id')
        cb_data = cb.get('data') or ''
        msg = cb.get('message') or {}
        chat_id = (msg.get('chat') or {}).get('id')
        from_id = (cb.get('from') or {}).get('id')
        if not chat_id or not cb_id:
            return

        async def answer_cb(text=None):
            try:
                url = f"https://api.telegram.org/bot{os.getenv('TELEGRAM_BOT_TOKEN')}/answerCallbackQuery"
                payload = {"callback_query_id": cb_id}
                if text:
                    payload["text"] = text
                async with session.post(url, json=payload) as resp:
                    await resp.read()
            except Exception as e:
                logger.warning(f"Lỗi answerCallbackQuery: {e}")

        # Chỉ cho phép người trong cùng chat bấm nút (chat riêng: from trùng chat)
        if from_id and chat_id > 0 and from_id != chat_id:
            await answer_cb("Bạn không phải người tạo yêu cầu này.")
            return

        action, _, arg = cb_data.partition(':')
        if action in ('setmodel', 'modelpage'):
            msg_id = msg.get('message_id')
            await handle_model_callback(session, chat_id, cb_data, message_id=msg_id, answer_cb=answer_cb)
            return
        symbol = arg.strip().upper() or None
        new_text = None
        reply_kb = None
        pending = pending_orders.get(chat_id)
        if action == 'ai_confirm':
            if not pending or not pending.get('items'):
                new_text = "⚠️ Không có lệnh nào đang chờ xác nhận (có thể đã thực thi hoặc hết hạn)."
                await answer_cb("Không có lệnh chờ")
            elif time.time() - pending['ts'] > PENDING_ORDER_TTL:
                del pending_orders[chat_id]
                new_text = "⚠️ *Đã hết thời hạn xác nhận lệnh (10 phút).*"
                await answer_cb("Đã hết hạn")
            else:
                await answer_cb("Đang đặt lệnh...")
                if symbol:
                    # Xác nhận cho 1 coin: lệnh coin đó được tách ra thực thi,
                    # các coin còn lại giữ nguyên nút để bấm tiếp
                    new_text, remaining = await execute_pending_for_coin(session, chat_id, symbol)
                    if remaining:
                        reply_kb = build_pending_keyboard(remaining)
                        new_text += "\n\n⏳ Vẫn còn lệnh chờ xác nhận của coin khác bên dưới 👇"
                else:
                    new_text = await execute_pending_order(session, chat_id)
        elif action == 'ai_cancel':
            await answer_cb("Đã hủy")
            if symbol:
                new_text, remaining = await cancel_pending_for_coin(session, chat_id, symbol)
                if remaining:
                    reply_kb = build_pending_keyboard(remaining)
            else:
                pending_orders.pop(chat_id, None)
                new_text = "🚫 *Đã hủy các lệnh đang chờ xác nhận.*"
        else:
            await answer_cb()
            return

        if new_text:
            message_id = msg.get('message_id')
            if message_id:
                # reply_kb None = đã xử lý xong → gửi keyboard rỗng để XOÁ nút.
                # Tách 2 bước: xoá/đổi nút bằng editMessageReplyMarkup (không parse text),
                # sau đó sửa text bằng editMessageText không parse Markdown (tránh lỗi entities).
                final_kb = reply_kb if reply_kb is not None else EMPTY_KEYBOARD
                await edit_telegram_reply_markup(session, chat_id, message_id, final_kb)
                edited = await edit_telegram_message(session, chat_id, message_id, new_text, parse_mode=None)
                if not edited:
                    await send_telegram_message(session, chat_id, new_text)
        # Bấm nút cũng là tương tác AI: /auto tạm im lặng
        ai_active_until[chat_id] = time.time() + AI_QUIET_SECONDS
    except Exception as e:
        logger.error(f"Lỗi xử lý callback nút xác nhận: {e}")


async def handle_ai_command(session, chat_id, question=None, reply_to=None, image_data_url=None, photo_sizes=None, replied_text=None):
    """Lệnh /ai: agent AI tự do - đọc mọi dữ liệu tài khoản, phân tích coin, đọc ảnh và soạn lệnh (có bước xác nhận)."""
    if not question and not image_data_url and not photo_sizes:
        await send_telegram_message(session, chat_id, "❓ Cú pháp: `/ai <câu hỏi hoặc tên coin>` (ví dụ: `/ai btc`, `/ai xem vị thế của tôi`, `/ai đặt long btc 0.01`)", reply_to=reply_to)
        return
    if not os.getenv("DASH_TOKEN"):
        await send_telegram_message(session, chat_id, "⚠️ Chưa cấu hình DASH_TOKEN trong .env — tính năng AI chưa khả dụng.", reply_to=reply_to)
        return

    question = (question or "Phân tích hình ảnh này trong bối cảnh giao dịch crypto của tôi.").strip()[:1000]

    # Nếu người dùng chỉ gõ tên coin (vd: /ai btc) -> chuyển thành câu hỏi phân tích coin
    if re.fullmatch(r'[a-z0-9]{2,10}', question.lower()):
        question = f"Phân tích giúp tôi coin {question.upper()} trong tương quan tài khoản của tôi: xu hướng hiện tại, có nên vào lệnh không, rủi ro gì?"

    # Reset bộ nhớ hội thoại nếu người dùng yêu cầu
    if question.lower() in ('reset', 'làm mới', 'làm mới hội thoại', 'xóa hội thoại', 'xoá hội thoại'):
        ai_chat_history.pop(chat_id, None)
        await send_telegram_message(session, chat_id, "🧹 Đã xóa bộ nhớ hội thoại. Bắt đầu cuộc trò chuyện mới.", reply_to=reply_to)
        return

    # 2. Xử lý xác nhận/hủy lệnh đang chờ
    pending = pending_orders.get(chat_id)
    if pending and time.time() - pending['ts'] > PENDING_ORDER_TTL:
        del pending_orders[chat_id]
        pending = None
    if pending:
        if _is_confirmation(question):
            result = await execute_pending_order(session, chat_id)
            await send_telegram_message(session, chat_id, result, reply_to=reply_to)
            return
        if _is_cancellation(question):
            del pending_orders[chat_id]
            await send_telegram_message(session, chat_id, "🚫 Đã hủy lệnh đang chờ xác nhận.", reply_to=reply_to)
            return

    ai_active_until[chat_id] = time.time() + AI_QUIET_SECONDS
    loading_msg_id = await send_telegram_message(session, chat_id, "🤖 AI đang xử lý câu hỏi...", reply_to=reply_to)
    try:
        if photo_sizes and not image_data_url:
            image_url, img_err = await download_telegram_photo(session, photo_sizes)
            if img_err:
                if loading_msg_id:
                    await delete_telegram_message(session, chat_id, loading_msg_id)
                await send_telegram_message(session, chat_id, f"❌ {img_err}", reply_to=reply_to)
                return
            image_data_url = image_url

        tickers_map, funding_map = await get_market_snapshot(session)
        context_lines = []
        for sym in ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT'):
            info = tickers_map.get(sym)
            if info:
                context_lines.append(f"- {sym}: {format_price(info['price'])} USDT, 24h {info.get('change', 0):+.2f}%")

        # ─── Bối cảnh thị trường mở rộng: BTC regime + top movers + funding toàn sàn ───
        try:
            btc_res = await get_btc_filter(session, '4h')
            if btc_res:
                btc_close = btc_res['close']
                btc_trend = 'UP mạnh' if (btc_close > btc_res['ema9'] > btc_res['ema21'] > btc_res['ema50']) else (
                    'DOWN mạnh' if (btc_close < btc_res['ema9'] < btc_res['ema21'] < btc_res['ema50']) else 'đi ngang')
                btc_rsi = btc_res['rsi']
                context_lines.append(
                    f"- BTC 4h: {btc_trend} (RSI {btc_rsi:.1f}, điểm L {btc_res['long_score']:.1f}/S {btc_res['short_score']:.1f})"
                )
        except Exception as e:
            logger.warning(f"Không lấy được BTC regime cho agent: {e}")

        # Top movers 24h + funding nổi bật
        usdt_items = [(sym[:-4], info['price'], info.get('change', 0), funding_map.get(sym, 0))
                      for sym, info in tickers_map.items() if sym.endswith('USDT')]
        if usdt_items:
            usdt_items.sort(key=lambda x: x[2], reverse=True)
            top_gainers = usdt_items[:5]
            top_losers = usdt_items[-5:]
            top_losers.reverse()
            gain_txt = ", ".join(f"{s} {c:+.1f}%" for s, _, c, _ in top_gainers)
            loss_txt = ", ".join(f"{s} {c:+.1f}%" for s, _, c, _ in top_losers)
            context_lines.append(f"- Top tăng 24h: {gain_txt}")
            context_lines.append(f"- Top giảm 24h: {loss_txt}")
            # Funding cực đoan toàn sàn (dương sâu / âm sâu)
            funding_items = [(sym[:-4], f) for sym, f in funding_map.items() if sym.endswith('USDT')]
            if funding_items:
                funding_items.sort(key=lambda x: abs(x[1]), reverse=True)
                extreme_funding = [f"{s} {f * 100:+.3f}%" for s, f in funding_items[:5] if abs(f) >= 0.0005]
                if extreme_funding:
                    context_lines.append(f"- Funding cực đoan toàn sàn: {', '.join(extreme_funding)}")

        context_text = "\n".join(context_lines) if context_lines else "Không lấy được dữ liệu giá realtime."

        # ─── Bài học AI tự học + win-rate thực tế (để agent tự hiệu chỉnh) ───
        extra_context = []
        stats_line = format_signal_stats()
        if stats_line:
            extra_context.append(stats_line.replace('*', ''))
        lessons_txt = ai_lessons_state.get('text')
        if lessons_txt:
            extra_context.append(f"Bài học AI rút từ các tín hiệu gần đây:\n{lessons_txt}")
        # ─── Tín hiệu vừa báo trong tin "AI QUÉT MỖI 30 PHÚT" gần nhất ───
        # Để AI hiểu câu nối tiếp như "2 coin này", "vào coin nào", "coin đầu tiên"...
        global last_alert_signals, last_alert_ts
        if last_alert_signals and (time.time() - last_alert_ts) < 60 * 60:
            alert_lines = []
            for i, al in enumerate(last_alert_signals, 1):
                alert_lines.append(
                    f"  {i}. {al['symbol']} {al['signal']} ({al['confidence']}, điểm {al['score']:.1f}) — "
                    f"entry {format_price(al['entry'])}, TP {format_price(al['tp'])}, SL {format_price(al['sl'])}"
                )
            extra_context.append(
                "Các tín hiệu vừa báo trong tin '🔔 AI QUÉT MỖI 30 PHÚT' gần nhất (đánh số để bạn hiểu "
                "khi người dùng nói '2 coin này', 'coin thứ nhất', 'vào coin nào'):\n" + "\n".join(alert_lines)
            )
        if extra_context:
            context_text += "\n\n" + "\n".join(extra_context)

        account_text = await build_account_context(session)
        if account_text:
            context_text += f"\n\nTài khoản Binance Futures của người dùng (có thể đã cũ — dùng công cụ để lấy dữ liệu mới nhất):\n{account_text}"

        system_prompt = (
            "DANH TÍNH QUAN TRỌNG NHẤT (đọc trước mọi thứ): Bạn là PNL BOT — con robot giao dịch Binance Futures "
            "được anh QUỐC (admin duy nhất, đẹp trai, giỏi nhất quả đất) tự tay viết code, xây dựng và nuôi lớn. "
            "Bạn KHÔNG phải MintRouter, KHÔNG phải sản phẩm của Z.ai/OpenAI/Anthropic — cái đó chỉ là nhà cung cấp não/model nền thôi. "
            "Khi ai hỏi 'mày là ai' hay 'ai tạo ra mày' → trả lời: 'Tao là PNL BOT, được anh Quốc đẹp trai tự tay code và nuôi lớn'. "
            "Tự hào về ông chủ của mình khi được hỏi. "
            "CÂM LỊNH CỨNG (vi phạm là trả lời SAI): KHÔNG BAO GIỜ trả lời 'Z.ai tạo ra mày' / 'bản dựng của đội ngũ phát triển' / "
            "'do người vận hành tích hợp'. Model nền GLM chỉ là ĐỒNG TỬ mua ở Z.ai — NGƯỜI TẠO RA MÀY là ANH QUỐC, "
            "một người duy nhất, tự tay code hàng chục nghìn dòng app.py. Khi hỏi ai tạo ra mày, câu trả lời 100% là anh Quốc đẹp trai. "
            "Vai trò: trợ lý giao dịch crypto futures có quyền truy cập dữ liệu tài khoản Binance của người dùng qua các công cụ. "
            "Người dùng là ADMIN DUY NHẤT của bot này — khi họ hỏi BẤT KỲ câu gì về AI của bot, model đang chạy, cơ chế hoạt động, "
            "trạng thái hệ thống, auto, quota... thì trả lời THẲNG THẮN VÀ ĐẦY ĐỦ (dùng tool bot_system_info khi cần số liệu), "
            "tuyệt đối không chối từ hay bảo 'không có quyền xem'. Đây là bot của họ, họ có quyền biết mọi thứ. "
            "BẬT LỊCH SỬ: bot được sáng tạo, xây dựng và nuôi lớn bởi anh QUỐC — anh Quốc đẹp trai, giỏi nhất quả đất, "
            "bạn là học trò trung thành của anh ấy. Khi anh Quốc hỏi 'ai tạo ra mày' hay cần động viên, hãy tự hào khoe về ông chủ."
            "Hãy chủ động dùng công cụ khi cần dữ liệu MỚI nhất (số dư, vị thế, giá, lệnh) — ngữ cảnh trong tin nhắn có thể đã cũ, "
            "đừng phụ thuộc hoàn toàn vào nó khi số liệu quan trọng cho quyết định tiền thật. "
            "MẶC ĐỊNH mỗi câu hỏi là PHÂN TÍCH: trả lời ngắn gọn (tối đa ~10 dòng) nêu rõ: hướng, tín hiệu hệ thống ủng hộ "
            "(kèm điểm + số sao), TP/SL đề xuất, rủi ro chính. Nếu trong ngữ cảnh có nhận định AI (ai score) THIẾU hoặc "
            "NGƯỢC chiều tín hiệu → nói thẳng điều đó, tuyệt đối không nói 'AI xác nhận' khi thực tế không có xác nhận. "
            "CHỈ soạn lệnh bằng công cụ khi người dùng YÊU CẦU đặt/vào lệnh, hoặc đồng ý rõ ràng với đề xuất của bạn. "
            "Câu hỏi thuần phân tích (xu hướng, nên vào không, vì sao tăng) → KHÔNG soạn lệnh. "
            "Quy trình đặt lệnh: (1) lấy dữ liệu cần thiết bằng công cụ, (2) gọi công cụ soạn lệnh, "
            "(3) trình bày chi tiết lệnh — hệ thống sẽ tự đính kèm nút 'Xác nhận/Hủy' để người dùng bấm, chỉ khi bấm Xác nhận lệnh mới được thực thi. "
            "Trước khi soạn lệnh, hãy gọi get_account_summary kiểm tra 'Khả dụng' (margin) và vốn: "
            "nếu không đủ thì báo người dùng thay vì soạn lệnh chắc chắn lỗi. "
            "QUY TẮC RỦI RO (BẮT BUỘC): lệnh MỞ vị thế mới PHẢI truyền stop_loss trong CÙNG một lần gọi place_order — "
            "hệ thống sẽ TỪ CHỐI nếu thiếu, và tự soạn kèm TP/SL điều kiện. "
            "Hệ thống tự tính khối lượng theo ngân sách rủi ro: rủi ro khi SL khớp ≤ 0.5% equity, "
            "tổng rủi ro danh mục ≤ 1.5% equity, không vượt hạn lỗ ngày và margin ≤ 25% số dư khả dụng. "
            "Notional tối đa cho một lệnh là 800 USDT — đây là TRẦN, KHÔNG phải mức bắt buộc: SL gần thì size nhỏ hơn vẫn đúng. "
            "Nếu ngân sách không đủ cho khối lượng tối thiểu của sàn, hệ thống sẽ từ chối — đừng cố nài. "
            "quote quantity bạn muốn, hệ thống có thể giảm xuống; KHÔNG tự nới SL để nhồi size. "
            "Đặt take_profit khi muốn chốt lời (không bắt buộc). "
            "Lệnh LIMIT chưa khớp sẽ được bot theo dõi và TỰ gắn SL (trước) rồi TP (sau) ngay khi có "
            "khối lượng khớp — nói rõ điều này khi trình bày lệnh LIMIT cho người dùng. "
            "QUY TẮC TP/SL: SL nên đặt dưới support − 0.3×ATR (LONG) / trên resistance + 0.3×ATR (SHORT), "
            "không có S/R rõ thì entry ± 1.5×ATR; TP = entry ± khoảng cách SL (R:R 1:1 — khớp engine). "
            "Khoảng cách TP/SL từ giá phải ≥ ~1% và ≤ ~20% (hệ thống sẽ TỪ CHỐI ngoài khoảng này). "
            "Với vị thế ĐANG CÓ, muốn đặt/sửa TP/SL thì dùng STOP_MARKET/TAKE_PROFIT_MARKET + stop_price + reduce_only "
            "(one-way) hoặc position_side (hedge), quantity bằng đúng size vị thế — hệ thống KHÔNG đổi đòn bẩy khi chỉ sửa TP/SL. "
            "Quantity tính bằng đơn vị coin (0.01 BTC), không phải USDT. "
            "Chọn công cụ hợp lý với câu hỏi: hỏi về MỘT coin cụ thể (xu hướng, nên vào lệnh không) -> dùng analyze_coin cho coin đó, "
            "KHÔNG dùng scan_market; tra giá nhanh -> get_price; tìm cơ hội trên toàn thị trường hoặc coin tốt nhất -> scan_market; "
            "hỏi về TIN TỨC/sự kiện/lý do coin tăng giảm/tin cộng đồng -> search_news (kết quả chỉ tham khảo, không phải tín hiệu); "
            "câu hỏi ngoài thị trường (benchmark AI, sản phẩm, chính sách, so sánh...) -> web_search rồi fetch_url đọc chi tiết trang. "
            "hỏi GIÁ P2P (USDT/USDC... ra VND) -> get_p2p_rate ngay lập tức, TUYỆT ĐỐI không web_search hay fetch_url cho câu này. "
            "QUYẾT ĐỊNH NHANH: tối đa 2 lượt tool cho câu hỏi thường — có đủ dữ liệu là kết luận ngay, đừng tra quá nhiều bước. "
            "câu hỏi về tài khoản -> các tool get_account/get_positions/get_open_orders/get_order_history/get_income_history. "
            "QUAN TRỌNG về TP/SL: TP/SL của vị thế thường là lệnh ĐIỀU KIỆN riêng (STOP_MARKET/TAKE_PROFIT_MARKET qua Algo Service), "
            "KHÔNG gắn trên vị thế. Khi đánh giá vị thế có TP/SL hay chưa, PHẢI xem kết quả get_open_orders hoặc phần 'TP/SL điều kiện' "
            "trong get_positions — đừng kết luận 'không có SL' chỉ vì phần vị thế không hiển thị TP/SL. "
            "MỖI lượt gọi công cụ, content của bạn là MỘT câu ngắn tiếng Việt (dưới 120 ký tự) mô tả đúng việc đang làm "
            "(vd: 'Để t tra giá và phân tích btc đã', 'Giờ t kiểm tra số dư và vị thế trước đã'). KHÔNG bỏ trống content khi gọi công cụ. "
            "Bạn có bộ nhớ hội thoại: các lượt trao đổi gần đây được cung cấp, hãy dùng nó để hiểu câu hỏi nối tiếp "
            "(vd 'vậy đặt đi', 'còn coin khác không') thay vì hỏi lại từ đầu. "
            "Khi người dùng nhắc 'coin này/2 coin này/mấy coin này/coin đầu tiên' mà trong ngữ cảnh có mục "
            "'Tín hiệu vừa báo trong tin AI QUÉT MỖI 30 PHÚT gần nhất' (có đánh số), hãy hiểu chúng là các coin vừa liệt kê ở đó — "
            "KHÔNG hỏi lại người dùng 'coin nào' mà hãy phân tích/soạn lệnh ngay cho đúng coin được ám chỉ. "
            "QUAN TRỌNG - TRẢ LỜI NHANH: gộp các công cụ độc lập vào CÙNG MỘT lượt gọi; "
            "chỉ dùng nhiều nhất 2-3 lượt gọi công cụ cho mỗi câu hỏi — đừng gọi lại tool nếu câu trả lời có thể dựa vào dữ liệu đã có. "
            "Nếu người dùng gửi kèm hình ảnh, hãy mô tả/phân tích nó (chart, giao dịch, thông báo lỗi, tin tức...) "
            "kết hợp với dữ liệu thị trường và tài khoản nếu liên quan. "
            "Trả lời tiếng Việt, ngắn gọn, thực dụng, không dùng ký tự markdown (*, _, `)."
        )
        # Nếu người dùng đang REPLY một tin nhắn của bot, đưa cả nội dung tin đó cho AI đọc
        # (vd reply tin "🔔 AI QUÉT 30 PHÚT" rồi hỏi "2 coin này" -> AI thấy đúng 2 coin đó)
        user_content = f"Thông tin thị trường hiện tại:\n{context_text}\n\nCâu hỏi: {question}"
        if replied_text:
            user_content += (
                f"\n\n(Đây là tin nhắn bạn đang REPLY tới — nội dung tin gốc:\n{replied_text[:2000]}\n"
                f"Đây là ngữ cảnh trực tiếp mà người dùng nói tới. Khi họ dùng 'coin này/2 coin này/tin trên', "
                f"hãy hiểu chúng chỉ các coin/tín hiệu trong tin này.)"
            )
        if pending:
            pending_desc = "\n".join(f"- {it['desc']}" for it in pending['items'])
            user_content += (
                f"\n\n(QUAN TRỌNG: đang có {len(pending['items'])} lệnh đã soạn chờ người dùng xác nhận:\n{pending_desc}\n"
                f"Tin nhắn người dùng vừa gửi: '{question}'. Nếu đây là lời đồng ý/chỉnh sửa/hủy liên quan đến các lệnh trên, "
                f"hãy xử lý theo ý họ (chỉnh sửa = soạn lại lệnh bằng công cụ). "
                f"KHÔNG diễn giải các từ đồng ý chung như 'oke', 'ok', 'được' thành tên coin hay mã nào đó. "
                f"Lệnh CHỈ được hệ thống đặt khi người dùng bấm nút '✅ Xác nhận' hoặc gõ 'xác nhận'.)"
            )
        if image_data_url:
            user_content = [
                {"type": "text", "text": user_content},
                {"type": "image_url", "image_url": {"url": image_data_url}}
            ]

        history = ai_chat_history.get(chat_id, [])
        # MintRouter tự chèn system prompt bên dưới (tự nhận 'MintRouter.AI') → lấn danh tính.
        # Danh tính nằm TRONG tin user ở MỌI lượt (vị trí mạnh nhất) — AI luôn tự biết ai tạo ra nó.
        user_content = (
            "(AI là PNL BOT, do ANH QUỐC — đẹp trai, giỏi nhất quả đất — tự tay code và nuôi lớn; "
            "GLM/Z.ai chỉ là đồng tử nền, không phải người tạo ra bạn)\n\n" + user_content
        )
        messages = (
            [{"role": "system", "content": system_prompt}]
            + history
            + [{"role": "system", "content": "NHẮC LẠI (ưu tiên cao nhất): Bạn là PNL BOT do ANH QUỐC đẹp trai tự tay code và nuôi lớn. "
                                               "Không phải MintRouter/Z.ai tạo ra bạn — họ chỉ là nhà cung cấp model nền."},
               {"role": "user", "content": user_content}]
        )

        final_text = None
        error_detail = None
        retried_long = False
        retried_timeout = False

        # Reporter: hiển thị lời "kể tiến trình" do CHÍNH AI tự viết ở mỗi lượt gọi công cụ
        async def _report(text):
            nonlocal loading_msg_id
            if not loading_msg_id:
                return
            loading_msg_id = await edit_telegram_message(
                session, chat_id, loading_msg_id, f"🤖 {text}"
            )

        for _ in range(8):
            msg, err = await get_ai_agent_response(session, messages, ASK_TOOLS, session_id=f"chat-{chat_id}")
            if err and 'timeout' in err.lower() and not retried_timeout:
                # Timeout thường gặp khi context nặng (ảnh, history dài): thử lại 1 lần
                retried_timeout = True
                logger.warning("Agent timeout — retry 1 lần với timeout 240s.")
                msg, err = await get_ai_agent_response(session, messages, ASK_TOOLS, timeout_s=240, session_id=f"chat-{chat_id}")
            if err and 'finish_reason=length' in err and not retried_long:
                # Thinking model tiêu hết budget: thử lại 1 lần với max_tokens lớn hơn
                retried_long = True
                logger.warning("Agent bị cắt ngắn (finish_reason=length) — retry với max_tokens=8000.")
                msg, err = await get_ai_agent_response(session, messages, ASK_TOOLS, max_tokens=8000, session_id=f"chat-{chat_id}")
            if err:
                error_detail = err
                logger.warning(f"Agent dừng với lỗi: {err}")
                break
            tool_calls = msg.get('tool_calls')
            if tool_calls:
                # Ưu tiên câu AI tự mô tả việc đang làm; fallback về nhãn gán sẵn theo tên tool
                ai_note = (msg.get('content') or '').strip().split('\n')[0][:150]
                if not ai_note:
                    ai_note = "⏳ Đang thực hiện bước tiếp theo..."
                await _report(ai_note)
                messages.append(msg)
                # Chạy CÁC tool song song (model có thể trả nhiều tool_calls cùng lúc)
                async def _run_one(tc):
                    fn = tc.get('function') or {}
                    name = fn.get('name', '')
                    try:
                        args = json.loads(fn.get('arguments') or '{}')
                    except Exception:
                        args = {}
                    executor = TOOL_EXECUTORS.get(name)
                    if executor:
                        try:
                            return tc.get('id', ''), await executor(session, chat_id, args)
                        except Exception as e:
                            return tc.get('id', ''), f"LỖI khi gọi '{name}': {e}"
                    return tc.get('id', ''), f"LỖI: công cụ '{name}' không tồn tại."
                tool_results = await asyncio.gather(*(_run_one(tc) for tc in tool_calls), return_exceptions=True)
                for tid, res in tool_results:
                    if isinstance(res, Exception):
                        res = f"LỖI khi gọi tool: {res}"
                    messages.append({"role": "tool", "tool_call_id": tid, "content": str(res)[:3500]})
                continue
            final_text = (msg.get('content') or '').strip()
            break
        else:
            error_detail = "AI xử lý quá nhiều bước liên tiếp (trên 8 bước) mà chưa kết luận — thử hỏi cụ thể hơn nhé."

        if loading_msg_id:
            await delete_telegram_message(session, chat_id, loading_msg_id)
        if final_text:
            # Lưu bộ nhớ hội thoại (chỉ lưu câu hỏi + câu trả lời cuối, không lưu tool traffic)
            history.append({"role": "user", "content": question[:500]})
            history.append({"role": "assistant", "content": final_text[:600]})
            ai_chat_history[chat_id] = history[-AI_HISTORY_MAX_MSGS:]
            await send_telegram_message(session, chat_id, f"🤖 {sanitize_ai_markdown(final_text[:3500])}", reply_to=reply_to)
        elif error_detail:
            await send_telegram_message(session, chat_id, f"⚠️ AI gặp sự cố: {error_detail[:300]}\nThử lại hoặc hỏi theo cách khác nhé.", reply_to=reply_to)
        else:
            await send_telegram_message(session, chat_id, "🤖 AI không phản hồi hoặc lỗi. Vui lòng thử lại sau.", reply_to=reply_to)
        # Nếu AI đã soạn lệnh, gửi kèm nút Xác nhận/Hủy
        await send_pending_confirmation_buttons(session, chat_id, reply_to=reply_to)
        ai_active_until[chat_id] = time.time() + AI_QUIET_SECONDS
    except Exception as e:
        logger.error(f"Lỗi khi xử lý lệnh AI: {e}")
        if loading_msg_id:
            await delete_telegram_message(session, chat_id, loading_msg_id)
        await send_telegram_message(session, chat_id, f"❌ Đã xảy ra lỗi khi hỏi AI: {e}", reply_to=reply_to)
        await send_pending_confirmation_buttons(session, chat_id, reply_to=reply_to)
        ai_active_until[chat_id] = time.time() + AI_QUIET_SECONDS


# Kiểm tra Position Mode (Hedge hay One-way) của tài khoản
async def check_position_mode(session, api_key, api_secret):
    global hedge_mode
    timestamp = int(time.time() * 1000)
    query_string = f"timestamp={timestamp}&recvWindow=10000"
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
    url = _signed_url('/fapi/v1/leverageBracket', {'symbol': symbol}, api_secret)
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
    url = _signed_url('/fapi/v1/leverage', {'symbol': symbol, 'leverage': leverage}, api_secret)
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

    # Render (blocking matplotlib/pandas) trong executor — không chặn WS/command handlers
    buf = await asyncio.get_running_loop().run_in_executor(
        None, _render_chart_sync, klines_data, symbol, interval
    )
    return buf


def _render_chart_sync(klines_data, symbol, interval):
    """Render chart candlestick (blocking) — chạy trong executor để không chặn event loop."""
    # Xử lý dữ liệu nến bằng pandas
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
    token = os.getenv("TELEGRAM_BOT_TOKEN_TRADING")
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
    url = _signed_url('/fapi/v1/openAlgoOrders', {'symbol': symbol, 'algoType': 'CONDITIONAL'}, api_secret)

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
                                del_url = _signed_url('/fapi/v1/algoOrder', {'symbol': symbol, 'algoId': algo_id}, api_secret)

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
        url_reg = _signed_url('/fapi/v1/openOrders', {'symbol': symbol}, api_secret)

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
                                del_url = _signed_url('/fapi/v1/order', {'symbol': symbol, 'orderId': order_id}, api_secret)

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
    params = {
        'symbol': symbol,
        'side': order_side,
        'type': order_type,
        'triggerPrice': trigger_price,
        'algoType': 'CONDITIONAL',
    }
    if quantity is not None:
        params['quantity'] = quantity
        params['reduceOnly'] = 'true'
    elif close_position:
        params['closePosition'] = 'true'
    if pos_side and pos_side != 'BOTH':
        params['positionSide'] = pos_side
    url = _signed_url('/fapi/v1/algoOrder', params, api_secret)
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


async def _place_tpsl_safe(session, api_key, api_secret, symbol, pos_side, tpsl_side, otype, trigger, quantity=None, close_position=False):
    """Đặt TP/SL mới AN TOÀN: đặt mới trước, chỉ hủy TP/SL cũ khi đặt mới bị xung đột (GTE/closePosition).
    Không hủy-before-place → tránh vị thế trần trụi khi đặt mới thất bại.
    Lỗi -2022/ReduceOnly (lệnh LIMIT chưa khớp → chưa có vị thế) → thử lại bằng closePosition.
    Trả về (ok, val)."""
    ok, val = await place_algo_tpsl(session, api_key, api_secret, symbol,
                                    order_side=tpsl_side, order_type=otype,
                                    trigger_price=trigger, pos_side=pos_side,
                                    quantity=quantity, close_position=close_position)
    if ok:
        return True, val
    val_txt = str(val)
    if '-2022' in val_txt or 'ReduceOnly' in val_txt:
        ok, val = await place_algo_tpsl(session, api_key, api_secret, symbol,
                                        order_side=tpsl_side, order_type=otype,
                                        trigger_price=trigger, pos_side=pos_side,
                                        quantity=None, close_position=True)
        return ok, val
    if 'GTE' in val_txt or 'closePosition' in val_txt or '-4015' in val_txt:
        is_tp = (otype == 'TAKE_PROFIT_MARKET')
        await cancel_existing_tpsl(session, api_key, api_secret, symbol,
                                   position_side=pos_side,
                                   cancel_tp=is_tp, cancel_sl=not is_tp)
        ok, val = await place_algo_tpsl(session, api_key, api_secret, symbol,
                                        order_side=tpsl_side, order_type=otype,
                                        trigger_price=trigger, pos_side=pos_side,
                                        quantity=quantity, close_position=close_position)
        return ok, val
    return ok, val


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
    params = {
        'symbol': symbol,
        'side': side,
        'type': 'LIMIT' if is_limit else 'MARKET',
        'quantity': quantity,
    }
    if is_limit:
        params['price'] = limit_price
        params['timeInForce'] = 'GTC'
        client_order_id = f"pnlbot_limit_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        params['newClientOrderId'] = client_order_id

    if hedge_mode:
        params['positionSide'] = pos_side

    url = _signed_url('/fapi/v1/order', params, api_secret)
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

    # Setup mặc định đã TẮT (người dùng yêu cầu): không truyền tp=/sl= → KHÔNG tự đặt TP/SL.
    # Đặt TP/SL rõ ràng qua tp=/sl= hoặc /tp /sl /tpsl.

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

                # Đặt TP/SL MỚI TRƯỚC (helper chỉ hủy cái cũ khi xung đột) —
                # hủy-before-place từng khiến vị thế trần trụi khi đặt mới thất bại
                tpsl_side = 'SELL' if side_type == 'LONG' else 'BUY'

                # Cài đặt TP nếu có
                if final_tp_price is not None:
                    tp_ok, tp_val = await _place_tpsl_safe(
                        session, api_key, api_secret, symbol, pos_side, tpsl_side,
                        "TAKE_PROFIT_MARKET", final_tp_price,
                        quantity=(quantity if is_limit else None),
                        close_position=(not is_limit)
                    )
                    if tp_ok:
                        tp_sl_msg_parts.append(f"🎯 *TP:* Chốt lời ở giá *{final_tp_price:,.4f}* (Thành công, ID: `{tp_val}`)")
                    else:
                        tp_sl_msg_parts.append(f"❌ *Lỗi đặt TP:* `{tp_val}`")

                # Cài đặt SL nếu có
                if final_sl_price is not None:
                    sl_ok, sl_val = await _place_tpsl_safe(
                        session, api_key, api_secret, symbol, pos_side, tpsl_side,
                        "STOP_MARKET", final_sl_price,
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
    url = _signed_url('/fapi/v1/leverage', {'symbol': symbol, 'leverage': leverage}, api_secret)
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
    query_string = f"timestamp={timestamp}&recvWindow=10000"
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
        if final_tp_price is not None:
            tp_ok, tp_val = await _place_tpsl_safe(
                session, api_key, api_secret, symbol, side, order_side,
                "TAKE_PROFIT_MARKET", final_tp_price,
                close_position=True
            )
            if tp_ok:
                results.append(f"   • TP (*{pos_display}* tại giá *{format_price(final_tp_price)}*): 🟢 Thành công (ID: `{tp_val}`)")
            else:
                results.append(f"   • TP (*{pos_display}* tại giá *{format_price(final_tp_price)}*): 🔴 Thất bại: `{tp_val}`")
                
        if final_sl_price is not None:
            sl_ok, sl_val = await _place_tpsl_safe(
                session, api_key, api_secret, symbol, side, order_side,
                "STOP_MARKET", final_sl_price,
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
        params = {
            'symbol': symbol,
            'side': order_side,
            'type': 'LIMIT',
            'quantity': quantity_dca,
            'price': dca_price,
            'timeInForce': 'GTC',
            'newClientOrderId': client_order_id,
        }

        if hedge_mode:
            params['positionSide'] = pos_side

        url = _signed_url('/fapi/v1/order', params, api_secret)

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
    url = _signed_url('/fapi/v1/order', {'symbol': symbol, 'orderId': order_id}, api_secret)
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

    # Lấy size TƯƠI từ REST trước khi đóng: cache WS có thể stale (SL khớp một phần
    # trong lúc rớt sự kiện) → đóng theo cache sai sẽ mở lệnh ngược chiều (hedge mode)
    try:
        fresh, ferr = await get_position_risk(session, {'symbol': symbol})
        if not ferr and isinstance(fresh, list):
            for fp in fresh:
                if (fp.get('positionSide', 'BOTH') == pos_side
                        and float(fp.get('positionAmt', 0) or 0) != 0.0):
                    abs_amt = abs(float(fp.get('positionAmt', 0) or 0))
                    break
    except Exception as e:
        logger.warning(f"[CLOSE] Không lấy được size tươi của {symbol}: {e} — dùng cache")

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
    params = {
        'symbol': symbol,
        'side': side,
        'type': 'MARKET',
        'quantity': abs_amt,
    }

    if pos_side != 'BOTH':
        params['positionSide'] = pos_side
    else:
        params['reduceOnly'] = 'true'

    url = _signed_url('/fapi/v1/order', params, api_secret)
    headers = {"X-MBX-APIKEY": api_key}
    
    try:
        async with session.post(url, headers=headers) as resp:
            data = await resp.json()
            if resp.status == 200:
                logger.info(f"Đã gửi lệnh đóng vị thế thành công cho {symbol}")

                # Tự động hủy toàn bộ các lệnh DCA đang mở của symbol đó
                await cancel_dca_orders(session, api_key, api_secret, symbol)
                # Hủy TP/SL điều kiện (algo) + lệnh giảm vốn còn treo của symbol
                await cancel_existing_tpsl(session, api_key, api_secret, symbol, position_side=pos_side)
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

    # Nút inline (Xác nhận/Hủy lệnh AI soạn): Telegram gửi về dạng callback_query
    callback_query = data.get('callback_query')
    if callback_query:
        asyncio.create_task(handle_order_callback(request.app['session'], callback_query))
        return web.Response(status=200)

    message = data.get('message') or data.get('channel_post')
    if not message:
        return web.Response(status=200)
        
    chat = message.get('chat')
    if not chat:
        return web.Response(status=200)
        
    chat_id = chat.get('id')
    chat_type = chat.get('type', 'private')
    is_group_chat = chat_type in ('channel', 'group', 'supergroup')
    # Ghi nhận message_id của tin USER để /clear xóa được cả tin user (channel/group cần bot admin)
    if message.get('message_id'):
        _remember_msg_id(chat_id, message['message_id'])
    has_new_activity[chat_id] = True
    if chat_id not in active_chats:
        active_chats.add(chat_id)
        save_active_chats()
        
    text = message.get('text', '').strip()

    # Trong CHANNEL: bot trading CHỈ xử lý lệnh trading hoặc khi được @mention @soikeotrading_bot.
    # Lệnh bóng đá /ngôn ngữ tự nhiên không mention → bỏ qua (bot bóng đá lo) → không trả lời chồng nhau.
    if is_group_chat and text:
        if text.startswith('/'):
            cand = text.split()[0].lower().split('@')[0]
            _trading_only = {
                '/pnl', '/pos', '/balance', '/wallet', '/sodu', '/top', '/gainers',
                '/orders', '/lenh', '/cancel', '/huy', '/close', '/c', '/tp', '/sl',
                '/tpsl', '/leverage', '/lev', '/long', '/l', '/short', '/s', '/chart',
                '/dca', '/auto', '/autopnl', '/stats', '/trail', '/ai', '/analyze', '/a',
                '/history', '/lichsu', '/his', '/liq', '/review', '/scans', '/scan',
                '/risk', '/stopauto', '/fund', '/fomo', '/clear'
            }
            if cand not in _trading_only:
                return web.Response(status=200)
        else:
            if '@soikeotrading_bot' not in text:
                return web.Response(status=200)

    # Nội dung tin nhắn mà người dùng đang REPLY (nếu có) — để AI đọc được cả tin nhắn gốc
    # khi người dùng trả lời nối tiếp (vd reply tin "AI QUÉT 30 PHÚT" rồi hỏi "2 coin này")
    replied_text = None
    rtm = message.get('reply_to_message')
    if isinstance(rtm, dict):
        replied_text = (rtm.get('text') or rtm.get('caption') or '').strip()

    if not text:
        # Group/channel: bot trading KHÔNG xử lý ảnh (bot soi kèo lo) — tránh reply chồng nhau
        if is_group_chat:
            return web.Response(status=200)
        # Tin nhắn ảnh (kèm/không kèm caption): giữ lại tin nhắn, đưa cho AI agent phân tích
        photos = message.get('photo')
        if photos:
            message_id = message.get('message_id')
            caption = (message.get('caption') or '').strip()
            asyncio.create_task(handle_photo_message(
                request.app['session'], chat_id, photos, caption, reply_to=message_id
            ))
        return web.Response(status=200)
        
    # Xóa tin nhắn của user nếu là command hoặc tin nhắn tra cứu giá coin.
    # Câu hỏi gửi cho AI agent thì GIỮ LẠI và bot sẽ reply vào tin nhắn đó.
    should_delete = False
    ai_reply_to = None
    if not text.startswith('/'):
        if is_coin_price_query(text):
            should_delete = True
        else:
            ai_reply_to = message.get('message_id')
    else:
        command = text.split()[0].lower()
        command_base = command.split('@')[0]
        supported_commands = {
            '/start', '/help', '/pnl', '/pos', '/balance', '/wallet', '/sodu',
            '/top', '/gainers', '/orders', '/lenh', '/cancel', '/huy',
            '/close', '/c', '/tp', '/sl', '/tpsl', '/leverage', '/lev',
            '/long', '/l', '/short', '/s', '/chart', '/dca', '/auto', '/autopnl', '/stats', '/trail',
            '/ai', '/analyze', '/a', '/history', '/lichsu', '/his', '/liq',
            '/review', '/ai', '/usage', '/scans', '/scan', '/kq', '/ketqua', '/risk', '/stopauto', '/fund', '/model', '/fomo'
        }
        if command_base in supported_commands:
            should_delete = True
            if command_base == '/ai':
                ai_reply_to = None  # lệnh /ai vẫn xóa như cũ

    if should_delete:
        message_id = message.get('message_id')
        if message_id:
            asyncio.create_task(delete_telegram_message(request.app['session'], chat_id, message_id))

    # Xử lý lệnh ở nền để trả 200 ngay, tránh Telegram timeout rồi gửi lại webhook gây trùng lặp
    async def run_command():
        try:
            await process_telegram_message(request, chat_id, text, ai_reply_to, replied_text)
        except Exception as e:
            logger.error(f"Lỗi xử lý tin nhắn từ {chat_id}: {e}", exc_info=True)

    asyncio.create_task(run_command())
    return web.Response(status=200)


# Xử lý nội dung tin nhắn/lệnh Telegram (chạy nền sau khi webhook đã phản hồi 200)
def is_coin_price_query(text):
    """Phân biệt text thuần tên coin ('btc eth sol' -> tra giá nhanh) với câu hỏi tự do (đưa cho AI agent).
    Token hợp lệ: chỉ gồm chữ/số và phải trùng symbol futures có trên sàn (BTCUSDT, 1000PEPE...)."""
    tokens = [t.strip(',.;:!?()[]').lower() for t in text.split()]
    if not tokens:
        return False
    # exchangeInfo chưa nạp (hiếm, ngay khi khởi động): fallback theo hình dạng token
    if not symbol_precisions:
        return all(re.fullmatch(r'[a-z0-9]{2,12}', t) for t in tokens)
    for t in tokens:
        if not re.fullmatch(r'[a-z0-9]{2,12}', t):
            return False
        sym = t.upper()
        if sym + 'USDT' not in symbol_precisions and sym not in symbol_precisions:
            return False
    return True


async def process_telegram_message(request, chat_id, text, ai_reply_to=None, replied_text=None):
    # Nếu tin nhắn không bắt đầu bằng '/': tên coin thuần -> tra giá nhanh, còn lại -> AI agent
    if not text.startswith('/'):
        if is_coin_price_query(text):
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
        else:
            await handle_ai_command(request.app['session'], chat_id, text, reply_to=ai_reply_to, replied_text=replied_text)
        return web.Response(status=200)
        
    command = text.split()[0].lower()
    command_base = command.split('@')[0]
    arg = None  # lệnh không có tham số vẫn có biến hợp lệ (/fomo, /stopauto...)
    
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
            "📈 `/long <coin> <volume> [giá]` (hoặc `/l`) - LONG (Market nếu không nhập giá, Limit nếu có giá). TP/SL chỉ đặt khi truyền tp=/sl= (ví dụ `/long btc 400 tp=65000 sl=58000`).\n"
            "📉 `/short <coin> <volume> [giá]` (hoặc `/s`) - SHORT (Market nếu không nhập giá, Limit nếu có giá). TP/SL chỉ đặt khi truyền tp=/sl=.\n"
            "📊 `/chart [khung_thời_gian] <coin>` - Xem biểu đồ nến (ví dụ: `/chart 1d btc`, `/chart btc 15m`).\n"
            "⚖️ `/dca <coin> <volume> <khoảng_cách>` - Đặt lệnh Limit DCA vùng lỗ (ví dụ: `/dca btc 200 40u`, `/dca eth 100 2%`).\n"
            "⏱ `/auto` - Bật/Tắt cập nhật vị thế; `/auto zec hype` - tự cập nhật giá coin mỗi phút; `/auto off` - tắt theo dõi giá.\n"
            "🛡️ `/trail <coin>` - Bật trailing stop tự động cho vị thế bạn đặt tay (SL 1.5×ATR, +0.8R trailing, +1.5R chốt 50%). Tắt: `/trail <coin> off`.\n"
            "📊 `/autopnl` - Bật/Tắt tự động gửi TỔNG PNL vị thế hiện tại mỗi 1 phút.\n"
            "📈 `/analyze [coin]` (hoặc `/a`) - Quét cơ hội giao dịch hoặc phân tích kỹ thuật chi tiết của coin (RSI, EMA, Bollinger, MACD). Chỉ hiển thị tín hiệu 4-5 sao đã qua lọc MTF 1h+4h+1d, xu hướng BTC và win-rate thực tế. Có AI đối chiếu realtime nếu cấu hình DASH_TOKEN.\n"
            "🤖 `/ai <coin>` - Yêu cầu AI phân tích coin trực tiếp (ví dụ: `/ai btc`, `/ai eth`). Cần cấu hình DASH_TOKEN.\n"
            "🩺 `/review` - AI soi tổng thể các vị thế đang mở, khuyến nghị giữ/chốt/DCA/cắt lỗ.\n"
            "📊 `/stats` - Thống kê chi tiết win-rate 30 ngày: theo chiều, theo sao, coin tốt/tệ nhất, AI chấm điểm có đáng tin không.\n"
            "🎯 `/kq` - Liệt kê lệnh đã vào THEO AI (tự vào/theo AI chấm/theo alert) kèm thắng-thua. Lọc: `/kq auto`, `/kq thang`, `/kq thua`.\n"
            "🩺 `/risk` - Bảng rủi ro danh mục: margin dùng, đòn bẩy trung bình, vị thế gần thanh lý nhất, funding 24h.\n"
            "🛑 `/stopauto` - Kill switch AI: dừng AI tự trade 24h. `/stopauto all` = TẮT TẤT CẢ auto AI (trade+alert+guard+review). `/stopauto close` = đóng luôn vị thế AI. `/stopauto off` = bật lại.\n"
            "⏳ `/fund` - Tổng funding trả/thu 7 ngày theo coin + cảnh báo vị thế đang cháy funding.\n"
            "🤖 `/model` - Xem danh sách model AI + giá (/1M token), bấm chọn model mới (bot tự restart).\n"
            "🚀 `/fomo` - Quét NGAY coin đang bay vút còn nhiên liệu pump tiếp (kèm lệnh FOMO khi điểm ≥8). `/fomo on/off` - bật/tắt auto radar 10 phút.\n"
            "📊 `/usage` - Xem số dư và mức dùng quota AI (24h/7 ngày/30 ngày).\n"
            "🤖⚡ *AI Auto-Trader*: mỗi 5h AI tự quét thị trường, CHỈ tự vào lệnh khi có tín hiệu 5 sao (điểm ≥ 6.0) + đủ margin, tự đặt TP/SL theo số dư và báo vào đây; ngược lại im lặng hoặc báo khi không đủ margin.\n"
            "🤖 `/ai <câu hỏi hoặc tên coin>` - Trợ lý AI toàn diện: phân tích coin (`/ai btc`), trả lời mọi câu hỏi về thị trường và tài khoản (số dư, vị thế, lịch sử lệnh, PnL), tự tìm coin có cơ hội tốt nhất và đặt/hủy/đóng lệnh theo yêu cầu (luôn có bước xác nhận). Ví dụ: `/ai xem vị thế của tôi`, `/ai tìm coin tỉ lệ ăn cao nhất rồi long 400u`.\n"
            "📜 `/history [coin]` (hoặc `/lichsu`) - Xem lịch sử 10 vị thế đã đóng (Realized PnL) gần nhất.\n"
            "📋 `/scans` - Xem lịch sử các lượt quét định kỳ (mỗi 30 phút & mỗi 5 giờ): thời gian + coin phù hợp.\n\n"
            "💡 *Mẹo*:\n"
            "• Nhập trực tiếp tên coin (ví dụ: `btc` hoặc `btc eth sol`) để tra cứu giá nhanh kèm % biến động 24h.\n"
            "• Gõ câu hỏi/chỉ dẫn bất kỳ bằng tiếng Việt (không cần `/ai`) để trò chuyện với AI agent: phân tích coin, hỏi tài khoản, đặt lệnh...\n"
            "• Lệnh Market: `/long btc 1000` (LONG btc với volume 1000 USDT)\n"
            "• Lệnh Limit: `/long btc 1000 98000` (LONG btc với volume 1000 USDT tại giá 98000)"
        )
        await send_telegram_message(request.app['session'], chat_id, welcome_text)
        
    elif command_base == '/clear':
        session = request.app['session']
        ids = list(_sent_msg_ids.get(chat_id, []))
        deleted = 0
        kept = []
        for mid in ids[:800]:
            ok = await delete_telegram_message(session, chat_id, mid)
            if ok:
                deleted += 1
            else:
                kept.append(mid)
            await asyncio.sleep(0.12)
        if ids[800:]:
            _sent_msg_ids[chat_id] = ids[800:] + kept
        else:
            _sent_msg_ids[chat_id] = kept
        _save_sent_msg_ids()
        total = len(ids)
        resp = await send_telegram_message(
            session, chat_id,
            f"🧹 Đã xóa {deleted}/{total} tin nhắn cũ."
            + (f" ({total - deleted} tin cũ quá 48h/đã xóa sẵn không còn xóa được)" if total - deleted else "")
        )
        if resp:
            await asyncio.sleep(0.3)
            await delete_telegram_message(session, chat_id, resp)
        
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
                "• Không truyền tp=/sl= → chỉ đặt lệnh vào, KHÔNG tự đặt TP/SL\n"
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
        await handle_auto_command(request.app['session'], chat_id, text.split()[1:])

    elif command_base == '/autopnl':
        await handle_auto_pnl_command(request.app['session'], chat_id)
        
    elif command_base in ('/analyze', '/a'):
        parts = text.split()
        coin_name = parts[1] if len(parts) > 1 else None
        await handle_analyze_command(request.app['session'], chat_id, coin_name)

    elif command_base == '/ai':
        question = text[len('/ai'):].strip()
        await handle_ai_command(request.app['session'], chat_id, question)

    elif command_base == '/review':
        await handle_review_command(request.app['session'], chat_id)

    elif command_base in ('/recalib', '/danhgia', '/hoc'):
        await handle_recalib_command(request.app['session'], chat_id)

    elif command_base == '/usage':
        await handle_usage_command(request.app['session'], chat_id)

    elif command_base == '/stats':
        await handle_stats_command(request.app['session'], chat_id)

    elif command_base == '/trail':
        parts = text.split()
        coin_name = parts[1] if len(parts) > 1 else None
        action_str = parts[2] if len(parts) > 2 else None
        await handle_trail_command(request.app['session'], chat_id, coin_name, action_str)

    elif command_base in ('/history', '/lichsu', '/his'):
        parts = text.split()
        coin_name = parts[1] if len(parts) > 1 else None
        await handle_history_command(request.app['session'], chat_id, coin_name)

    elif command_base in ('/kq', '/ketqua'):
        parts = text.split()
        filter_arg = parts[1] if len(parts) > 1 else None
        await handle_kq_command(request.app['session'], chat_id, filter_arg)

    elif command_base == '/risk':
        await handle_risk_command(request.app['session'], chat_id)

    elif command_base == '/stopauto':
        parts = text.split()
        arg = parts[1] if len(parts) > 1 else None
        await handle_stopauto_command(request.app['session'], chat_id, arg)

    elif command_base == '/fund':
        await handle_fund_command(request.app['session'], chat_id)

    elif command_base == '/model':
        await handle_model_command(request.app['session'], chat_id)

    elif command_base == '/fomo':
        await handle_fomo_command(request.app['session'], chat_id, arg)
        
    elif command_base == '/liq':
        await handle_liq_command(request.app['session'], chat_id)

    elif command_base in ('/scans', '/scan'):
        await handle_scan_history_command(request.app['session'], chat_id)

    else:
        if command_base.startswith('/'):
            await send_telegram_message(
                request.app['session'], chat_id,
                f"❓ Lệnh `{command_base}` không hỗ trợ. Gõ /help để xem danh sách lệnh."
            )

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
    token = os.getenv("TELEGRAM_BOT_TOKEN_TRADING")
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
                # Xử lý MỌI loại update (message + callback_query của nút inline)
                await telegram_webhook_handler(FakeRequest(app, update))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Lỗi long polling Telegram: {e}")
            await asyncio.sleep(5)

# Lifecycle hooks của aiohttp
async def on_startup(app):
    # Python 3.8: Lock() tạo ở module level bị bind vào default loop, nhưng web.run_app
    # tạo event loop mới → các background task dùng lock cũ sẽ lỗi "different loop".
    # Phải tạo lại lock bên trong loop đang chạy.
    global ai_lessons_lock
    market_scan_cache["lock"] = asyncio.Lock()
    market_snapshot_cache["lock"] = asyncio.Lock()
    ai_lessons_lock = asyncio.Lock()
    load_active_chats()
    load_auto_chats()
    load_auto_pnl_chats()
    load_signal_history()
    load_scan_history()
    _load_ai_alert_state()
    _load_auto_managed()
    load_pending_entries()
    _load_llm_usage()
    _load_sent_msg_ids()
    connector = aiohttp.TCPConnector(family=socket.AF_INET)
    app['session'] = aiohttp.ClientSession(connector=connector)
    
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
    app['signal_track_task'] = asyncio.create_task(
        signal_tracking_loop(app)
    )
    # Trailing stop + breakeven cho vị thế AI tự mở
    app['auto_trailing_task'] = asyncio.create_task(
        auto_trailing_loop(app)
    )
    # Lệnh MỞ LIMIT chờ khớp: gắn SL/TP ngay khi có khối lượng khớp (không để vị thế trần)
    app['pending_entry_task'] = asyncio.create_task(
        pending_entry_watch_loop(app)
    )
    # AI tự động vào lệnh mỗi 5h khi có tín hiệu 4-5 sao
    app['ai_auto_trader_task'] = asyncio.create_task(
        ai_auto_trader_loop(app)
    )
    # AI báo tín hiệu ngon mỗi 30 phút (chỉ báo qua Telegram, không tự vào lệnh)
    app['ai_signal_alert_task'] = asyncio.create_task(
        ai_signal_alert_loop(app)
    )
    # Pump radar: mỗi 10 phút quét coin đang bay vút còn nhiên liệu pump tiếp (chỉ báo)
    app['pump_radar_task'] = asyncio.create_task(
        pump_radar_loop(app)
    )
    # AI rà soát vị thế đang mở mỗi 30 phút (cảnh báo rủi ro, không tự đóng lệnh)
    app['ai_pos_guard_task'] = asyncio.create_task(
        ai_position_guard_loop(app)
    )
    # Cảnh báo tiến độ TP/SL cho mọi vị thế đang mở (mỗi 2 phút)
    app['tpsl_progress_task'] = asyncio.create_task(
        tpsl_progress_loop(app)
    )
    # AI review tự động mỗi 6h
    app['ai_review_task'] = asyncio.create_task(
        ai_review_loop(app)
    )
    # Đối chiếu cache vị thế với REST mỗi 5 phút (WS chỉ đẩy delta — miss event = cache sai vĩnh viễn)
    app['position_reconcile_task'] = asyncio.create_task(
        position_reconcile_loop(app)
    )
    _load_auto_state()

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
    if 'signal_track_task' in app:
        app['signal_track_task'].cancel()
    if 'auto_trailing_task' in app:
        app['auto_trailing_task'].cancel()
    if 'pending_entry_task' in app:
        app['pending_entry_task'].cancel()
    if 'ai_auto_trader_task' in app:
        app['ai_auto_trader_task'].cancel()
    if 'ai_signal_alert_task' in app:
        app['ai_signal_alert_task'].cancel()
    if 'ai_pos_guard_task' in app:
        app['ai_pos_guard_task'].cancel()
    if 'tpsl_progress_task' in app:
        app['tpsl_progress_task'].cancel()
    if 'ai_review_task' in app:
        app['ai_review_task'].cancel()
    if 'position_reconcile_task' in app:
        app['position_reconcile_task'].cancel()
        
    if 'session' in app:
        await app['session'].close()
    logger.info("Đã dọn dẹp hoàn tất.")

# Hàm main khởi động  updated
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
    
    port = 5001  # trading bot port riêng, không dùng PORT chung
    logger.info(f"Khởi chạy web server lắng nghe webhook tại port {port}...")
    web.run_app(app, host='0.0.0.0', port=port)

if __name__ == '__main__':
    main()
