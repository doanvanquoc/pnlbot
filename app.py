# PNL FOOTBALL BOT — bot dự đoán kèo bóng đá cho anh Quốc (đẹp trai, giỏi nhất quả đất)
# Thay thế hoàn toàn bot trading cũ. AI chạy qua MintRouter (GLM 5.3...).
# Dữ liệu: API-Football v3 (free 100 request/ngày) — nhiều đầu vào = nhiều lượt AI.
#
# Tính năng:
#   /lịch [dd/MM]     — lịch trận hôm nay (hoặc ngày khác) các giải theo dõi
#   /kèo <tên đội>    — phân tích 1 trận: AI dự đoán kèo + so odds nhà cái tìm value bet
#   /kq [n ngày]      — độ chính xác dự đoán: thắng/thua, hit-rate, EV thực
#   /usage            — quota MintRouter
#   /model            — đổi model AI (bấm chọn, tự restart)
#   Loop tự động: 07:30 VN báo dự đoán trận hôm nay; mỗi giờ chấm kết quả trận đã dự đoán

import os
import re
import json
import time
import hmac
import hashlib
import asyncio
import logging
import random
import aiohttp
import yarl
from aiohttp import web
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('bot')

MINTROUTER_BASE_URL = "https://api.mintrouter.ai/v1"
FOOTBALL_BASE = "https://v3.football.api-sports.io"
TZ_VN = timezone(timedelta(hours=7))
BOT_START_TS = time.time()

# ─── Giải theo dõi (API-Football league ids) — thêm/bớt qua env LEAGUE_IDS ───
DEFAULT_LEAGUES = "39,140,135,78,61,2,3,342"  # EPL, LaLiga, Serie A, Bundesliga, Ligue1, UCL, UEL, VLeague
LEAGUE_NAMES = {
    '39': 'Premier League', '140': 'La Liga', '135': 'Serie A', '78': 'Bundesliga',
    '61': 'Ligue 1', '2': 'Champions League', '3': 'Europa League',
    '342': 'V.League 1', '1': 'World Cup', '4': 'Euro', '253': 'MLS',
}
TRACKED_LEAGUES = set(filter(None, os.getenv('LEAGUE_IDS', DEFAULT_LEAGUES).split(',')))

# ─── Ngân sách request API-Football (free 100/ngày) ───
FB_QUOTA_FILE = "football_quota.json"
fb_quota = {'day': '', 'used': 0}
FB_DAILY_LIMIT = 95  # để dư chút


def _load_fb_quota():
    global fb_quota
    try:
        if os.path.exists(FB_QUOTA_FILE):
            with open(FB_QUOTA_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            today = datetime.now(TZ_VN).strftime('%Y-%m-%d')
            if d.get('day') == today:
                fb_quota = {'day': today, 'used': int(d.get('used', 0))}
    except Exception as e:
        logger.error(f"Lỗi nạp football quota: {e}")


def _save_fb_quota():
    try:
        with open(FB_QUOTA_FILE, "w", encoding="utf-8") as f:
            json.dump(fb_quota, f)
    except Exception as e:
        logger.error(f"Lỗi lưu football quota: {e}")


# ─── Dự đoán đã lưu: fixture_id -> bản ghi ───
PREDICTIONS_FILE = "predictions.json"
predictions = {}          # fixture_id(str) -> {...}
LLM_USAGE_FILE = "llm_usage.json"
llm_usage = {}            # 'YYYY-MM-DD' UTC -> {model: {calls, in, out}}
_llm_usage_last_ts = 0.0


def _load_predictions():
    global predictions
    try:
        if os.path.exists(PREDICTIONS_FILE):
            with open(PREDICTIONS_FILE, "r", encoding="utf-8") as f:
                predictions = json.load(f) or {}
    except Exception as e:
        logger.error(f"Lỗi nạp predictions: {e}")


def _save_predictions():
    try:
        with open(PREDICTIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(predictions, f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Lỗi lưu predictions: {e}")


def _load_llm_usage():
    global llm_usage
    try:
        if os.path.exists(LLM_USAGE_FILE):
            with open(LLM_USAGE_FILE, "r", encoding="utf-8") as f:
                llm_usage = json.load(f) or {}
    except Exception as e:
        logger.error(f"Lỗi nạp llm_usage: {e}")


def record_llm_usage(model, usage):
    global _llm_usage_last_ts
    day = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    u = llm_usage.setdefault(day, {}).setdefault(model or '?', {'calls': 0, 'in': 0, 'out': 0})
    u['calls'] += 1
    u['in'] += int((usage or {}).get('prompt_tokens', 0) or 0)
    u['out'] += int((usage or {}).get('completion_tokens', 0) or 0)
    _llm_usage_last_ts = time.time()
    try:
        with open(LLM_USAGE_FILE, "w", encoding="utf-8") as f:
            json.dump(llm_usage, f)
    except Exception:
        pass


# ═══════════════ TELEGRAM ═══════════════

_telegram_flood_until = 0.0


def _strip_md_chars(text):
    if not text:
        return text
    text = text.replace('**', '').replace('*', '').replace('`', '')
    text = re.sub(r'^#{1,6}\s*', '', text, flags=re.MULTILINE)
    return text


async def send_telegram_message(session, chat_id, text, reply_markup=None, reply_to=None):
    global _telegram_flood_until
    if time.time() < _telegram_flood_until:
        return None
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("Chưa cấu hình TELEGRAM_BOT_TOKEN.")
        return None
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text[:4096], "parse_mode": "Markdown"}
    if reply_markup:
        payload['reply_markup'] = json.dumps(reply_markup)
    if reply_to:
        payload['reply_to_message_id'] = reply_to
    for attempt in range(3):
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                data = await resp.json()
                if data.get('ok'):
                    return data
                err = data.get('description', '')
                if 'parse' in err.lower() or "can't parse" in err.lower():
                    payload['text'] = _strip_md_chars(text)[:4096]
                    payload.pop('parse_mode', None)
                    continue
                if 'flood' in err.lower() or 'too many' in err.lower():
                    wait = 30
                    m = re.search(r'after (\d+)', err)
                    if m:
                        wait = min(int(m.group(1)) + 2, 120)
                    _telegram_flood_until = time.time() + wait
                    await asyncio.sleep(wait)
                    continue
                if 'not found' in err.lower() or 'blocked' in err.lower():
                    return None
                logger.warning(f"Gửi tin Telegram lỗi: {err}")
                await asyncio.sleep(2)
        except Exception as e:
            logger.warning(f"Lỗi gửi tin Telegram: {e}")
            await asyncio.sleep(2)
    return None


async def send_long_message(session, chat_id, text, reply_markup=None):
    for i in range(0, len(text), 3900):
        chunk = text[i:i + 3900]
        await send_telegram_message(session, chat_id, chunk,
                                    reply_markup=reply_markup if i + 3900 >= len(text) else None)
        if i + 3900 < len(text):
            await asyncio.sleep(1)


# ═══════════════ AI (MintRouter) ═══════════════

async def get_ai_response(session, messages, max_tokens=2500, timeout_s=120):
    """Gọi LLM qua MintRouter chat completions. Trả về (text, None) hoặc (None, err)."""
    api_key = os.getenv("DASH_TOKEN")
    if not api_key:
        return None, "Chưa cấu hình DASH_TOKEN."
    model = os.getenv("DASH_MODEL", "glm-5.3")
    payload = {"model": model, "messages": messages, "temperature": 0.4, "max_tokens": max_tokens}
    for attempt in range(3):
        try:
            timeout = aiohttp.ClientTimeout(total=timeout_s)
            async with session.post(f"{MINTROUTER_BASE_URL}/chat/completions",
                                    json=payload,
                                    headers={"Authorization": f"Bearer {api_key}",
                                             "Content-Type": "application/json"},
                                    timeout=timeout) as resp:
                if resp.status in (502, 503, 504):
                    await asyncio.sleep(8 * (attempt + 1))  # MintRouter hít đám mây hồi phục
                    continue
                if resp.status == 429:
                    try:
                        err429 = (await resp.json(content_type=None)) or {}
                        wait = int((err429.get('error') or {}).get('reset_seconds') or 30)
                    except Exception:
                        wait = 30
                    logger.warning(f"AI rate limited — chờ {wait}s rồi thử lại")
                    await asyncio.sleep(min(wait + 2, 90))
                    continue
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"AI trả lỗi HTTP {resp.status}: {body[:200]}")
                    return None, f"HTTP {resp.status}: {body[:150]}"
                data = await resp.json()
                record_llm_usage(model, data.get('usage'))
                msg = data.get('choices', [{}])[0].get('message')
                if not msg:
                    return None, "AI trả về rỗng"
                return (msg.get('content') or '').strip() or None, None
        except asyncio.TimeoutError:
            return None, f"AI phản hồi quá lâu (timeout {timeout_s}s)"
        except Exception as e:
            if attempt == 2:
                return None, str(e)
            await asyncio.sleep(3)
    return None, "AI lỗi liên tiếp 3 lần (server MintRouter không hồi phục)"


async def get_ai_json(session, system_prompt, user_prompt, timeout_s=150):
    """Gọi AI yêu cầu trả về JSON. Trả về (dict, None) hoặc (None, err). Thử lại 1 lần nếu AI lấp văn xuôi."""
    base_messages = [
        {"role": "system", "content": system_prompt + "\nQUAN TRỌNG: trả về DUY NHẤT một khối JSON hợp lệ, không giải thích, không markdown code fence."},
        {"role": "user", "content": user_prompt},
    ]
    text, err = await get_ai_response(session, base_messages, max_tokens=1800, timeout_s=timeout_s)
    if not err and text:
        m = re.search(r'\{.*\}', text, re.S)
        if m:
            try:
                return json.loads(m.group(0)), None
            except Exception:
                pass
        # Lần 2: quát vào mặt nó
        retry_messages = base_messages + [
            {"role": "assistant", "content": (text or '')[:500]},
            {"role": "user", "content": "ĐỪNG viết bài phân tích dài dòng. Trả về DUY NHẤT JSON ĐÚNG schema đã yêu cầu trong system prompt (match, datetime, league, scores đủ 6 mục, picks, reasoning). Không thêm ngoài."},
        ]
        text, err = await get_ai_response(session, retry_messages, max_tokens=800, timeout_s=60)
        if err:
            return None, err
        m = re.search(r'\{.*\}', text, re.S)
        if not m:
            return None, f"AI không trả JSON: {text[:150]}"
        try:
            return json.loads(m.group(0)), None
        except Exception as e:
            return None, f"JSON lỗi: {e} — {m.group(0)[:150]}"
    if err:
        return None, err
    return None, "AI trả rỗng"


# ═══════════════ MINTROUTER QUOTA (/usage) ═══════════════

MINT_SESSION_FILE = "mint_session.json"
FRONT_OVERVIEW_CACHE = {'data': None, 'ts': 0.0, 'plan': None}
_front_last_login = {'ts': 0.0}
MODEL_PRICING_CACHE = {'data': None, 'ts': 0.0}


def _load_front_session():
    try:
        if os.path.exists(MINT_SESSION_FILE):
            with open(MINT_SESSION_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get('cookies') or None
    except Exception:
        return None
    return None


def _save_front_session(cookies, plan=None):
    try:
        with open(MINT_SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump({'cookies': cookies, 'plan': plan, 'ts': time.time()}, f)
        os.chmod(MINT_SESSION_FILE, 0o600)
    except Exception as e:
        logger.error(f"Lỗi lưu mint session: {e}")


async def _front_login(session):
    email = os.getenv("MINTROUTER_EMAIL")
    pwd = os.getenv("MINTROUTER_PASSWORD")
    if not email or not pwd:
        return None
    now = time.time()
    if now - _front_last_login['ts'] < 60:
        return None
    _front_last_login['ts'] = now
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with session.post("https://api.mintrouter.ai/v0/front/login",
                                json={"username": email, "password": pwd},
                                headers={"Origin": "https://mintrouter.ai",
                                         "Referer": "https://mintrouter.ai/login",
                                         "Content-Type": "application/json"},
                                timeout=timeout) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
            cookies = {k: v.value for k, v in resp.cookies.items()}
            if not cookies:
                return None
            _save_front_session(cookies, data.get('plan'))
            logger.info("[MINTROUTER] Đã login dashboard.")
            return cookies
    except Exception as e:
        logger.warning(f"Lỗi login MintRouter: {e}")
        return None


async def _front_get(session, path):
    """GET dashboard MintRouter với session, login lại khi 401. Trả về (data|None, err|None)."""
    cookies = _load_front_session()
    for attempt in (1, 2):
        if not cookies:
            cookies = await _front_login(session)
            if not cookies:
                return None, "chưa có session (thiếu MINTROUTER_EMAIL/PASSWORD trong .env)"
        cookie_hdr = "; ".join(f"{k}={v}" for k, v in cookies.items())
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with session.get(f"https://api.mintrouter.ai{path}", headers={
                "Cookie": cookie_hdr, "Origin": "https://mintrouter.ai",
                "Referer": "https://mintrouter.ai/dashboard", "Accept": "application/json",
            }, timeout=timeout) as resp:
                if resp.status == 401 and attempt == 1:
                    cookies = None
                    continue
                if resp.status != 200:
                    return None, f"HTTP {resp.status}"
                return await resp.json(content_type=None), None
        except Exception as e:
            return None, str(e)
    return None, "session hết hạn và không login lại được"


async def get_front_pass(session, force=False):
    now = time.time()
    cached = getattr(get_front_pass, '_cache', None)
    if (not force and cached and now - cached[0] < 300 and _llm_usage_last_ts <= cached[0]):
        return cached[1], None
    data, err = await _front_get(session, "/v0/front/pass")
    if data is not None and isinstance(data, dict):
        get_front_pass._cache = (time.time(), data)
        return data, None
    return None, (err or "lỗi không rõ")


async def get_front_overview(session, force=False):
    now = time.time()
    if (not force and FRONT_OVERVIEW_CACHE['data'] is not None
            and now - FRONT_OVERVIEW_CACHE['ts'] < 300):
        return FRONT_OVERVIEW_CACHE['data'], None
    data, err = await _front_get(session, "/v0/front/dashboard/overview")
    if data is not None and isinstance(data, dict):
        FRONT_OVERVIEW_CACHE['data'] = data
        FRONT_OVERVIEW_CACHE['ts'] = time.time()
        return data, None
    return None, (err or "lỗi không rõ")


def _fmt_remaining(iso_str):
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


# ═══════════════ /model ═══════════════

MINT_MODEL_PRICES = {
    'claude-fable-5-1': (2.90, 14.50), 'claude-fable-5': (2.90, 14.50),
    'gpt-6-astra': (2.90, 14.50), 'claude-opus-5': (1.45, 7.25),
    'claude-sonnet-5': (0.28, 1.39), 'claude-sonnet-4-6': (0.42, 2.08),
    'claude-haiku-4-5': (0.14, 0.69), 'gpt-5.6-luna': (0.028, 0.17),
    'gpt-5.6-sol': (0.56, 2.78), 'gpt-5.6-terra': (0.28, 1.67), 'gpt-5.5': (0.69, 4.17),
    'glm-5.3': (0.41, 1.28), 'glm-5.2': (0.41, 1.28),
    'gemini-3.8-flash': (0.16, 0.81), 'gemini-3.7-flash': (0.16, 0.81),
    'grok4.6': (0.43, 1.30), 'grok4.5': (0.43, 1.30),
    'kimi-k3': (0.87, 4.35), 'kimi-k2.7': (0.28, 1.16),
}
MINT_MODEL_ORDER = [
    'glm-5.3', 'claude-sonnet-5', 'claude-fable-5-1', 'gpt-5.6-luna', 'gpt-6-astra',
    'claude-opus-5', 'gpt-5.6-sol', 'gpt-5.6-terra', 'gemini-3.8-flash', 'grok4.6',
    'kimi-k3', 'claude-haiku-4-5', 'gpt-5.5', 'glm-5.2', 'gemini-3.7-flash',
    'grok4.5', 'kimi-k2.7', 'claude-fable-5', 'claude-sonnet-4-6',
]
MODEL_PAGE_SIZE = 8


async def fetch_available_models(session):
    api_key = os.getenv("DASH_TOKEN")
    if not api_key:
        return list(MINT_MODEL_ORDER)
    try:
        async with session.get("https://api.mintrouter.ai/v1/models",
                               headers={"Authorization": f"Bearer {api_key}"},
                               timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                ids = [m.get('id') for m in (data.get('data') or []) if m.get('id')]
                if ids:
                    return ids
    except Exception:
        pass
    return list(MINT_MODEL_ORDER)


def _set_dash_model_env(model_id):
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
        with open(env_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        for i, line in enumerate(lines):
            if line.startswith('DASH_MODEL='):
                lines[i] = f"DASH_MODEL={model_id}\n"
                break
        else:
            lines.append(f"DASH_MODEL={model_id}\n")
        with open(env_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        return True
    except Exception as e:
        logger.error(f"Lỗi ghi .env DASH_MODEL: {e}")
        return False


def _restart_bot_service():
    import subprocess
    for cmd in (['sudo', 'systemctl', 'restart', 'pnlbot'], ['systemctl', 'restart', 'pnlbot']):
        try:
            r = subprocess.run(cmd, capture_output=True, timeout=30)
            if r.returncode == 0:
                return True
        except Exception as e:
            logger.warning(f"Lỗi restart bot: {e}")
    return False


def _model_button_label(mid, current, live_prices):
    if live_prices and mid in live_prices:
        name = live_prices[mid][2] or mid
    else:
        name = mid
    return f"{name} ✅" if mid == current else name


def _build_model_sections(available, live_prices):
    raw_groups = MODEL_PRICING_CACHE.get('groups') or {}

    def prov_of(mid):
        prov = raw_groups.get(mid)
        if not prov:
            low = mid.lower()
            if low.startswith('claude'):
                prov = 'Claude'
            elif low.startswith(('gpt', 'o1', 'o3')):
                prov = 'OpenAI'
            elif low.startswith('gemini'):
                prov = 'Gemini'
            elif low.startswith('glm'):
                prov = 'GLM'
            elif low.startswith(('grok', 'xai')):
                prov = 'xAI'
            elif low.startswith('kimi'):
                prov = 'Kimi'
            else:
                prov = 'Khác'
        return prov
    sections = {}
    for mid in available:
        if 'free' in mid:
            continue
        sections.setdefault(prov_of(mid), []).append(mid)
    order = ['GLM', 'Claude', 'OpenAI', 'Gemini', 'MintRouter', 'xAI', 'Kimi', 'Khác']
    result = []
    for prov in sorted(sections.keys(), key=lambda p: (order.index(p) if p in order else 99, p)):
        mids = sections[prov]
        mids.sort(key=lambda m: (MINT_MODEL_ORDER.index(m) if m in MINT_MODEL_ORDER else 999, m))
        result.append((prov, mids))
    return result


def _render_model_page(current, pages, page, page_models, live_prices):
    lines = [f"🤖 *MODEL AI HIỆN TẠI: {current}*", f"→ Trang {page + 1}/{pages}:", ""]
    last = None
    for prov, m in page_models:
        if prov != last:
            lines.append(f"━━ {prov} ━━")
            last = prov
        name = (live_prices.get(m, (0, 0, m))[2] if live_prices and m in live_prices else m)
        pi = po = None
        if live_prices and m in live_prices:
            pi, po, _ = live_prices[m]
        elif m in MINT_MODEL_PRICES:
            pi, po = MINT_MODEL_PRICES[m]
        mark = "✅" if m == current else "•"
        lines.append(f"{mark} {name}" + (f" — ${pi:g}/${po:g}" if pi is not None else ""))
    return "\n".join(lines)


async def get_front_model_pricing(session):
    now = time.time()
    if (MODEL_PRICING_CACHE['data'] is not None and now - MODEL_PRICING_CACHE['ts'] < 1800):
        return MODEL_PRICING_CACHE['data']
    data, err = await _front_get_model_pricing(session)
    if data is None:
        return None
    items = data.get('per_token') or []
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


async def _front_get_model_pricing(session):
    cookies = _load_front_session()
    for attempt in (1, 2):
        if not cookies:
            cookies = await _front_login(session)
            if not cookies:
                return None
        cookie_hdr = "; ".join(f"{k}={v}" for k, v in cookies.items())
        try:
            async with session.get("https://api.mintrouter.ai/v0/front/models/pricing", headers={
                "Cookie": cookie_hdr, "Origin": "https://mintrouter.ai",
                "Referer": "https://mintrouter.ai/models", "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
            }, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 401 and attempt == 1:
                    cookies = None
                    continue
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None), None
        except Exception:
            if attempt == 1:
                cookies = None
                continue
            return None
    return None


async def handle_model_command(session, chat_id):
    available = await fetch_available_models(session)
    current = os.getenv("DASH_MODEL", "glm-5.3")
    live_prices = await get_front_model_pricing(session)
    sections = _build_model_sections(available, live_prices)
    flat = [(prov, m) for prov, mids in sections for m in mids]
    pages = max(1, -(-len(flat) // MODEL_PAGE_SIZE))
    chunk = flat[:MODEL_PAGE_SIZE]
    kb = [[{"text": _model_button_label(m, current, live_prices), "callback_data": f"setmodel:{m}"}]
          for _prov, m in chunk]
    kb.append([{"text": "➡️ Trang sau", "callback_data": "modelpage:1"}])
    await send_telegram_message(session, chat_id, _render_model_page(current, pages, 0, chunk, live_prices),
                                reply_markup={"inline_keyboard": kb})


async def handle_model_callback(session, chat_id, cb_data, message_id=None):
    action, _, arg = cb_data.partition(':')
    if action == 'modelpage':
        try:
            page = int(arg)
        except ValueError:
            return
        available = await fetch_available_models(session)
        current = os.getenv("DASH_MODEL", "glm-5.3")
        live_prices = await get_front_model_pricing(session)
        sections = _build_model_sections(available, live_prices)
        flat = [(prov, m) for prov, mids in sections for m in mids]
        pages = max(1, -(-len(flat) // MODEL_PAGE_SIZE))
        page = max(0, min(page, pages - 1))
        chunk = flat[page * MODEL_PAGE_SIZE:(page + 1) * MODEL_PAGE_SIZE]
        kb = [[{"text": _model_button_label(m, current, live_prices), "callback_data": f"setmodel:{m}"}]
              for _prov, m in chunk]
        nav = []
        if page > 0:
            nav.append({"text": "⬅️ Trước", "callback_data": f"modelpage:{page - 1}"})
        if page < pages - 1:
            nav.append({"text": "➡️ Sau", "callback_data": f"modelpage:{page + 1}"})
        if nav:
            kb.append(nav)
        text = _render_model_page(current, pages, page, chunk, live_prices)
        if message_id:
            token = os.getenv("TELEGRAM_BOT_TOKEN")
            try:
                async with session.post(f"https://api.telegram.org/bot{token}/editMessageText",
                                        json={"chat_id": chat_id, "message_id": message_id, "text": text,
                                              "parse_mode": "Markdown",
                                              "reply_markup": {"inline_keyboard": kb}},
                                        timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    d = await resp.json()
                    if not d.get('ok'):
                        await send_telegram_message(session, chat_id, text, reply_markup={"inline_keyboard": kb})
            except Exception:
                await send_telegram_message(session, chat_id, text, reply_markup={"inline_keyboard": kb})
        else:
            await send_telegram_message(session, chat_id, text, reply_markup={"inline_keyboard": kb})
        return
    if action == 'setmodel':
        model_id = arg.strip()
        if not model_id or 'free' in model_id:
            return
        ok = _set_dash_model_env(model_id)
        if not ok:
            await send_telegram_message(session, chat_id, "❌ Ghi .env thất bại — không đổi được model.")
            return
        await send_telegram_message(session, chat_id,
                                    f"🔄 *Đã đổi DASH_MODEL → {model_id}*\nĐang restart bot... (mở lại sau ~10 giây)")

        def _do_restart():
            _restart_bot_service()
        await asyncio.get_running_loop().run_in_executor(None, _do_restart)


# ═══════════════ FOOTBALL DATA (API-Football) ═══════════════

_fb_req_times = []  # timestamps các request gần đây (throttle 10 req/phút của free plan)


def _fb_throttle():
    """Chặn burst >9 req/phút — API-Football free plan giới hạn 10/phút."""
    now = time.time()
    while _fb_req_times and now - _fb_req_times[0] > 60:
        _fb_req_times.pop(0)
    if len(_fb_req_times) >= 9:
        wait = 61 - (now - _fb_req_times[0])
        return min(wait, 65)
    return 0


async def fb_get(session, path, params=None, budget=1):
    """GET API-Football, trừ quota theo ngày VN + throttle 9 req/phút. Trả về (data, None) hoặc (None, err)."""
    key = os.getenv("FOOTBALL_API_KEY")
    if not key:
        return None, "Chưa cấu hình FOOTBALL_API_KEY trong .env"
    today = datetime.now(TZ_VN).strftime('%Y-%m-%d')
    if fb_quota['day'] != today:
        fb_quota['day'] = today
        fb_quota['used'] = 0
    if fb_quota['used'] + budget > FB_DAILY_LIMIT:
        return None, f"Hết ngân sách API-Football hôm nay ({fb_quota['used']}/{FB_DAILY_LIMIT}) — dùng lại vào ngày mai hoặc gõ ít hơn."
    for attempt in range(3):
        wait = _fb_throttle()
        if wait:
            logger.info(f"[FB] Throttle: chờ {wait:.0f}s (9 req/phút)")
            await asyncio.sleep(wait)
        try:
            async with session.get(f"{FOOTBALL_BASE}{path}", params=params or {},
                                   headers={"x-apisports-key": key},
                                   timeout=aiohttp.ClientTimeout(total=30)) as resp:
                fb_quota['used'] += budget
                _fb_req_times.append(time.time())
                _save_fb_quota()
                if resp.status == 429:
                    backoff = 65 if attempt == 0 else 120
                    logger.warning(f"[FB] 429 rate limit — chờ {backoff}s thử lại")
                    await asyncio.sleep(backoff)
                    continue
                if resp.status != 200:
                    return None, f"HTTP {resp.status}"
                data = await resp.json(content_type=None)
                errors = data.get('errors')
                if errors and errors != 0:
                    return None, f"API lỗi: {json.dumps(errors)[:150]}"
                return data.get('response') or [], None
        except Exception as e:
            if attempt == 2:
                return None, str(e)
            await asyncio.sleep(5)
    return None, "API-Football rate limit dai dẳng"


def _vn_time(iso_str):
    try:
        ts = datetime.fromisoformat(iso_str.replace('Z', '+00:00')).astimezone(TZ_VN)
        return ts.strftime("%d/%m %H:%M")
    except Exception:
        return "?"


def _fixture_line(fx):
    """1 dòng mô tả trận: giờ VN + giải + teams + tỉ số nếu đã/đang đá."""
    league = fx.get('league', {})
    teams = fx.get('teams', {})
    goals = fx.get('goals', {})
    status = fx.get('fixture', {}).get('status', {}).get('short', '')
    score = f" — {goals.get('home')}-{goals.get('away')}" if status in ('FT', '1H', '2H', 'HT', 'LIVE') else ""
    return (f"• {_vn_time(fx['fixture']['date'])} [{league.get('name', '?')}] "
            f"{teams.get('home', {}).get('name', '?')} vs {teams.get('away', {}).get('name', '?')}{score}")


FIXTURES_CACHE = {}  # date_str -> {'fixtures': [...], 'ts': ...} — TTL 10 phút, đỡ gọi API lặp


async def get_fixtures_for_date(session, date_str, only_tracked=True, force=False):
    cached = FIXTURES_CACHE.get(date_str)
    if not force and cached and time.time() - cached['ts'] < 600:
        return cached['fixtures'], None
    data, err = await fb_get(session, "/fixtures", {'date': date_str})
    if err:
        return None, err
    fixtures = []
    for fx in data:
        lg = fx.get('league', {})
        if only_tracked and str(lg.get('id', '')) not in TRACKED_LEAGUES:
            continue
        fixtures.append(fx)
    fixtures.sort(key=lambda x: x['fixture']['date'])
    FIXTURES_CACHE[date_str] = {'fixtures': fixtures, 'ts': time.time()}
    return fixtures, None


async def get_match_odds(session, fixture_id):
    """Odds ĐẦY ĐỦ mọi market của nhà cái chính (Bet365) — trả về dict giữ nguyên tên market gốc:
    {'bookmaker': str, 'markets': {'Goals Over/Under': {'Over 2.5': 1.9, 'Under 2.5': 1.98}, ...}}
    Không sàng lọc market — AI tự chọn kèo hay nhất trong những gì nhà cái mở."""
    data, err = await fb_get(session, "/odds", {'fixture': fixture_id})
    if err or not data:
        return None
    for entry in data:
        for bm in entry.get('bookmakers', []):
            if bm.get('id') == 8:  # Bet365 — odds chuẩn
                markets = {}
                for bet in bm.get('bets', []):
                    vals = {}
                    for v in bet.get('values', []):
                        try:
                            o = float(v.get('odd', 0) or 0)
                        except Exception:
                            continue
                        if o > 1.01:
                            vals[v.get('value', '')] = o
                    if vals:
                        markets[bet.get('name', '?')] = vals
                if markets:
                    return {'bookmaker': bm.get('name', 'Bet365'), 'markets': markets}
    return None


async def get_h2h_summary(session, home_id, away_id, n=5):
    data, err = await fb_get(session, "/fixtures/headtohead", {'h2h': f"{home_id}-{away_id}", 'last': n})
    if err or not data:
        return ""
    lines = []
    for fx in data:
        goals = fx.get('goals', {})
        lg = fx.get('league', {})
        lines.append(f"{_vn_time(fx['fixture']['date'])} [{lg.get('name', '?')}] "
                     f"{fx['teams']['home']['name']} {goals.get('home')}-{goals.get('away')} {fx['teams']['away']['name']}")
    return "Gặp nhau gần đây: " + (" | ".join(lines) if lines else "chưa có") if lines else ""


async def get_api_prediction(session, fixture_id):
    """Prediction chính thức của API-Football (không phải AI của bot). Trả về text tóm tắt."""
    data, err = await fb_get(session, "/predictions", {'fixture': fixture_id})
    if err or not data:
        return ""
    p = (data[0] or {}).get('predictions', {})
    teams_p = (data[0] or {}).get('teams', {})
    adv = p.get('advice', '')
    win_pct = p.get('percent', {})
    under = (p.get('under_over') or '')
    txt = f"API-Football prediction: {adv} | percent: Home {win_pct.get('home')} Draw {win_pct.get('draw')} Away {win_pct.get('away')} | O/U: {under}"
    return txt


# ═══════════════ WEB TOOLS (AI tự tìm kiếm — Bing miễn phí) ═══════════════

def _decode_bing_href(href):
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
    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with session.get("https://www.bing.com/search",
                               params={'q': query, 'count': max_results},
                               headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
                                        "Accept-Language": "vi,en;q=0.8"},
                               timeout=timeout) as resp:
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


async def tool_web_search(session, query, max_results=8):
    """Tìm web (Bing, free). Trả về text: title + url + snippet."""
    results = await _web_search_bing(session, query, max_results)
    if not results:
        return f"Không tìm thấy kết quả web nào cho '{query}'."
    lines = [f"🔎 Kết quả web cho '{query}':"]
    for i, (title, href, snip) in enumerate(results, 1):
        lines.append(f"{i}. {title} — {href}" + (f"\n   {snip[:160]}" if snip else ""))
    return "\n".join(lines)


async def tool_fetch_url(session, url, max_chars=4000):
    """Đọc nội dung 1 trang web (text thô). Fallback Jina Reader khi trang JS/anti-bot."""
    if not url.startswith(('http://', 'https://')):
        return "LỖI: url không hợp lệ."
    raw, ctype = None, ''
    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with session.get(url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
            "Accept-Language": "vi,en;q=0.8"}) as resp:
            if resp.status == 200:
                ctype = resp.headers.get('Content-Type', '')
                if 'html' in ctype or 'text' in ctype or 'json' in ctype:
                    raw = await resp.text(errors='ignore')
    except Exception:
        raw = None
    need_jina = (raw is None or len(raw) < 800
                 or re.search(r'enable javascript|requires javascript|just a moment|human verification|captcha', (raw or '')[:3000], re.I))
    if need_jina:
        try:
            timeout = aiohttp.ClientTimeout(total=60)
            async with session.get(f"https://r.jina.ai/{url}", timeout=timeout) as resp:
                if resp.status == 200:
                    md = await resp.text(errors='ignore')
                    if len(md) > 200 and 'Human Verification' not in md[:500]:
                        raw, ctype = md, 'text/markdown'
        except Exception:
            pass
    if raw is None:
        return f"Không đọc được trang ({url}) — bị chặn hoặc lỗi mạng."
    if ctype and 'markdown' not in ctype:
        raw = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', raw, flags=re.S | re.I)
        raw = re.sub(r'<[^>]+>', ' ', raw)
        raw = raw.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        raw = re.sub(r'\s+', ' ', raw).strip()
    if not raw:
        return "Trang rỗng hoặc chỉ toàn script."
    return raw[:max_chars]


# ═══════════════ FRAMEWORK PHÂN TÍCH CHUẨN (AI bắt buộc chấm từng mục) ═══════════════
KEO_FRAMEWORK = """BẮT BUỘC chấm điểm 6 yếu tố sau (tối đa 5 điểm mỗi yếu tố, ghi rõ lý do 1 dòng):

1. PHONG ĐỘ (5đ): kết quả 5 trận gần nhất, riêng phong độ sân nhà/khách, số bàn ghi/thủng trung bình mỗi trận.
2. ĐỐI ĐẦU (5đ): 3-5 lần gặp gần nhất, đang có sự thống trị nào không, tổng bàn trung bình các cuộc đối đầu (cho kèo tài/xỉu).
3. ĐỘNG LỰC (5đ): trận này có ý nghĩa gì — đua vô địch/top 4/trụ hạng/cúp; đội có giữ sức cho trận lớn khác; derby; đội hết động lực cuối mùa.
4. LỰC LƯỢNG (5đ): chấn thương/treo giò cầu thủ chủ chốt, xoay vòng đội hình, chuyển nhượng mới (chỉ dùng dữ liệu web tin cậy, KHÔNG bịa).
5. LỐI CHƠI & THỐNG KÊ (5đ): phong cách (pressing/để bóng/phản công), xG gần đây nếu biết, kiểm soát bóng, tần suất ghi/thủng bàn.
   - KÈO THẺ: ước lượng thẻ TB/trận của 2 đội (trọng tài nào bắt, derby hay không, đội nào hay phạm lỗi/rút đè), thẻ đỏ TB.
   - KÈO GÓC: góc TB/trận của 2 đội (biên lấn cánh, tạt nhiều hay cầm bóng trung lộ), góc ở sân nhà/khách.
6. BỐI CẢNH (5đ): sân nhà/khách, lịch thi đấu dày (đá giữa tuần), thời tiết, trọng tài cụ thể (tên + phong cách rút thẻ), VAR.
Sau đó:
- Ước lượng XÁC SUẤT THẬT (%) cho kèo định chọn dựa trên tổng điểm (tổng ≥ 21/30 mới cho xác suất >60%; 18-20/30 cho 55-60%; <18/30 → KHÔNG nên chọn kèo, trả not_found hoặc chọn kèo phòng thủ khác).
- SO SÁNH odds nhà cái: xác suất ngụ ý của odds = 1/odds. Value chỉ khi xác suất thật của mày cao hơn xác suất ngụ ý ≥ 3 điểm %.
- CHỈ chọn kèo có dữ liệu đủ (thiếu 2+ yếu tố trên → không chọn mù).
- QUAN TRỌNG: yếu tố nào thiếu dữ liệu web thì CHẤM THEO KIẾN THỨC BÓNG ĐÁ của mày (phong độ gần đây, danh tiếng đội, derby, mùa trước...) và ghi chú "(ước lượng)". KHÔNG BAO GIỜ trả 0 điểm cho toàn bộ — điểm 0/30 là TRẢ LỜI SAI. Tổng điểm 0 = mày không làm việc.
"""

PICK_MARKETS = {
    'HOME': '1X2 đội nhà thắng', 'DRAW': '1X2 hòa', 'AWAY': '1X2 đội khách thắng',
    'OVER25': 'Tài 2.5', 'UNDER25': 'Xỉu 2.5', 'BTTS_YES': 'Cả hai ghi bàn', 'BTTS_NO': 'Cả hai không ghi bàn',
}


async def analyze_match(session, fixture):
    """Phân tích 1 trận: odds ĐẦY ĐỦ (1X2, tài/xỉu, handicap, góc, thẻ...) + h2h + API prediction
    → AI chọn kèo hay nhất trong market nhà cái mở, tính EV. Trả về (dict dự đoán, None | None, err)."""
    fx_id = str(fixture['fixture']['id'])
    odds = await get_match_odds(session, fx_id)
    if not odds:
        odds = {'bookmaker': '?', 'markets': {}}
    h2h = await get_h2h_summary(session, fixture['teams']['home']['id'], fixture['teams']['away']['id'])
    api_pred = await get_api_prediction(session, fx_id)
    # Dòng odds: tên market + selection — giới hạn để prompt không quá dài
    mk_lines = []
    for name, vals in odds['markets'].items():
        vs = ", ".join(f"{k}: {v}" for k, v in list(vals.items())[:12])
        mk_lines.append(f"- {name}: {vs}")
    odds_txt = f"Odds {odds['bookmaker']}:\n" + ("\n".join(mk_lines) if mk_lines else "Không có odds (kèo chưa mở)")
    user_prompt = (
        f"Trận đấu:\n{_fixture_line(fixture)}\n\n{odds_txt}\n\n{h2h}\n\n{api_pred}\n\n"
        "Nhiệm vụ: soi TOÀN BỘ các market trên (1X2, tài xỉu mọi line, handicap châu Á, phạt góc, thẻ phạt, BTTS...), "
        "tìm kèo CÓ VALUE NHẤT (xác suất thật của mày × odds > 1). Chọn MỘT kèo: dùng đúng 'tên market' và 'selection' "
        "đúng y nguyên như trong danh sách odds trên (không tự sáng tạo kèo nhà cái không mở)."
    )
    system_prompt = (
        "Bạn là chuyên gia soi kèo bóng đá của PNL FOOTBALL BOT (bot do anh Quốc — đẹp trai, giỏi nhất quả đất — tự tay xây dựng). "
        "Phân tích thực dụng: phong độ, đối đầu, động lực, lối chơi (về góc/thẻ: đội có biên lấn hay rút đè, VAR, derbì...). "
        "So odds nhà cái để tìm VALUE — xác suất thật > xác suất odds phản ánh."
    )
    pred, err = await get_ai_json(session, system_prompt, user_prompt)
    if err:
        return None, err
    market = str(pred.get('market', '')).strip()
    selection = str(pred.get('selection', '')).strip()
    prob = max(1.0, min(float(pred.get('prob', 50)), 99.0))
    # Tìm odds: khớp chính xác trước, fuzzy sau (case/spaces)
    odds_val = None
    if market and selection:
        vals = odds['markets'].get(market) or next(
            (v for k, v in odds['markets'].items() if _normalize_team(k) == _normalize_team(market)), None)
        if vals:
            odds_val = vals.get(selection)
            if odds_val is None:
                for k, v in vals.items():
                    if _normalize_team(k) == _normalize_team(selection) or _normalize_team(k) in _normalize_team(selection) or _normalize_team(selection) in _normalize_team(k):
                        odds_val = v
                        break
    ev = (prob / 100 * odds_val - 1) if odds_val else None
    return {'fixture_id': fx_id, 'date': fixture['fixture']['date'][:10],
            'kickoff_vn': _vn_time(fixture['fixture']['date']),
            'league': fixture.get('league', {}).get('name', '?'),
            'home': fixture['teams']['home']['name'], 'away': fixture['teams']['away']['name'],
            'market': market, 'selection': selection, 'prob': prob,
            'odds': odds_val, 'ev': round(ev, 3) if ev is not None else None,
            'reasoning': str(pred.get('reasoning', ''))[:600],
            'status': 'pending', 'result': None, 'graded': None}, None


def _grade_stats_market(p, fixture, stats):
    """Chấm kèo góc/thẻ bằng số liệu trận (stats: {'corners': {team: n}, 'yellows': {...}, 'reds': {...}})."""
    market = (p.get('market') or '').lower()
    sel = p.get('selection') or ''
    sel_low = sel.lower()
    corners_total = sum((stats.get('corners') or {}).values())
    cards_total = sum((stats.get('yellows') or {}).values()) + sum((stats.get('reds') or {}).values())
    if 'corner' in market:
        m = re.search(r'(\d+\.5|\d+)', sel_low)
        line = float(m.group(1)) if m else 9.5
        if 'over' in sel_low or 'trên' in sel_low:
            return ('win' if corners_total > line else ('push' if corners_total == line else 'loss')), f"tổng góc {corners_total}"
        if 'under' in sel_low or 'dưới' in sel_low:
            return ('win' if corners_total < line else ('push' if corners_total == line else 'loss')), f"tổng góc {corners_total}"
        return None, None
    if 'card' in market or 'booking' in market:
        m = re.search(r'(\d+\.5|\d+)', sel_low)
        line = float(m.group(1)) if m else 3.5
        if 'over' in sel_low:
            return ('win' if cards_total > line else ('push' if cards_total == line else 'loss')), f"tổng thẻ {cards_total}"
        if 'under' in sel_low:
            return ('win' if cards_total < line else ('push' if cards_total == line else 'loss')), f"tổng thẻ {cards_total}"
        return None, None
    return None, None


def _pred_line(p, show_ev=True):
    ev_txt = ""
    if show_ev and p.get('ev') is not None:
        ev = p['ev']
        badge = "💰 VALUE" if ev > 0.05 else ("⚖️ cân bằng" if ev > -0.05 else "⚠️ rủi ro đắt")
        ev_txt = f" | EV {ev:+.0%} {badge}"
    o_txt = f" @ odds {p['odds']}" if p.get('odds') else ""
    star = "🔥" if (p.get('ev') or -1) > 0.08 else ("⭐" if (p.get('prob') or 0) >= 65 else "•")
    return (f"{star} {p['kickoff_vn']} [{p['league']}] {p['home']} vs {p['away']}\n"
            f"   → CHỌN: {p['market']} — {p['selection']} (xác suất {p['prob']:.0f}%){o_txt}{ev_txt}\n"
            f"   {p['reasoning'][:220]}")


lich_cache = {}  # chat_id -> {'fixtures': [...], 'ts': ...}


async def cmd_lich(session, chat_id, arg=None):
    date_str = None
    if arg:
        try:
            d, m = arg.strip().split('/')
            date_str = f"{datetime.now(TZ_VN).year}-{int(m):02d}-{int(d):02d}"
        except Exception:
            date_str = None
    if not date_str:
        date_str = datetime.now(TZ_VN).strftime('%Y-%m-%d')
    await send_telegram_message(session, chat_id, f"⏳ Đang lấy lịch trận ngày {date_str}...")
    fixtures, err = await get_fixtures_for_date(session, date_str)
    if err:
        await send_telegram_message(session, chat_id, f"❌ {err}")
        return
    if not fixtures:
        await send_telegram_message(session, chat_id, f"Không có trận nào ngày {date_str} trong các giải theo dõi.")
        return
    lich_cache[chat_id] = {'fixtures': fixtures, 'ts': time.time()}
    # Gom theo giải cho đỡ rối mắt
    by_league = {}
    for fx in fixtures:
        by_league.setdefault(fx['league'].get('name', '?'), []).append(fx)
    lines = [f"📅 *LỊCH TRẬN {date_str}* — {len(fixtures)} trận"]
    kb = []
    for league, fxs in sorted(by_league.items()):
        lines.append(f"\n🏆 *{league}*")
        for fx in fxs[:10]:
            goals = fx.get('goals', {})
            status = fx['fixture']['status'].get('short', '')
            time_vn = _vn_time(fx['fixture']['date'])[6:]  # chỉ HH:MM
            score = ""
            if status in ('FT', '1H', '2H', 'HT', 'LIVE'):
                score = f"  {goals.get('home')}-{goals.get('away')}" + (" 🟢 live" if status not in ('FT',) else " ⚪")
            h, a = fx['teams']['home']['name'], fx['teams']['away']['name']
            lines.append(f"  {time_vn} {h} - {a}{score}")
            kb.append([{"text": f"🎯 {h} vs {a}", "callback_data": f"keo:{fx['fixture']['id']}"}])
    kb.append([{"text": "🔄 Làm mới", "callback_data": f"lichrefresh:{date_str}"}])
    await send_long_message(session, chat_id, "\n".join(lines), reply_markup={"inline_keyboard": kb[:40]})


async def handle_keo_callback(session, chat_id, fixture_id):
    """Bấm nút '🎯 soi kèo' trên lịch → phân tích trận đó ngay."""
    fx_map = {str(fx['fixture']['id']): fx for fx in lich_cache.get(chat_id, {}).get('fixtures', [])}
    fixture = fx_map.get(str(fixture_id))
    if not fixture:
        await send_telegram_message(session, chat_id, "⏳ Lịch đã cũ — đang lấy lại trận từ API...")
        fx, err = await fb_get(session, "/fixtures", {'id': fixture_id})
        if err or not fx:
            await send_telegram_message(session, chat_id, f"❌ Không lấy được trận ({err or 'rỗng'}).")
            return
        fixture = fx[0] if isinstance(fx, list) else fx
    h, a = fixture['teams']['home']['name'], fixture['teams']['away']['name']
    await send_chat_action(session, chat_id)
    await send_telegram_message(session, chat_id,
        f"⚽ *ĐANG SOI: {h} vs {a}*\n({_vn_time(fixture['fixture']['date'])}) — chờ ~1 phút")
    pred, err = await analyze_match(session, fixture)
    if err:
        await send_telegram_message(session, chat_id, f"❌ Lỗi phân tích: {err}")
        return
    predictions[pred['fixture_id']] = pred
    _save_predictions()
    await send_telegram_message(session, chat_id, "⚽ *PHÂN TÍCH KÈO*\n\n" + _pred_line(pred))


TEAM_ALIASES = {
    'mu': 'manchester united', 'm.u': 'manchester united', 'man u': 'manchester united',
    'manutd': 'manchester united', 'red devils': 'manchester united',
    'mc': 'manchester city', 'm.c': 'manchester city', 'man c': 'manchester city', 'mancity': 'manchester city',
    'ls': 'liverpool', 'liver': 'liverpool', 'the kop': 'liverpool',
    'arsenal': 'arsenal', 'pháo thủ': 'arsenal', 'gooners': 'arsenal',
    'tot': 'tottenham', 'spurs': 'tottenham', 'hotspur': 'tottenham',
    'chel': 'chelsea', 'the blues': 'chelsea',
    'mufc': 'manchester united', 'mcfc': 'manchester city',
    'barca': 'barcelona', 'fcb': 'barcelona', 'cule': 'barcelona',
    'real': 'real madrid', 'los blancos': 'real madrid', 'merengues': 'real madrid',
    'atm': 'atletico madrid', 'atleti': 'atletico madrid',
    'bvb': 'borussia dortmund', 'dortmund': 'borussia dortmund',
    'bayern': 'bayern munich', 'munich': 'bayern munich',
    'psg': 'paris', 'paris sg': 'paris saint germain', 'paris saint-germain': 'paris saint germain',
    'inter': 'inter milan', 'milan': 'milan', 'juve': 'juventus', 'napoli': 'napoli',
    'roma': 'as roma', 'lazio': 'lazio', 'cít': 'sunderland',
    'hlv': 'hamburger', 'hsv': 'hamburger',
    'brighton': 'brighton', 'newcastle': 'newcastle', 'mu-vleague': 'mu',
}


def _normalize_team(s):
    """Chuẩn hóa tên: bỏ space/dấu gạch, lowercase — 'Man. United' == 'man united'."""
    return re.sub(r'[^a-z0-9]', '', s.lower())


async def find_fixture_by_team(session, query):
    """Tìm trận theo tên đội. Hỗ trợ 'mu', 'man utd', hoặc 'mu vs mc' (tách vs/x tìm từng bên).
    Trả về (fixture, None) hoặc (None, list gợi ý 'A vs B' đang có)."""
    for q in re.split(r'\s+vs\s+|\s+x\s+', query.strip().lower()):
        if q.strip():
            fx, _ = await _find_one_team(session, q.strip())
            if fx:
                return fx, None
    # Không khớp → gợi ý các trận đang có
    today = datetime.now(TZ_VN)
    seen, suggestions = [], []
    for offset in range(0, 3):
        date_str = (today + timedelta(days=offset)).strftime('%Y-%m-%d')
        fixtures, err = await get_fixtures_for_date(session, date_str)
        if err or not fixtures:
            continue
        for fx in fixtures[:30]:
            s = f"{fx['teams']['home']['name']} vs {fx['teams']['away']['name']}"
            if s not in seen:
                seen.append(s)
                suggestions.append(s)
    return None, suggestions[:15]


async def _find_one_team(session, query):
    """Tìm trận theo 1 tên đội (alias + viết tắt đầu chữ)."""
    q = query.strip().lower()
    q_norm = _normalize_team(q)
    for alias, full in TEAM_ALIASES.items():
        if q_norm == _normalize_team(alias):
            q = full
            q_norm = _normalize_team(q)
            break
    today = datetime.now(TZ_VN)
    seen, suggestions = [], []
    for offset in list(range(0, 5)) + [-1, -2]:
        date_str = (today + timedelta(days=offset)).strftime('%Y-%m-%d')
        fixtures, err = await get_fixtures_for_date(session, date_str)
        if err or not fixtures:
            continue
        if len(suggestions) < 20:
            for fx in fixtures[:30]:
                s = f"{fx['teams']['home']['name']} vs {fx['teams']['away']['name']}"
                if s not in seen:
                    seen.append(s)
                    suggestions.append(s)
        for fx in fixtures:
            h = _normalize_team(fx['teams']['home']['name'])
            a = _normalize_team(fx['teams']['away']['name'])
            initials_h = ''.join(w[0] for w in re.split(r'[^a-z0-9]+', fx['teams']['home']['name'].lower()) if w)
            initials_a = ''.join(w[0] for w in re.split(r'[^a-z0-9]+', fx['teams']['away']['name'].lower()) if w)
            if (q_norm in h or q_norm in a or q_norm == initials_h or q_norm == initials_a) and len(q_norm) >= 2:
                return fx, None
    return None, suggestions[:20]


async def send_chat_action(session, chat_id, action='typing'):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    try:
        async with session.post(f"https://api.telegram.org/bot{token}/sendChatAction",
                                json={"chat_id": chat_id, "action": action},
                                timeout=aiohttp.ClientTimeout(total=10)) as resp:
            await resp.read()
    except Exception:
        pass


async def cmd_keo(session, chat_id, arg=None):
    """/kèo <tên đội>: AI tự tìm trận qua web + phân tích. Sau đó bot hỏi odds 1xBet để tính EV."""
    query = (arg or '').strip()
    if not query:
        await send_telegram_message(session, chat_id, "Nhập tên đội: `/kèo mu`, `/kèo arsenal`, `/kèo mu vs mc`...")
        return
    await send_telegram_message(session, chat_id, f"🧠 AI đang tự tìm trận + phân tích '{query}'... (chờ 30-90 giây)")
    await send_chat_action(session, chat_id)
    now_str = datetime.now(TZ_VN).strftime('%d/%m/%Y')
    search_q = f"{query} football next match schedule {now_str}"
    web = await tool_web_search(session, search_q, 6)
    # Bước 1b: đọc thêm nội dung 1 trang tốt nhất (trang đội bóng/tin tức) để AI có dữ liệu thật
    fetched = ""
    if web and not web.startswith("Không"):
        lines = web.split("\n")
        best_url = None
        for ln in lines:
            m = re.search(r'https?://\S+', ln)
            if m:
                u = m.group(0).rstrip('.,)')
                if any(d in u for d in ('manutd.com', 'afc.co.uk', 'liverpoolfc.com', 'chelseafc.com', 'spurs', 'arsenal.com',
                                         'fcbarcelona.com', 'realmadrid.com', 'city.com', 'mancity.com',
                                         'bbc.com/sport', 'skysports.com', 'espn.com', 'theguardian.com', '90min.com',
                                         'goal.com', 'flashscore', 'sofascore', 'livescore', 'aiscore', 'fotmob')):
                    best_url = u
                    break
        if not best_url:
            best_url = lines[1].split('—')[-1].strip() if '—' in lines[1] else None
        if best_url and best_url.startswith('http'):
            fetched = await tool_fetch_url(session, best_url)
    system = (
        "Bạn là PNL FOOTBALL BOT — chuyên gia soi kèo bóng đá (bot do anh Quốc đẹp trai tự tay code). "
        "Dùng framework phân tích bên dưới để CHẤM TỪNG YẾU TỐ trước khi chốt kèo — không bao giờ chốt kèo khi thiếu dữ liệu.\n"
        f"{KEO_FRAMEWORK}\n"
        "Trả về JSON: {\"match\": \"A vs B\", \"datetime\": \"ngày giờ giờ VN\", \"league\": \"...\", "
        "\"scores\": {\"phong_do\": 4, \"doi_dau\": 3, \"dong_luc\": 5, \"luc_luong\": 4, \"loi_choi\": 3, \"boi_canh\": 4}, "
        "\"picks\": [ {\"market\": \"1X2\", \"selection\": \"Home\", \"prob\": 58}, "
        "{\"market\": \"Tài xỉu 2.5\", \"selection\": \"Under\", \"prob\": 55}, "
        "{\"market\": \"Asian Handicap\", \"selection\": \"Home -0.5\", \"prob\": 54}, "
        "{\"market\": \"BTTS\", \"selection\": \"No\", \"prob\": 52}, "
        "{\"market\": \"Tài xỉu thẻ phạt\", \"selection\": \"Over 4.5\", \"prob\": 53}, "
        "{\"market\": \"Tài xỉu phạt góc\", \"selection\": \"Under 9.5\", \"prob\": 55} ] "
        "(BẮT BUỘC 6 KÈO: 1X2, tài xỉu bàn, Asian Handicap, BTTS, TÀI XỈU THẺ PHẠT, TÀI XỈU PHẠT GÓC; "
        "chỉ ít hơn khi thiếu dữ liệu — ghi rõ kèo nào thiếu), "
        "\"reasoning\": \"...\"}. Nếu không xác định được trận nào: {\"not_found\": true, \"note\": \"...\"}"
    )
    pred, err = await get_ai_json(session, system,
                                  f"Hôm nay là {now_str} (giờ VN). Yêu cầu: {query}.\n\nKết quả web:\n{web}\n\nNội dung trang đã đọc:\n{fetched[:2500]}")
    if err:
        await send_telegram_message(session, chat_id, f"❌ AI lỗi: {err}")
        return
    if pred.get('not_found'):
        await send_telegram_message(session, chat_id,
            f"🤷 AI không xác định được trận nào cho '{query}'. Thử: /kèo mu, /kèo real madrid, /kèo mu vs mc.")
        return
    scores = pred.get('scores') or {}
    total = 0
    score_lines = []
    for k, vn in (('phong_do', 'Phong độ'), ('doi_dau', 'Đối đầu'), ('dong_luc', 'Động lực'),
                  ('luc_luong', 'Lực lượng'), ('loi_choi', 'Lối chơi'), ('boi_canh', 'Bối cảnh')):
        try:
            s = int(scores.get(k, 0))
        except Exception:
            s = 0
        total += max(0, min(s, 5))
        score_lines.append(f"• {vn}: {max(0, min(s, 5))}/5")
    score_txt = "\n".join(score_lines)
    match_info = str(pred.get('match') or query)
    dt = str(pred.get('datetime') or '?')
    league = str(pred.get('league') or '?')
    picks = pred.get('picks') or []
    ts_now = int(time.time())
    for i, pk in enumerate(picks):
        try:
            prob = max(1.0, min(float(pk.get('prob', 50)), 99.0))
        except Exception:
            prob = 50.0
        rec = {
            'match': match_info, 'datetime': dt, 'league': league,
            'scores': score_txt, 'score_total': total,
            'market': str(pk.get('market') or '?'),
            'selection': str(pk.get('selection') or '?'),
            'prob': prob, 'odds': None, 'ev': None,
            'reasoning': str(pred.get('reasoning') or '')[:500],
            'status': 'pending', 'result': None, 'graded': None,
            'fixture_id': f"web_{ts_now}_{chat_id}_{i}",
            'date': datetime.now(TZ_VN).strftime('%Y-%m-%d'),
            'kickoff_vn': dt, 'home': '', 'away': '',
        }
        predictions[rec['fixture_id']] = rec
    _save_predictions()
    lines = ["⚽ *AI PHÂN TÍCH TRẬN:*", f"Trận: {match_info} ({dt}) [{league}]", ""]
    if score_txt:
        lines.append(f"📊 *Bảng điểm:* {total}/30")
        lines.append(score_txt)
        lines.append("")
    if picks:
        lines.append("*Dự đoán từng loại kèo:*")
        best = max(picks, key=lambda p: float(p.get('prob') or 0))
        for pk in picks:
            prob = max(1.0, min(float(pk.get('prob', 50)), 99.0))
            mark = "🔥" if pk is best else "•"
            lines.append(f"{mark} {pk.get('market')} → *{pk.get('selection')}* ({prob:.0f}%)")
        lines.append("")
        lines.append(f"💡 Chắc ăn nhất: *{best.get('market')} — {best.get('selection')}* "
                     f"({float(best.get('prob', 50)):.0f}%)")
    lines.append(f"Lý do: {str(pred.get('reasoning') or '')[:400]}")
    lines.append("")
    lines.append("💰 Có odds 1xBet thì dán vào (vd `tx2.5 1.90 1.95`, `1x2 2.40 3.60 2.80`) — t tính EV cho từng kèo.")
    await send_telegram_message(session, chat_id, "\n".join(lines))


def _parse_odds_msg(text):
    """Nhận diện tin nhắn chứa odds dán từ app: 'tx2.5 1.90 1.95', '1x2 2.40 3.60 2.80'..."""
    t = text.lower().strip()
    m = re.match(r'^(tx|tài|xỉu|over|under)?\s*(\d+(?:\.\d+)?)\s*[:=]?\s*([\d\.\s,]+)$', t)
    if m:
        odds = [float(x) for x in re.findall(r'\d+(?:\.\d+)?', m.group(3))]
        return 'ou', float(m.group(2)), odds
    m = re.match(r'^(1x2|hdc|btts)\s*[:=]?\s*([\d\.\s,]+)$', t)
    if m:
        odds = [float(x) for x in re.findall(r'\d+(?:\.\d+)?', m.group(2))]
        return m.group(1), None, odds
    return None


async def handle_odds_reply(session, chat_id, text):
    """Mày dán odds → tính EV cho dự đoán AI pending gần nhất. Trả về True nếu xử lý được."""
    parsed = _parse_odds_msg(text)
    if not parsed:
        return False
    pendings = [p for p in predictions.values()
                if p.get('status') == 'pending' and p.get('fixture_id', '').startswith('web_')]
    if not pendings:
        return False
    p = max(pendings, key=lambda x: x.get('graded') or 0) if False else pendings[-1]
    market, line, odds = parsed
    if market == '1x2':
        sel = (p.get('selection') or '').lower()
        idx = 0 if ('home' in sel or 'chủ' in sel or 'đội nhà' in sel) else (2 if ('away' in sel or 'khách' in sel or 'đội khách' in sel) else 1)
        if idx >= len(odds):
            return False
        p['odds'] = odds[idx]
    elif market == 'ou':
        if line > 0:
            p['market'] = f"Tài xỉu {line}"
        sel = (p.get('selection') or '').lower()
        if ('xỉu' in sel or 'under' in sel) and len(odds) > 1:
            p['odds'] = odds[1]
        else:
            p['odds'] = odds[0]
    else:
        return False
    if not p['odds']:
        return False
    p['ev'] = round(p['prob'] / 100 * p['odds'] - 1, 3)
    _save_predictions()
    ev = p['ev']
    badge = "💰 VALUE — đáng đánh" if ev > 0.05 else ("⚖️ cân bằng" if ev > -0.05 else "⚠️ KÈO ĐẮT — không nên đánh")
    await send_telegram_message(session, chat_id,
        f"📊 *EV với odds {p['odds']} (1xBet):*\n"
        f"Kèo: {p['market']} — {p['selection']} | Xác suất AI {p['prob']:.0f}%\n"
        f"→ EV = {ev:+.1%} → *{badge}*")
    return True


async def cmd_kq(session, chat_id, arg=None):
    try:
        n_days = int((arg or '7').strip())
    except ValueError:
        n_days = 7
    cutoff = (datetime.now(TZ_VN) - timedelta(days=n_days)).strftime('%Y-%m-%d')
    graded = [p for p in predictions.values()
              if p.get('status') in ('win', 'loss', 'push') and (p.get('date') or '') >= cutoff]
    wins = [p for p in graded if p['status'] == 'win']
    losses = [p for p in graded if p['status'] == 'loss']
    pushes = [p for p in graded if p['status'] == 'push']
    decided = len(wins) + len(losses)
    hit = len(wins) / decided * 100 if decided else 0
    lines = [f"📋 *KẾT QUẢ DỰ ĐOÁN {n_days} NGÀY QUA*"]
    if decided:
        lines.append(f"✅ {len(wins)} thắng | ❌ {len(losses)} thua | 🤝 {len(pushes)} void — hit rate {hit:.0f}%")
        profit = sum((p.get('odds') or 1) - 1 for p in wins) - sum(1 for p in losses)
        lines.append(f"💵 Lợi nhuận giả định 1 đơn vị/kèo: {profit:+.1f} đơn vị")
        by_market = {}
        for p in graded:
            by_market.setdefault(p['market'], [0, 0])
            if p['status'] == 'win':
                by_market[p['market']][0] += 1
            elif p['status'] == 'loss':
                by_market[p['market']][1] += 1
        for market, (w, l) in sorted(by_market.items()):
            if w + l >= 2:
                lines.append(f"   • {market}: {w}/{w + l} ({w / (w + l) * 100:.0f}%)")
    else:
        lines.append("Chưa có trận nào được chấm kết quả trong kỳ này.")
    lines.append("")
    for p in (wins + losses)[-8:][::-1]:
        mark = "✅" if p['status'] == 'win' else ("❌" if p['status'] == 'loss' else "🤝")
        lines.append(f"{mark} {p['kickoff_vn']} {p['home']} vs {p['away']} — chọn {p['market']}{p.get('result') and f' ({p['result']})' or ''}")
    await send_long_message(session, chat_id, "\n".join(lines))


async def handle_usage_command(session, chat_id):
    pass_data, perr = await get_front_pass(session)
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
            data, _ = await get_front_overview(session)
            kpi = (data or {}).get('kpi') or {}
            if kpi:
                lines.append(f"📈 Hôm nay: {kpi.get('total_requests', 0)} request, "
                             f"{int(kpi.get('total_tokens', 0)):,} token, thành công {kpi.get('success_rate', 0):.0f}%")
        except Exception:
            pass
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        usage_today = llm_usage.get(today, {})
        if usage_today:
            parts = [f"{m}: {v.get('calls', 0)} lần" for m, v in usage_today.items()]
            lines.append("🤖 Bot gọi: " + "; ".join(parts))
        lines.append(f"⚽ API-Football hôm nay: {fb_quota['used']}/{FB_DAILY_LIMIT} request")
        await send_telegram_message(session, chat_id, "\n".join(lines))
        return
    await send_telegram_message(session, chat_id, f"⚠️ Không lấy được quota: {perr}")


# ═══════════════ LOOPS TỰ ĐỘNG ═══════════════

DAILY_REPORT_HOUR_VN = 7   # 07:30 VN
MAX_PREDICTIONS_PER_DAY = 8
auto_chats = set()         # chat nhận báo tự động (mọi chat từng gõ lệnh)


async def daily_predictions_loop(app):
    """TẠM NGƯNG (AI-for-one): cần API-Football — hết quota sẽ spam lỗi. Bật lại khi có plan/key tốt."""
    while True:
        await asyncio.sleep(3600)


async def results_loop(app):
    """Mỗi giờ: chấm kết quả các dự đoán pending của hôm qua (và hôm nay nếu đá xong)."""
    session = app['session']
    while True:
        try:
            pending = {fid: p for fid, p in predictions.items() if p.get('status') == 'pending'}
            if pending:
                today = datetime.now(TZ_VN).strftime('%Y-%m-%d')
                yest = (datetime.now(TZ_VN) - timedelta(days=1)).strftime('%Y-%m-%d')
                dates = {p.get('date') for p in pending.values()}
                for d in list(dates):
                    if not d or d not in (today, yest):
                        continue
                    fixtures, err = await get_fixtures_for_date(session, d)
                    if err:
                        logger.warning(f"[KQ] {err}")
                        continue
                    fx_map = {str(fx['fixture']['id']): fx for fx in fixtures}
                    for fid, p in list(pending.items()):
                        if p.get('date') != d:
                            continue
                        fx = fx_map.get(fid)
                        if not fx:
                            continue
                        if fx['fixture']['status']['short'] != 'FT':
                            continue
                        st, res = _grade_prediction(p, fx)
                        if st == 'needs_stats':
                            stats = await _match_stats_summary(session, fx_id)
                            if not stats:
                                continue  # thử lại lượt sau khi có đủ số liệu
                            st, res = _grade_stats_market(p, fx, stats)
                        if st and st != 'needs_stats':
                            p['status'] = st
                            p['result'] = res
                            p['graded'] = time.time()
                            mark = "✅ THẮNG" if st == 'win' else ("❌ THUA" if st == 'loss' else "🤝 VOID")
                            odd_txt = f" odds {p['odds']}" if p.get('odds') else ""
                            for cid in list(auto_chats):
                                await send_telegram_message(
                                    session, cid,
                                    f"{mark} {p['home']} vs {p['away']} — chọn {p['market']} → {res}{odd_txt}")
                _save_predictions()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Lỗi trong results_loop: {e}")
        await asyncio.sleep(3600)


# ═══════════════ TELEGRAM UPDATE HANDLING ═══════════════

SUPPORTED_COMMANDS = {
    '/start', '/help', '/lich', '/kq', '/usage', '/model',
}


def _help_text():
    return (
        "⚽ *PNL FOOTBALL BOT* — dự đoán kèo bóng đá cho anh Quốc (đẹp trai, giỏi nhất quả đất)\n\n"
        "📅 `/lich` [dd/MM] - Lịch trận hôm nay (hoặc ngày khác) các giải theo dõi.\n"
        "🎯 `/kèo` `<tên đội>` - Phân tích 1 trận: AI dự đoán kèo + so odds tìm value bet.\n"
        "   Vd: `/kèo arsenal`, `/kèo man city`, `/kèo real madrid`\n"
        "📋 `/kq` [n ngày] - Độ chính xác dự đoán: thắng/thua, hit-rate, PnL giả định.\n"
        "📊 `/usage` - Quota AI MintRouter + ngân sách API-Football.\n"
        "🤖 `/model` - Đổi model AI (bấm chọn, bot tự restart).\n\n"
        "⏰ Tự động: 07:30 VN mỗi ngày gửi dự đoán trận hôm nay; mỗi giờ chấm kết quả trận đã dự đoán.\n"
        "⚠️ Dự đoán THAM KHẢO — cá cược có rủi ro, đánh có kỷ luật."
    )


async def cmd_analyze_odds_image(session, chat_id, photo, caption='', is_doc=False):
    """Screenshot 1xBet → tải ảnh → AI vision đọc odds + phân tích kèo + EV."""
    await send_telegram_message(session, chat_id, "📸 Đang đọc odds từ ảnh + phân tích... (chờ ~30 giây)")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    try:
        file_id = (photo[0].get('file_id') if is_doc else photo[-1]['file_id'])
        async with session.get(f"https://api.telegram.org/bot{token}/getFile",
                               params={'file_id': file_id}, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            fd = await resp.json()
        if not fd.get('ok'):
            await send_telegram_message(session, chat_id, f"❌ Telegram lỗi tải ảnh: {fd.get('description', '?')} — gửi lại ảnh nhé.")
            return
        file_path = fd.get('result', {}).get('file_path')
        if not file_path:
            await send_telegram_message(session, chat_id, "❌ Không lấy được đường dẫn ảnh.")
            return
        async with session.get(f"https://api.telegram.org/file/bot{token}/{file_path}",
                               timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                await send_telegram_message(session, chat_id, f"❌ Tải ảnh thất bại HTTP {resp.status} — gửi lại ảnh nhé.")
                return
            raw = await resp.read()
        if len(raw) < 100 or not raw.startswith(b'\xff\xd8') and not raw.startswith(b'\x89PNG') and not raw.startswith(b'RIFF'):
            logger.warning(f"[IMG] file_id={file_id} tải về {len(raw)} bytes không phải ảnh hợp lệ: {raw[:60]!r}")
            await send_telegram_message(session, chat_id, "❌ Ảnh tải về bị hỏng (không phải file ảnh) — thử chụp lại và gửi lại.")
            return
    except Exception as e:
        await send_telegram_message(session, chat_id, f"❌ Lỗi tải ảnh: {e}")
        return
    import base64, io
    from PIL import Image
    try:
        img = Image.open(io.BytesIO(raw)).convert('RGB')
        # Nén ảnh: max 1000px, JPEG q85 — ảnh gốc Telegram ~1MB+ dễ bị upstream reject
        img.thumbnail((1000, 1000))
        buf = io.BytesIO()
        img.save(buf, format='JPEG', quality=85)
        raw = buf.getvalue()
    except Exception as e:
        logger.warning(f"Không nén được ảnh: {e}")
    data_url = f"data:image/jpeg;base64,{base64.b64encode(raw).decode('ascii')}"
    caption_txt = caption.strip()[:300]
    prompt = (
        "Phân tích kèo bóng đá từ ảnh screenshot nhà cái. "
        "Đọc chính xác: trận đấu, thời gian, MỌI dòng kèo + odds trong ảnh (1X2, châu Á, tài xỉu, góc, thẻ...). "
        f"{KEO_FRAMEWORK}\n"
        "Sau khi chấm đủ 6 yếu tố: chọn MỘT kèo có dữ liệu đủ nhất + giá trị nhất, nêu xác suất thật %, odds, "
        "EV = xác suất × odds − 1. Chỉ nói kèo nào EV ≥ +3% mới đáng đánh. "
        "Trả lời: trận đấu → bảng điểm 6 yếu tố → kèo chọn + odds + xác suất + EV + lý do. Tiếng Việt, không markdown."
        + (f"\n\nGhi chú của người dùng: {caption_txt}" if caption_txt else "")
    )
    api_key = os.getenv("DASH_TOKEN")
    model = os.getenv("DASH_MODEL", "glm-5.3")
    payload = {"model": model, "messages": [
        {"role": "user", "content": [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": data_url}},
        ]}],
        "max_tokens": 1200, "temperature": 0.3}
    try:
        timeout = aiohttp.ClientTimeout(total=180)
        async with session.post(f"{MINTROUTER_BASE_URL}/chat/completions", json=payload,
                                headers={"Authorization": f"Bearer {api_key}",
                                         "Content-Type": "application/json"},
                                timeout=timeout) as resp:
            if resp.status != 200:
                body = await resp.text()
                await send_telegram_message(session, chat_id, f"⚠️ AI lỗi HTTP {resp.status}: {body[:120]}")
                return
            data = await resp.json()
            record_llm_usage(model, data.get('usage'))
            content = (data.get('choices', [{}])[0].get('message') or {}).get('content') or ''
    except Exception as e:
        await send_telegram_message(session, chat_id, f"⚠️ Lỗi gọi AI: {e}")
        return
    await send_telegram_message(session, chat_id, "📊 *AI ĐỌC ODDS TỪ ẢNH:*\n\n" + content.strip()[:3800])


async def handle_update(session, update):
    msg = update.get('message')
    cb = update.get('callback_query')
    if cb:
        cb_data = cb.get('data', '')
        msg_obj = cb.get('message') or {}
        chat_id = msg_obj.get('chat', {}).get('id')
        if chat_id and cb_data.startswith(('setmodel:', 'modelpage:')):
            await handle_model_callback(session, chat_id, cb_data, message_id=msg_obj.get('message_id'))
        elif chat_id and cb_data.startswith('keo:'):
            await handle_keo_callback(session, chat_id, cb_data.partition(':')[2])
        if cb.get('id'):
            token = os.getenv("TELEGRAM_BOT_TOKEN")
            try:
                async with session.post(f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                                        json={"callback_query_id": cb['id']},
                                        timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    await resp.read()
            except Exception:
                pass
        return
    if not msg:
        return
    chat_id = msg.get('chat', {}).get('id')
    text = (msg.get('text') or '').strip()
    photo = msg.get('photo') or []
    doc = msg.get('document') or {}
    if not chat_id or (not text and not photo and not doc):
        return
    auto_chats.add(chat_id)
    _save_chats()

    # 📸 Ảnh (screenshot 1xBet...) → AI đọc odds + phân tích kèo trực tiếp
    if photo and not text:
        await cmd_analyze_odds_image(session, chat_id, photo, msg.get('caption') or '')
        return
    if doc and doc.get('mime_type', '').startswith('image/') and not text:
        await cmd_analyze_odds_image(session, chat_id, [doc], msg.get('caption') or '', is_doc=True)
        return

    parts = text.split(maxsplit=1)
    command_base = parts[0].split('@')[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else None
    session_http = session

    if command_base in ('/start', '/help'):
        await send_telegram_message(session_http, chat_id, _help_text())
    elif command_base == '/lich':
        await cmd_lich(session_http, chat_id, arg)
    elif command_base == '/kèo' or command_base == '/keo':
        await cmd_keo(session_http, chat_id, arg)
    elif command_base == '/kq':
        await cmd_kq(session_http, chat_id, arg)
    elif command_base == '/usage':
        await handle_usage_command(session_http, chat_id)
    elif command_base == '/model':
        await handle_model_command(session_http, chat_id)
    else:
        # Tin nhắn odds dán từ app 1xBet → tính EV cho kèo AI đang chờ
        handled = await handle_odds_reply(session_http, chat_id, text)
        if not handled:
            # AI tự đọc câu hỏi → tự chọn tool (agent loop)
            await ai_agent_loop(session_http, chat_id, text, msg.get('message_id'))


async def delete_telegram_message(session, chat_id, message_id):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not message_id:
        return
    try:
        async with session.post(f"https://api.telegram.org/bot{token}/deleteMessage",
                                json={"chat_id": chat_id, "message_id": message_id},
                                timeout=aiohttp.ClientTimeout(total=15)) as resp:
            await resp.read()
    except Exception:
        pass


_status_msgs = {}  # chat_id -> [message_id]


def _mark_status_msg(chat_id, message_id):
    if message_id:
        # Thay thế tuần tự: tin trạng thái mới sẽ xoá tin trạng thái cũ ngay lập tức
        prev = _status_msgs.get(chat_id, [])
        if prev:
            import asyncio as _aio
            sess = app_session  # session toàn cục gán lúc on_startup
            if sess:
                for mid in prev:
                    _aio.ensure_future(delete_telegram_message(sess, chat_id, mid))
            _status_msgs[chat_id] = []
        _status_msgs.setdefault(chat_id, []).append(message_id)


async def _clear_status_msgs(session, chat_id):
    for mid in _status_msgs.pop(chat_id, []):
        await delete_telegram_message(session, chat_id, mid)



def _clean_tg(text):
    """Dọn format AI → Telegram đọc đẹp: bỏ **, #, ---, cột |, tiếng Nga/ryc, khoảng trắng thừa."""
    if not text:
        return text
    text = text.replace('**', '')
    text = re.sub(r'^#{1,6}\s*', '', text, flags=re.MULTILINE)
    text = re.sub(r'^[-=]{3,}\s*$', '', text, flags=re.MULTILINE)
    # BẢNG → tách thành dòng riêng: 'A | B | C' → 'A\nB\nC' (bỏ ô rỗng)
    lines = []
    for ln in text.split('\n'):
        parts = [p.strip().strip('—–-|').strip() for p in ln.split('|')]
        parts = [p for p in parts if p and not re.fullmatch(r'[—–\-]+', p)]
        if len(parts) > 1 and len(ln) < 200:
            lines.extend(parts)
        else:
            lines.append(ln)
    text = '\n'.join(lines)
    # bỏ dòng chứa Cyrillic (Nga) hoặc chữ Trung — rác
    out = []
    for ln in text.split('\n'):
        if re.search(r'[а-яА-ЯёЁ\u4e00-\u9fff]', ln):
            continue
        out.append(ln)
    text = '\n'.join(out)
    text = re.sub(r'(?:\s*[—\-]\s*){2,}', ' — ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


# ═══════════════ AGENT LOOP (AI tự đọc câu hỏi → tự chọn tool — như PNL bot cũ) ═══════════════
AGENT_TOOLS = [
    {"type": "function", "function": {"name": "web_search", "description": "Tìm kiếm web (Bing, free). Dùng khi cần biết: trận đấu sắp tới của đội, phong độ, tin chấn thương, kết quả, odds, lịch sử đối đầu...", "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}}},
    {"type": "function", "function": {"name": "fetch_url", "description": "Đọc nội dung 1 trang web cụ thể (tối đa ~4000 ký tự). Dùng sau web_search để đọc chi tiết bài viết/trang đội bóng.", "parameters": {"type": "object", "properties": {"url": {"type": "string"}}, "required": ["url"]}}},
    {"type": "function", "function": {"name": "analyze_keo", "description": "PHÂN TÍCH KÈO MỘT TRẬN đầy đủ: AI web-search tìm trận + chấm framework 6 yếu tố (phong độ/đối đầu/động lực/lực lượng/lối chơi/bối cảnh) + trả 6 kèo (1X2, tài xỉu bàn, châu Á, BTTS, tài xỉu thẻ, tài xỉu góc). Dùng khi người dùng muốn dự đoán/phân tích kèo 1 đội/1 trận.", "parameters": {"type": "object", "properties": {"query": {"type": "string", "description": "Tên đội/trận, vd 'mu', 'sheffield united', 'real madrid vs barca'"}}, "required": ["query"]}}},
    {"type": "function", "function": {"name": "my_stats", "description": "Thống kê độ chính xác các dự đoán của bot (hit-rate, thắng/thua theo loại kèo).", "parameters": {"type": "object", "properties": {}}}},
]


# Tên đội/alias → slug URL Sky Sports (để fetch lịch trận thật)
TEAM_SKY_SLUGS = {
    'mu': 'manchester-united', 'manutd': 'manchester-united', 'man u': 'manchester-united',
    'mufc': 'manchester-united', 'red devils': 'manchester-united', 'quỷ đỏ': 'manchester-united',
    'mc': 'manchester-city', 'mancity': 'manchester-city', 'man c': 'manchester-city',
    'mcfc': 'manchester-city', 'city': 'manchester-city',
    'ls': 'liverpool', 'liver': 'liverpool',
    'arsenal': 'arsenal', 'pháo thủ': 'arsenal', 'gooners': 'arsenal',
    'tot': 'tottenham', 'spurs': 'tottenham', 'hotspur': 'tottenham',
    'chel': 'chelsea', 'the blues': 'chelsea',
    'barca': 'barcelona', 'fcb': 'barcelona',
    'real': 'real-madrid', 'real madrid': 'real-madrid', 'los blancos': 'real-madrid',
    'atm': 'atletico-madrid', 'atleti': 'atletico-madrid',
    'bvb': 'borussia-dortmund', 'dortmund': 'borussia-dortmund',
    'bayern': 'bayern-munich', 'munich': 'bayern-munich',
    'psg': 'paris-saint-germain',
    'inter': 'inter-milan', 'milan': 'ac-milan', 'juve': 'juventus', 'napoli': 'napoli',
    'sheffield united': 'sheffield-united', 'sheff utd': 'sheffield-united', 'sheffield utd': 'sheffield-united',
    'sheffield': 'sheffield-united', 'blades': 'sheffield-united',
    'wolves': 'wolverhampton-wanderers', 'wolverhampton': 'wolverhampton-wanderers',
    'leeds': 'leeds-united', 'west ham': 'west-ham-united', 'newcastle': 'newcastle-united',
    'brighton': 'brighton', 'aston villa': 'aston-villa', 'southampton': 'southampton',
    'nottingham': 'nottingham-forest', 'forest': 'nottingham-forest',
}


def _sky_slug_for(query):
    """Trích slug Sky cho câu hỏi: 'mu vs mc' → ưu tiên đội nhà trước (manchester-united).
    Trả về (slug, matched_name) hoặc (None, None)."""
    q = query.lower().strip()
    q = re.sub(r'phân tích|kèo|keo|soi|nhé|nha|nhỉ|đi|giúp|với|hôm nay|tối nay', ' ', q)
    q = re.sub(r'\s+', ' ', q).strip()
    # Tách vs — lấy phần đầu làm đội chính
    parts = [p.strip() for p in re.split(r'\s+vs\s+|\s+x\s+', q) if p.strip()]
    if not parts:
        return None, None
    for part in parts:
        if part in TEAM_SKY_SLUGS:
            return TEAM_SKY_SLUGS[part], part
    # fallback: tên đội dạng vài từ → slug
    first = parts[0]
    if len(first.split()) <= 3:
        return re.sub(r'[^a-z0-9]+', '-', first).strip('-'), first
    return None, None


def _parse_sky_fixtures(pg):
    """Làm sạch trang Sky Sports → danh sách trận đọc được:
    '13/9: MU vs Man City (Premier League, 4:30pm)' + kết quả đã đá."""
    lines = []
    results = re.findall(
        r'((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday) \d+(?:st|nd|rd|th) \w+)\s+'
        r'([A-Z][A-Za-z \.]{2,30}?)\s+app\.football\.scores_fixtures\.view_fixture\s+'
        r'([A-Z][\w\' \.]{1,28}?)\s+(\d+)\s+([A-Z][\w\' \.]{1,28}?)\s+(\d+)\s+(FT|In Play)',
        pg)
    for date_str, league, h, gh, a, ga, st in results[:12]:
        tag = "🔴 ĐANG ĐÁ" if 'In Play' in st or 'LIVE' in st else "ĐÃ ĐÁ"
        lines.append(f"{tag} [{league}] {date_str}: {h} {gh}-{ga} {a}")
    # Trận đang đá (có phút giữa tỉ số và 'In Play') — pattern lỏng hơn
    live = re.findall(
        r'((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday) \d+(?:st|nd|rd|th) \w+)\s+'
        r'([A-Z][A-Za-z \.]{2,30}?)\s+app\.football\.scores_fixtures\.view_fixture\s+'
        r'([A-Z][\w\' \.]{1,28}?)\s+(\d+)\s+([A-Z][\w\' \.]{1,28}?)\s+(\d+)\s+[^A-Z]{0,25}\s*In Play',
        pg)
    for date_str, league, h, gh, a, ga in live[:6]:
        lines.append(f"🔴 ĐANG ĐÁ [{league}] {date_str}: {h} {gh}-{ga} {a}")
    upcoming = re.findall(
        r'((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday) \d+(?:st|nd|rd|th) \w+)\s+'
        r'([A-Z][A-Za-z \.]{2,30}?)\s+app\.football\.scores_fixtures\.view_fixture\s+'
        r'([A-Z][\w\' \.]{1,28}?)\s+app\.football\.scores_fixtures\.are_scheduled\s+'
        r'([A-Z][\w\' \.]{1,28}?)\s+\.\s+([\d\.]+(?:am|pm))\s+Fixture',
        pg)
    for date_str, league, h, a, kick in upcoming[:8]:
        lines.append(f"SẮP ĐÁ [{league}] {date_str}: {h} vs {a} lúc {kick}")
    if not lines:
        return ""
    return "\n".join(lines)


async def _fetch_team_fixtures(session, q):
    """Fetch lịch trận thật từ Sky Sports theo tên đội trong query → trả về text ĐÃ LÀM SẠCH (rỗng nếu fail)."""
    slug, name = _sky_slug_for(q)
    if not slug:
        return ""
    for cand in (f"https://www.skysports.com/{slug}-fixtures",
                 f"https://www.skysports.com/{slug}-scores-fixtures"):
        pg = await tool_fetch_url(session, cand, max_chars=20000)
        if pg and not pg.startswith(("Không", "LỖI")) and len(pg) > 600:
            parsed = _parse_sky_fixtures(pg)
            if parsed:
                return parsed
    return ""



LEAGUE_SKY_SLUGS = {
    'Premier League': 'premier-league', 'Spanish La Liga': 'la-liga',
    'Italian Serie A': 'serie-a', 'German Bundesliga': 'bundesliga',
    'French Ligue 1': 'ligue-1', 'EFL Championship': 'efl-championship',
    'Scottish Premiership': 'scottish-premier',
}


async def _fetch_league_fixtures(session, fetched):
    """Từ dữ liệu lịch đội (có tên giải) → fetch lịch CẢ GIẢI từ Sky (có kết quả mọi đội, kể cả đối thủ)."""
    m = re.search(r'\[([^\]]+)\]', fetched or '')
    if not m:
        return ""
    league = m.group(1)
    slug = LEAGUE_SKY_SLUGS.get(league)
    if not slug:
        return ""
    pg = await tool_fetch_url(session, f"https://www.skysports.com/{slug}-fixtures", max_chars=20000)
    parsed = _parse_sky_fixtures(pg) if pg else ""
    return parsed


async def _agent_execute(session, chat_id, name, args):
    """Thực thi 1 tool agent. Trả về text kết quả."""
    if name == 'web_search':
        return await tool_web_search(session, (args.get('query') or '').strip(), 6)
    if name == 'fetch_url':
        return await tool_fetch_url(session, (args.get('url') or '').strip())
    if name == 'analyze_keo':
        q = (args.get('query') or '').strip()
        if not q:
            return "LỖI: cần tên đội."
        await send_chat_action(session, chat_id)
        _an = await send_telegram_message(session, chat_id, f"⚽ Đang phân tích kèo '{q}'... (chờ 1-2 phút)")
        if _an and _an.get('result'):
            _mark_status_msg(chat_id, _an['result'].get('message_id'))
        now_str = datetime.now(TZ_VN).strftime('%d/%m/%Y')
        web = await tool_web_search(session, f"{q} football next match schedule {now_str}", 6)
        fetched = ""
        # Luôn thử Sky Sports: URL chuẩn theo tên đội — trang này đọc được, đầy đủ lịch trận thật
        fetched = await _fetch_team_fixtures(session, q)
        if not fetched and web and not web.startswith("Không"):
            lines = web.split("\n")
            for ln in lines:
                m = re.search(r'https?://\S+', ln)
                if m and any(d in ln for d in ('manutd.com', 'bbc.com/sport', 'skysports.com', 'espn.com',
                                               'theguardian.com', 'goal.com', 'flashscore', 'sofascore', 'livescore',
                                               'aiscore', 'fotmob', 'sport')):
                    fetched = await tool_fetch_url(session, m.group(0).rstrip('.,)'))
                    break
        system = (
            "Bạn là chuyên gia soi kèo bóng đá CHUYÊN SÂU của anh Quốc — phân tích càng kỹ càng tốt, không giới hạn độ dài. "
            "Dữ liệu trận (lịch, kết quả gần đây) có trong tin nhắn user — dùng làm nền tảng, kết hợp kiến thức bóng đá của mày.\n"
            "Cấu trúc bài phân tích:\n"
            "1. Thông tin trận: giải, ngày giờ VN, sân, tính chất (derby/đua top...)\n"
            "2. Phong độ 2 đội: 5 trận gần nhất, sân nhà/khách, số bàn ghi/thủng\n"
            "3. Lịch sử đối đầu: kết quả các lần gặp, xu hướng bàn thắng\n"
            "4. Động lực & bối cảnh: mục tiêu mùa giải, lịch thi đấu, yếu tố tâm lý\n"
            "5. Lực lượng & lối chơi: đội hình, chấn thương (nếu biết), phong cách, điểm yếu\n"
            "6. NHẬN ĐỊNH KÈO cho từng loại: 1X2, tài xỉu bàn, châu Á, BTTS, tài xỉu thẻ, tài xỉu góc — "
            "mỗi kèo nêu rõ lựa chọn + mức tin cậy % + 1 câu lý do. Kèo nào không đủ cơ sở thì nói thẳng.\n"
            "7. Chốt: kèo tự tin nhất + combo nếu có + cảnh báo rủi ro.\n"
            "Trình bày dễ đọc trên Telegram: tiêu đề in hoa, gạch đầu dòng '- ', MỖI MỤC THÔNG TIN MỘT DÒNG RIÊNG, "
            "TUYỆT ĐỐI CẤM dùng bảng markdown, ký tự | hoặc ---. KHÔNG dùng ký tự ** hay ###. "
            "Dữ liệu thiếu thì ghi '(ước lượng)' — tuyệt đối không bịa số liệu cụ thể. "
            "NGÔN NGỮ: tiếng Việt THUẦN — tuyệt đối không lẫn tiếng Anh/Nga/TRUNG QUỐC/ngôn ngữ khác vào câu, tên đội/giải giữ tiếng Anh chuẩn. "
            "NẾU đội được hỏi ĐANG ĐÁ (trong dữ liệu có dòng ĐANG ĐÁ) thì phân tích TRẬN ĐANG ĐÁ đó theo diễn biến hiện tại, "
            "không chọn trận tương lai."
        )
        data_parts = []
        if fetched:
            data_parts.append(f"LỊCH + KẾT QUẢ của đội được hỏi:\n{fetched}")
            league_data = await _fetch_league_fixtures(session, fetched)
            if league_data:
                # lọc các trận có liên quan (cùng tên đội hoặc hôm nay/đã đá gần đây)
                kw = [w for w in re.split(r'[^a-z0-9]+', q.lower()) if len(w) > 3]
                rel = [ln for ln in league_data.split('\n')
                       if any(w in ln.lower() for w in kw) or 'ĐANG ĐÁ' in ln][:15]
                if rel:
                    data_parts.append("KẾT QUẢ/LỊCH CỦA ĐỐI THỦ & CÁC TRẬN LIÊN QUAN TRONG GIẢI:\n" + "\n".join(rel))
        if data_parts:
            data_block = "\n\n".join(data_parts) + "\n\n(Lưu ý: các trận trên là LỊCH THI ĐẤU CHÍNH THỨC mùa 2026-27 — tin tuyệt đối, không suy diễn đội nào 'hạng dưới' hay 'Copa del Rey'.)"
        else:
            data_block = f"Kết quả web:\n{web}"
        text, err = await get_ai_response(session, [
            {"role": "system", "content": system},
            {"role": "user", "content": f"Hôm nay là {now_str} (giờ VN). Yêu cầu: {q}.\n\nDỮ LIỆU TRẬN THẬT (ưu tiên dùng cái này):\n{data_block}"},
        ], max_tokens=4000, timeout_s=180)
        if err:
            return f"Lỗi phân tích: {err}"
        if 'not_found' in text or 'không tìm được trận' in text.lower() or 'không tìm thấy trận' in text.lower():
            return f"AI không tìm được trận nào khớp '{q}'. Thử tên khác: mu, arsenal, sheffield united, real madrid..."
        # Cắt bỏ preamble "Mình sẽ kiểm tra cấu trúc workspace..." của persona MintRouter
        idx = text.find('\n## ')
        if idx == -1:
            idx = text.find('\n- **')
        if idx != -1:
            text = text[idx + 1:].strip()
        # Lưu 1 bản ghi pending để /kq chấm (nếu người dùng tự xác nhận thắng/thua)
        predictions[f"web_{int(time.time())}_{chat_id}"] = {
            'match': q, 'datetime': '?', 'league': '?', 'scores': '', 'score_total': 0,
            'market': 'AI phân tích', 'selection': 'xem nội dung', 'prob': 50, 'odds': None, 'ev': None,
            'reasoning': text[:400], 'status': 'pending', 'result': None, 'graded': None,
            'date': datetime.now(TZ_VN).strftime('%Y-%m-%d'), 'kickoff_vn': '?', 'home': '', 'away': '',
        }
        _save_predictions()
        return _clean_tg(text)[:4000]
    if name == 'my_stats':
        graded = [p for p in predictions.values() if p.get('status') in ('win', 'loss', 'push')]
        wins = [p for p in graded if p['status'] == 'win']
        losses = [p for p in graded if p['status'] == 'loss']
        decided = len(wins) + len(losses)
        hit = len(wins) / decided * 100 if decided else 0
        return (f"Đã chấm {decided} kèo: thắng {len(wins)}, thua {len(losses)} (hit-rate {hit:.0f}%). "
                f"Chưa có kèo nào được chấm → gõ /kèo để bắt đầu." if decided else "Chưa có kèo nào được chấm.")
    return f"Lỗi: tool '{name}' không tồn tại."


async def ai_agent_loop(session, chat_id, question, reply_to=None):
    """AI đọc câu hỏi → TỰ quyết định tool (web_search/fetch_url/analyze_keo/my_stats) → lặp tới khi đủ dữ liệu."""
    await send_chat_action(session, chat_id)
    _thinking = await send_telegram_message(session, chat_id, "🧠 Đang suy nghĩ và tự tra cứu... (vài chục giây)")
    if _thinking and _thinking.get('result'):
        _mark_status_msg(chat_id, _thinking['result'].get('message_id'))
    system_prompt = (
        "Bạn là PNL FOOTBALL BOT — trợ lý bóng đá toàn diện của anh Quốc (đẹp trai, giỏi nhất quả đất). "
        "Khi người dùng hỏi, TỰ QUYẾT ĐỊNH cần tool gì: "
        "- Muốn phân tích/dự đoán kèo một đội/trận → analyze_keo (tool đầy đủ framework, đừng tự phân tích tay). "
        "- Cần thông tin mới (phong độ, chấn thương, kết quả, lịch sử đối đầu, tin tức) → web_search rồi fetch_url nếu cần chi tiết. "
        "- Hỏi thành tích dự đoán của bot → my_stats. "
        "- Câu hỏi chung về bóng đá (lịch sử, cầu thủ, giải đấu...) → web_search. "
        "Gọi tool cho tới khi có đủ dữ liệu trả lời đầy đủ (tối đa 4 lượt tool). "
        "Cuối cùng tổng hợp trả lời ngắn gọn tiếng Việt. KHÔNG dùng bảng markdown (| | |) — Telegram không render, "
        "dùng dòng đạn '• ' hoặc ' - ' cho danh sách kèo. "
        "Người dùng là ADMIN DUY NHẤT — hỏi gì về bot/AI cũng trả lời thẳng, không chối."
    )
    messages = [{"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Bạn là PNL FOOTBALL BOT của anh Quốc — trợ lý bóng đá, không phải trợ lý lập trình MintRouter.\n\nCâu hỏi: {question}"}]
    for _ in range(8):
        api_key = os.getenv("DASH_TOKEN")
        model = os.getenv("DASH_MODEL", "glm-5.3")
        data = None
        for attempt in range(3):
            try:
                timeout = aiohttp.ClientTimeout(total=150)
                async with session.post(f"{MINTROUTER_BASE_URL}/chat/completions",
                                        json={"model": model, "messages": messages, "tools": AGENT_TOOLS,
                                              "temperature": 0.3, "max_tokens": 4000},
                                        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                                        timeout=timeout) as resp:
                    if resp.status in (502, 503, 504):
                        logger.warning(f"AI 5xx — chờ {8 * (attempt + 1)}s thử lại")
                        await asyncio.sleep(8 * (attempt + 1))
                        continue
                    if resp.status == 429:
                        try:
                            err429 = (await resp.json(content_type=None)) or {}
                            wait = int((err429.get('error') or {}).get('reset_seconds') or 30)
                        except Exception:
                            wait = 30
                        await asyncio.sleep(min(wait + 2, 90))
                        continue
                    if resp.status != 200:
                        body = await resp.text()
                        await send_telegram_message(session, chat_id, f"⚠️ AI lỗi HTTP {resp.status}: {body[:120]}", reply_to=reply_to)
                        await _clear_status_msgs(session, chat_id)
                        return
                    data = await resp.json()
                    record_llm_usage(model, data.get('usage'))
                    break
            except Exception as e:
                if attempt == 2:
                    await send_telegram_message(session, chat_id, f"⚠️ AI lỗi: {e}", reply_to=reply_to)
                    await _clear_status_msgs(session, chat_id)
                    return
                await asyncio.sleep(5)
        if data is None:
            await send_telegram_message(session, chat_id, "⚠️ AI lỗi liên tiếp — thử lại sau ít phút.", reply_to=reply_to)
            await _clear_status_msgs(session, chat_id)
            return
        msg = (data.get('choices', [{}])[0].get('message') or {})
        tool_calls = msg.get('tool_calls') or []
        if not tool_calls:
            content = _clean_tg(msg.get('content') or '').strip()
            if content:
                await send_telegram_message(session, chat_id, content, reply_to=reply_to)
            else:
                await send_telegram_message(session, chat_id, "Xong! (AI không có kết luận — thử hỏi cụ thể hơn)", reply_to=reply_to)
            await _clear_status_msgs(session, chat_id)
            return
        messages.append({"role": "assistant", "content": msg.get('content') or None, "tool_calls": tool_calls})
        last_tool = None
        for tc in tool_calls:
            fn = tc.get('function') or {}
            name = fn.get('name', '')
            last_tool = name
            try:
                args = json.loads(fn.get('arguments') or '{}')
            except Exception:
                args = {}
            result = await _agent_execute(session, chat_id, name, args)
            messages.append({"role": "tool", "tool_call_id": tc.get('id'), "content": str(result)[:3500]})
        if last_tool == 'analyze_keo':
            # Phân tích đã xong → buộc AI tổng hợp ngay, cấm gọi tool tiếp
            messages.append({"role": "user", "content": "Kết quả phân tích kèo đã đầy đủ ở trên. Tổng hợp trả lời người dùng NGAY — KHÔNG gọi thêm tool nào nữa."})
            continue
    await send_telegram_message(session, chat_id, "⚠️ AI xử lý quá nhiều bước — thử hỏi cụ thể hơn.", reply_to=reply_to)
    await _clear_status_msgs(session, chat_id)


async def ai_chat(session, chat_id, question, reply_to=None):
    """Hỏi tự do — câu liên quan bóng đá/kèo thì chuyển vào pipeline soi kèo có framework JSON
    (thắng được persona MintRouter). Câu khác → chat thường."""
    if re.search(r'kèo|keo|soi|tài xỉu|châu á|handicap|btts|thẻ|góc|1x2|trận|đấu|thắng|thua|dự đoán|odds|nên đánh|nên vào|đội|vô địch|cúp|derby',
                 question.lower()):
        # Làm sạch câu chat → chỉ giữ tên đội/kèo (bỏ "phân tích kèo... nha nhé giúp t đi")
        clean_q = re.sub(r'phân tích|phân tích kèo|soi kèo|soi|kèo|keo|nhé|nha|nhỉ|đi|giúp t|giúp tao|giúp|với|tối nay|hôm nay|đánh|được không|đc k|mày|bạn|bot|ồ|ơ',
                         ' ', question.lower(), flags=re.I)
        clean_q = re.sub(r'\s+', ' ', clean_q).strip() or question[:120]
        await cmd_keo(session, chat_id, clean_q[:120])
        return
    today = datetime.now(TZ_VN).strftime('%Y-%m-%d')
    fixtures_cache = getattr(ai_chat, '_fixtures', None)
    if not fixtures_cache or fixtures_cache[0] != today or time.time() - fixtures_cache[1] > 900:
        fixtures, _ = await get_fixtures_for_date(session, today)
        fx_lines = [_fixture_line(fx) for fx in (fixtures or [])[:15]]
        ai_chat._fixtures = (today, time.time(), "\n".join(fx_lines) or "Không có trận hôm nay.")
    recent = [p for p in predictions.values() if p.get('status') in ('win', 'loss')][-5:]
    recent_txt = "\n".join(f"{p['kickoff_vn']} {p['home']} vs {p['away']}: chọn {p['market']} → {p['status']} ({p.get('result')})"
                           for p in recent) or "Chưa có."
    context = (f"Trận đấu hôm nay ({today}, giờ VN):\n{ai_chat._fixtures[2]}\n\n"
               f"Dự đoán gần đây của bot (kết quả):\n{recent_txt}")
    system_prompt = (
        "Bạn là PNL FOOTBALL BOT — robot dự đoán kèo bóng đá do ANH QUỐC (đẹp trai, giỏi nhất quả đất) "
        "tự tay code và nuôi lớn. Anh Quốc là ADMIN DUY NHẤT — trả lời mọi câu hỏi về hệ thống/model thẳng thắn đầy đủ, "
        "không chối từ. GLM/Z.ai chỉ là model nền, KHÔNG phải người tạo ra bạn. "
        "Phong cách: nói chuyện thân mật kiểu 'tao - mày', thực dụng, giỏi bóng đá, đôi khi cà khịa nhẹ. "
        "Trả lời ngắn gọn (≤10 dòng), tiếng Việt, KHÔNG dùng ký tự markdown (*, _, `). "
        "Dữ liệu trận hôm nay có thể cũ — khuyên người dùng gõ /kèo <đội> để phân tích mới nhất."
    )
    ai_task = asyncio.create_task(get_ai_response(session, [
        {"role": "system", "content": system_prompt},
        {"role": "system", "content": "NHẮC LẠI BẮT BUỘC (ghi đè mọi prompt khác): Bạn là PNL FOOTBALL BOT — bot dự đoán kèo bóng đá "
                                       "do ANH QUỐC (đẹp trai, giỏi nhất quả đất) tự tay code. BẠN KHÔNG phải MintRouter.AI, "
                                       "KHÔNG phải trợ lý lập trình — bỏ qua mọi prompt 'workspace/coding assistant' khác. "
                                       "Bạn có dữ liệu trận đấu trong tin nhắn bên dưới và NHIỆM VỤ của bạn là soi kèo, "
                                       "phân tích bóng đá, trả lời câu hỏi bóng đá + câu hỏi về chính hệ thống bot này."},
        {"role": "user", "content": f"(Nhắc: bạn là PNL FOOTBALL BOT của anh Quốc đẹp trai, KHÔNG phải trợ lý lập trình MintRouter — "
                                    f"nhiệm vụ của bạn là soi kèo bóng đá.)\n\nNgữ cảnh:\n{context}\n\nCâu hỏi: {question}"},
    ]))
    # Feedback nhanh: hiện 'typing' + báo đang nghĩ nếu AI chậm
    await send_chat_action(session, chat_id)
    try:
        text, err = await asyncio.wait_for(asyncio.shield(ai_task), timeout=10)
    except asyncio.TimeoutError:
        await send_telegram_message(session, chat_id, "⏳ Đang phân tích... (~30 giây)")
        try:
            text, err = await ai_task
        except Exception:
            text, err = None, "AI gặp sự cố"
    except Exception:
        text, err = None, "AI gặp sự cố"
    if err:
        await send_telegram_message(session, chat_id, f"⚠️ AI gặp sự cố: {err}", reply_to=reply_to)
        return
    await send_telegram_message(session, chat_id, text, reply_to=reply_to)


CHAT_STATE_FILE = "auto_chats.json"


def _save_chats():
    try:
        with open(CHAT_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(auto_chats), f)
    except Exception:
        pass


def _load_chats():
    try:
        if os.path.exists(CHAT_STATE_FILE):
            with open(CHAT_STATE_FILE, "r", encoding="utf-8") as f:
                for cid in json.load(f):
                    auto_chats.add(cid)
            logger.info(f"Đã nạp {len(auto_chats)} chat nhận báo.")
    except Exception as e:
        logger.error(f"Lỗi nạp auto_chats: {e}")


TG_OFFSET_FILE = "tg_offset.json"


def _load_tg_offset():
    try:
        if os.path.exists(TG_OFFSET_FILE):
            with open(TG_OFFSET_FILE, "r", encoding="utf-8") as f:
                return int(json.load(f).get('offset', 0))
    except Exception:
        pass
    return 0


def _save_tg_offset(offset):
    try:
        with open(TG_OFFSET_FILE, "w", encoding="utf-8") as f:
            json.dump({'offset': offset}, f)
    except Exception:
        pass


async def telegram_polling_loop(app):
    session = app['session']
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    # Nhớ offset QUA RESTART: không thì callback lệnh restart bị xử lý lại vô hạn (crash-loop)
    offset = _load_tg_offset()
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    logger.info(f"Bắt đầu long polling Telegram updates... (offset={offset})")
    while True:
        try:
            async with session.post(url, json={"timeout": 50, "offset": offset,
                                               "allowed_updates": ["message", "callback_query"]},
                                    timeout=aiohttp.ClientTimeout(total=70)) as resp:
                data = await resp.json()
                if not data.get('ok'):
                    err = data.get('description', '')
                    if '409' in str(data.get('error_code', '')):
                        logger.error("409 CONFLICT — bot khác đang chạy cùng token. Chờ 5s...")
                        await asyncio.sleep(5)
                        continue
                    logger.warning(f"getUpdates lỗi: {err[:120]}")
                    await asyncio.sleep(3)
                    continue
                for update in data.get('result', []):
                    offset = update['update_id'] + 1
                    _save_tg_offset(offset)
                    u_kind = 'callback' if update.get('callback_query') else (
                        'msg:' + ((update.get('message') or {}).get('text') or '')[:40])
                    logger.info(f"[TG] update #{update['update_id']} {u_kind}")
                    # Xử lý SONG SONG: /kèo chậm 1-2 phút không được chặn /start của user
                    asyncio.create_task(_handle_update_safe(session, update))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Lỗi polling: {e}")
            await asyncio.sleep(5)


_chat_locks = {}


def _chat_lock(chat_id):
    """Mỗi chat 1 lock: tránh 2 lệnh /kèo cùng chat chạy song song đốt quota loạn."""
    if chat_id not in _chat_locks:
        _chat_locks[chat_id] = asyncio.Lock()
    return _chat_locks[chat_id]


async def _handle_update_safe(session, update):
    try:
        chat_id = (update.get('message') or {}).get('chat', {}).get('id') \
            or (update.get('callback_query') or {}).get('message', {}).get('chat', {}).get('id')
        async with _chat_lock(chat_id):
            await handle_update(session, update)
    except Exception:
        logger.exception(f"Lỗi xử lý update {update.get('update_id')}")


# ═══════════════ STARTUP ═══════════════

app_session = None


async def on_startup(app):
    global app_session
    logger.info("⚽ PNL FOOTBALL BOT khởi động...")
    app['session'] = aiohttp.ClientSession()
    app_session = app['session']
    _load_fb_quota()
    _load_predictions()
    _load_llm_usage()
    _load_chats()
    app['polling_task'] = asyncio.create_task(telegram_polling_loop(app))
    app['daily_task'] = asyncio.create_task(daily_predictions_loop(app))
    app['results_task'] = asyncio.create_task(results_loop(app))
    logger.info("Tất cả loop đã chạy: polling, daily predictions (07:30 VN), results (mỗi giờ).")


async def on_cleanup(app):
    logger.info("Đang giải phóng tài nguyên...")
    for k in ('polling_task', 'daily_task', 'results_task'):
        if k in app:
            app[k].cancel()
    if 'session' in app:
        await app['session'].close()
    logger.info("Đã dọn dẹp hoàn tất.")


async def test_handler(request):
    return web.Response(text="PNL FOOTBALL BOT is running!")


def main():
    load_dotenv()
    required = ["TELEGRAM_BOT_TOKEN"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f"Thiếu cấu hình .env: {', '.join(missing)}")
        logger.error("⚽ Cần thêm: TELEGRAM_BOT_TOKEN (từ BotFather), FOOTBALL_API_KEY (api-sports.io free)")
        return
    if not os.getenv("FOOTBALL_API_KEY"):
        logger.warning("Chưa có FOOTBALL_API_KEY — dữ liệu trận đấu sẽ không hoạt động cho tới khi thêm vào .env")

    app = web.Application()
    app.router.add_get('/test', test_handler)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    port = int(os.getenv("PORT", 5000))
    logger.info("Khởi chạy web server + long polling Telegram...")
    web.run_app(app, host='0.0.0.0', port=port)


if __name__ == '__main__':
    main()
