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
    try:
        timeout = aiohttp.ClientTimeout(total=timeout_s)
        async with session.post(f"{MINTROUTER_BASE_URL}/chat/completions",
                                json=payload,
                                headers={"Authorization": f"Bearer {api_key}",
                                         "Content-Type": "application/json"},
                                timeout=timeout) as resp:
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
        return None, str(e)


async def get_ai_json(session, system_prompt, user_prompt, timeout_s=150):
    """Gọi AI yêu cầu trả về JSON. Trả về (dict, None) hoặc (None, err)."""
    messages = [
        {"role": "system", "content": system_prompt + "\nQUAN TRỌNG: trả về DUY NHẤT một khối JSON hợp lệ, không giải thích, không markdown code fence."},
        {"role": "user", "content": user_prompt},
    ]
    text, err = await get_ai_response(session, messages, max_tokens=1800, timeout_s=timeout_s)
    if err:
        return None, err
    m = re.search(r'\{.*\}', text, re.S)
    if not m:
        return None, f"AI không trả JSON: {text[:150]}"
    try:
        return json.loads(m.group(0)), None
    except Exception as e:
        return None, f"JSON lỗi: {e} — {m.group(0)[:150]}"


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

async def fb_get(session, path, params=None, budget=1):
    """GET API-Football, trừ quota theo ngày VN. Trả về (data, None) hoặc (None, err)."""
    key = os.getenv("FOOTBALL_API_KEY")
    if not key:
        return None, "Chưa cấu hình FOOTBALL_API_KEY trong .env"
    today = datetime.now(TZ_VN).strftime('%Y-%m-%d')
    if fb_quota['day'] != today:
        fb_quota['day'] = today
        fb_quota['used'] = 0
    if fb_quota['used'] + budget > FB_DAILY_LIMIT:
        return None, f"Hết ngân sách API-Football hôm nay ({fb_quota['used']}/{FB_DAILY_LIMIT}) — dùng lại vào ngày mai hoặc gõ ít hơn."
    try:
        async with session.get(f"{FOOTBALL_BASE}{path}", params=params or {},
                               headers={"x-apisports-key": key},
                               timeout=aiohttp.ClientTimeout(total=30)) as resp:
            fb_quota['used'] += budget
            _save_fb_quota()
            if resp.status != 200:
                return None, f"HTTP {resp.status}"
            data = await resp.json(content_type=None)
            errors = data.get('errors')
            if errors and errors != 0:
                return None, f"API lỗi: {json.dumps(errors)[:150]}"
            return data.get('response') or [], None
    except Exception as e:
        return None, str(e)


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


async def get_fixtures_for_date(session, date_str, only_tracked=True):
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
    return fixtures, None


async def get_match_odds(session, fixture_id):
    """Odds 1X2 + O/U 2.5 + BTTS của trận. Trả về dict rút gọn hoặc None."""
    data, err = await fb_get(session, "/odds", {'fixture': fixture_id})
    if err or not data:
        return None
    # Dùng bookmaker đầu tiên (Bet365 ưu tiên), bet ids: 1=Match Winner, 5=Goals O/U, 8=Both Teams Score
    for entry in data:
        for bm in entry.get('bookmakers', []):
            if bm.get('id') == 8:  # Bet365
                odds = {'bookmaker': bm.get('name', '?'), '1X2': {}, 'OU25': {}, 'BTTS': {}}
                for bet in bm.get('bets', []):
                    if bet.get('id') == 1:
                        for v in bet.get('values', []):
                            odds['1X2'][v.get('value', '')] = float(v.get('odd', 0) or 0)
                    elif bet.get('id') == 5:
                        for v in bet.get('values', []):
                            label = v.get('value', '')
                            if 'Over' in label and '2.5' in label:
                                odds['OU25']['Over'] = float(v.get('odd', 0) or 0)
                            elif 'Under' in label and '2.5' in label:
                                odds['OU25']['Under'] = float(v.get('odd', 0) or 0)
                    elif bet.get('id') == 8:
                        for v in bet.get('values', []):
                            label = v.get('value', '')
                            if 'Yes' in label:
                                odds['BTTS']['Yes'] = float(v.get('odd', 0) or 0)
                            elif 'No' in label:
                                odds['BTTS']['No'] = float(v.get('odd', 0) or 0)
                return odds
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


# ═══════════════ ĐỌC TRẬN + AI DỰ ĐOÁN ═══════════════

PICK_MARKETS = {
    'HOME': '1X2 đội nhà thắng', 'DRAW': '1X2 hòa', 'AWAY': '1X2 đội khách thắng',
    'OVER25': 'Tài 2.5', 'UNDER25': 'Xỉu 2.5', 'BTTS_YES': 'Cả hai ghi bàn', 'BTTS_NO': 'Cả hai không ghi bàn',
}


async def analyze_match(session, fixture):
    """Phân tích 1 trận: odds + h2h + API prediction → AI dự đoán JSON.
    Trả về (dict dự đoán | None, err)."""
    fx_id = str(fixture['fixture']['id'])
    odds = await get_match_odds(session, fx_id)
    if not odds:
        odds = {}
    h2h = await get_h2h_summary(session, fixture['teams']['home']['id'], fixture['teams']['away']['id'])
    api_pred = await get_api_prediction(session, fx_id)
    odds_txt = (f"Odds {odds.get('bookmaker', 'nhà cái')}: 1X2 {odds.get('1X2', {})}, "
                f"O/U 2.5 {odds.get('OU25', {})}, BTTS {odds.get('BTTS', {})}") if odds else "Không có odds (kèo chưa mở)"
    user_prompt = (
        f"Trận đấu:\n{_fixture_line(fixture)}\n\n"
        f"{odds_txt}\n\n{h2h}\n\n{api_pred}\n\n"
        f"Phân tích phong độ, đối đầu, kèo — chọn MỘT kèo (HOME/DRAW/AWAY/OVER25/UNDER25/BTTS_YES/BTTS_NO), "
        f"đánh giá xác suất (%) và so odds để tìm value bet (xác suất AI × odds > 1 = đáng đánh)."
    )
    system_prompt = (
        "Bạn là chuyên gia soi kèo bóng đá của PNL FOOTBALL BOT (bot do anh Quốc — đẹp trai, giỏi nhất quả đất — tự tay xây dựng). "
        "Phân tích thực dụng: phong độ gần đây, đối đầu, động lực, lực lượng; kết hợp odds nhà cái để tìm VALUE (xác suất thật > xác suất odds). "
        "Không để ý kiến API-Fobile định hướng mù quáng — chê/c'value tùy phân tích của mày."
    )
    pred, err = await get_ai_json(session, system_prompt, user_prompt)
    if err:
        return None, err
    pick = str(pred.get('pick', '')).upper().replace(' ', '').replace('.', '')
    if pick not in PICK_MARKETS:
        pick = next((k for k in PICK_MARKETS if k in pick), None)
        if not pick:
            return None, f"AI chọn kèo lạ: {pred.get('pick')}"
    prob = float(pred.get('prob', 50))
    prob = max(1.0, min(prob, 99.0))
    odds_val = None
    if pick in ('HOME', 'DRAW', 'AWAY'):
        odds_val = odds.get('1X2', {}).get({'HOME': 'Home', 'DRAW': 'Draw', 'AWAY': 'Away'}.get(pick))
    elif pick in ('OVER25', 'UNDER25'):
        odds_val = odds.get('OU25', {}).get('Over' if pick == 'OVER25' else 'Under')
    elif pick in ('BTTS_YES', 'BTTS_NO'):
        odds_val = odds.get('BTTS', {}).get('Yes' if pick == 'BTTS_YES' else 'No')
    ev = (prob / 100 * odds_val - 1) if odds_val else None
    return {'fixture_id': fx_id, 'date': fixture['fixture']['date'][:10],
            'kickoff_vn': _vn_time(fixture['fixture']['date']),
            'league': fixture.get('league', {}).get('name', '?'),
            'home': fixture['teams']['home']['name'], 'away': fixture['teams']['away']['name'],
            'pick': pick, 'market': PICK_MARKETS[pick], 'prob': prob,
            'odds': odds_val, 'ev': round(ev, 3) if ev is not None else None,
            'reasoning': str(pred.get('reasoning', ''))[:600],
            'status': 'pending', 'result': None, 'graded': None}, None


def _pred_line(p, show_ev=True):
    ev_txt = ""
    if show_ev and p.get('ev') is not None:
        ev = p['ev']
        badge = "💰 VALUE" if ev > 0.05 else ("⚖️ cân bằng" if ev > -0.05 else "⚠️ rủi ro đắt")
        ev_txt = f" | EV {ev:+.0%} {badge}"
    o_txt = f" @ odds {p['odds']}" if p.get('odds') else ""
    star = "🔥" if (p.get('ev') or -1) > 0.08 else ("⭐" if (p.get('prob') or 0) >= 65 else "•")
    return (f"{star} {p['kickoff_vn']} [{p['league']}] {p['home']} vs {p['away']}\n"
            f"   → CHỌN: {p['market']} (xác suất {p['prob']:.0f}%){o_txt}{ev_txt}\n"
            f"   {p['reasoning'][:220]}")


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
    lines = [f"📅 *LỊCH TRẬN {date_str}* — {len(fixtures)} trận:"]
    for fx in fixtures[:25]:
        lines.append(_fixture_line(fx))
    await send_long_message(session, chat_id, "\n".join(lines))


async def cmd_keo(session, chat_id, arg=None):
    query = (arg or '').strip()
    if not query:
        await send_telegram_message(session, chat_id, "Nhập tên đội: `/kèo arsenal` — không dùng dấu tiếng Việt cũng được (ARS không hợp, dùng tên tiếng Anh).")
        return
    await send_telegram_message(session, chat_id, f"⏳ Tìm trận của '{query}' + phân tích...")
    today = datetime.now(TZ_VN)
    fixture = None
    # Tìm trong 5 ngày tới rồi 3 ngày vừa qua (để xem lại)
    for offset in range(0, 5):
        date_str = (today + timedelta(days=offset)).strftime('%Y-%m-%d')
        fixtures, err = await get_fixtures_for_date(session, date_str)
        if err or not fixtures:
            continue
        for fx in fixtures:
            h = fx['teams']['home']['name'].lower()
            a = fx['teams']['away']['name'].lower()
            if query.lower() in h or query.lower() in a:
                fixture = fx
                break
        if fixture:
            break
    if not fixture:
        await send_telegram_message(session, chat_id,
            f"Không tìm thấy trận nào có '{query}' trong 5 ngày tới (các giải theo dõi). "
            f"Kiểm tra lại tên tiếng Anh (vd: man city, arsenal, real madrid, lyon...).")
        return
    pred, err = await analyze_match(session, fixture)
    if err:
        await send_telegram_message(session, chat_id, f"❌ Lỗi phân tích: {err}")
        return
    predictions[pred['fixture_id']] = pred
    _save_predictions()
    lines = ["⚽ *PHÂN TÍCH KÈO*", _pred_line(pred)]
    await send_telegram_message(session, chat_id, "\n\n".join(lines))


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
    """Mỗi ngày 07:30 VN: dự đoán các trận hôm nay + báo Telegram."""
    session = app['session']
    last_report_day = None
    while True:
        try:
            now_vn = datetime.now(TZ_VN)
            today = now_vn.strftime('%Y-%m-%d')
            should_run = now_vn.hour >= DAILY_REPORT_HOUR_VN
            if should_run and last_report_day != today:
                last_report_day = today
                fixtures, err = await get_fixtures_for_date(session, today)
                if err:
                    logger.warning(f"[DAILY] {err}")
                    continue
                # Chỉ trận chưa đá (kickoff còn tương lai, +1h trước giờ đá)
                upcoming = [fx for fx in fixtures
                            if fx['fixture']['status']['short'] == 'NS'
                            and datetime.fromisoformat(fx['fixture']['date'].replace('Z', '+00:00'))
                            > datetime.now(timezone.utc) + timedelta(hours=1)]
                if not upcoming:
                    logger.info("[DAILY] Hôm nay không có trận nào sắp đá.")
                    continue
                upcoming = upcoming[:MAX_PREDICTIONS_PER_DAY]
                preds = []
                for fx in upcoming:
                    pred, err = await analyze_match(session, fx)
                    if err:
                        logger.warning(f"[DAILY] Lỗi phân tích {fx['teams']['home']['name']}: {err}")
                        continue
                    preds.append(pred)
                    predictions[pred['fixture_id']] = pred
                _save_predictions()
                if preds:
                    preds.sort(key=lambda p: -(p.get('ev') or -1))
                    lines = ["⚽ *DỰ ĐOÁN KÈO HÔM NAY* — xếp theo EV (đáng đánh nhất trên cùng):", ""]
                    for p in preds:
                        lines.append(_pred_line(p))
                        lines.append("")
                    lines.append("⚠️ Dự đoán chỉ THAM KHẢO — cá cược có rủi ro, đánh có kỷ luật (không all-in, không gà gáy).")
                    text = "\n".join(lines)
                    for cid in list(auto_chats):
                        await send_long_message(session, cid, text)
                    logger.info(f"[DAILY] Đã gửi {len(preds)} dự đoán.")
                else:
                    logger.info("[DAILY] Không phân tích được trận nào hôm nay.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Lỗi trong daily_predictions_loop: {e}")
        await asyncio.sleep(600)  # check mỗi 10 phút, chạy 1 lần/ngày


def _grade_prediction(p, fixture):
    """Chấm 1 dự đoán theo kết quả thật. Trả về 'win'/'loss'/'push' + result text."""
    gh, ga = fixture['goals']['home'], fixture['goals']['away']
    if gh is None or ga is None:
        return None, None
    gh, ga = int(gh), int(ga)
    total = gh + ga
    pick = p['pick']
    result_text = f"tỷ số {gh}-{ga}"
    if pick in ('HOME', 'DRAW', 'AWAY'):
        outcome = 'HOME' if gh > ga else ('DRAW' if gh == ga else 'AWAY')
        if outcome == pick:
            return 'win', result_text
        if gh == ga:
            return 'push', result_text
        return 'loss', result_text
    if pick == 'OVER25':
        return ('win' if total >= 3 else ('push' if total == 2 else 'loss')), f"tổng {total} bàn"
    if pick == 'UNDER25':
        return ('win' if total <= 2 else ('push' if total == 2 else 'loss')), f"tổng {total} bàn"
    if pick == 'BTTS_YES':
        return ('win' if gh > 0 and ga > 0 else 'loss'), result_text
    if pick == 'BTTS_NO':
        return ('win' if gh == 0 or ga == 0 else 'loss'), result_text
    return None, None


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
                        if st:
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


async def handle_update(session, update):
    msg = update.get('message')
    cb = update.get('callback_query')
    if cb:
        cb_data = cb.get('data', '')
        msg_obj = cb.get('message') or {}
        chat_id = msg_obj.get('chat', {}).get('id')
        if chat_id and cb_data.startswith(('setmodel:', 'modelpage:')):
            await handle_model_callback(session, chat_id, cb_data, message_id=msg_obj.get('message_id'))
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
    if not chat_id or not text:
        return
    auto_chats.add(chat_id)
    _save_chats()

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
        # Tin nhắn tự do → AI trò chuyện bóng đá
        await ai_chat(session_http, chat_id, text, msg.get('message_id'))


async def ai_chat(session, chat_id, question, reply_to=None):
    """Hỏi tự do về bóng đá — AI trả lời với ngữ cảnh: trận hôm nay + lịch sử dự đoán gần đây."""
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
    text, err = await get_ai_response(session, [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Ngữ cảnh:\n{context}\n\nCâu hỏi: {question}"},
    ])
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


async def telegram_polling_loop(app):
    session = app['session']
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    offset = 0
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    logger.info("Bắt đầu long polling Telegram updates...")
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
                    asyncio.create_task(handle_update(session, update))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Lỗi polling: {e}")
            await asyncio.sleep(5)


# ═══════════════ STARTUP ═══════════════

async def on_startup(app):
    logger.info("⚽ PNL FOOTBALL BOT khởi động...")
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
