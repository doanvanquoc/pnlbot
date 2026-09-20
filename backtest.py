"""Backtest rule-only 1h: python3 backtest.py [số_coin] [số_nến_1h].

Tái sử dụng app_trading.analyze_market, KHÔNG replay AI, MTF, BTC filter, OI,
orderbook, account risk guard hay execution 30 giây. Không phải EV full strategy.
Baseline/current cố định trước khi xem test; split thời gian 70/30, purge horizon.
Fill next-open; taker fee 5bp mỗi fill, adverse slippage 5bp mỗi chiều mặc định.
SL trước partial/TP trong nến mơ hồ; trailing từ close có hiệu lực nến tiếp theo.
Funding public lịch sử bắt buộc; không đọc dotenv hoặc gọi bot lifecycle.
"""
import asyncio
import hashlib
import json
import math
import os
import sys
import time
from dataclasses import dataclass

import aiohttp
import pandas as pd

import app_trading as engine

MIN_BARS = 300
MAX_HOLD_BARS = int(engine.AUTO_MAX_HOLD_HOURS)
BINANCE_MAX_LIMIT = 1500
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest_cache")
PUBLIC_URL = "https://fapi.binance.com"
FEE_RATE = 0.0005
SLIPPAGE = 0.0005
TRAILING_CONFIGS = [
    ("Baseline TP/SL", {}),
    ("Current exits (xấp xỉ 1h)", {
        'breakeven_rr': engine.AUTO_BE_RR,
        'trail_start_rr': engine.AUTO_TRAIL_START_RR,
        'trail_atr_mult': engine.AUTO_TRAIL_ATR_MULT,
        'trail_min_rr': engine.AUTO_TRAIL_MIN_RR,
        'cancel_tp_on_trail': engine.AUTO_CANCEL_TP_ON_TRAIL,
        'partial_tp_rr': engine.AUTO_PARTIAL_TP_RR,
        'partial_tp_pct': engine.AUTO_PARTIAL_TP_PCT,
    }),
]
KLINE_COLUMNS = [
    'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
    'quote_asset_volume', 'number_of_trades', 'taker_buy_base', 'taker_buy_quote', 'ignore',
]


async def public_json(session, path, params=None):
    async with session.get(PUBLIC_URL + path, params=params) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Public data {path}: HTTP {resp.status}")
        return await resp.json()


def save_snapshot(path, payload):
    """Không ghi đè snapshot đã tồn tại; JSON public, không pickle executable."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    try:
        with open(path, 'x', encoding='utf-8') as stream:
            json.dump(payload, stream, allow_nan=False, separators=(',', ':'))
    except FileExistsError:
        pass


async def fetch_klines(session, symbol, interval, total_bars, end_time=None):
    end_time = int(end_time if end_time is not None else time.time() * 1000)
    path = os.path.join(CACHE_DIR, f"v2_{symbol}_{interval}_{total_bars}_{end_time}.json")
    if os.path.exists(path):
        with open(path, encoding='utf-8') as stream:
            rows = json.load(stream)
    else:
        rows = []
        cursor = end_time
        # Một nến đang chạy có thể bị loại, nên tải thêm một nến.
        while len(rows) < total_bars + 1:
            limit = min(BINANCE_MAX_LIMIT, total_bars + 1 - len(rows))
            page = await public_json(session, '/fapi/v1/klines', {
                'symbol': symbol, 'interval': interval, 'limit': limit, 'endTime': cursor,
            })
            if not isinstance(page, list) or not page:
                raise RuntimeError(f"Thiếu klines {symbol}")
            next_cursor = int(page[0][0]) - 1
            if next_cursor >= cursor:
                raise RuntimeError(f"Klines không tiến trang: {symbol}")
            rows = page + rows
            cursor = next_cursor
        rows = [row for row in rows if int(row[6]) < end_time][-total_bars:]
        if len(rows) != total_bars:
            raise RuntimeError(f"Không đủ {total_bars} nến đóng: {symbol}")
        save_snapshot(path, rows)
    df = pd.DataFrame(rows, columns=KLINE_COLUMNS)
    for col in ('open', 'high', 'low', 'close', 'volume'):
        df[col] = df[col].astype(float)
    for col in ('open_time', 'close_time'):
        df[col] = df[col].astype('int64')
    if len(df) != total_bars or not df['open_time'].is_unique:
        raise ValueError(f"Snapshot klines không hợp lệ: {symbol}")
    if interval == '1h' and not df['open_time'].diff().iloc[1:].eq(3600000).all():
        raise ValueError(f"Snapshot thiếu nến 1h: {symbol}")
    if not (df['close_time'] < end_time).all():
        raise ValueError(f"Snapshot chứa nến chưa đóng: {symbol}")
    return df


async def fetch_funding(session, symbol, start_time, end_time):
    """Lấy toàn bộ lịch sử trong khoảng; lỗi/thiếu lịch sử không quy về zero."""
    path = os.path.join(CACHE_DIR, f"v2_funding_{symbol}_{start_time}_{end_time}.json")
    if os.path.exists(path):
        with open(path, encoding='utf-8') as stream:
            rows = json.load(stream)
    else:
        rows = []
        cursor = start_time
        while cursor <= end_time:
            page = await public_json(session, '/fapi/v1/fundingRate', {
                'symbol': symbol, 'startTime': cursor, 'endTime': end_time, 'limit': 1000,
            })
            if not isinstance(page, list):
                raise RuntimeError(f"Funding không hợp lệ: {symbol}")
            if not page:
                break
            next_cursor = int(page[-1]['fundingTime']) + 1
            if next_cursor <= cursor:
                raise RuntimeError(f"Funding không tiến trang: {symbol}")
            rows.extend(page)
            cursor = next_cursor
        if not rows:
            raise RuntimeError(f"Không có funding lịch sử: {symbol}; không giả định bằng 0")
        save_snapshot(path, rows)
    times = [int(row['fundingTime']) for row in rows]
    # Ngưỡng 24h chỉ kiểm tra coverage, KHÔNG giả định settlement luôn 8h.
    if (not times or times != sorted(set(times)) or times[0] < start_time
            or times[-1] > end_time or max(b - a for a, b in
            zip([start_time] + times, times + [end_time])) > 86400000):
        raise ValueError(f"Funding thiếu coverage: {symbol}")
    for row in rows:
        if (not math.isfinite(float(row['fundingRate']))
                or not math.isfinite(float(row['markPrice'])) or not float(row['markPrice']) > 0):
            raise ValueError(f"Funding thiếu rate/markPrice: {symbol}")
    return rows


@dataclass(frozen=True)
class SimulationResult:
    outcome: str
    r: float
    exit_idx: int
    exit_ts: int
    cashflows: tuple


def simulate_outcome(df, start_idx, side, tp, sl, max_hold, breakeven_rr=0.0,
                     trail_start_rr=None, trail_atr_mult=1.5, cancel_tp_on_trail=False,
                     atr=None, partial_tp_rr=0.0, partial_tp_pct=0.5, trail_min_rr=0.0,
                     fee_rate=FEE_RATE, slippage=SLIPPAGE, funding_events=None,
                     include_funding=True, return_details=False):
    """R = risk dự kiến ở signal close; actual entry = next-open + adverse slip.

    Nến thiếu horizon trả censored, không biến thành expiry 0R. Expiry đóng toàn bộ
    phần còn lại theo close có phí/slip. Gap SL/TP khớp open thực tế, không giá cũ.
    Intrabar không biết timestamp: fill gán close_time, funding cả lượng đầu nến
    tới timestamp đó (bảo thủ về thời gian phơi nhiễm, không suy diễn high/low order).
    funding_events=[] chỉ dành cho dữ liệu injected đã xác nhận không có settlement.
    """
    if side not in ('LONG', 'SHORT') or start_idx < 1 or max_hold < 1:
        raise ValueError("Side/start/horizon không hợp lệ")
    if not (0 <= fee_rate < 1 and 0 <= slippage < 1 and 0 < partial_tp_pct <= 1):
        raise ValueError("Phí/slippage/partial không hợp lệ")
    if include_funding and funding_events is None:
        raise ValueError("Yêu cầu funding nhưng chưa có dữ liệu lịch sử")
    if start_idx + max_hold > len(df):
        result = SimulationResult('censored', 0.0, -1, -1, ())
        return result if return_details else (result.outcome, result.r)
    direction = 1 if side == 'LONG' else -1
    reference = float(df['close'].iloc[start_idx - 1])
    risk = direction * (reference - float(sl))
    if (not math.isfinite(risk) or risk <= 0 or not math.isfinite(float(tp))
            or float(sl) <= 0 or float(tp) <= 0 or direction * (float(tp) - reference) <= 0):
        raise ValueError("TP/SL không hợp lệ tại signal close")
    entry = float(df['open'].iloc[start_idx]) * (1 + direction * slippage)
    entry_ts = int(df['open_time'].iloc[start_idx])
    if not math.isfinite(entry) or entry <= 0:
        raise ValueError("Entry không hợp lệ")
    sl_cur, tp_cur = float(sl), float(tp)
    if atr is None:
        history = df.iloc[:start_idx]
        previous = history['close'].shift(1)
        tr = pd.concat([history['high'] - history['low'],
                        (history['high'] - previous).abs(),
                        (history['low'] - previous).abs()], axis=1).max(axis=1)
        atr = float(tr.ewm(span=14, adjust=False).mean().iloc[-1])
    if not math.isfinite(float(atr)) or atr <= 0:
        raise ValueError("ATR không hợp lệ")
    remaining, partial_done = 1.0, False
    flows = [(entry_ts, -entry * fee_rate / risk)]
    funding = sorted(funding_events or [], key=lambda row: int(row['fundingTime']))
    funding_idx = 0

    def accrue(until):
        nonlocal funding_idx
        if not include_funding:
            return
        while funding_idx < len(funding) and int(funding[funding_idx]['fundingTime']) <= until:
            row = funding[funding_idx]
            ts = int(row['fundingTime'])
            if ts > entry_ts:
                rate, mark = float(row['fundingRate']), float(row['markPrice'])
                if not math.isfinite(rate) or not math.isfinite(mark) or mark <= 0:
                    raise ValueError("Funding không hợp lệ")
                flows.append((ts, -direction * rate * mark * remaining / risk))
            funding_idx += 1

    def close_part(price, qty, ts):
        fill = price * (1 - direction * slippage)
        flows.append((ts, qty * (direction * (fill - entry) - fill * fee_rate) / risk))

    def finish(j, ts, expired=False):
        r = sum(value for _, value in flows)
        outcome = 'expired' if expired else ('win' if r > 1e-10 else 'loss' if r < -1e-10 else 'be')
        result = SimulationResult(outcome, r, j, ts, tuple(flows))
        return result if return_details else (result.outcome, result.r)

    for j in range(start_idx, start_idx + max_hold):
        row = df.iloc[j]
        op, high, low, close = (float(row[key]) for key in ('open', 'high', 'low', 'close'))
        ts_open, ts_close = int(row['open_time']), int(row['close_time'])
        if (not all(math.isfinite(p) and p > 0 for p in (op, high, low, close))
                or low > min(op, close) or high < max(op, close) or low > high):
            raise ValueError("OHLC không hợp lệ")
        # Gap xảy ra trước mọi biến động intrabar và trước trailing mới.
        if direction * (op - sl_cur) <= 0 or (tp_cur is not None and direction * (op - tp_cur) >= 0):
            accrue(ts_open)
            close_part(op, remaining, ts_open)
            return finish(j, ts_open)
        accrue(ts_close)
        adverse, favorable = (low, high) if direction == 1 else (high, low)
        if direction * (adverse - sl_cur) <= 0:
            close_part(sl_cur, remaining, ts_close)
            return finish(j, ts_close)
        partial_price = entry + direction * risk * partial_tp_rr
        partial_hit = (not partial_done and partial_tp_rr > 0
                       and direction * (favorable - partial_price) >= 0)
        tp_hit = tp_cur is not None and direction * (favorable - tp_cur) >= 0
        if tp_hit and (not partial_hit or direction * (tp_cur - partial_price) <= 0):
            close_part(tp_cur, remaining, ts_close)
            return finish(j, ts_close)
        if partial_hit:
            close_part(partial_price, partial_tp_pct, ts_close)
            remaining -= partial_tp_pct
            partial_done = True
            if remaining <= 0:
                return finish(j, ts_close)
            # BE có hiệu lực từ nến kế tiếp; không giả high rồi low trong nến này.
            sl_cur = max(sl_cur, entry) if direction == 1 else min(sl_cur, entry)
        if tp_hit:
            close_part(tp_cur, remaining, ts_close)
            return finish(j, ts_close)
        if j == start_idx + max_hold - 1:
            close_part(close, remaining, ts_close)
            return finish(j, ts_close, expired=True)
        # Chỉ dùng close đã quan sát, không dùng extreme để giả replay từng tick.
        rr = direction * (close - entry) / risk
        target = None
        if trail_start_rr and rr >= trail_start_rr:
            target = close - direction * atr * trail_atr_mult
        elif breakeven_rr > 0 and rr >= breakeven_rr:
            target = entry
        if target is not None and direction * (target - sl_cur) >= max(trail_min_rr * risk, 1e-12):
            sl_cur = target
            if cancel_tp_on_trail and trail_start_rr and rr >= trail_start_rr:
                tp_cur = None


async def backtest_symbol(session, symbol, total_bars, step=3, df=None, funding_events=None):
    if df is None:
        df = await fetch_klines(session, symbol, '1h', total_bars)
    if len(df) < MIN_BARS + MAX_HOLD_BARS + 1:
        raise ValueError(f"Không đủ warmup/horizon: {symbol}")
    if funding_events is None:
        funding_events = await fetch_funding(session, symbol, int(df['open_time'].iloc[0]),
                                             int(df['close_time'].iloc[-1]))
    if any(symbol not in cache for cache in (engine.symbol_precisions,
            engine.symbol_price_precisions, engine.symbol_tick_sizes)):
        raise ValueError(f"Thiếu exchangeInfo cache: {symbol}")
    raw_signals = []
    for i in range(MIN_BARS, len(df) - MAX_HOLD_BARS, step):
        # Engine bỏ hàng cuối; sentinel là bản sao nến hiện tại, không đưa future vào.
        history = pd.concat([df.iloc[max(0, i - 498):i + 1], df.iloc[i:i + 1]], ignore_index=True)
        res = await engine.analyze_market(session, symbol, interval='1h', df=history, fetch_extras=False)
        if res is None:
            raise RuntimeError(f"Engine không trả dữ liệu {symbol}, bar {i}")
        if res.get('signal') not in ('LONG', 'SHORT') or res.get('confidence') not in ('Mạnh', 'Rất mạnh'):
            continue
        if not res.get('tp') or not res.get('sl'):
            raise ValueError(f"Signal thiếu TP/SL: {symbol}")
        raw_signals.append({
            'symbol': symbol, 'df': df, 'start': i + 1, 'side': res['signal'],
            'entry': res['close'], 'tp': res['tp'], 'sl': res['sl'],
            'score': res['long_score'] if res['signal'] == 'LONG' else res['short_score'],
            'ts': int(df['open_time'].iloc[i + 1]), 'atr': float(res['atr']),
            'funding_events': funding_events,
        })
    print(f"{symbol}: {len(raw_signals)} candidates rule-only; cuối mẫu thiếu horizon đã censor.")
    return raw_signals


def paired_results(signals, configs=TRAILING_CONFIGS):
    """Cùng entry cho mọi cấu hình; khóa symbol tới exit muộn nhất của cặp.

    Đây là paired exit experiment, không phải mỗi chiến lược tự tái nhập độc lập.
    """
    results = [[] for _ in configs]
    occupied = {}
    for signal in sorted(signals, key=lambda s: (s['ts'], s['symbol'])):
        if signal['ts'] <= occupied.get(signal['symbol'], -1):
            continue
        pair = [simulate_outcome(signal['df'], signal['start'], signal['side'],
                signal['tp'], signal['sl'], MAX_HOLD_BARS, atr=signal['atr'],
                funding_events=signal['funding_events'], return_details=True, **cfg)
                for _, cfg in configs]
        if any(result.outcome == 'censored' for result in pair):
            continue
        occupied[signal['symbol']] = max(result.exit_ts for result in pair)
        for target, result in zip(results, pair):
            target.append(result)
    return results


def summarize(results):
    counts = {key: sum(r.outcome == key for r in results) for key in ('win', 'loss', 'be', 'expired')}
    cashflows = {}
    for result in results:
        for ts, value in result.cashflows:
            cashflows[ts] = cashflows.get(ts, 0.0) + value
    equity = peak = max_dd = 0.0
    for ts in sorted(cashflows):
        equity += cashflows[ts]
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    gains = sum(max(r.r, 0) for r in results)
    losses = -sum(min(r.r, 0) for r in results)
    return {
        'total': len(results), 'counts': counts,
        'net_win_rate': sum(r.r > 0 for r in results) / len(results) if results else None,
        'ev_r': sum(r.r for r in results) / len(results) if results else None,
        'profit_factor': gains / losses if losses else None,
        'realized_max_dd_r': max_dd,
    }


def print_comparison(signals, split_ts):
    # Purge toàn bộ horizon trước ranh giới, không dùng actual exit để chọn train.
    train = [s for s in signals if int(s['df']['close_time'].iloc[s['start'] + MAX_HOLD_BARS - 1]) < split_ts]
    test = [s for s in signals if s['ts'] >= split_ts]
    reports = {}
    for label, subset in (('TRAIN 70%', train), ('TEST ngoài mẫu 30%', test)):
        reports[label] = {}
        print(f"\n{label} — không chọn/tune config từ TEST")
        for (name, _), results in zip(TRAILING_CONFIGS, paired_results(subset)):
            stats = summarize(results)
            reports[label][name] = stats
            if not stats['total']:
                print(f"{name}: 0 lệnh; không thể kết luận EV.")
                continue
            pf = f"{stats['profit_factor']:.2f}" if stats['profit_factor'] is not None else 'N/A (không có lỗ)'
            print(f"{name}: n={stats['total']}, net WR={stats['net_win_rate']:.1%}, "
                  f"EV rule-only={stats['ev_r']:+.3f}R, PF={pf}, "
                  f"realized MaxDD={stats['realized_max_dd_r']:.3f}R, {stats['counts']}")
    return reports


async def main():
    num_coins = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    total_bars = int(sys.argv[2]) if len(sys.argv) > 2 else 2000
    if len(sys.argv) > 3 or not 1 <= num_coins <= 100 or not MIN_BARS + MAX_HOLD_BARS + 1 <= total_bars <= 100000:
        raise ValueError("Cách dùng: backtest.py [1..100 coin] [đủ warmup+horizon .. 100000 nến]")
    started = time.monotonic()
    manifest_path = os.path.join(CACHE_DIR, f"v2_dataset_{num_coins}_{total_bars}.json")
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        if os.path.exists(manifest_path):
            with open(manifest_path, encoding='utf-8') as stream:
                manifest = json.load(stream)
        else:
            info = await public_json(session, '/fapi/v1/exchangeInfo')
            tickers = await public_json(session, '/fapi/v1/ticker/24hr')
            eligible = {s['symbol'] for s in info['symbols'] if s.get('status') == 'TRADING'
                        and s.get('quoteAsset') == 'USDT' and s.get('contractType') == 'PERPETUAL'}
            ranked = sorted((t for t in tickers if t['symbol'] in eligible),
                            key=lambda t: float(t['quoteVolume']), reverse=True)
            coins = [t['symbol'] for t in ranked[:num_coins]]
            if len(coins) != num_coins:
                raise RuntimeError("Thiếu symbol đủ điều kiện")
            manifest = {'end_time': int(time.time() * 1000), 'coins': coins,
                        'exchange_info': [s for s in info['symbols'] if s['symbol'] in coins]}
            save_snapshot(manifest_path, manifest)
            # Nếu một tiến trình khác thắng exclusive create, dùng đúng snapshot đó.
            with open(manifest_path, encoding='utf-8') as stream:
                manifest = json.load(stream)
        for info in manifest['exchange_info']:
            symbol = info['symbol']
            engine.symbol_precisions[symbol] = int(info['quantityPrecision'])
            engine.symbol_price_precisions[symbol] = int(info['pricePrecision'])
            engine.symbol_tick_sizes[symbol] = float(next(f['tickSize'] for f in info['filters'] if f['filterType'] == 'PRICE_FILTER'))
        print("RULE-ONLY 1h, không phải AI+MTF/full strategy; không có historical AI verdict.")
        print(f"Phí taker/fill={FEE_RATE:.2%}, adverse slip/chiều={SLIPPAGE:.2%}; funding public bắt buộc.")
        print("SL-first; trailing close→nến sau; intrabar fill timestamp=close; expiry MTM có costs.")
        print("MaxDD cashflow realized theo thời gian, KHÔNG phải mark-to-market portfolio drawdown.")
        print("1R cố định/lệnh; chưa replay leverage/liquidation, tick/lot/min-notional hay giới hạn tài khoản.")
        print("OHLC trade-price xấp xỉ trigger MARK_PRICE; funding intrabar dùng lượng đầu nến.")
        print("Universe theo volume hiện tại có selection/survivorship bias; không chứng minh AI cải thiện.")
        print(f"Snapshot immutable: {os.path.basename(manifest_path)}, coins={manifest['coins']}")
        all_signals, datasets = [], []
        digest = hashlib.sha256()
        for symbol in manifest['coins']:
            df = await fetch_klines(session, symbol, '1h', total_bars, manifest['end_time'])
            funding = await fetch_funding(session, symbol, int(df['open_time'].iloc[0]), int(df['close_time'].iloc[-1]))
            digest.update(symbol.encode())
            digest.update(df.to_json(orient='split', double_precision=15).encode())
            digest.update(json.dumps(funding, sort_keys=True).encode())
            datasets.append(df)
            all_signals.extend(await backtest_symbol(session, symbol, total_bars, df=df, funding_events=funding))
        start = max(int(df['open_time'].iloc[MIN_BARS + 1]) for df in datasets)
        end = min(int(df['close_time'].iloc[-1]) for df in datasets)
        split_ts = start + int((end - start) * 0.7)
        print(f"Dataset SHA256={digest.hexdigest()}, split UTC={pd.to_datetime(split_ts, unit='ms', utc=True)}")
        print_comparison(all_signals, split_ts)
        print(f"Hoàn tất backtest rule-only trong {time.monotonic() - started:.1f}s.")


if __name__ == '__main__':
    try:
        asyncio.run(asyncio.wait_for(main(), timeout=1800))
    except (Exception, KeyboardInterrupt) as exc:
        print(f"Backtest thất bại: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)