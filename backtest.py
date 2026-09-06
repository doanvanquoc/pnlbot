"""
Backtest hiệu chỉnh ngưỡng điểm cho scoring engine của lệnh /a.

Cách dùng:
    python3 backtest.py [số_coin] [số_nến_1h]

Ví dụ:
    python3 backtest.py            # 10 coin, 2000 nến 1h (~83 ngày)
    python3 backtest.py 5 3000

Script tái sử dụng đúng engine chấm điểm của app.py (analyze_market), mô phỏng
kết quả TP chạm trước hay SL chạm trước (RR 1:1, SL ưu tiên khi cùng nến),
rồi thống kê win-rate theo band độ tin cậy (4⭐ Mạnh / 5⭐ Rất mạnh).

Ngoài ra còn sweep các cấu hình bảo vệ (breakeven + trailing stop) để đo lợi ích
TRƯỚC khi bật ngoài thực tế: win-rate, EV/R, profit factor, max drawdown.
"""
import asyncio
import os
import sys
import time

import aiohttp
import pandas as pd
from dotenv import load_dotenv

from app import analyze_market

MIN_BARS = 300          # Số nến warmup tối thiểu trước khi đánh giá tín hiệu
MAX_HOLD_BARS = 72      # Giữ vị thế tối đa 72 nến 1h (~3 ngày) rồi tính expired
BINANCE_MAX_LIMIT = 1500

# Cache klines xuống đĩa để chạy lại sweep nhanh (key: symbol_interval_bars)
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest_cache")

# Các cấu hình bảo vệ lệnh để sweep (so sánh trên cùng bộ tín hiệu)
TRAILING_CONFIGS = [
    ("Engine TP/SL (không bảo vệ)", dict()),
    ("+ BE 0.5R + Trail 0.8R/1.0ATR", dict(breakeven_rr=0.5, trail_start_rr=0.8, trail_atr_mult=1.0)),
    ("+ BE 0.5R + Trail 0.8R/1.0ATR, cancel TP", dict(breakeven_rr=0.5, trail_start_rr=0.8, trail_atr_mult=1.0, cancel_tp_on_trail=True)),
    ("+ Trail 0.8R/1.0ATR, cancel TP", dict(trail_start_rr=0.8, trail_atr_mult=1.0, cancel_tp_on_trail=True)),
    ("+ Trail + partial TP 1.0R (50%)", dict(trail_start_rr=0.8, trail_atr_mult=1.0, cancel_tp_on_trail=True, partial_tp_rr=1.0, partial_tp_pct=0.5)),
    ("+ Trail + partial TP 1.5R (50%)", dict(trail_start_rr=0.8, trail_atr_mult=1.0, cancel_tp_on_trail=True, partial_tp_rr=1.5, partial_tp_pct=0.5)),
    ("+ Trail + partial TP 2.0R (50%)", dict(trail_start_rr=0.8, trail_atr_mult=1.0, cancel_tp_on_trail=True, partial_tp_rr=2.0, partial_tp_pct=0.5)),
]


async def fetch_klines(session, symbol, interval, total_bars):
    """Lấy `total_bars` nến lịch sử (đã đóng) bằng cách phân trang ngược theo endTime.
    Có cache xuống đĩa (backtest_cache/) để chạy lại sweep nhanh."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(CACHE_DIR, f"{symbol}_{interval}_{total_bars}.pkl")
    if os.path.exists(cache_path):
        try:
            df = pd.read_pickle(cache_path)
            if len(df) >= total_bars - 2:
                return df
        except Exception as e:
            print(f"⚠️ Cache {symbol} lỗi ({e}), tải lại.")
    all_klines = []
    end_time = None
    while len(all_klines) < total_bars:
        limit = min(BINANCE_MAX_LIMIT, total_bars - len(all_klines))
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
        if end_time is not None:
            url += f"&endTime={end_time}"
        async with session.get(url) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise Exception(f"Lỗi lấy klines {symbol}: HTTP {resp.status} - {body[:200]}")
            data = await resp.json()
        if not isinstance(data, list) or not data:
            break
        all_klines = data + all_klines
        end_time = data[0][0] - 1
        if len(data) < limit:
            break
    # Bỏ nến cuối (có thể đang hình thành)
    df = pd.DataFrame(all_klines[:-1], columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base', 'taker_buy_quote', 'ignore'
    ])
    for col in ('open', 'high', 'low', 'close', 'volume'):
        df[col] = df[col].astype(float)
    try:
        df.to_pickle(cache_path)
    except Exception as e:
        print(f"⚠️ Không lưu được cache {symbol}: {e}")
    return df


def simulate_outcome(df, start_idx, side, tp, sl, max_hold, breakeven_rr=0.0,
                     trail_start_rr=None, trail_atr_mult=1.5, cancel_tp_on_trail=False, atr=None,
                     partial_tp_rr=0.0, partial_tp_pct=0.5):
    """Mô phỏng từ nến start_idx (vào lệnh ở open của nến này).

    Trả về (outcome, r):
      outcome: 'win' / 'loss' / 'be' (breakeven scratch, r≈0) / 'expired'.
      r: kết quả tính theo đơn vị R (risk). win = +RR (hoặc dương ít hơn nếu trailing
         cắt ở SL đã trên entry), loss = -1 (hoặc âm ít hơn), be/expired = 0.

    breakeven_rr > 0: khi giá đạt +breakeven_rr×R theo hướng đúng → kéo SL về entry.
    trail_start_rr > 0: khi giá đạt +trail_start_rr×R → trailing SL cách giá hiện tại
                        trail_atr_mult×ATR (chỉ siết chặt, không nới).
    cancel_tp_on_trail: khi trailing bắt đầu → BỎ TP cố định, chỉ thoát bằng trailing
                        stop (chiến lược "để lời chạy").
    partial_tp_rr > 0: khi giá đạt +partial_tp_rr×R → CHỐT LỜI MỘT PHẦN (partial_tp_pct
                       của khối lượng) tại mức đó, phần còn lại chạy tiếp với SL kéo
                       về entry + trailing. R quy đổi theo toàn bộ vị thế.
    SL luôn được ưu tiên khi TP và SL cùng chạm trong 1 nến (giả định bảo thủ).
    """
    entry = float(df['close'].iloc[start_idx - 1])
    sl_cur = float(sl)
    tp_f = float(tp)
    risk = abs(entry - sl_cur)
    if risk <= 0 or risk != risk:
        return 'expired', 0.0
    if atr is None:
        if 'atr' in df.columns:
            atr = float(df['atr'].iloc[start_idx - 1])
        else:
            high_low = df['high'] - df['low']
            high_close = (df['high'] - df['close'].shift(1)).abs()
            low_close = (df['low'] - df['close'].shift(1)).abs()
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = float(tr.ewm(span=14, adjust=False).mean().iloc[start_idx - 1])
    atr = float(atr)
    if atr <= 0 or atr != atr:
        atr = risk / 1.5

    partial_done = False  # đã chốt lời một phần chưa
    partial_gain = 0.0    # R đã khóa từ phần chốt sớm (quy theo toàn bộ vị thế)
    remaining = 1.0       # tỷ lệ khối lượng còn giữ (1.0 → sau partial là 1 - pct)

    for j in range(start_idx, min(start_idx + max_hold, len(df))):
        high = float(df['high'].iloc[j])
        low = float(df['low'].iloc[j])
        # Chốt lời một phần: khi giá chạm +partial_tp_rr×R
        if not partial_done and partial_tp_rr and partial_tp_rr > 0:
            if (side == 'LONG' and high >= entry + risk * partial_tp_rr) or \
               (side == 'SHORT' and low <= entry - risk * partial_tp_rr):
                partial_done = True
                partial_gain = partial_tp_pct * partial_tp_rr
                remaining = 1.0 - partial_tp_pct
                # Kéo SL về entry cho phần còn lại (bảo toàn vốn), TP vẫn giữ
                if side == 'LONG' and sl_cur < entry:
                    sl_cur = entry
                elif side == 'SHORT' and sl_cur > entry:
                    sl_cur = entry
                if remaining <= 0:
                    return 'win', partial_gain
                continue
        if side == 'LONG':
            if low <= sl_cur:
                r = partial_gain + (sl_cur - entry) / risk * remaining
                if sl_cur >= entry:
                    return ('win', r) if r >= 0.1 else ('be', r)
                return 'loss', r
            if tp_f is not None and high >= tp_f:
                return 'win', partial_gain + (tp_f - entry) / risk * remaining
            if trail_start_rr and trail_start_rr > 0:
                if high >= entry + risk * trail_start_rr:
                    new_sl = max(sl_cur, high - atr * trail_atr_mult)
                    if new_sl > sl_cur:
                        sl_cur = new_sl
                    if cancel_tp_on_trail:
                        tp_f = None
                elif breakeven_rr > 0 and high >= entry + risk * breakeven_rr and sl_cur < entry:
                    sl_cur = entry
            elif breakeven_rr > 0 and high >= entry + risk * breakeven_rr and sl_cur < entry:
                sl_cur = entry
        else:
            if high >= sl_cur:
                r = partial_gain + (entry - sl_cur) / risk * remaining
                if sl_cur <= entry:
                    return ('win', r) if r >= 0.1 else ('be', r)
                return 'loss', r
            if tp_f is not None and low <= tp_f:
                return 'win', partial_gain + (entry - tp_f) / risk * remaining
            if trail_start_rr and trail_start_rr > 0:
                if low <= entry - risk * trail_start_rr:
                    new_sl = min(sl_cur, low + atr * trail_atr_mult)
                    if new_sl < sl_cur:
                        sl_cur = new_sl
                    if cancel_tp_on_trail:
                        tp_f = None
                elif breakeven_rr > 0 and low <= entry - risk * breakeven_rr and sl_cur > entry:
                    sl_cur = entry
            elif breakeven_rr > 0 and low <= entry - risk * breakeven_rr and sl_cur > entry:
                sl_cur = entry
    return 'expired', partial_gain if partial_done else 0.0


async def backtest_symbol(session, symbol, total_bars, step=3):
    df = await fetch_klines(session, symbol, '1h', total_bars)
    if len(df) < MIN_BARS + 50:
        print(f"⚠️ {symbol}: chỉ có {len(df)} nến, bỏ qua.")
        return {}, []

    stats = {}
    raw_signals = []  # dict: df, start, side, entry, tp, sl, score, ts, rr
    evaluated = 0
    for i in range(MIN_BARS, len(df) - 2, step):
        # Truyền df.iloc[:i+2]: analyze_market sẽ bỏ nến cuối,
        # nên engine đánh giá đúng với dữ liệu đến nến i, entry = close[i]
        try:
            res = await analyze_market(session, symbol, interval='1h', df=df.iloc[:i + 2], fetch_extras=False)
        except Exception as e:
            print(f"⚠️ {symbol} lỗi tại nến {i}: {e}")
            continue
        if not res or res.get('signal') not in ('LONG', 'SHORT'):
            continue
        if res.get('confidence') not in ('Mạnh', 'Rất mạnh'):
            continue
        if not res.get('tp') or not res.get('sl'):
            continue

        evaluated += 1
        outcome, _ = simulate_outcome(df, i + 1, res['signal'], res['tp'], res['sl'], MAX_HOLD_BARS,
                                      atr=(res.get('atr') or None))
        band = '5⭐' if res['confidence'] == 'Rất mạnh' else '4⭐'
        st = stats.setdefault(band, {'win': 0, 'loss': 0, 'expired': 0, 'total': 0})
        st['total'] += 1
        if outcome in ('win', 'loss'):
            st[outcome] += 1
        else:
            st['expired'] += 1

        # Bucket theo điểm số gốc để tìm ngưỡng tối ưu
        score = res['long_score'] if res['signal'] == 'LONG' else res['short_score']
        if score < 4.5:
            bucket = '<4.5'
        elif score < 5.0:
            bucket = '4.5-5.0'
        elif score < 6.0:
            bucket = '5.0-6.0'
        elif score < 8.0:
            bucket = '6.0-8.0'
        else:
            bucket = '8.0+'
        st2 = stats.setdefault(f'score:{bucket}', {'win': 0, 'loss': 0, 'expired': 0, 'total': 0})
        st2['total'] += 1
        if outcome in ('win', 'loss'):
            st2[outcome] += 1
        else:
            st2['expired'] += 1

        raw_signals.append({
            'df': df,
            'start': i + 1,
            'side': res['signal'],
            'entry': res['close'],
            'tp': res['tp'],
            'sl': res['sl'],
            'score': score,
            'ts': int(df['open_time'].iloc[i + 1]),
            'rr': 1.0,
            'atr': float(res.get('atr') or 0) or None,
        })

    print(f"✅ {symbol}: {evaluated} tín hiệu 4-5 sao được mô phỏng.")
    return stats, raw_signals


def merge_stats(target, source):
    for band, st in source.items():
        t = target.setdefault(band, {'win': 0, 'loss': 0, 'expired': 0, 'total': 0})
        for k in t:
            t[k] += st[k]


def print_report(all_stats, elapsed):
    print("\n" + "=" * 60)
    print(f"📊 KẾT QUẢ BACKTEST ({elapsed:.0f}s) — TP/SL theo engine, hold tối đa {MAX_HOLD_BARS} nến 1h")
    print("=" * 60)
    if not all_stats:
        print("Không có tín hiệu nào được mô phỏng.")
        return
    total_all = {'win': 0, 'loss': 0, 'expired': 0, 'total': 0}
    for band in ('5⭐', '4⭐'):
        if band not in all_stats:
            continue
        st = all_stats[band]
        decided = st['win'] + st['loss']
        wr = st['win'] / decided * 100 if decided else 0.0
        print(f"{band} (Mạnh) : {st['total']} tín hiệu | Win {st['win']} / Loss {st['loss']} "
              f"| Hết hạn {st['expired']} | Win-rate = {wr:.1f}%")
        for k in total_all:
            total_all[k] += st[k]
    decided_all = total_all['win'] + total_all['loss']
    wr_all = total_all['win'] / decided_all * 100 if decided_all else 0.0
    print("-" * 60)
    print(f"TỔNG   : {total_all['total']} tín hiệu | Win {total_all['win']} / Loss {total_all['loss']} "
          f"| Hết hạn {total_all['expired']} | Win-rate = {wr_all:.1f}%")

    # Win-rate theo bucket điểm số
    score_buckets = {k: v for k, v in all_stats.items() if k.startswith('score:')}
    if score_buckets:
        print("\n📈 Win-rate theo điểm số (tìm ngưỡng cắt tối ưu):")
        for bucket in ('<4.5', '4.5-5.0', '5.0-6.0', '6.0-8.0', '8.0+'):
            key = f'score:{bucket}'
            if key not in score_buckets:
                continue
            st = score_buckets[key]
            decided = st['win'] + st['loss']
            wr = st['win'] / decided * 100 if decided else 0.0
            print(f"   Score {bucket:>7} : {st['total']:>4} tín hiệu | Win {st['win']:>3} / Loss {st['loss']:>3} "
                  f"| Hết hạn {st['expired']:>3} | Win-rate = {wr:.1f}%")

    print("\n💡 Gợi ý hiệu chỉnh app.py:")
    print("   • Cắt tín hiệu ở bucket có win-rate > 50% (nâng ngưỡng trong scan + /a)")
    print("   • expired nhiều → tăng RR hoặc giảm MAX_HOLD trong thực tế")
    print("   • Chạy thêm với nhiều coin / nhiều nến để mẫu đáng tin cậy hơn (≥ 30 mẫu/band)")


def analyze_config(signals, cfg):
    """Tính thống kê + equity (max drawdown) cho một cấu hình bảo vệ trên cùng bộ tín hiệu."""
    by_ts = sorted(signals, key=lambda s: s['ts'])
    counts = {'win': 0, 'loss': 0, 'be': 0, 'expired': 0}
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    gross_win = gross_loss = 0.0
    total_r = 0.0
    for s in by_ts:
        outcome, r = simulate_outcome(s['df'], s['start'], s['side'], s['tp'], s['sl'],
                                      MAX_HOLD_BARS, atr=s['atr'], **cfg)
        counts[outcome] += 1
        total_r += r
        equity += r
        if r > 0:
            gross_win += r
        elif r < 0:
            gross_loss += -r
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
    decided = counts['win'] + counts['loss']
    wr = counts['win'] / decided * 100 if decided else 0.0
    pf = gross_win / gross_loss if gross_loss > 0 else float('inf')
    ev = total_r / len(by_ts) if by_ts else 0.0
    return counts, wr, ev, pf, max_dd, len(by_ts)


def print_trailing_sweep(raw_signals):
    """Sweep breakeven + trailing stop trên cùng bộ tín hiệu để đo lợi ích trước khi bật thật."""
    if not raw_signals:
        return
    print("\n🛡️ SWEEP BREAKEVEN + TRAILING (cùng bộ tín hiệu, TP cố định theo engine):")
    header = (f"{'Cấu hình':>27} | {'Tổng':>4} | {'W':>3} | {'L':>3} | {'BE':>3} | "
              f"{'Exp':>4} | {'WR':>6} | {'EV/R':>6} | {'PF':>5} | {'MaxDD(R)':>8}")
    print(header)
    print("-" * len(header))
    for name, cfg in TRAILING_CONFIGS:
        counts, wr, ev, pf, max_dd, total = analyze_config(raw_signals, cfg)
        pf_txt = "∞" if pf == float('inf') else f"{pf:.2f}"
        print(f"{name:>27} | {total:>4} | {counts['win']:>3} | {counts['loss']:>3} | "
              f"{counts['be']:>3} | {counts['expired']:>4} | {wr:>5.1f}% | {ev:>+5.2f} | "
              f"{pf_txt:>5} | {max_dd:>7.2f}R")
    print("\n💡 BE (breakeven scratch) là lệnh trượt giá về đúng entry → không thắng cũng không thua.")
    print("   Chọn cấu hình có EV/R cao nhất + MaxDD thấp nhất để bật trong auto_place_order.")


def print_rr_sweep(raw_signals):
    """Sweep Risk:Reward trên cùng bộ tín hiệu để tìm cấu hình có edge dương."""
    if not raw_signals:
        return
    print("\n🔁 SWEEP RISK:REWARD (cùng bộ tín hiệu, TP = entry ± risk × RR):")
    print(f"{'RR':>5} | {'Decided':>7} | {'Win':>5} | {'Loss':>5} | {'Expired':>7} | {'Win-rate':>8} | {'EV/1R':>6}")
    print("-" * 62)
    for rr in (1.0, 1.5, 2.0, 2.5, 3.0):
        win = loss = expired = 0
        total_r = 0.0
        for s in raw_signals:
            entry = s['entry']
            risk = abs(entry - s['sl'])
            if risk <= 0:
                continue
            tp = entry + risk * rr if s['side'] == 'LONG' else entry - risk * rr
            outcome, r = simulate_outcome(s['df'], s['start'], s['side'], tp, s['sl'], MAX_HOLD_BARS, atr=s['atr'])
            if outcome == 'win':
                win += 1
                total_r += r
            elif outcome == 'loss':
                loss += 1
                total_r += r
            else:
                expired += 1
        decided = win + loss
        wr = win / decided * 100 if decided else 0.0
        ev = total_r / decided if decided else 0.0
        print(f"1:{rr:<3.1f} | {decided:>7} | {win:>5} | {loss:>5} | {expired:>7} | {wr:>7.1f}% | {ev:>+6.2f}")


async def main():
    load_dotenv()
    num_coins = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    total_bars = int(sys.argv[2]) if len(sys.argv) > 2 else 2000

    started = time.time()
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Lấy top coin theo volume 24h
        async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr") as resp:
            tickers = await resp.json()
        usdt = [t for t in tickers if t['symbol'].endswith('USDT')]
        usdt.sort(key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
        coins = [t['symbol'] for t in usdt[:num_coins]]
        print(f"Backtest {len(coins)} coin: {coins}\n")

        all_stats = {}
        all_signals = []
        for symbol in coins:
            try:
                stats, signals = await backtest_symbol(session, symbol, total_bars)
                merge_stats(all_stats, stats)
                all_signals.extend(signals)
            except Exception as e:
                print(f"❌ {symbol}: {e}")

        print_report(all_stats, time.time() - started)
        print_trailing_sweep(all_signals)
        print_rr_sweep(all_signals)


if __name__ == '__main__':
    asyncio.run(main())