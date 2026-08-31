"""
Backtest hiệu chỉnh ngưỡng điểm cho scoring engine của lệnh /a.

Cách dùng:
    python3 backtest.py [số_coin] [số_nến_1h]

Ví dụ:
    python3 backtest.py            # 10 coin, 2000 nến 1h (~83 ngày)
    python3 backtest.py 5 3000

Script tái sử dụng đúng engine chấm điểm của app.py (analyze_market), mô phỏng
kết quả TP chạm trước hay SL chạm trước (RR 1:2, SL ưu tiên khi cùng nến),
rồi thống kê win-rate theo band độ tin cậy (4⭐ Mạnh / 5⭐ Rất mạnh).
Dùng kết quả này để hiệu chỉnh ngưỡng điểm trong app.py thay vì cảm tính.
"""
import asyncio
import sys
import time

import aiohttp
import pandas as pd
from dotenv import load_dotenv

from app import analyze_market

MIN_BARS = 300          # Số nến warmup tối thiểu trước khi đánh giá tín hiệu
MAX_HOLD_BARS = 72      # Giữ vị thế tối đa 72 nến 1h (~3 ngày) rồi tính expired
BINANCE_MAX_LIMIT = 1500


async def fetch_klines(session, symbol, interval, total_bars):
    """Lấy `total_bars` nến lịch sử (đã đóng) bằng cách phân trang ngược theo endTime."""
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
    return df


def simulate_outcome(df, start_idx, side, tp, sl, max_hold):
    """Mô phỏng từ nến start_idx: trả về 'win', 'loss' hoặc 'expired'."""
    for j in range(start_idx, min(start_idx + max_hold, len(df))):
        high = df['high'].iloc[j]
        low = df['low'].iloc[j]
        # Ưu tiên SL khi cả TP và SL đều chạm trong cùng 1 nến (giả định bảo thủ)
        if side == 'LONG':
            if low <= sl:
                return 'loss'
            if high >= tp:
                return 'win'
        else:
            if high >= sl:
                return 'loss'
            if low <= tp:
                return 'win'
    return 'expired'


async def backtest_symbol(session, symbol, total_bars, step=3):
    df = await fetch_klines(session, symbol, '1h', total_bars)
    if len(df) < MIN_BARS + 50:
        print(f"⚠️ {symbol}: chỉ có {len(df)} nến, bỏ qua.")
        return {}, []

    stats = {}
    raw_signals = []  # (df, start_idx, side, entry, sl, score) để sweep RR
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
        outcome = simulate_outcome(df, i + 1, res['signal'], res['tp'], res['sl'], MAX_HOLD_BARS)
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

        raw_signals.append((df, i + 1, res['signal'], res['close'], res['sl'], score))

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


def print_rr_sweep(raw_signals):
    """Sweep Risk:Reward trên cùng bộ tín hiệu để tìm cấu hình có edge dương."""
    if not raw_signals:
        return
    print("\n🔁 SWEEP RISK:REWARD (cùng bộ tín hiệu, TP = entry ± risk × RR):")
    print(f"{'RR':>5} | {'Decided':>7} | {'Win':>5} | {'Loss':>5} | {'Expired':>7} | {'Win-rate':>8} | {'EV/1R':>6}")
    print("-" * 62)
    for rr in (1.0, 1.5, 2.0, 2.5, 3.0):
        win = loss = expired = 0
        for df, start, side, entry, sl, score in raw_signals:
            risk = abs(entry - sl)
            if risk <= 0:
                continue
            tp = entry + risk * rr if side == 'LONG' else entry - risk * rr
            outcome = simulate_outcome(df, start, side, tp, sl, MAX_HOLD_BARS)
            if outcome == 'win':
                win += 1
            elif outcome == 'loss':
                loss += 1
            else:
                expired += 1
        decided = win + loss
        wr = win / decided * 100 if decided else 0.0
        ev = (win * rr - loss) / decided if decided else 0.0
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
        print_rr_sweep(all_signals)


if __name__ == '__main__':
    asyncio.run(main())
