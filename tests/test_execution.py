"""Test hồi quy cho lớp thực thi lệnh thật (app_trading): sizing theo ngân sách rủi ro,
kế toán giá khớp, thay SL (mới trước cũ), bảo vệ entry và tham số hedge/one-way.

Không gọi API thật: mọi hàm Binance được thay bằng fake trong từng test.
"""
import asyncio
import os
import sys
import time
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import app_trading as at  # noqa: E402

SESSION = SimpleNamespace()


def _run(coro):
    return asyncio.run(coro)


class FakeApi:
    """Giả lập binance_signed_request: ghi lại call, trả kết quả theo kịch bản."""

    def __init__(self, handlers=None):
        self.calls = []
        self.handlers = dict(handlers or {})

    async def __call__(self, session, method, path, params=None, base_url="https://fapi.binance.com"):
        self.calls.append((method, path, dict(params or {})))
        handler = self.handlers.get((method, path))
        if handler is None:
            return {}, None
        if isinstance(handler, list):
            return handler.pop(0) if len(handler) > 1 else handler[0]
        if callable(handler):
            return handler(params or {})
        return handler

    def calls_to(self, method, path):
        return [p for m, pth, p in self.calls if m == method and pth == path]


def patch_env(monkeypatch, api, *, hedge=False, position=(), qty_p=3, price_p=2, step=0.001):
    """Thay toàn bộ I/O của module bằng fake (không chạm mạng)."""
    monkeypatch.setattr(at, "binance_signed_request", api)
    monkeypatch.setattr(at, "hedge_mode", hedge)

    async def fake_precisions(session, symbol):
        return qty_p, price_p, 10 ** (-price_p)

    async def fake_constraints(session, symbol, qty_precision):
        return step, step, 10.0

    async def fake_get_position_risk(session, params=None):
        return position, None

    async def fake_set_leverage(session, api_key, api_secret, symbol, leverage):
        return True

    async def fake_price(session, symbol):
        return 100.0

    monkeypatch.setattr(at, "get_symbol_precisions", fake_precisions)
    monkeypatch.setattr(at, "_symbol_constraints", fake_constraints)
    monkeypatch.setattr(at, "get_position_risk", fake_get_position_risk)
    monkeypatch.setattr(at, "set_leverage", fake_set_leverage)
    monkeypatch.setattr(at, "get_single_price", fake_price)


# ─── Sizing theo ngân sách rủi ro ───

def test_risk_budget_allowance_subtracts_open_risk_from_both_caps():
    # equity 10.000 → 0.5%/lệnh = 50; trần danh mục 1.5% = 150
    assert at._risk_budget_allowance(10_000, 0.0, 1000.0) == pytest.approx(50.0)
    # rủi ro đang mở ăn vào CẢ trần danh mục và hạn lỗ ngày
    assert at._risk_budget_allowance(10_000, 140.0, 1000.0) == pytest.approx(10.0)
    assert at._risk_budget_allowance(10_000, 0.0, 30.0) == pytest.approx(30.0)
    assert at._risk_budget_allowance(10_000, 160.0, 1000.0) == pytest.approx(-10.0)


def test_plan_entry_size_caps_at_per_trade_budget_with_cost_buffer():
    ok, qty, _ = at._plan_entry_size(100.0, 99.0, 'LONG', equity=10_000, available=5_000,
                                     open_risk=0.0, daily_remaining=1_000.0, leverage=10, step=0.001)
    assert ok is True
    risk_per_unit = 1.0 + 100.0 * at.AUTO_TRADE_COST_PCT
    assert qty * risk_per_unit <= 50.0 + 1e-9      # không vượt 0.5% equity (đã gồm đệm phí)
    assert qty * risk_per_unit > 49.0              # nhưng dùng gần hết ngân sách
    assert qty % 0.001 == pytest.approx(0.0, abs=1e-9)


def test_plan_entry_size_respects_portfolio_and_daily_room():
    ok, qty, _ = at._plan_entry_size(100.0, 99.0, 'LONG', equity=10_000, available=5_000,
                                     open_risk=145.0, daily_remaining=1_000.0, leverage=10, step=0.001)
    assert ok is True
    assert qty * (1.0 + 100.0 * at.AUTO_TRADE_COST_PCT) <= 5.0 + 1e-9   # room = 150 - 145

    ok, qty, _ = at._plan_entry_size(100.0, 99.0, 'LONG', equity=10_000, available=5_000,
                                     open_risk=0.0, daily_remaining=3.0, leverage=10, step=0.001)
    assert ok is True
    assert qty * (1.0 + 100.0 * at.AUTO_TRADE_COST_PCT) <= 3.0 + 1e-9


def test_plan_entry_size_refuses_when_budget_exhausted():
    ok, qty, msg = at._plan_entry_size(100.0, 99.0, 'LONG', equity=10_000, available=5_000,
                                       open_risk=200.0, daily_remaining=1_000.0, leverage=10, step=0.001)
    assert ok is False and qty is None
    assert "ngân sách" in msg


def test_plan_entry_size_enforces_margin_and_notional_caps():
    ok, qty, _ = at._plan_entry_size(100.0, 99.0, 'LONG', equity=10_000, available=4.0,
                                     open_risk=0.0, daily_remaining=1_000.0, leverage=10, step=0.001)
    assert ok is True
    assert qty * 100.0 / 10 <= 1.0 + 1e-9          # margin ≤ 25% khả dụng

    ok, qty, _ = at._plan_entry_size(100.0, 99.0, 'LONG', equity=10_000, available=5_000,
                                     open_risk=0.0, daily_remaining=1_000.0, leverage=10, step=0.001,
                                     notional_cap=at.AI_VOLUME_MAX)
    assert ok is True
    assert qty * 100.0 <= at.AI_VOLUME_MAX + 1e-9  # 200/400/800 chỉ còn là TRẦN


def test_plan_entry_size_rejects_wrong_side_sl_and_below_min_lot():
    ok, _, msg = at._plan_entry_size(100.0, 101.0, 'LONG', equity=10_000, available=5_000,
                                     open_risk=0.0, daily_remaining=100.0, leverage=10, step=0.001)
    assert ok is False and "sai phía" in msg

    # ngân sách/ngày còn 0.05 USDT, SL cách entry rất xa ⇒ size < khối lượng tối thiểu → từ chối
    ok, _, msg = at._plan_entry_size(100.0, 90.0, 'LONG', equity=10_000, available=5_000,
                                     open_risk=0.0, daily_remaining=0.05, leverage=10, step=0.001,
                                     min_qty=1.0)
    assert ok is False and "tối thiểu" in msg


# ─── Kế toán giá khớp thật ───

def test_fill_from_order_uses_executed_qty_and_avg_price():
    assert at._fill_from_order({'executedQty': '0.5', 'avgPrice': '99.5'}) == (0.5, 99.5)
    # thiếu avgPrice nhưng có cumQuote → suy ra giá khớp trung bình
    assert at._fill_from_order({'executedQty': '2', 'avgPrice': '0', 'cumQuote': '201'}) == (2, 100.5)
    # không có bằng chứng khớp → None (tuyệt đối không lấy ticker làm giá vào)
    assert at._fill_from_order({'executedQty': '0', 'avgPrice': '0'}) is None
    assert at._fill_from_order({'orderId': 1, 'status': 'NEW'}) is None
    assert at._fill_from_order(None) is None


# ─── Entry: side BUY/SELL, SL trước TP, tham số hedge ───

async def _run_entry(monkeypatch, *, hedge, sl_ok=True, fill=('0.5', '100.0'), risk_budget=1000.0,
                     open_symbols=()):
    api = FakeApi({
        ('POST', '/fapi/v1/order'): ({'orderId': 555, 'executedQty': fill[0], 'avgPrice': fill[1],
                                      'status': 'FILLED'}, None),
        ('POST', '/fapi/v1/algoOrder'): [({'algoId': 'SL1'}, None) if sl_ok else (None, 'algo down'),
                                         ({'algoId': 'TP1'}, None)],
    })
    patch_env(monkeypatch, api, hedge=hedge,
              position=[{'symbol': 'BTCUSDT', 'positionSide': 'BOTH', 'positionAmt': '0.5',
                         'entryPrice': '100', 'markPrice': '100', 'leverage': '10'}])
    res = await at._execute_protected_entry(
        SESSION, symbol='BTCUSDT', side='LONG', quantity=0.5, price=100.0, sl_price=99.0,
        tp_price=101.0, qty_p=3, price_p=2, step=0.001, pos_side=('LONG' if hedge else 'BOTH'),
        max_lev=20, open_symbols=open_symbols, risk_budget=risk_budget)
    return api, res


def test_entry_sends_buy_side_and_places_sl_before_tp(monkeypatch):
    api, res = _run(_run_entry(monkeypatch, hedge=False))
    assert res['ok'] is True
    entry = api.calls_to('POST', '/fapi/v1/order')[0]
    assert entry['side'] == 'BUY'                  # API cần BUY/SELL, KHÔNG phải LONG
    assert entry['type'] == 'MARKET'
    assert entry['newOrderRespType'] == 'RESULT'
    assert [p['type'] for p in api.calls_to('POST', '/fapi/v1/algoOrder')] == \
        ['STOP_MARKET', 'TAKE_PROFIT_MARKET']      # SL luôn trước TP
    sl_call = api.calls_to('POST', '/fapi/v1/algoOrder')[0]
    assert sl_call['reduceOnly'] == 'true' and 'positionSide' not in sl_call
    assert res['sl_id'] == 'SL1' and res['tp_id'] == 'TP1'
    assert res['entry_qty'] == pytest.approx(0.5) and res['filled_qty'] == pytest.approx(0.5)


def test_entry_hedge_uses_position_side_without_reduce_only(monkeypatch):
    api, res = _run(_run_entry(monkeypatch, hedge=True))
    assert res['ok'] is True
    entry = api.calls_to('POST', '/fapi/v1/order')[0]
    assert entry['positionSide'] == 'LONG' and 'reduceOnly' not in entry
    for call in api.calls_to('POST', '/fapi/v1/algoOrder'):
        assert call['positionSide'] == 'LONG' and 'reduceOnly' not in call


def test_entry_without_sl_closes_emergency_and_reports_not_protected(monkeypatch):
    api, res = _run(_run_entry(monkeypatch, hedge=False, sl_ok=False))
    assert res['ok'] is False and res['stage'] == 'sl'
    assert res['entry_qty'] == pytest.approx(0.5)   # vẫn báo khớp để đối soát PnL
    assert res['close_pending'] is False            # đóng khẩn cấp đã được xác nhận khớp
    close_calls = api.calls_to('POST', '/fapi/v1/order')[1:]
    assert close_calls and close_calls[0]['reduceOnly'] == 'true'
    assert close_calls[0]['side'] == 'SELL'         # chỉ giảm vị thế LONG, không mở chiều ngược
    assert [p['type'] for p in api.calls_to('POST', '/fapi/v1/algoOrder')] == ['STOP_MARKET']


def test_entry_rejects_when_symbol_already_has_position(monkeypatch):
    api = FakeApi()
    patch_env(monkeypatch, api, hedge=False)
    res = _run(at._execute_protected_entry(
        SESSION, symbol='BTCUSDT', side='LONG', quantity=0.5, price=100.0, sl_price=99.0,
        tp_price=0.0, qty_p=3, price_p=2, step=0.001, pos_side='BOTH', max_lev=20,
        open_symbols=['BTCUSDT']))
    assert res['ok'] is False and res['stage'] == 'flat_check'
    assert api.calls == []                          # không gửi lệnh nào


def test_entry_trims_size_when_slippage_breaks_risk_budget(monkeypatch):
    # rủi ro thật 0.5 × (1 + phí/trượt) vượt ngân sách 0.2 → phải giảm size ngay sau khi khớp
    api, res = _run(_run_entry(monkeypatch, hedge=False, fill=('0.5', '100.0'), risk_budget=0.2))
    assert res['ok'] is True
    assert res['entry_qty'] == pytest.approx(0.5)
    assert res['filled_qty'] < 0.5
    reduce_calls = api.calls_to('POST', '/fapi/v1/order')[1:]
    assert reduce_calls and reduce_calls[0]['reduceOnly'] == 'true'
    sl_call = api.calls_to('POST', '/fapi/v1/algoOrder')[0]
    assert float(sl_call['quantity']) == pytest.approx(res['filled_qty'])


# ─── Thay SL: mới trước, cũ sau ───

async def _replace(monkeypatch, *, new_ok=True, cancel_ok=True):
    calls = []

    async def fake_place(session, symbol, close_side, order_type, trigger_price, quantity,
                         pos_side=None, client_id=None):
        calls.append(('place', order_type, trigger_price, quantity))
        return (True, 'NEW1') if new_ok else (False, 'rejected')

    async def fake_cancel(session, api_key, api_secret, symbol, algo_id):
        calls.append(('cancel', algo_id))
        return cancel_ok

    monkeypatch.setattr(at, "_place_conditional_tpsl", fake_place)
    monkeypatch.setattr(at, "_cancel_algo_sl", fake_cancel)
    monkeypatch.setattr(at, "_save_auto_managed", lambda: None)
    meta = {'symbol': 'BTCUSDT', 'pos_side': 'BOTH', 'sl_algo_id': 'OLD1', 'last_sl': 95.0,
            'sl_initial': 95.0}
    result = await at._replace_protective_sl(SESSION, meta, 99.0, 0.5, 'SELL', None, 2, 3)
    return calls, meta, result


def test_replace_sl_places_new_before_cancelling_old(monkeypatch):
    calls, meta, result = _run(_replace(monkeypatch, new_ok=True))
    assert calls[0][0] == 'place' and calls[1] == ('cancel', 'OLD1')
    assert result == 'NEW1' and meta['sl_algo_id'] == 'NEW1' and meta['last_sl'] == 99.0


def test_replace_sl_keeps_old_when_new_fails(monkeypatch):
    calls, meta, result = _run(_replace(monkeypatch, new_ok=False))
    assert [c[0] for c in calls] == ['place']       # KHÔNG hủy SL cũ khi SL mới lỗi
    assert result is None and meta['sl_algo_id'] == 'OLD1' and meta['last_sl'] == 95.0


def test_replace_sl_keeps_new_protection_when_old_cancel_fails(monkeypatch):
    calls, meta, result = _run(_replace(monkeypatch, new_ok=True, cancel_ok=False))
    assert result == 'NEW1' and meta['sl_algo_id'] == 'NEW1' and meta['last_sl'] == 99.0
    assert meta['stale_sl_ids'] == ['OLD1']         # SL mới vẫn hiệu lực, chỉ ghi nợ dọn SL cũ


# ─── Đóng khẩn cấp: không gửi trùng, xác nhận khớp, đúng hedge/one-way ───

def test_emergency_close_never_resubmits_after_ambiguous_error(monkeypatch):
    api = FakeApi({
        ('POST', '/fapi/v1/order'): (None, 'timeout -1007'),
        ('GET', '/fapi/v1/order'): [({'orderId': 9, 'status': 'NEW'}, None)],
    })
    patch_env(monkeypatch, api, position=[{'symbol': 'BTCUSDT', 'positionSide': 'BOTH',
                                           'positionAmt': '0.5', 'entryPrice': '100',
                                           'markPrice': '100', 'leverage': '10'}])
    ok, msg = _run(at._emergency_reduce_close(SESSION, 'BTCUSDT', 'LONG', 'BOTH'))
    assert ok is False and 'treo' in msg
    assert len(api.calls_to('POST', '/fapi/v1/order')) == 1   # KHÔNG gửi lại lần hai


def test_emergency_close_confirms_fill_via_query(monkeypatch):
    api = FakeApi({
        ('POST', '/fapi/v1/order'): (None, 'network reset'),
        ('GET', '/fapi/v1/order'): [({'orderId': 9, 'executedQty': '0.5', 'avgPrice': '99.0'}, None)],
    })
    patch_env(monkeypatch, api, position=[{'symbol': 'BTCUSDT', 'positionSide': 'BOTH',
                                           'positionAmt': '0.5', 'entryPrice': '100',
                                           'markPrice': '100', 'leverage': '10'}])
    ok, msg = _run(at._emergency_reduce_close(SESSION, 'BTCUSDT', 'LONG', 'BOTH'))
    assert ok is True and '0.5' in msg
    assert len(api.calls_to('POST', '/fapi/v1/order')) == 1


def test_emergency_close_clamps_to_live_hedge_position(monkeypatch):
    api = FakeApi({
        ('POST', '/fapi/v1/order'): ({'orderId': 7, 'executedQty': '0.4', 'avgPrice': '100'}, None),
    })
    patch_env(monkeypatch, api, hedge=True,
              position=[{'symbol': 'ETHUSDT', 'positionSide': 'SHORT', 'positionAmt': '-0.4',
                         'entryPrice': '100', 'markPrice': '100', 'leverage': '10'}])
    ok, _ = _run(at._emergency_reduce_close(SESSION, 'ETHUSDT', 'SHORT', 'SHORT', qty_cap=0.9))
    assert ok is True
    call = api.calls_to('POST', '/fapi/v1/order')[0]
    assert call['side'] == 'BUY' and call['positionSide'] == 'SHORT'
    assert 'reduceOnly' not in call
    assert float(call['quantity']) == pytest.approx(0.4)      # kẹp theo vị thế thật, không gửi dư


def test_emergency_close_noop_when_already_flat(monkeypatch):
    api = FakeApi()
    patch_env(monkeypatch, api, position=[])
    ok, msg = _run(at._emergency_reduce_close(SESSION, 'BTCUSDT', 'LONG', 'BOTH'))
    assert ok is True and 'phẳng' in msg
    assert api.calls == []


def test_emergency_close_hedge_flat_does_not_reopen_position(monkeypatch):
    # Vị thế hedge đã phẳng: gửi BUY positionSide SHORT sẽ MỞ SHORT mới → phải chặn
    api = FakeApi({
        ('POST', '/fapi/v1/order'): ({'orderId': 7, 'executedQty': '0.4', 'avgPrice': '100'}, None),
    })
    patch_env(monkeypatch, api, hedge=True, position=[])
    ok, _ = _run(at._emergency_reduce_close(SESSION, 'ETHUSDT', 'SHORT', 'SHORT'))
    assert ok is True and api.calls == []


# ─── Đọc rủi ro vị thế: SL hợp lệ, thiếu khối lượng, closePosition ───

def test_stop_candidates_require_correct_side_and_trigger_side():
    orders = [
        {'symbol': 'BTCUSDT', 'orderType': 'STOP_MARKET', 'side': 'SELL', 'positionSide': 'BOTH',
         'triggerPrice': '95', 'quantity': '1'},
        # SL ngược chiều (BUY cho vị thế LONG) → không tính
        {'symbol': 'BTCUSDT', 'orderType': 'STOP_MARKET', 'side': 'BUY', 'positionSide': 'BOTH',
         'triggerPrice': '95', 'quantity': '1'},
        # trigger nằm trên mark của LONG → đã kích hoạt/sai phía → không tính
        {'symbol': 'BTCUSDT', 'orderType': 'STOP_MARKET', 'side': 'SELL', 'positionSide': 'BOTH',
         'triggerPrice': '105', 'quantity': '1'},
    ]
    cands = at._position_stop_candidates(orders, 'BTCUSDT', 'LONG', 'BOTH', 100.0, mark=100.0,
                                        pos_qty=1.0)
    assert len(cands) == 1 and cands[0]['trigger'] == 95.0
    risk, protected = at._stop_risk_usdt('LONG', 100.0, 1.0, cands)
    assert protected is True and risk == pytest.approx(5.0)


def test_stop_risk_flags_partial_cover_and_accepts_close_position_orders():
    partial = [{'trigger': 95.0, 'qty': 0.4, 'id': 'a'}]
    risk, protected = at._stop_risk_usdt('LONG', 100.0, 1.0, partial)
    assert protected is False and risk == 0.0       # SL thiếu khối lượng ⇒ chưa được bảo vệ

    cands = at._position_stop_candidates(
        [{'symbol': 'BTCUSDT', 'orderType': 'STOP_MARKET', 'side': 'SELL', 'positionSide': 'BOTH',
          'triggerPrice': '95', 'quantity': '0', 'closePosition': True}],
        'BTCUSDT', 'LONG', 'BOTH', 100.0, mark=100.0, pos_qty=2.5)
    risk, protected = at._stop_risk_usdt('LONG', 100.0, 2.5, cands)
    assert protected is True and risk == pytest.approx(12.5)


def test_stop_risk_treats_breakeven_sl_as_protection_with_zero_risk():
    cands = [{'trigger': 100.0, 'qty': 1.0, 'id': 'x'}]
    risk, protected = at._stop_risk_usdt('LONG', 100.0, 1.0, cands)
    assert protected is True and risk == pytest.approx(0.0)


# ─── Thứ tự thực thi & phân loại lệnh ───

def test_execution_plan_orders_sl_before_tp_and_exits_first():
    items = [
        {'type': 'place_algo_order', 'desc': 'TP',
         'params': {'symbol': 'BTCUSDT', 'type': 'TAKE_PROFIT_MARKET'}},
        {'type': 'place_order', 'desc': 'MO', 'params': {'symbol': 'BTCUSDT', 'type': 'MARKET'}},
        {'type': 'place_algo_order', 'desc': 'SL',
         'params': {'symbol': 'BTCUSDT', 'type': 'STOP_MARKET'}},
        {'type': 'cancel_order', 'desc': 'HUY', 'params': {'symbol': 'BTCUSDT'}},
    ]
    assert [it['desc'] for _, it in at._plan_execution_order(items)] == ['HUY', 'MO', 'SL', 'TP']


def test_open_order_classification_ignores_exits_and_reduce_only():
    assert at._is_open_order_item({'type': 'place_order', 'params': {'type': 'MARKET'}}) is True
    assert at._is_open_order_item({'type': 'place_order', 'is_exit': True,
                                   'params': {'type': 'MARKET'}}) is False
    assert at._is_open_order_item({'type': 'place_order', 'is_close': True,
                                   'params': {'type': 'MARKET'}}) is False
    assert at._is_open_order_item({'type': 'place_order',
                                   'params': {'type': 'MARKET', 'reduceOnly': 'true'}}) is False
    assert at._is_open_order_item({'type': 'place_algo_order',
                                   'params': {'type': 'STOP_MARKET'}}) is False


# ─── Đòn bẩy an toàn cho SL ───

def test_safe_leverage_keeps_sl_outside_liquidation_zone():
    assert at._safe_leverage_for_sl(100.0, 99.0, 125) == 50   # SL cách 1% ⇒ lev ≤ 50
    assert at._safe_leverage_for_sl(100.0, 99.0, 20) == 20
    assert at._safe_leverage_for_sl(100.0, 100.0, 20) == 20
    assert at._safe_leverage_for_sl(100.0, 99.0, 0) == 1
    assert at._safe_leverage_for_sl(None, 99.0, 20) == 1


# ─── Lệnh MỞ LIMIT chờ khớp: gắn bảo vệ khi có vị thế ───

def _pending_pend(**over):
    pend = {'symbol': 'BTCUSDT', 'side': 'LONG', 'pos_side': 'BOTH', 'order_id': '77',
            'client_order_id': 'cid1', 'quantity': 0.5, 'sl': 99.0, 'tp': 101.0,
            'limit_price': 100.0, 'entry_time': 1000.0, 'ts': time.time(), 'signal_id': None,
            'sl_algo_id': None, 'tp_algo_id': None, 'source': 'ai_chat'}
    pend.update(over)
    return pend


def _patch_pending_env(monkeypatch, api, position, tmp_path, hedge=False):
    patch_env(monkeypatch, api, hedge=hedge, position=position)
    monkeypatch.setattr(at, 'PENDING_ENTRIES_FILE', str(tmp_path / 'pending_entries.json'))
    monkeypatch.setattr(at, 'pending_entries', {})
    monkeypatch.setattr(at, '_save_auto_managed', lambda: None)
    monkeypatch.setattr(at, 'auto_managed', {})
    monkeypatch.setattr(at, 'save_pending_entries', lambda: None)
    recorded = []

    def fake_record(res, ai_verdict=None, origin='manual', execution=None):
        recorded.append({'res': res, 'origin': origin, 'execution': execution})
        return 'sig_1'

    monkeypatch.setattr(at, 'record_signal', fake_record)
    return recorded


def test_pending_entry_watcher_places_sl_before_tp_when_filled(monkeypatch, tmp_path):
    api = FakeApi({
        ('GET', '/fapi/v1/order'): ({'orderId': 77, 'status': 'FILLED', 'executedQty': '0.5',
                                     'avgPrice': '99.8'}, None),
        ('POST', '/fapi/v1/algoOrder'): [({'algoId': 'SL1'}, None), ({'algoId': 'TP1'}, None)],
    })
    live = [{'symbol': 'BTCUSDT', 'positionSide': 'BOTH', 'positionAmt': '0.5',
             'entryPrice': '99.8', 'markPrice': '100', 'leverage': '10'}]
    recorded = _patch_pending_env(monkeypatch, api, live, tmp_path)
    pend = _pending_pend()
    monkeypatch.setitem(at.pending_entries, 'cid1', pend)

    _run(at._refresh_pending_entries(SESSION))
    algo_types = [p['type'] for p in api.calls_to('POST', '/fapi/v1/algoOrder')]
    assert algo_types == ['STOP_MARKET', 'TAKE_PROFIT_MARKET']     # SL trước, TP sau
    assert at.pending_entries == {}                                # lệnh FILLED → xong, bỏ theo dõi
    assert at.auto_managed['BTCUSDT_BOTH']['managed'] is False
    assert at.auto_managed['BTCUSDT_BOTH']['sl_algo_id'] == 'SL1'
    assert recorded and recorded[0]['execution']['quantity'] == 0.5
    assert recorded[0]['execution']['entry_time'] == pytest.approx(1000.0)   # mốc TRƯỚC khi gửi
    assert recorded[0]['execution']['order_id'] == '77'


def test_pending_entry_watcher_does_nothing_while_order_unfilled(monkeypatch, tmp_path):
    api = FakeApi({
        ('GET', '/fapi/v1/order'): ({'orderId': 77, 'status': 'NEW', 'executedQty': '0'}, None),
    })
    recorded = _patch_pending_env(monkeypatch, api, [], tmp_path)
    monkeypatch.setitem(at.pending_entries, 'cid1', _pending_pend())

    _run(at._refresh_pending_entries(SESSION))
    assert api.calls_to('POST', '/fapi/v1/algoOrder') == []        # chưa có vị thế → không đặt SL
    assert 'cid1' in at.pending_entries and recorded == []


def test_pending_entry_watcher_emergency_closes_when_sl_fails(monkeypatch, tmp_path):
    api = FakeApi({
        ('GET', '/fapi/v1/order'): ({'orderId': 77, 'status': 'FILLED', 'executedQty': '0.5',
                                     'avgPrice': '99.8'}, None),
        ('POST', '/fapi/v1/algoOrder'): (None, 'algo down'),
        ('POST', '/fapi/v1/order'): ({'orderId': 88, 'executedQty': '0.5', 'avgPrice': '99.7'}, None),
    })
    live = [{'symbol': 'BTCUSDT', 'positionSide': 'BOTH', 'positionAmt': '0.5',
             'entryPrice': '99.8', 'markPrice': '100', 'leverage': '10'}]
    _patch_pending_env(monkeypatch, api, live, tmp_path)
    monkeypatch.setitem(at.pending_entries, 'cid1', _pending_pend())

    _run(at._refresh_pending_entries(SESSION))
    closes = api.calls_to('POST', '/fapi/v1/order')
    assert closes and closes[0]['reduceOnly'] == 'true'            # đóng khẩn cấp, không mở chiều ngược
    assert at.auto_managed['BTCUSDT_BOTH']['close_pending'] is False
    assert 'cid1' in at.pending_entries                            # vẫn giữ theo dõi để chắc chắn phẳng


def test_pending_entry_watcher_drops_canceled_order(monkeypatch, tmp_path):
    api = FakeApi({
        ('GET', '/fapi/v1/order'): ({'orderId': 77, 'status': 'CANCELED', 'executedQty': '0'}, None),
    })
    _patch_pending_env(monkeypatch, api, [], tmp_path)
    monkeypatch.setitem(at.pending_entries, 'cid1', _pending_pend())

    _run(at._refresh_pending_entries(SESSION))
    assert at.pending_entries == {}
    assert api.calls_to('POST', '/fapi/v1/algoOrder') == []


def test_pending_entry_watcher_drops_when_position_already_flat(monkeypatch, tmp_path):
    # Lệnh FILLED nhưng vị thế đã flat (đã bị đóng) → không đặt SL/TP, bỏ theo dõi
    api = FakeApi({
        ('GET', '/fapi/v1/order'): ({'orderId': 77, 'status': 'FILLED', 'executedQty': '0.5',
                                     'avgPrice': '99.8'}, None),
    })
    _patch_pending_env(monkeypatch, api, [], tmp_path)
    monkeypatch.setitem(at.pending_entries, 'cid1', _pending_pend())

    _run(at._refresh_pending_entries(SESSION))
    assert at.pending_entries == {}
    assert api.calls_to('POST', '/fapi/v1/algoOrder') == []
    assert api.calls_to('POST', '/fapi/v1/order') == []


def test_pending_entries_persist_round_trip(monkeypatch, tmp_path):
    monkeypatch.setattr(at, 'PENDING_ENTRIES_FILE', str(tmp_path / 'pe.json'))
    monkeypatch.setattr(at, 'pending_entries', {'cid1': _pending_pend()})
    at.save_pending_entries()
    monkeypatch.setattr(at, 'pending_entries', {})
    at.load_pending_entries()
    assert list(at.pending_entries) == ['cid1']
    assert at.pending_entries['cid1']['sl'] == 99.0
    assert at.pending_entries['cid1']['order_id'] == '77'


def test_limit_entry_without_immediate_fill_is_tracked_not_protected(monkeypatch, tmp_path):
    api = FakeApi({
        ('POST', '/fapi/v1/order'): ({'orderId': 91, 'status': 'NEW', 'executedQty': '0'}, None),
    })
    patch_env(monkeypatch, api, position=[])
    monkeypatch.setattr(at, 'PENDING_ENTRIES_FILE', str(tmp_path / 'pe.json'))
    monkeypatch.setattr(at, 'pending_entries', {})
    monkeypatch.setattr(at, 'save_pending_entries', lambda: None)
    monkeypatch.setattr(at, '_save_auto_managed', lambda: None)

    async def fake_max_lev(session, symbol):
        return 20, None

    async def fake_snap(session):
        return {'equity': 10_000.0, 'available': 5_000.0, 'wallet': 10_000.0, 'upnl': 0.0,
                'daily_remaining': 50.0, 'open_risk': 0.0, 'unprotected': [],
                'open_symbols': []}, None

    monkeypatch.setattr(at, '_max_leverage_strict', fake_max_lev)
    monkeypatch.setattr(at, '_account_risk_snapshot', fake_snap)

    items = [
        {'type': 'place_order', 'desc': 'MO LIMIT BTC', 'origin': 'ai',
         'params': {'symbol': 'BTCUSDT', 'side': 'BUY', 'type': 'LIMIT', 'quantity': '0.5',
                    'price': '100', 'timeInForce': 'GTC'}},
        {'type': 'place_algo_order', 'desc': 'SL', 'origin': 'ai',
         'params': {'symbol': 'BTCUSDT', 'type': 'STOP_MARKET', 'side': 'SELL',
                    'triggerPrice': '99', 'quantity': '0.5', 'reduceOnly': 'true'}},
        {'type': 'place_algo_order', 'desc': 'TP', 'origin': 'ai',
         'params': {'symbol': 'BTCUSDT', 'type': 'TAKE_PROFIT_MARKET', 'side': 'SELL',
                    'triggerPrice': '101', 'quantity': '0.5', 'reduceOnly': 'true'}},
    ]
    text = _run(at._execute_items(SESSION, items))
    assert api.calls_to('POST', '/fapi/v1/algoOrder') == []       # chưa khớp → KHÔNG đặt SL/TP
    assert len(at.pending_entries) == 1                            # đã lưu để watcher gắn bảo vệ sau
    pend = next(iter(at.pending_entries.values()))
    assert pend['sl'] == 99.0 and pend['order_id'] == '91'
    assert 'CHƯA KHỚP' in text and 'SL TRƯỚC' in text


# ─── Cổng rollout auto thật ───

def test_auto_trade_gate_defaults_off_and_requires_explicit_true(monkeypatch):
    monkeypatch.delenv('AUTO_TRADE_ENABLED', raising=False)
    monkeypatch.setattr(at, 'AUTO_TRADE_ENABLED', False)
    assert at._auto_trade_enabled() is False
    monkeypatch.setenv('AUTO_TRADE_ENABLED', 'true')
    assert at._auto_trade_enabled() is True
    for value in ('1', 'yes', 'TRUE ', 'on', ''):
        monkeypatch.setenv('AUTO_TRADE_ENABLED', value)
        assert at._auto_trade_enabled() is (value.strip().lower() == 'true')
    monkeypatch.delenv('AUTO_TRADE_ENABLED', raising=False)
    monkeypatch.setattr(at, 'AUTO_TRADE_ENABLED', True)
    assert at._auto_trade_enabled() is True   # không có env → lấy hằng số


def test_auto_trade_guard_blocks_real_entries_when_gate_off(monkeypatch):
    monkeypatch.setenv('AUTO_TRADE_ENABLED', 'false')

    async def boom(*args, **kwargs):
        raise AssertionError("cổng tắt thì KHÔNG được gọi API")

    monkeypatch.setattr(at, "binance_signed_request", boom)
    ok, reason = _run(at._auto_trade_guard(SESSION))
    assert ok is False and 'AUTO_TRADE_ENABLED' in reason


# ─── Bước khối lượng theo LOT_SIZE ───

def test_round_to_step_and_min_step_fallback():
    assert at._round_to_step(1.23456, 0.001) == pytest.approx(1.234)
    assert at._round_to_step(1.23456, 0.05) == pytest.approx(1.2)
    assert at._round_to_step(0.9, 1.0) == pytest.approx(0.0)
    assert at._round_to_step(1.23, 0) == pytest.approx(1.23)
    assert at._min_step_qty(3) == pytest.approx(0.001)


def test_symbol_constraints_falls_back_to_precision_when_filters_missing(monkeypatch):
    monkeypatch.setattr(at, "_SYMBOL_FILTERS", {})
    monkeypatch.setattr(at, "_SYMBOL_FILTERS_TS", 0.0)

    async def fake_loader(session, ttl=3600):
        return False

    monkeypatch.setattr(at, "_load_symbol_filters", fake_loader)
    step, min_qty, min_notional = _run(at._symbol_constraints(SESSION, 'BTCUSDT', 3))
    assert step == pytest.approx(0.001) and min_qty == 0.0 and min_notional == 0.0
