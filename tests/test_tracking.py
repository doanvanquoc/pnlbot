import time
import unittest
from unittest.mock import AsyncMock, patch

import pandas as pd

import app_trading as bot


class CandleResponse:
    status = 200

    def __init__(self, candles):
        self.candles = candles

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def json(self):
        return self.candles


class CandleSession:
    def __init__(self, candles):
        self.candles = candles
        self.requests = []

    def get(self, url, params):
        self.requests.append(params)
        return CandleResponse(self.candles)


class TrackingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.history = patch.object(bot, 'signal_history', [])
        self.history.start()
        self.save = patch.object(bot, 'save_signal_history')
        self.save.start()
        self.addCleanup(self.history.stop)
        self.addCleanup(self.save.stop)
        self.signal = {'symbol': 'TESTUSDT', 'signal': 'LONG', 'close': 100.,
                       'tp': 110., 'sl': 90., 'long_score': 5.5, 'short_score': 1.,
                       'confidence': 'Mạnh'}

    async def test_intraminute_hit_is_seen_when_candle_closes(self):
        sig = {'symbol': 'TESTUSDT', 'side': 'LONG', 'ts': 630.,
               'tp': 110., 'sl': 90., 'status': 'open', 'next_open_ms': 660000}
        session = CandleSession([[660000, 100, 105, 99, 102]])
        await bot._track_paper_signal(session, sig, 660.1)
        self.assertEqual(sig['next_open_ms'], 660000)
        session.candles = [[660000, 100, 111, 99, 110]]
        await bot._track_paper_signal(session, sig, 690.1)
        self.assertEqual(sig['status'], 'open')
        await bot._track_paper_signal(session, sig, 720.1)
        self.assertEqual(sig['status'], 'win')
        self.assertEqual(sig['closed_ts'], 720.)

    async def test_data_gap_does_not_advance_or_expire(self):
        sig = {'symbol': 'TESTUSDT', 'side': 'LONG', 'ts': 630.,
               'tp': 110., 'sl': 90., 'status': 'open', 'next_open_ms': 660000}
        session = CandleSession([[720000, 100, 111, 99, 110]])
        await bot._track_paper_signal(session, sig, 300000.)
        self.assertEqual(sig['next_open_ms'], 660000)
        self.assertEqual(sig['status'], 'open')

    async def test_ambiguous_candle_prefers_stop(self):
        sig = {'symbol': 'TESTUSDT', 'side': 'LONG', 'ts': 630.,
               'tp': 110., 'sl': 90., 'status': 'open', 'next_open_ms': 660000}
        await bot._track_paper_signal(CandleSession([[660000, 100, 111, 89, 100]]), sig, 720.)
        self.assertEqual(sig['status'], 'loss')

    def test_alert_cannot_suppress_real_fill_and_order_is_idempotent(self):
        paper_id = bot.record_signal(self.signal, origin='alert')
        execution = {'order_id': 42, 'quantity': 2., 'position_side': 'BOTH', 'entry_time': time.time()}
        actual = dict(self.signal, close=101.)
        real_id = bot.record_signal(actual, origin='auto', execution=execution)
        self.assertNotEqual(paper_id, real_id)
        self.assertEqual(bot.record_signal(actual, origin='auto', execution=execution), real_id)
        self.assertEqual(len(bot.signal_history), 2)
        self.assertEqual(bot.signal_history[1]['entry'], 101.)

    def test_pending_execution_survives_pruning(self):
        sig = {'id': 'old', 'ts': 0., 'status': 'open', 'execution': {'order_id': 1}}
        bot.signal_history.append(sig)
        bot.prune_signal_history(max_keep=1)
        self.assertIn(sig, bot.signal_history)

    def test_paper_losses_cannot_block_real_trading_or_teach_ai(self):
        for i in range(20):
            bot.signal_history.append({'ts': time.time(), 'status': 'loss', 'side': 'LONG',
                                       'confidence': 'Mạnh', 'origin': 'auto'})
        self.assertTrue(bot.band_winrate_ok('Mạnh'))
        self.assertTrue(bot.side_winrate_ok('LONG'))
        self.assertIsNone(bot.build_signal_lessons_digest())
        self.assertEqual(bot.get_signal_stats(), {})

    def execution_signal(self):
        return {'symbol': 'TESTUSDT', 'side': 'LONG', 'entry': 100., 'sl': 90.,
                'status': 'open', 'ts': 1., 'execution': {'order_id': 10, 'quantity': 2.,
                'position_side': 'BOTH', 'entry_time': 1.}}

    def fills(self):
        return [
            {'id': 1, 'orderId': 10, 'side': 'BUY', 'positionSide': 'BOTH', 'qty': '2',
             'realizedPnl': '0', 'commission': '.1', 'commissionAsset': 'USDT', 'time': 1000},
            {'id': 2, 'orderId': 11, 'side': 'SELL', 'positionSide': 'BOTH', 'qty': '1',
             'realizedPnl': '15', 'commission': '.1', 'commissionAsset': 'USDT', 'time': 2000},
            {'id': 3, 'orderId': 12, 'side': 'SELL', 'positionSide': 'BOTH', 'qty': '1',
             'realizedPnl': '-5', 'commission': '.1', 'commissionAsset': 'USDT', 'time': 3000},
        ]

    def test_partial_fills_aggregate_and_deduplicate(self):
        fills = self.fills()
        result = bot._execution_trade_result(self.execution_signal(), fills + [fills[-1]])
        assert result is not None
        self.assertEqual(result['gross_pnl'], 10.)
        self.assertAlmostEqual(result['commission'], .3)
        self.assertEqual(result['trade_ids'], [1, 2, 3])
        self.assertIsNone(bot._execution_trade_result(self.execution_signal(), fills[:-1]))

    def test_interleaved_new_entry_is_not_misattributed(self):
        fills = self.fills()
        fills[1] = dict(fills[1], side='BUY')
        self.assertIsNone(bot._execution_trade_result(self.execution_signal(), fills))

    async def test_net_pnl_uses_funding_and_all_partial_fills(self):
        sig = self.execution_signal()
        bot.signal_history.append(sig)
        with patch.object(bot, 'binance_signed_request', AsyncMock(return_value=(self.fills(), None))), \
             patch.object(bot, 'fetch_income_paginated', AsyncMock(return_value=([
                 {'symbol': 'TESTUSDT', 'asset': 'USDT', 'income': '-1.2'}], None))), \
             patch.object(bot.time, 'time', return_value=100.):
            self.assertTrue(await bot.reconcile_execution_outcomes(None))
        self.assertEqual(sig['status'], 'win')
        self.assertAlmostEqual(sig['net_pnl'], 8.5)
        self.assertAlmostEqual(sig['realized_r'], .425)

    async def test_missing_funding_does_not_finalize_as_zero(self):
        sig = self.execution_signal()
        bot.signal_history.append(sig)
        with patch.object(bot, 'binance_signed_request', AsyncMock(return_value=(self.fills(), None))), \
             patch.object(bot, 'fetch_income_paginated', AsyncMock(return_value=(None, 'unavailable'))), \
             patch.object(bot.time, 'time', return_value=100.):
            self.assertFalse(await bot.reconcile_execution_outcomes(None))
        self.assertEqual(sig['status'], 'open')
        self.assertNotIn('net_pnl', sig)

    def test_non_usdt_commission_is_not_silently_counted_as_usdt(self):
        fills = self.fills()
        fills[0] = dict(fills[0], commissionAsset='BNB')
        self.assertIsNone(bot._execution_trade_result(self.execution_signal(), fills))

    async def test_equal_directional_moves_do_not_create_short_bias(self):
        frame = pd.DataFrame({'open': [100.] * 100, 'close': [100.] * 100,
                              'high': [101. + i * .25 for i in range(100)],
                              'low': [99. - i * .25 for i in range(100)], 'volume': [100.] * 100})
        with patch.object(bot, 'get_symbol_precisions', AsyncMock(return_value=(3, 2, .01))):
            result = await bot.analyze_market(None, 'TESTUSDT', df=frame, fetch_extras=False)
        self.assertEqual(result['plus_di'], 0.)
        self.assertEqual(result['minus_di'], 0.)
        self.assertEqual(result['signal'], 'NEUTRAL')

    async def test_model_change_invalidates_ai_verdict(self):
        verdicts = [{'direction': 'LONG'}, {'direction': 'NEUTRAL'}]
        with patch.object(bot, 'ai_verdict_cache', {}), \
             patch.object(bot, 'get_ai_lessons', AsyncMock(return_value=None)), \
             patch.object(bot, 'get_ai_analysis', AsyncMock(side_effect=verdicts)) as analyze:
            with patch.dict(bot.os.environ, {'DASH_MODEL': 'model-a'}):
                first = await bot.get_ai_verdict_cached(None, 'TEST', 'same indicators')
            with patch.dict(bot.os.environ, {'DASH_MODEL': 'model-b'}):
                second = await bot.get_ai_verdict_cached(None, 'TEST', 'same indicators')
        self.assertEqual(first['direction'], 'LONG')
        self.assertEqual(second['direction'], 'NEUTRAL')
        self.assertEqual(analyze.await_count, 2)

    async def test_no_verified_history_discards_old_lessons(self):
        with patch.object(bot, 'ai_lessons_state', {'text': 'stale lessons', 'ts': time.time(), 'resolved_count': 0}), \
             patch.object(bot, 'ai_lessons_lock', __import__('asyncio').Lock()):
            self.assertIsNone(await bot.get_ai_lessons(None))


class AutoPriceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.price_chats = patch.object(bot, 'auto_price_chats', {})
        self.last_messages = patch.object(bot, 'last_auto_price_messages', {})
        self.activity = patch.object(bot, 'has_new_activity', {})
        self.position_chats = patch.object(bot, 'auto_chats', set())
        self.position_messages = patch.object(bot, 'last_auto_messages', {})
        self.price_chats.start()
        self.last_messages.start()
        self.activity.start()
        self.position_chats.start()
        self.position_messages.start()
        self.addCleanup(self.price_chats.stop)
        self.addCleanup(self.last_messages.stop)
        self.addCleanup(self.activity.stop)
        self.addCleanup(self.position_chats.stop)
        self.addCleanup(self.position_messages.stop)

    async def test_auto_coin_list_is_normalized_and_enabled(self):
        prices = [('ZECUSDT', {'price': 50., 'change': 2., 'funding_rate': 0.}),
                  ('HYPEUSDT', {'price': 40., 'change': -1., 'funding_rate': 0.})]
        with patch.object(bot, 'get_coin_prices', AsyncMock(return_value=prices)), \
             patch.object(bot, 'send_telegram_message', AsyncMock(side_effect=[1, 2])) as send, \
             patch.object(bot, 'save_auto_chats'):
            await bot.handle_auto_command(None, 123, ['zec', 'HYPE'])
        self.assertEqual(bot.auto_price_chats[123], ['ZECUSDT', 'HYPEUSDT'])
        self.assertEqual(bot.last_auto_price_messages[123], 2)
        self.assertIn('ZEC', send.await_args_list[0].args[2])

    async def test_auto_coin_switches_off_position_tracking(self):
        bot.auto_chats.add(123)
        bot.last_auto_messages[123] = 8
        prices = [('ZECUSDT', {'price': 50., 'change': 2., 'funding_rate': 0.})]
        with patch.object(bot, 'get_coin_prices', AsyncMock(return_value=prices)), \
             patch.object(bot, 'delete_telegram_message', AsyncMock()) as delete, \
             patch.object(bot, 'send_telegram_message', AsyncMock(side_effect=[1, 2])), \
             patch.object(bot, 'save_auto_chats'):
            await bot.handle_auto_command(None, 123, ['zec'])
        self.assertNotIn(123, bot.auto_chats)
        delete.assert_awaited_once_with(None, 123, 8)

    async def test_auto_off_disables_price_and_position_tracking(self):
        bot.auto_price_chats[123] = ['ZECUSDT']
        bot.last_auto_price_messages[123] = 9
        bot.auto_chats.add(123)
        bot.last_auto_messages[123] = 8
        with patch.object(bot, 'delete_telegram_message', AsyncMock()) as delete, \
             patch.object(bot, 'send_telegram_message', AsyncMock()), \
             patch.object(bot, 'save_auto_chats'):
            await bot.handle_auto_command(None, 123, ['off'])
        self.assertNotIn(123, bot.auto_price_chats)
        self.assertNotIn(123, bot.auto_chats)
        self.assertEqual(delete.await_count, 2)

    async def test_unknown_coin_does_not_replace_existing_list(self):
        bot.auto_price_chats[123] = ['ZECUSDT']
        with patch.object(bot, 'get_coin_prices', AsyncMock(return_value=[('NOPEUSDT', None)])), \
             patch.object(bot, 'send_telegram_message', AsyncMock()), \
             patch.object(bot, 'save_auto_chats') as save:
            await bot.handle_auto_command(None, 123, ['nope'])
        self.assertEqual(bot.auto_price_chats[123], ['ZECUSDT'])
        save.assert_not_called()


if __name__ == '__main__':
    unittest.main()
