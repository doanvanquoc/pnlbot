import unittest
from unittest.mock import patch

import pandas as pd

import backtest


def candles(*rows):
    return pd.DataFrame([
        {'open_time': i * 3600000, 'close_time': (i + 1) * 3600000 - 1,
         'open': op, 'high': high, 'low': low, 'close': close, 'volume': 1.0}
        for i, (op, high, low, close) in enumerate(rows)
    ])


def simulate(df, **kwargs):
    params = dict(start_idx=1, side='LONG', tp=140, sl=90, max_hold=len(df) - 1,
                  atr=5, fee_rate=0, slippage=0, funding_events=[], return_details=True)
    params.update(kwargs)
    return backtest.simulate_outcome(df, **params)


class BacktestSimulationTests(unittest.TestCase):
    def test_stop_precedes_partial_and_tp_on_ambiguous_bar(self):
        long = candles((100, 100, 100, 100), (100, 150, 85, 120))
        short = candles((100, 100, 100, 100), (100, 115, 50, 80))
        for df, side, sl, tp in ((long, 'LONG', 90, 140), (short, 'SHORT', 110, 60)):
            with self.subTest(side=side):
                result = simulate(df, side=side, sl=sl, tp=tp, partial_tp_rr=1.5)
                self.assertEqual(result.outcome, 'loss')
                self.assertAlmostEqual(result.r, -1)

    def test_expiry_marks_remaining_after_partial(self):
        df = candles((100, 100, 100, 100), (100, 116, 95, 113), (113, 114, 105, 110))
        result = simulate(df, partial_tp_rr=1.5)
        self.assertEqual(result.outcome, 'expired')
        self.assertAlmostEqual(result.r, 0.5 * 1.5 + 0.5 * 1.0)
        self.assertEqual(result.exit_ts, int(df.iloc[-1]['close_time']))

    def test_expiry_without_partial_is_not_zero(self):
        df = candles((100, 100, 100, 100), (100, 103, 94, 95))
        self.assertAlmostEqual(simulate(df).r, -0.5)

    def test_next_open_fill_fee_and_slippage(self):
        df = candles((100, 100, 100, 100), (105, 112, 102, 110))
        result = simulate(df, fee_rate=0.001, slippage=0.002)
        entry, exit_price = 105 * 1.002, 110 * 0.998
        expected = (exit_price - entry - 0.001 * (entry + exit_price)) / 10
        self.assertAlmostEqual(result.r, expected)

    def test_stop_gap_fills_open_not_stale_stop(self):
        df = candles((100, 100, 100, 100), (100, 102, 95, 100), (80, 85, 75, 82))
        result = simulate(df)
        self.assertAlmostEqual(result.r, -2)
        self.assertEqual(result.exit_ts, int(df.iloc[2]['open_time']))

    def test_entry_gap_beyond_protection_does_not_invent_profit(self):
        for op in (80, 150):
            with self.subTest(op=op):
                df = candles((100, 100, 100, 100), (op, op + 1, op - 1, op))
                result = simulate(df, fee_rate=0.001, slippage=0.001)
                self.assertLess(result.r, 0)
                self.assertEqual(result.exit_ts, int(df.iloc[1]['open_time']))

    def test_partial_breakeven_only_applies_next_bar(self):
        df = candles((100, 100, 100, 100), (100, 116, 95, 112), (98, 101, 95, 99))
        result = simulate(df, partial_tp_rr=1.5)
        self.assertEqual(result.exit_idx, 2)
        self.assertAlmostEqual(result.r, 0.75 - 0.1)

    def test_trailing_uses_prior_close_not_same_bar_extremes(self):
        df = candles((100, 100, 100, 100), (100, 125, 95, 110), (104, 108, 100, 105))
        result = simulate(df, trail_start_rr=0.8, trail_atr_mult=1)
        self.assertEqual(result.exit_idx, 2)
        self.assertAlmostEqual(result.r, 0.4)

    def test_funding_uses_remaining_and_direction(self):
        df = candles((100, 100, 100, 100), (100, 116, 95, 112), (112, 114, 105, 110))
        funding = [
            {'fundingTime': 5000000, 'fundingRate': 0.01, 'markPrice': 100},
            {'fundingTime': 8000000, 'fundingRate': 0.01, 'markPrice': 100},
        ]
        self.assertAlmostEqual(simulate(df, partial_tp_rr=1.5, funding_events=funding).r, 1.25 - 0.15)
        short = candles((100, 100, 100, 100), (100, 104, 95, 100))
        self.assertAlmostEqual(simulate(short, side='SHORT', sl=110, tp=60,
                                       funding_events=funding[:1]).r, 0.1)

    def test_requested_funding_cannot_silently_default_to_zero(self):
        df = candles((100, 100, 100, 100), (100, 105, 95, 100))
        with self.assertRaisesRegex(ValueError, 'funding'):
            simulate(df, funding_events=None)

    def test_incomplete_horizon_is_censored_even_if_stop_seen(self):
        df = candles((100, 100, 100, 100), (100, 105, 85, 100))
        result = simulate(df, max_hold=2)
        self.assertEqual(result.outcome, 'censored')
        self.assertEqual(result.cashflows, ())

    def test_drawdown_orders_real_cashflows_not_signal_entries(self):
        results = [
            backtest.SimulationResult('loss', -2, 5, 50, ((50, -2),)),
            backtest.SimulationResult('loss', -3, 1, 10, ((10, -3),)),
            backtest.SimulationResult('win', 5, 3, 30, ((30, 5),)),
        ]
        stats = backtest.summarize(results)
        self.assertEqual(stats['realized_max_dd_r'], 3)
        self.assertEqual(stats['ev_r'], 0)

    def test_paired_comparison_blocks_overlapping_symbol(self):
        df = candles((100, 100, 100, 100), *[(100, 102, 95, 100)] * 6)
        signals = [dict(symbol='BTCUSDT', df=df, start=i, side='LONG', tp=140, sl=90,
                        ts=int(df.iloc[i]['open_time']), atr=5, funding_events=[])
                   for i in (1, 2, 4)]
        with patch.object(backtest, 'MAX_HOLD_BARS', 3):
            results = backtest.paired_results(signals, [('baseline', {}), ('current', {})])
        self.assertEqual([len(group) for group in results], [2, 2])
        self.assertEqual([r.exit_idx for r in results[0]], [3, 6])


if __name__ == '__main__':
    unittest.main()
