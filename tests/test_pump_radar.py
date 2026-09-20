import unittest

import app_trading as bot

# Bộ chỉ số "mọi tầng momentum còn nguyên": base cộng hết = 12 (trước khi trần 10)
FULL_STACK_15M = {'close': 100., 'ema9': 98., 'ema21': 95., 'vwap': 90.,
                  'vol_ratio': 3.0, 'signal': 'LONG'}
FULL_STACK_1H = {'close': 100., 'ema9': 98., 'ema21': 95., 'signal': 'LONG',
                 'confidence': 'Rất mạnh'}


def score_with(change24, funding):
    return bot._pump_score(FULL_STACK_15M, FULL_STACK_1H, change24, funding, 5.0, 1.2)[0]


class PumpScoreTests(unittest.TestCase):
    def test_fresh_momentum_reaches_threshold(self):
        # Coin mới bay (+20%/24h), funding âm: đủ tầng -> trần 10 -> đạt ngưỡng báo
        self.assertGreaterEqual(score_with(20.0, -0.0002), bot.PUMP_SCORE_MIN)

    def test_late_stage_coin_is_not_alertable(self):
        # Regression: coin +150%/24h từng đạt đúng 10/10 vì trần điểm nuốt mất hình phạt,
        # khiến radar báo "FOMO ngay" đúng loại kèo cháy đuồi mà điểm phạt sinh ra để loại.
        self.assertLess(score_with(150.0, -0.0002), bot.PUMP_SCORE_MIN)

    def test_moderately_late_coin_is_penalized(self):
        # +80%/24h: phạt 1 điểm -> 9 < ngưỡng 10 (cũ là 11 -> trần 10)
        self.assertLess(score_with(80.0, -0.0002), bot.PUMP_SCORE_MIN)

    def test_crowded_long_funding_is_penalized(self):
        # funding 0.3%/kỳ: long quá đông, mất 2 điểm +2 squeeze
        self.assertLess(score_with(20.0, 0.003), bot.PUMP_SCORE_MIN)

    def test_score_floor_is_zero(self):
        # Phạt chồng: funding đông + OI rút + bay quá xa -> không âm
        score, reasons = bot._pump_score(FULL_STACK_15M, FULL_STACK_1H, 200.0, 0.004, -5.0, 0.9)
        self.assertGreaterEqual(score, 0.0)
        self.assertTrue(any('VERY late' in r for r in reasons))


class PumpMessageTests(unittest.TestCase):
    def make_candidate(self, **overrides):
        c = {'symbol': 'TESTUSDT', 'change24': 20.0, 'score': 10.0,
             'reasons': ['15m EMA stack chuẩn 🟢'], 'signal15m': 'LONG', 'signal1h': 'LONG',
             'confidence': 'Rất mạnh', 'close': 100.0, 'fomo_tp': 103.0, 'fomo_sl': 97.5,
             'vwap15m': 95.0, 'tp': 110.0, 'sl': 90.0}
        c.update(overrides)
        return c

    def fmt(self, cands):
        msg = bot._fmt_pump_message(cands)
        self.assertIsNotNone(msg)
        assert msg is not None  # cho Pyright: msg là str sau assert trên
        return msg

    def test_hot_candidate_shows_real_sl_distance_and_risk(self):
        # SL thực là -2.5%: message phải in đúng từ dữ liệu, kèm rủi ro 0.5% equity
        msg = self.fmt([self.make_candidate()])
        self.assertIn('−2.5%', msg)
        self.assertIn('+3.0%', msg)
        self.assertIn('0.5% equity', msg)
        self.assertIn('Vào được', msg)

    def test_near_threshold_candidate_shows_pullback_plan(self):
        c = self.make_candidate(score=9.0)
        msg = self.fmt([c])
        self.assertIn('Chờ pullback', msg)
        self.assertNotIn('Vào được', msg)

    def test_momentum_broken_candidate_drops_below_threshold(self):
        # Giá live gãy dưới EMA9 15m: −2 điểm phải đẩy kèo 10/10 xuống dưới ngưỡng,
        # và message phải ghi rõ lý do thay vì vẫn bảo "FOMO ngay".
        c = self.make_candidate(ema9_15m=101.0)
        bot._apply_live_entry(c, 100.0)  # close 100 < EMA9 101 -> gãy
        self.assertLess(c['score'], bot.PUMP_SCORE_MIN)
        msg = self.fmt([c])
        self.assertIn('gãy dưới EMA9', msg)
        self.assertNotIn('Vào được', msg)

    def test_momentum_intact_candidate_unaffected(self):
        c = self.make_candidate(ema9_15m=98.0)
        bot._apply_live_entry(c, 100.0)  # close 100 > EMA9 98 -> nguyên vẹn
        self.assertGreaterEqual(c['score'], bot.PUMP_SCORE_MIN)

    def test_live_entry_recomputes_scalp_levels(self):
        # Support xa hơn 2.5% bị bỏ, dùng luôn entry*0.975; resistance xa thì TP = +3%.
        c = self.make_candidate(support15m=95.0, resistance15m=108.0)
        bot._apply_live_entry(c, 100.0)
        self.assertEqual(c['fomo_sl'], 97.5)
        self.assertEqual(c['fomo_tp'], 103.0)

    def test_live_entry_accepts_near_support(self):
        c = self.make_candidate(support15m=98.0, resistance15m=103.0)
        bot._apply_live_entry(c, 100.0)
        self.assertEqual(c['fomo_sl'], 98.0)
        self.assertEqual(c['fomo_tp'], 103.0)

    def test_none_when_empty(self):
        self.assertIsNone(bot._fmt_pump_message([]))



if __name__ == '__main__':
    unittest.main()
