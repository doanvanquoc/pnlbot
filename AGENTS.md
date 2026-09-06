# PnL Bot

## Quy tắc bắt buộc sau khi sửa code

- Mỗi lần sửa xong code (app.py, app_bybit.py, backtest.py...), **PHẢI restart bot** bằng lệnh:
  ```
  sudo systemctl restart pnlbot
  ```
- Sau khi restart, kiểm tra bot chạy ổn định:
  ```
  systemctl status pnlbot --no-pager
  ```
- Không kết thúc task khi chưa restart bot.

## Quy tắc định hướng giá trị

- Mọi thay đổi code phải hướng tới mục tiêu giúp người dùng **kiếm được tiền** khi trade theo bot.
- Ưu tiên cải thiện chất lượng tín hiệu, tỉ lệ thắng, quản lý rủi ro (SL/TP), và tránh các thay đổi làm bot vào lệnh liều hoặc mất kiểm soát vốn.

## Quy tắc kiểm chứng bằng backtest

- Bất kỳ thay đổi nào liên quan tới **flow trading của AI** (chấm điểm/tín hiệu, ngưỡng vào lệnh, SL/TP, trailing/breakeven, kích thước lệnh...) đều **PHẢI chạy backtest để đo lại trước khi áp dụng thật**:
  ```
  python3 backtest.py 6 2000
  ```
- Đối chiếu kết quả trước/sau thay đổi: win-rate, **EV/R, profit factor, max drawdown**. Chỉ giữ thay đổi nếu EV/R tăng hoặc drawdown giảm đáng kể, và tuyệt đối không để EV/R âm.
- Không thay đổi ngưỡng/chiến lược trading theo cảm tính — phải có số liệu backtest hỗ trợ.