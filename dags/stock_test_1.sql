INSERT INTO group_3_stock_test_1 (
  symbol,
  price_date,
  open_price,
  high_price,
  low_price,
  close_price,
  volume,
  adj_close_price
)
SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    adjclose
FROM US_STOCKS_DAILY.PUBLIC.STOCK_HISTORY
WHERE date = current_date();