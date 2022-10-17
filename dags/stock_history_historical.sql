INSERT INTO stock_history_historical
    SELECT
        *
        ,current_date() as load_id
        ,sha2(symbol) as symbol_id
    FROM "US_STOCKS_DAILY"."PUBLIC"."STOCK_HISTORY"