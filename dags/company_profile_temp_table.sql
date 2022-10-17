CREATE OR REPLACE temp table prestage_company_profile
AS
SELECT
    *
    ,sha2(symbol) as symbol_id
FROM
(
    SELECT
      *
      ,rank() over (partition by symbol order by rnm desc) as rk
    FROM
    (
        SELECT
           *
          ,row_number() over (order by (select 0)) as rnm
        FROM "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE"
        ) a
) b
WHERE rk = 1;