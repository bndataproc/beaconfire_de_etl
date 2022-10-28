-- deduplicated
CREATE OR REPLACE temp table prestage_company_profile AS
SELECT
    MD5_NUMBER_LOWER64(symbol) as symbol_id
    ,*
FROM
(
    SELECT
        *
        ,row_number() over (partition by symbol order by id desc) as rnm
    FROM "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE"
) a
WHERE rnm = 1;


MERGE INTO DIM_COMPANY_PROFILE AS T
USING prestage_company_profile AS S
ON T.symbol_id=S.symbol_id
WHEN NOT MATCHED THEN
INSERT (
    symbol_id
    ,symbol
    ,price
    ,beta
    ,volavg
    ,mktcap
    ,lastdiv
    ,range
    ,changes
    ,companyname
    ,exchange
    ,industry
    ,sector
    ,dcfdiff
    ,dcf)
VALUES (
    S.symbol_id
    ,S.symbol
    ,S.price
    ,S.beta
    ,S.volavg
    ,S.mktcap
    ,S.lastdiv
    ,S.range
    ,S.changes
    ,S.companyname
    ,S.exchange
    ,S.industry
    ,S.sector
    ,S.dcfdiff
    ,S.dcf)
WHEN MATCHED THEN
UPDATE SET T.price=S.price
           ,T.beta=S.beta
           ,T.volavg=S.volavg
           ,T.mktcap=S.mktcap
           ,T.lastdiv=T.lastdiv
           ,T.range=S.range
           ,T.changes=S.changes
           ,T.companyname=S.companyname
           ,T.exchange=S.exchange
           ,T.industry=S.industry
           ,T.sector=S.sector
           ,T.dcfdiff=S.dcfdiff
           ,T.dcf=S.dcf;