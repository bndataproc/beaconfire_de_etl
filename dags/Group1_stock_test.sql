CREATE database stock_group1_dev;
create schema group1_schema;


CREATE TABLE stock_group1_dev.group1_schema.SYMBOLS_group1 as SELECT * FROM "US_STOCKS_DAILY"."PUBLIC"."SYMBOLS";
CREATE TABLE stock_group1_dev.group1_schema.COMPANY_PROFILE_group1 as SELECT * FROM "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE";
CREATE TABLE stock_group1_dev.group1_schema.stock_history_group1 as SELECT * FROM "US_STOCKS_DAILY"."PUBLIC"."stock_history";