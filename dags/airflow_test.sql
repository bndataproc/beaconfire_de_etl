CREATE OR REPLACE TRANSIENT TABLE airflow_function_test (name VARCHAR(250), id INT, load_utc_ts datetime);

INSERT INTO airflow_function_test VALUES ('name', 5, sysdate());
