-- Вызов функций, процедур
CALL load_D_product_A (p_seans_load_id, p_load_status);
CALL load_D_product_A (log_dwh_load_A.seans_load_id, load_status);
SELECT public.dwh_load (log_dwh_load_A.seans_load_id, log_dwh_load_A.load_period, log_dwh_load_A.load_status) FROM log_dwh_load_A;
SELECT public.dwh_load (); -- не вызывается
SELECT public.dwh_load (p_seans_load_id, p_load_period, v_load_status); -- не вызывается
SELECT public.dwh_load (1, 1, 'Started'); -- вызывается, жалуется на public.fill_table_load_err()

SELECT public.fill_table_load_err();
SELECT public.fill_table_load_err(p_err_type, p_err_text);

CALL load_D_counteragent_A(1, 'Started');

-- Исправление ошибок
ALTER TABLE E_log_table_load_err_A ADD source_system_ID BIGINT NOT NULL;
ALTER TABLE e_log_table_load_err_a DROP COLUMN source_system_id;
ALTER TABLE e_log_table_load_err_a ADD source_system_id bigint;

ALTER TABLE e_log_table_load_err_a ALTER COLUMN source_system_id TYPE bigint;
--ALTER TABLE products DROP COLUMN description;

ALTER TABLE D_counteragent_A DROP COLUMN last_name;
ALTER TABLE D_counteragent_A RENAME COLUMN first_name TO counteragent_name;

ALTER TABLE SA_counteragent_A DROP COLUMN last_name;
ALTER TABLE SA_counteragent_A RENAME COLUMN first_name TO counteragent_name;



