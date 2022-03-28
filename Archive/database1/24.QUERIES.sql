-- Вызов функций, процедур

SELECT public.dwh_load (
	p_load_period => 1,
	p_source_system_id => 111,
	p_seans_load_id => 11
); 
SELECT * FROM log_dwh_load_a;
SELECT * FROM log_table_load_a;

-- вызывается, жалуется на public.fill_table_load_err()
-- SELECT * FROM e_log_table_load_err_a;
-- SELECT public.fill_table_load_err();
-- SELECT public.fill_table_load_err(p_err_type, p_err_text);+
SELECT public.fill_table_load_err(
	p_trg_table => 'TABLE',
	p_err_type => 'TYPE',
	p_err_text => 'TEXT',
	p_key_value => 'RRR',
	p_sa_load_id => 1,
	p_load_id => 1,
	p_seans_load_id => 1,
	p_load_name => 'FFF',
	p_source_system_id => 1);
SELECT * FROM e_log_table_load_err_a;
CALL load_D_counteragent_A(1, 'Started');

SELECT fill_table_load_log(
	p_trg_table => 'TABLE1',
    p_date_begin => '01.01.2001',
    p_date_end => '01.01.2021',
    p_source_system_id => 123,
    p_load_id => 001,
    p_seans_load_id => 002,
    p_load_name => 'Load_name',
    p_load_status => 'Finished',
    p_src_table => 'sa_product_a', -- p_src_table 
	p_t_row_count => 0,
    p_e_row_count => 0,
	p_s_row_count => 0
	);
SELECT * FROM log_table_load_A;
SELECT * FROM E_log_table_load_err_A;

-- Run this.
CALL load_D_product_A (
	p_seans_load_id => 126
	--p_load_status => 'Started',
	--p_source_system_id => 1
);

SELECT * FROM D_product_A; 
SELECT * FROM SA_product_A;
SELECT * FROM e_log_table_load_err_a;
--DELETE FROM e_log_table_load_err_a;
SELECT * FROM log_table_load_A
ORDER BY load_id DESC;
SELECT * FROM log_dwh_load_A;

-- UPDATE таблица SET поле = значение
UPDATE D_product_A SET product_name = 'Onion' WHERE product_id = 26; 
DELETE FROM D_product_A;
DELETE FROM SA_product_A;
--delete from D_product_A where not (delete=not);

ALTER TABLE D_product_A ADD CONSTRAINT U_D_product_A_client UNIQUE (product_code); 
-- НЕ мб уникальным ключом, т.к. имеет дубликаты. Чтобы не было дубликатов - вручную прописать зависимости, как
--определяется product_code. Текущий product_code раве предыдущему id.
ALTER TABLE IF EXISTS public.d_product_a 
    ADD CONSTRAINT U_D_product_A_client  UNIQUE (product_code);
	
	




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

ALTER TABLE e_log_table_load_err_a ALTER COLUMN proc_name TYPE VARCHAR;
ALTER TABLE e_log_table_load_err_a DROP COLUMN proc_name;
ALTER TABLE e_log_table_load_err_a ADD proc_name VARCHAR;

-- ALTER [ COLUMN ] имя_столбца { SET | DROP } NOT NULL
ALTER TABLE e_log_table_load_err_a ALTER COLUMN error_code DROP NOT NULL; 
-- ERROR:  column "error_code" is in a primary key
-- SQL state: 42P16

ALTER TABLE e_log_table_load_err_a DROP COLUMN error_code;
ALTER TABLE e_log_table_load_err_a ADD error_code bigint;

DELETE FROM log_table_load_A;
SELECT * FROM log_table_load_A;
--ЗАПОЛНИТЬ SA product, затем процедуру, заполняющую данными. 
-- Отлаживать без exception, чтобы не записывать в БД ошибочное, а чтобы вываливалось как ошибка.
-- Кусочки кода run, отдельо их execute.
-- Вызов функции без параметров - PERFORM.
SELECT * FROM SA_product_A;

INSERT INTO SA_product_A ( 
	product_id,
    product_code,
    product_name,
    price,
    source_system_id,
    sa_load_id,
    seans_load_id,
    load_period,
    load_status,
    is_last
)
VALUES (
	'2', '03', 'Cherry', '56', '002', 2, 2, 10, 'Started', 'true' 
);


INSERT INTO SA_product_A ( 
	product_id,
    product_code,
    product_name,
    price,
    source_system_id,
    sa_load_id,
    seans_load_id,
    load_period,
    load_status,
    is_last
)
VALUES (
	'3', '04', 'Apple', '57', '003', 3, 3, 11, 'Started', 'true' 
);

INSERT INTO SA_counteragent_A (
	counteragent_id, counteragent_code, counteragent_name, source_system_id, sa_load_id, seans_load_id, 
	load_period, load_status, is_last
)
VALUES (
'1', '03', ''
)


