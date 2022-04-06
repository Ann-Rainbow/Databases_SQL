-- Вызов функций, процедур

SELECT public.dwh_load (
	p_load_period => 1,
	p_source_system_id => 111,
	p_seans_load_id => 11
); 

--RUN THIS
SELECT public.dwh_load(
	p_load_period => 10,
	p_source_system_id => 3
	--p_seans_load_id => 1
);

SELECT * FROM log_dwh_load_a;
SELECT * FROM log_table_load_a
ORDER BY date_end DESC;

SELECT * FROM log_table_load_A
ORDER BY load_id DESC;
SELECT * FROM log_dwh_load_A;
SELECT * FROM e_log_table_load_err_a;

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

SELECT public.fill_table_load_log(
	p_trg_table => 'TABLE1'::character varying,
    p_date_begin => '01.01.2001'::date,
    p_date_end => '01.01.2021'::date,
    p_source_system_id => 1234:: bigint,
    p_load_id => 2::bigint,
    p_seans_load_id => 2::bigint,
    p_load_name => 'Load_name'::character varying,
    p_load_status => 'Finished'::character varying,
    p_src_table => 'sa_product_a'::character varying, -- p_src_table 
	p_t_row_count => 0::bigint,
    p_e_row_count => 0::bigint,
	p_s_row_count => 0::bigint
	);
	
	
SELECT * FROM log_table_load_A;
SELECT * FROM E_log_table_load_err_A;

-- Run this.
CALL load_D_product_A (
	p_seans_load_id => 113
	--p_load_status => 'Started',
	--p_source_system_id => 1
);

SELECT * FROM d_counteragent_a;
SELECT * FROM D_product_A; 
SELECT * FROM SA_product_A;
SELECT * FROM e_log_table_load_err_a;
--DELETE FROM e_log_table_load_err_a;
SELECT * FROM log_table_load_A
ORDER BY load_id DESC;
SELECT * FROM log_dwh_load_A;


CALL load_D_counteragent_A (
	p_seans_load_id => 13
);
Select * FROM D_product_A;
--SELECT * FROM SA_counteragent_A;
SELECT * FROM D_counteragent_A; 
SELECT * FROM e_log_table_load_err_a;
--DELETE FROM e_log_table_load_err_a;
SELECT * FROM log_table_load_A
ORDER BY load_id DESC;
SELECT * FROM log_dwh_load_A;


CALL load_F_invoice_A (
	p_seans_load_id => 16
);

SELECT * FROM F_invoice_A; 
SELECT * FROM e_log_table_load_err_a;
SELECT * FROM log_table_load_A
ORDER BY load_id DESC;

ALTER TABLE D_product_A DROP COLUMN load_status;
ALTER TABLE SA_product_A DROP COLUMN load_status;


ALTER TABLE D_counteragent_A DROP COLUMN load_status;
ALTER TABLE SA_counteragent_A DROP COLUMN load_status;

INSERT INTO d_counteragent_a VALUES (-1, 0, 'undef', -1, -1, -1, -1);
INSERT INTO d_product_a VALUES (-1, 0,'undef', 0, -1, -1, -1);
INSERT INTO d_product_a VALUES ('26', '03', 'Cherry', '56', '130', '113', 'Started', 'true' );
INSERT INTO SA_product_A VALUES ('03', 'Cherry', '56', '2', '10', 'true' );
INSERT INTO SA_product_A VALUES ('0', NULL, '0', '0', '0', '0');

SELECT * FROM D_product_A;
SELECT * FROM SA_product_A;
DELETE FROM D_product_A
WHERE product_src_code = 'undef';

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
SELECT * FROM SA_product_A;

SELECT dproduct.product_ID, saproduct.product_code, saproduct.product_name, saproduct.price, 
saproduct.sa_load_id, saproduct.load_period, saproduct.is_last --dproduct.product_src_code

	FROM SA_product_A AS saproduct 
    LEFT JOIN D_product_A dproduct 
        ON saproduct.product_code = dproduct.product_src_code 
    WHERE  
        saproduct.is_last = 'true';




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
'1', '03', 'Bolsheva Ann', '02', '01', '01', '10', 'Started', 'true');


SELECT COUNT(*) AS row
FROM SA_product_A
GROUP BY product_code

IF row = 1
--COUNT(*) = 1
 	THEN RAISE NOTICE '=1, not the error';

INSERT INTO SA_invoice_A (
	invoice_id, counteragent_id, counteragent_code, f_date, operation_type, source_system_id,  
	sa_load_id, seans_load_id, load_period, load_status, is_last
)
VALUES (
'1', '05', '05', '02.02.2022', '1', '01', '01', '02', '10', 'Started', 'true');

SELECT * FROM SA_invoice_A;

SELECT dproduct.product_ID, saproduct.product_code, saproduct.product_name, saproduct.price, 
	saproduct.sa_load_id, saproduct.load_period, saproduct.is_last,
	COALESCE(saproduct.product_name,'XXX'),--dproduct.product_src_code,
	ROW_NUMBER() OVER(PARTITION BY saproduct.product_code) AS over_prod

	FROM SA_product_A AS saproduct 
    LEFT JOIN D_product_A dproduct 
        ON saproduct.product_code = dproduct.product_src_code 
    WHERE  
        saproduct.is_last = 'true';