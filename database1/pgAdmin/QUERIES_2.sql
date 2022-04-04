
SELECT public.dwh_load (
	p_load_period => 10,
	p_source_system_id => 26,
	p_seans_load_id => 187
); 
SELECT * FROM log_dwh_load_a;
SELECT * FROM log_table_load_a
ORDER BY load_id DESC;

SELECT * FROM e_log_table_load_err_a
ORDER BY load_id DESC;

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
CALL load_D_counteragent_A(1);

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
	p_seans_load_id => 123
	--p_load_status => 'Started',
	--p_source_system_id => 1
);

SELECT public.dwh_load (10, 26, 11);
SELECT public.dwh_load (10, 26, 11);
SELECT public.dwh_load (202200000, 1, 189);
-- 	p_load_period => 10,
-- 	p_source_system_id => 26,
-- 	p_seans_load_id => 11
-- ); 

SELECT * FROM D_product_A; -- "duplicate key value violates unique constraint ""u_d_product_a_client"""
SELECT * FROM SA_product_A;
SELECT * FROM e_log_table_load_err_a
ORDER BY load_id DESC;
--DELETE FROM e_log_table_load_err_a;
SELECT * FROM log_table_load_A
ORDER BY load_id DESC;
SELECT * FROM log_dwh_load_A;
SELECT * FROM D_counteragent_A;
SELECT * FROM log_dwh_load_A;

-- CALL load_F_invoice_A (
-- 	p_seans_load_id => 123::bigint, p_load_id =>20::bigint
-- );

CALL load_F_invoice_A (153, 25); --load_period: 10, 20, 25
CALL load_F_invoice_line_A (177, 10); --load_period: 25
CALL load_D_product_A (21, 10); --load_period: 10, 11
CALL load_D_counteragent_A (146, 10); --load_period: 10, 20


CALL load_F_invoice_A (153, 25); --load_period: 
CALL load_F_invoice_line_A (177, 10); --load_period:  

CALL load_D_product_A (5, 202200000);
CALL load_D_product_A (6, 202201200);
CALL load_D_counteragent_A (7, 202200000);
CALL load_D_counteragent_A (8, 202201200);

CALL load_F_invoice_A (7, 202200000);
CALL load_F_invoice_A (7, 202201200);
CALL load_F_invoice_line_A (8, 202200000);
CALL load_F_invoice_line_A (9, 202201200);

SELECT * FROM SA_invoice_line_A;

SELECT * FROM F_invoice_line_A;
SELECT * FROM F_invoice_A;
SELECT * FROM SA_invoice_A;
SELECT * FROM SA_invoice_line_A;
SELECT * FROM D_product_A;
SELECT * FROM SA_product_A;
SELECT * FROM D_counteragent_A;
SELECT * FROM SA_counteragent_A;
SELECT * FROM e_log_table_load_err_a
ORDER BY load_id DESC;
SELECT * FROM log_table_load_A
ORDER BY load_id DESC;

SELECT * FROM D_counteragent_A;
SELECT counteragent_id, counteragent_src_code FROM D_counteragent_A; 
-- UPDATE таблица SET поле = значение
UPDATE D_product_A SET product_name = 'Onion' WHERE product_id = 26; 
DELETE FROM D_product_A;
DELETE FROM SA_product_A;
DELETE FROM e_log_table_load_err_A;
--delete from D_product_A where not (delete=not);
DELETE FROM F_invoice_A WHERE invoice_src_code = '1';
DELETE FROM F_invoice_A WHERE invoice_id = -1;

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
DELETE FROM F_invoice_A;
--ЗАПОЛНИТЬ SA product, затем процедуру, заполняющую данными. 
-- Отлаживать без exception, чтобы не записывать в БД ошибочное, а чтобы вываливалось как ошибка.
-- Кусочки кода run, отдельо их execute.
-- Вызов функции без параметров - PERFORM.
SELECT * FROM SA_product_A;
SELECT * FROM D_product_A;
SELECT * FROM D_counteragent_A;
SELECT * FROM SA_counteragent_A;
SELECT * FROM F_invoice_A;
SELECT * FROM SA_invoice_line_A;

INSERT INTO D_counteragent_A VALUES ( 2,'04', 'Maxim Danilov', 14, 14, 20);
INSERT INTO SA_counteragent_A VALUES ('04', 'Maxim Danilov', 14, 20, 'true');
INSERT INTO SA_product_A VALUES ('05', NULL, '65', 4, 10, 'true');

INSERT INTO SA_product_A ( product_id, product_code, product_name, price, source_system_id, sa_load_id,
    					seans_load_id, load_period,  load_status,  is_last)
VALUES ('2', '03', 'Cherry', '56', '002', 2, 2, 10, 'Started', 'true' );
INSERT INTO SA_invoice_line_A VALUES (50, 4, 10, 'true', '4');
INSERT INTO SA_invoice_line_A VALUES (65, 5, 10, 'true', '3');

INSERT INTO F_invoice_A VALUES (-1, -1, NULL, -1, -1, -1, -1, '0', '0', '0', -1);
SELECT * FROM D_product_A;
SELECT * FROM F_invoice_A;

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

INSERT INTO SA_counteragent_A VALUES (NULL, NULL, 3, 10, 'true');
SELECT * FROM SA_product_A;
SELECT * FROM SA_counteragent_A;
SELECT * FROM SA_invoice_A;	
SELECT * FROM SA_invoice_line_A;	
SELECT * FROM F_invoice_line_A;
SELECT * FROM F_invoice_A;	
SELECT * FROM D_product_A;	
SELECT * FROM D_counteragent_A;
UPDATE SA_invoice_line_A SET invoice_code = '3' WHERE sa_load_id = '5';
UPDATE SA_invoice_line_A SET load_period = '11' WHERE sa_load_id = '4';

--GRANT ALL ON ALL SEQUENCES IN SCHEMA schema_name TO user_name;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO postgres;

CREATE SEQUENCE IF NOT EXISTS seq_F_invoice INCREMENT BY 1
    MINVALUE 1 NO MAXVALUE START WITH 1;
    --[ OWNED BY { имя_таблицы.имя_столбца | NONE } ]
	
DELETE FROM SA_invoice_A;	
DELETE FROM e_log_table_load_err_a WHERE seans_load_id = 14;
DELETE FROM SA_product_A WHERE product_code = '0';
DELETE FROM F_invoice_A WHERE load_id = 240;
DELETE FROM D_product_A WHERE product_id = 1;
DELETE FROM SA_counteragent_A WHERE sa_load_id = 2;
DELETE FROM D_counteragent_A WHERE counteragent_id = 5;
DELETE FROM D_counteragent_A WHERE counteragent_id = 6;
DELETE FROM D_counteragent_A WHERE counteragent_id = 4;
DELETE FROM SA_counteragent_A WHERE sa_load_id = 3;
DELETE FROM F_invoice_line_A WHERE invoice_id = -1;
DELETE FROM SA_invoice_line_A WHERE sa_load_id = 6;


INSERT INTO SA_invoice_A VALUES ('05', '2022-02-02', 1, 1, 10, 'true', '1', '20');
INSERT INTO SA_invoice_A VALUES ('07', '2022-03-03', -1, 2, 20, 'true', '2', '30');
INSERT INTO SA_invoice_A VALUES ('03', '2022-05-03', 1, 3, 25, 'true', '3', '03');
INSERT INTO SA_invoice_A VALUES ('04', '2020-02-02', 1, 4, 10, 'true', '4', '04');
--UPDATE products SET price = 10 WHERE price = 5;
UPDATE SA_invoice_A SET product_code = '03' WHERE product_code = '30';
UPDATE SA_invoice_A SET product_code = '04' WHERE product_code = '20';

INSERT INTO F_invoice_A VALUES (-1, -1, NULL, -1, -1, -1, -1, '-1', '-1', '-1', -1); -- ДОДЕЛАТЬ!
INSERT INTO F_invoice_line_A VALUES (-1, 0, -1, -1, '0');
SELECT * FROM F_invoice_A;
SELECT * FROM SA_invoice_A;
SELECT * FROM SA_counteragent_A;
INSERT INTO SA_invoice_line_A VALUES ('100', 3, 25, 'true', 3 );
INSERT INTO F_invoice_A VALUES ('05', '2022-02-02', 1, 1, 10, 'true', '1', '20');
INSERT INTO SA_counteragent_A VALUES ('03', 'Bolsheva Ann', 1, 10, 'true');
INSERT INTO SA_counteragent_A VALUES ('05', NULL, 2, 10, 'true');
INSERT INTO SA_invoice_line_A VALUES (80, 6, 10, 'true', NULL);
INSERT INTO SA_invoice_line_A VALUES (NULL, 6, 10, 'true', '1');

--поменять тип данных поля.
ALTER TABLE F_invoice_A ALTER COLUMN counteragent_src_code TYPE character varying;
ALTER TABLE F_invoice_A ALTER COLUMN invoice_src_code TYPE character varying;
ALTER TABLE SA_invoice_A ALTER COLUMN invoice_code TYPE character varying;
ALTER TABLE SA_product_A ALTER COLUMN load_period TYPE character varying;
ALTER TABLE SA_counteragent_A ALTER COLUMN load_period TYPE character varying;
ALTER TABLE SA_counteragent_A ALTER COLUMN load_period TYPE bigint USING load_period::bigint;
ALTER TABLE SA_product_A ALTER COLUMN load_period TYPE bigint USING load_period::bigint;

SELECT MAX(pk_f_invoice_line_a) FROM F_invoice_line_A;   
SELECT nextval(pk_f_invoice_line_a);

-- CONSTRAINT fk_name
--     FOREIGN KEY (child_col1, child_col2, ... child_col_n)
--     REFERENCES parent_table (parent_col1, parent_col2, ... parent_col_n)
--     ON DELETE CASCADE
--     [ ON UPDATE { NO ACTION | CASCADE | SET NULL | SET DEFAULT } ] 
-- );
ALTER TABLE F_invoice_line_A ADD CONSTRAINT fk_f_invoice_line_a_invoice 
FOREIGN KEY (invoice_id) REFERENCES F_invoice_A (invoice_id) ON DELETE CASCADE;

UPDATE SA_product_A SET load_period = '000010000' WHERE sa_load_id = '3'; --YYYYQMMDD
UPDATE SA_product_A SET load_period = '202200000' WHERE sa_load_id = '2';
UPDATE SA_product_A SET load_period = '202200000' WHERE sa_load_id = '4';
UPDATE SA_counteragent_A SET load_period = '202200000' WHERE load_period = '10';
UPDATE SA_counteragent_A SET load_period = 202201200 WHERE load_period = '20';
UPDATE SA_counteragent_A SET load_period = 202200000 WHERE load_period = '202200000';
UPDATE SA_invoice_A SET load_period = 202200000 WHERE load_period = 10;
UPDATE SA_invoice_A SET load_period = 202200000 WHERE load_period = 20;
UPDATE SA_invoice_A SET load_period = 202201200 WHERE load_period = 25;
UPDATE SA_product_A SET load_period = 202201200 WHERE sa_load_id = 3;

UPDATE SA_invoice_line_A SET load_period = 202200000 WHERE load_period = 10;
UPDATE SA_invoice_line_A SET load_period = 202200000 WHERE load_period = 11;
UPDATE SA_invoice_line_A SET load_period = 202201200 WHERE load_period = 25;

UPDATE SA_invoice_line_A SET sa_load_id = 4 WHERE sa_load_id = 5;

SELECT * FROM SA_product_A;
SELECT * FROM SA_counteragent_A;
SELECT * FROM SA_invoice_A;
SELECT * FROM SA_invoice_line_A;