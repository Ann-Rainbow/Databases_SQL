CREATE TABLE D_product(
    product_id BIGSERIAL NOT NULL PRIMARY KEY,
    p_name VARCHAR(50) NOT NULL, -- можно ли называть столбец name, если высвечивается, что это
                                 -- зарезервированное слово?
    price MONEY NOT NULL,
    log_id INTEGER NOT NULL
);

CREATE TABLE D_counteragent(
    counteragent_id BIGSERIAL NOT NULL PRIMARY KEY,
    c_first_name VARCHAR(50) NOT NULL,
    c_last_name VARCHAR(50) NOT NULL,
    log_id INTEGER NOT NULL 

);

CREATE TABLE F_invoice(
    invoice_id BIGSERIAL NOT NULL PRIMARY KEY,
    f_date DATE NOT NULL,
    counteragent_id BIGSERIAL NOT NULL --FOREIGN KEY REFERENCES D_counteragent(counteragent_id),
    -- хз так ли связать с другой таблицей, -- могут ли быть разные типы данных в одинаковых столбцах
    -- разных таблиц?  -- Надо ли вписывать CONSTRAINT перед названием столбца?
    operation_type VARCHAR(7) NOT NULL, --income/outcome ИЛИ можно сделать 1/-1
    log_id INTEGER NOT NULL
);
ALTER TABLE F_invoice ADD CONSTRAINT F_counteragent_fkey foreign key (counteragent_id) REFERENCES D_counteragent;

CREATE TABLE F_invoice_line(
    invoice_id BIGSERIAL NOT NULL PRIMARY KEY, --FOREIGN KEY REFERENCES F_Invoice(invoice_id),
    -- можно ли так делать: первичный ключ и одновременно внешний?
    product_id BIGSERIAL NOT NULL, --FOREIGN KEY REFERENCES D_product(product_id),
    amount MONEY NULL,
    operation_type VARCHAR(7) NOT NULL,
    log_id INTEGER NOT NULL
);
ALTER TABLE F_invoice_line ADD CONSTRAINT F_invoice_fkey FOREIGN KEY (invoice_id) REFERENCES F_Invoice;
ALTER TABLE F_invoice_line ADD CONSTRAINT F_product_fkey FOREIGN KEY (product_id) REFERENCES D_product;


CREATE TABLE log_load_dwh(
    load_id BIGSERIAL NOT NULL PRIMARY KEY,
    load_started DATE NOT NULL, -- нужен формат даты именно yyyyQmmdd
    load_finished  DATE NOT NULL,
    load_result VARCHAR(50)
);


CREATE TABLE log_table_log(
    procedure_id SERIAL NOT NULL,
    proc_name VARCHAR(50) NOT NULL,
    load_id BIGSERIAL NOT NULL, --FOREIGN KEY REFERENCES log_load_dwh(load_id),
    load_started DATE NOT NULL,
    load_finished DATE NOT NULL,
    load_result VARCHAR(50) NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    log_id INTEGER NOT NULL --FOREIGN KEY REFERENCES D_product(log_id)  D_counteragent(log_id)
    --F_invoice(log_id)  F_invoice_line(log_id),
    -- Как сослаться внешним ключом на несколько таблиц?
);

ALTER TABLE log_table_log ADD CONSTRAINT log_load_fkey FOREIGN KEY (load_id) REFERENCES log_load_dwh; 
ALTER TABLE log_table_log ADD CONSTRAINT log_log_fkey  FOREIGN KEY (log_id) REFERENCES D_product; --D_counteragent, F_invoice, F_invoice_line; ОШИБКА
--ALTER TABLE log_table_log ADD CONSTRAINT log_log_fkey  FOREIGN KEY (log_id) REFERENCES D_counteragent; -- ОШИБКА
ALTER TABLE log_table_log ADD CONSTRAINT logcounter_log_fkey  FOREIGN KEY (log_id) REFERENCES D_counteragent;

CREATE TABLE log_error(
    error_code INTEGER NOT NULL PRIMARY KEY, -- можно это сделать первичным ключом?
    text_msg VARCHAR(50) NOT NULL,
    error_priority INTEGER NOT NULL,
    insert_date DATE NOT NULL,
    last_update DATE NOT NULL,
    load_id BIGSERIAL NOT NULL --FOREIGN KEY REFERENCES log_load_dwh(load_id),
);
ALTER TABLE log_error ADD CONSTRAINT log_load_fkey FOREIGN KEY (load_id) REFERENCES log_load_dwh; 
















