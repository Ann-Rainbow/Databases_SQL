-- Из внешнего мира в таблицу Staging кладутся сырые данные. Под каждую таблицу - staging таблицы. Все поля VARCHAR.
-- Надо ли связывать связями таблицу сырых данных со справочником/словарем, куда кладутся эти данные?
CREATE TABLE A_SA_product(
    product_ID  VARCHAR NOT NULL, -- столбцы должны содержать префикс sa? sa_product_ID?
    client_code VARCHAR NOT NULL, -- столбцы должны быть названы одинаково со столбцами таблицы, куда загружают данные?
    source_system_ID VARCHAR NOT NULL, 
    name VARCHAR(50) NOT NULL,
    price VARCHAR NOT NULL, 
    loAD_ID INTEGER NOT NULL,
    --log_ID INTEGER NOT NULL,  -- нужен ли столбец log_ID в таблице сырых данных? Скорее нет.
    -- CONSTRAINT PK#A_SA_product PRIMARY KEY (product_ID),
    -- CONSTRAINT U#A_SA_product#client UNIQUE (client_code)
);



CREATE TABLE A_D_product( -- A - идентификатор имени Ann в каждой таблице добавлен.
    product_ID BIGSERIAL NOT NULL, --PRIMARY KEY,
    client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация 
    source_system_ID BIGINT NOT NULL, -- источник информации, откуда пришла информация. Это тоже UNIQUE KEY?
    p_name VARCHAR(50) NOT NULL, -- можно ли называть столбец name, если высвечивается, что это
                                 -- зарезервированное слово?
    price REAL NOT NULL, --Can be amount in real type?
    --log_ID INTEGER NOT NULL,
    load_ID INTEGER NOT NULL,
    CONSTRAINT PK_A_D_product PRIMARY KEY (product_ID),  -- to declare primary key inside of create table or on alter table?
                                                    -- первичный и внешний ключи отдельными CONSTRAINT объявлять
    CONSTRAINT U#A_D_product#client UNIQUE (client_code)
    
);


CREATE TABLE A_SA_counteragent(
    counteragent_ID VARCHAR NOT NULL, -- PRIMARY KEY,
    client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация
    source_system_ID VARCHAR NOT NULL, -- источник информации, откуда пришла информация. Здесь тоже в сырых данных тип VARCHAR?
    c_first_name VARCHAR(50) NOT NULL,
    c_last_name VARCHAR(50) NOT NULL,
    -- CONSTRAINT PK#A_SA_counteragent PRIMARY KEY (counteragent_ID),
    -- CONSTRAINT U#A_SA_counteragent#client UNIQUE (client_code)

);


CREATE TABLE A_D_counteragent(
    counteragent_ID BIGSERIAL NOT NULL, -- PRIMARY KEY,
    client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация
    source_system_ID BIGINT NOT NULL, -- источник информации, откуда пришла информация
    c_first_name VARCHAR(50) NOT NULL,
    c_last_name VARCHAR(50) NOT NULL,
    load_ID INTEGER NOT NULL,
    
    CONSTRAINT PK#A_D_counteragent PRIMARY KEY (counteragent_ID),
    CONSTRAINT U#A_D_counteragent#client UNIQUE (client_code)

);


CREATE TABLE A_SA_invoice(
    invoice_ID VARCHAR NOT NULL, 
    client_code VARCHAR NOT NULL, 
    counteragent_ID VARCHAR NOT NULL,
    source_system_ID VARCHAR NOT NULL, 
    operation_type VARCHAR NOT NULL,
    f_date DATE NOT NULL,
    -- CONSTRAINT PK#A_SA_invoice PRIMARY KEY (invoice_ID),
    -- CONSTRAINT U#A_SA_invoice#client UNIQUE (client_code) 
);


CREATE TABLE A_F_invoice(
    invoice_ID BIGSERIAL NOT NULL, -- PRIMARY KEY,
    client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация
    
    counteragent_ID INTEGER NOT NULL, --FOREIGN KEY REFERENCES D_counteragent(counteragent_id),
    -- хз так ли связать с другой таблицей, -- могут ли быть разные типы данных в одинаковых столбцах
    -- разных таблиц?  -- Надо ли вписывать CONSTRAINT перед названием столбца?

    source_system_ID BIGINT NOT NULL, -- источник информации, откуда пришла информация
    operation_type INTEGER NOT NULL, --income/outcome ИЛИ можно сделать 1/-1. Столбец для определения прихода или расхода.
    f_date DATE NOT NULL,
    load_ID INTEGER NOT NULL,
    CONSTRAINT PK#A_F_invoice PRIMARY KEY (invoice_ID),
    CONSTRAINT U#A_F_invoice#client UNIQUE (client_code) 
);
ALTER TABLE A_F_invoice ADD CONSTRAINT FK#A_F_invoice#counteragent FOREIGN KEY (counteragent_ID) REFERENCES A_D_counteragent(counteragent_ID);


CREATE TABLE A_SA_invoice_line(
    invoice_ID VARCHAR NOT NULL, 
    client_code VARCHAR NOT NULL,
    product_ID VARCHAR NOT NULL, 
    source_system_ID VARCHAR NOT NULL, 
    amount VARCHAR NULL, 
    operation_type VARCHAR NOT NULL,

    -- CONSTRAINT PK#A_SA_invoice_line PRIMARY KEY (invoice_ID),
    -- CONSTRAINT U#A_SA_invoice_line#client UNIQUE (client_code)
);


CREATE TABLE A_F_invoice_line(
    invoice_ID BIGINT NOT NULL, -- PRIMARY KEY, --FOREIGN KEY REFERENCES F_Invoice(invoice_id),
    -- можно ли так делать: первичный ключ и одновременно внешний?

    client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация
    product_ID INTEGER NOT NULL, --FOREIGN KEY REFERENCES D_product(product_id),
    source_system_ID BIGINT NOT NULL, -- источник информации, откуда пришла информация
    amount REAL NULL, -- NULL means it can be NULL --there's no float type in pgsql -Can be amount in REAL type?
    operation_type INTEGER NOT NULL,

    load_ID INTEGER NOT NULL,
    CONSTRAINT PK#A_F_invoice_line PRIMARY KEY (invoice_ID),
    CONSTRAINT U#A_F_invoice_line#client UNIQUE (client_code)
);
ALTER TABLE A_F_invoice_line ADD CONSTRAINT FK#A_F_invoice_line#invoice FOREIGN KEY (invoice_ID) REFERENCES A_F_invoice(invoice_ID);
ALTER TABLE A_F_invoice_line ADD CONSTRAINT FK#A_F_invoice_line#product FOREIGN KEY (product_ID) REFERENCES A_D_product(product_ID);



CREATE TABLE A_log_dwh_load(
    load_ID BIGINT NOT NULL,-- PRIMARY KEY,
    seans_load_ID BIGINT,
    --client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация. Нужно ли в таблицах логов это?
    load_period DATE NOT NULL, -- нужен формат даты именно yyyyQmmdd
    --load_finished  DATE NOT NULL,
    load_result VARCHAR(50),
    CONSTRAINT PK#A_log_dwh_load PRIMARY KEY (load_ID), 
    --CONSTRAINT U#A_log_dwh_load#client UNIQUE (client_code)
);


CREATE TABLE A_log_table_load(
    --procedure_ID SERIAL NOT NULL,   -- ненужный столбец
    --proc_name VARCHAR(50) NOT NULL, -- ненужный столбец
    
    load_ID BIGINT, --let it be primary key --FOREIGN KEY REFERENCES log_load_dwh(load_id),
    --client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация
    seans_load_ID BIGINT,
    status VARCHAR(100),
    load_name VARCHAR(400),
    group_name VARCHAR(100),
    t_row_count BIGINT default 0,
    e_row_count BIGINT default 0,
    comm VARCHAR(4000),

    period BIGINT, --DATE NOT NULL, -- можно просто period? но тогда подсвечивается иначе.
    --load_started DATE NOT NULL, -- азвано date_begin
    --load_finished DATE NOT NULL, -- названо date_end
    --table_name VARCHAR(50) NOT NULL, -- названо trg_table
    trg_table VARCHAR(1000),
    date_begin DATE NOT NULL,
    date_end DATE,
    s_row_count BIGINT,
    --load_result VARCHAR(50) NOT NULL, -- нужен ли load_result?

    -- Нужен ли log_ID? Его в образце нет, хотя в логической схеме связывает таблицы/отношения.
    load_ID INTEGER NOT NULL --FOREIGN KEY REFERENCES D_product(log_id)  D_counteragent(log_id)
    --F_invoice(log_id)  F_invoice_line(log_id),
    -- Как сослаться внешним ключом на несколько таблиц?
    CONSTRAINT PK#A_log_table_load PRIMARY KEY (load_ID),

);

ALTER TABLE A_log_table_load ADD CONSTRAINT FK#A_log_dwh_load#load FOREIGN KEY (load_ID) REFERENCES A_log_dwh_load(load_ID); --почему выделяет load красным?
--ALTER TABLE A_log_table_load ADD CONSTRAINT log_log_fkey  FOREIGN KEY (log_id) REFERENCES A_D_product; --D_counteragent, F_invoice, F_invoice_line; ОШИБКА -- не ссылаться на логи, это не бизнес-ключ, нагрузка на сервер.
--ALTER TABLE log_table_log ADD CONSTRAINT log_log_fkey  FOREIGN KEY (log_id) REFERENCES D_counteragent; -- ОШИБКА
--ALTER TABLE A_log_table_load ADD CONSTRAINT logcounter_log_fkey  FOREIGN KEY (log_id) REFERENCES A_D_counteragent; -- не ссылаться на логи, это не бизнес-ключ, нагрузка на сервер.

CREATE TABLE A_E_log_table_load_err( -- E means error
    load_ID BIGINT NOT NULL, --FOREIGN KEY REFERENCES log_load_dwh(load_id),
    seans_load_ID BIGINT NOT NULL,
    sa_load_ID BIGINT,
    load_name VARCHAR(100) NOT NULL,
    trg_table VARCHAR(100) NOT NULL,
    source_system_ID BIGINT NOT NULL,
    load_period BIGINT NOT NULL,
    file_row_num BIGINT NOT NULL,
    key_value VARCHAR(100),
    --error_priority INTEGER NOT NULL, --вместо этого err_type
    --text_msg VARCHAR(50) NOT NULL, -- вместо этого err_text
    err_type VARCHAR(10) NOT NULL,
    err_text VARCHAR(500),


    -- Нужен ли error_code?  
    error_code INTEGER NOT NULL, -- PRIMARY KEY, -- можно это сделать первичным ключом?
    --client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация
    
    
    --insert_date DATE NOT NULL,
    --last_update DATE NOT NULL,

    CONSTRAINT PK#A_E_log_table_load_err PRIMARY KEY (error_code) 
);
ALTER TABLE A_E_log_table_load_err ADD CONSTRAINT FK#A_log_dwh_load#load FOREIGN KEY (load_ID) REFERENCES A_log_dwh_load(load_ID); --почему выделяет load красным?
-- Можно ли связать таблицы логов? ---логи констрэйнтами не обкладываются. нет смысла связывать жесткой сцепкой. не бизнес-информация. 
--логи не должны мешаться работе.















