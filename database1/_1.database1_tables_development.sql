-- Из внешнего мира в таблицу Staging кладутся сырые данные. Под каждую таблицу - staging таблицы. Все поля VARCHAR.
-- Надо ли связывать связями таблицу сырых данных со справочником/словарем, куда кладутся эти данные?
CREATE TABLE SA_product_A(
    product_ID  VARCHAR NOT NULL,
    product_code VARCHAR NULL,
    --counteragent_code VARCHAR NULL, 
    product_name VARCHAR(50) NOT NULL,
    price VARCHAR NOT NULL, 

    source_system_ID VARCHAR NOT NULL, 
    --load_id INTEGER, --ДОЛЖЕН БЫТЬ NOT NULL OR NULL?
    sa_load_ID BIGINT, -- тут должно быть sa_load_ID или load_ID ?
    seans_load_ID INTEGER NOT NULL,  --ДОЛЖЕН БЫТЬ - дописать в других SA также
    load_period INTEGER NOT NULL, 
    load_status VARCHAR, -- нужно ли это поле?
    is_last VARCHAR  -- ДОПОЛНИТЬ -- версии данных в стейджинге (SA)
                            -- нужен ли столбец log_ID в таблице сырых данных? Скорее нет.
                            -- Никаких констрейнтов в SA-таблицах?
    -- CONSTRAINT PK#A_SA_product PRIMARY KEY (product_ID), --НЕ ДОЛЖНО БЫТЬ В SA
    -- CONSTRAINT U#A_SA_product#client UNIQUE (client_code)
);



CREATE TABLE D_product_A( -- A - идентификатор имени Ann в каждой таблице добавлен.
    product_ID BIGSERIAL NOT NULL, --PRIMARY KEY, --ИЛИ СДЕЛАТЬ SEQUENCE
    product_code VARCHAR NULL, -- UNIQUE KEY, идентификатор ПРОДУКТА клиента, откуда пришла информация --LEAVE LIKE PRODUCT CODE CAN BE NULL?
    product_name VARCHAR NOT NULL, -- можно ли называть столбец name, если высвечивается, что это
    price NUMERIC NULL, --Can be amount in real type?  Changed from REAL to NUMERIC type
                                 -- зарезервированное слово?
    source_system_ID BIGINT NOT NULL, -- источник информации, откуда пришла информация. Это тоже UNIQUE KEY? --СЛУЖЕБНЫЕ ПОЛЯ НИЖЕ 
    load_ID INTEGER, -- Load_ID must be NOT NULL? OR NULL?
    seans_load_ID INTEGER NOT NULL,  --ДОЛЖЕН БЫТЬ - дописать в других SA также
    load_period INTEGER NOT NULL, 
    load_status VARCHAR,
    CONSTRAINT PK_D_product_A PRIMARY KEY (product_ID),  -- to declare primary key inside of create table or on alter table?
                                                    -- первичный и внешний ключи отдельными CONSTRAINT объявлять
    CONSTRAINT U_D_product_A_client UNIQUE (product_code) --НАИМЕНОВАНИЕ CONSTRAINT скорее всего исправить ?
    
);


CREATE TABLE SA_counteragent_A(
    counteragent_ID VARCHAR NOT NULL, -- PRIMARY KEY,
    counteragent_code VARCHAR NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация -- counteragent_code CAN BE NULL
<<<<<<< HEAD
    counteragent_name VARCHAR NOT NULL,
    --last_name VARCHAR NOT NULL,
=======
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
>>>>>>> new_branch

    source_system_ID VARCHAR NOT NULL, -- источник информации, откуда пришла информация. Здесь тоже в сырых данных тип VARCHAR?
    --load_ID INTEGER,
    sa_load_ID BIGINT, -- тут должно быть sa_load_ID или load_ID ?
    seans_load_ID INTEGER NOT NULL,  --ДОЛЖЕН БЫТЬ - дописать в других SA также
    load_period INTEGER NOT NULL, 
    load_status VARCHAR,
    is_last VARCHAR 
    -- CONSTRAINT PK#A_SA_counteragent PRIMARY KEY (counteragent_ID),
    -- CONSTRAINT U#A_SA_counteragent#client UNIQUE (client_code)

);


CREATE TABLE D_counteragent_A(
    counteragent_ID BIGSERIAL NOT NULL, -- PRIMARY KEY,
    counteragent_code VARCHAR NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация --COUNTERAGENTCODE
<<<<<<< HEAD
    counteragent_name VARCHAR NOT NULL,
    --last_name VARCHAR NOT NULL,
=======
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
>>>>>>> new_branch


    source_system_ID BIGINT NOT NULL, -- источник информации, откуда пришла информация
    load_ID INTEGER,
    load_status VARCHAR,
    seans_load_ID INTEGER NOT NULL,  --ДОЛЖЕН БЫТЬ - дописать в других SA также
    load_period INTEGER NOT NULL, 

    CONSTRAINT PK_D_counteragent_A PRIMARY KEY (counteragent_ID),
    CONSTRAINT U_D_counteragent_A_client UNIQUE (counteragent_code)

);


CREATE TABLE SA_invoice_A(
    invoice_ID VARCHAR NOT NULL, 
    counteragent_ID VARCHAR NOT NULL,
    counteragent_code VARCHAR NULL, 
    --product_code VARCHAR NULL,
    f_date DATE NOT NULL,
    operation_type VARCHAR NOT NULL,

    source_system_ID VARCHAR NOT NULL, 
    sa_load_ID BIGINT, -- тут должно быть sa_load_ID или load_ID ?
    --load_ID INTEGER,
    seans_load_ID INTEGER NOT NULL,  --ДОЛЖЕН БЫТЬ - дописать в других SA также
    load_period INTEGER NOT NULL, 
    load_status VARCHAR,
    is_last VARCHAR
    -- CONSTRAINT PK#A_SA_invoice PRIMARY KEY (invoice_ID),
    -- CONSTRAINT U#A_SA_invoice#client UNIQUE (client_code) 
);


CREATE TABLE F_invoice_A(
    invoice_ID BIGSERIAL NOT NULL, -- PRIMARY KEY,
    counteragent_ID INTEGER NOT NULL, --FOREIGN KEY REFERENCES D_counteragent(counteragent_id),
    counteragent_code VARCHAR NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация
    -- хз так ли связать с другой таблицей, -- могут ли быть разные типы данных в одинаковых столбцах
    -- разных таблиц?  -- Надо ли вписывать CONSTRAINT перед названием столбца?
    f_date DATE NOT NULL,
    operation_type INTEGER NOT NULL, --income/outcome ИЛИ можно сделать 1/-1. Столбец для определения прихода или расхода.

    source_system_ID BIGINT NOT NULL, -- источник информации, откуда пришла информация
    load_ID INTEGER,
    seans_load_ID INTEGER NOT NULL,  --ДОЛЖЕН БЫТЬ - дописать в других SA также
    load_period INTEGER NOT NULL, 
    load_status VARCHAR,

    CONSTRAINT PK_F_invoice_A PRIMARY KEY (invoice_ID),
    CONSTRAINT U_F_invoice_A_client UNIQUE (counteragent_code) 
);
ALTER TABLE F_invoice_A ADD CONSTRAINT FK_F_invoice_A_counteragent FOREIGN KEY (counteragent_ID) REFERENCES D_counteragent_A(counteragent_ID);


CREATE TABLE SA_invoice_line_A(
    invoice_ID VARCHAR NOT NULL, 
    counteragent_code VARCHAR NULL,
    product_ID VARCHAR NOT NULL, 
    product_code VARCHAR NOT NULL,
    amount VARCHAR NULL, 
    operation_type VARCHAR NOT NULL,

    source_system_ID VARCHAR NOT NULL,
    sa_load_ID BIGINT, -- тут должно быть sa_load_ID или load_ID ?
    --load_ID INTEGER,
    seans_load_ID INTEGER NOT NULL,  --ДОЛЖЕН БЫТЬ - дописать в других SA также
    load_period INTEGER NOT NULL, 
    is_last VARCHAR

    -- CONSTRAINT PK#A_SA_invoice_line PRIMARY KEY (invoice_ID),
    -- CONSTRAINT U#A_SA_invoice_line#client UNIQUE (client_code)
);


CREATE TABLE F_invoice_line_A(
    invoice_ID BIGINT NOT NULL, -- PRIMARY KEY, --FOREIGN KEY REFERENCES F_Invoice(invoice_id),
    -- можно ли так делать: первичный ключ и одновременно внешний?
    counteragent_code VARCHAR NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация
    product_ID INTEGER NOT NULL, --FOREIGN KEY REFERENCES D_product(product_id),
    product_code VARCHAR NOT NULL,
    amount NUMERIC NULL, -- NULL means it can be NULL --there's no float type in pgsql -Can be amount in REAL type? 
                         -- Changed from REAL to NUMERIC type
    operation_type INTEGER NOT NULL,

    source_system_ID BIGINT NOT NULL, -- источник информации, откуда пришла информация
    load_ID INTEGER,
    seans_load_ID INTEGER NOT NULL,  --ДОЛЖЕН БЫТЬ - дописать в других SA также
    load_period INTEGER NOT NULL, 

    CONSTRAINT PK_F_invoice_line_A PRIMARY KEY (invoice_ID),
    CONSTRAINT U_F_invoice_line_A_client UNIQUE (counteragent_code)
);
ALTER TABLE F_invoice_line_A ADD CONSTRAINT FK_F_invoice_line_A_invoice FOREIGN KEY (invoice_ID) REFERENCES F_invoice_A(invoice_ID);
ALTER TABLE F_invoice_line_A ADD CONSTRAINT FK_F_invoice_line_A_product FOREIGN KEY (product_ID) REFERENCES D_product_A(product_ID);



CREATE TABLE log_dwh_load_A( -- Как это лучше именовать? dwh_load?
    
    trg_table VARCHAR, -- НУЖНО ЛИ ТУТ ПОЛЕ TRG_TABLE? ИЛИ ЕГО ТОЛЬКО В ЛОГИРОВАНИИ ТАБЛИЦ ИСПОЛЬЗОВАТЬ?
    date_begin DATE NOT NULL, -- нужен формат даты именно yyyyQmmdd -- 
    date_end DATE, -- нужен формат даты именно yyyyQmmdd -- 
    t_row_count BIGINT default 0,
    e_row_count BIGINT default 0, -- кол-во ошибок за всю загрузку
    s_row_count BIGINT,
    --comm VARCHAR,
    --load_finished  DATE NOT NULL,
    --group_name VARCHAR,

    source_system_ID BIGINT NOT NULL,
    load_ID BIGINT,-- PRIMARY KEY, --ТУТ МОЖЕТ БЫТЬ ТИП ДАННЫХ УЖЕ ПОСЛЕДОВАТЕЛЬНОСТЬ ИНКРЕМЕНТИРУЮЩАЯСЯ? Т.Е. НЕ КАК В ЛОГИРОВАНИИ ТАБЛИЦ.
    seans_load_ID BIGINT NOT NULL,
    --client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация. Нужно ли в таблицах логов это?
    load_name VARCHAR,
    load_period BIGINT, -- нужен формат даты именно yyyyQmmdd -- 
    load_status VARCHAR,
    load_result VARCHAR,

    CONSTRAINT PK_log_dwh_load_A PRIMARY KEY (load_ID) 
    --CONSTRAINT U#A_log_dwh_load#client UNIQUE (client_code)

);


CREATE TABLE log_table_load_A(
    --procedure_ID SERIAL NOT NULL,   -- ненужный столбец
    --proc_name VARCHAR(50) NOT NULL, -- ненужный столбец
    
    
    --client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация
    trg_table VARCHAR, -- НУЖНО ЛИ ИСПОЛЬЗОВАТЬ SRC_TABLE?
    date_begin DATE NOT NULL,
    date_end DATE,
    t_row_count BIGINT default 0,
    e_row_count BIGINT default 0,
    s_row_count BIGINT,

    source_system_ID BIGINT NOT NULL,
    load_ID BIGINT, --let it be primary key --FOREIGN KEY REFERENCES log_load_dwh(load_id),
    seans_load_ID BIGINT NOT NULL,
    load_name VARCHAR, 
    load_period BIGINT, --DATE NOT NULL, -- можно просто period? но тогда подсвечивается иначе.
    load_status VARCHAR, -- changed from p_status to load_status
    --group_name VARCHAR,

    --comm VARCHAR,
    --load_started DATE NOT NULL, -- названо date_begin
    --load_finished DATE NOT NULL, -- названо date_end
    --table_name VARCHAR(50) NOT NULL, -- названо trg_table

    --load_result VARCHAR(50) NOT NULL, -- нужен ли load_result?

    -- Нужен ли log_ID? Его в образце нет, хотя в логической схеме связывает таблицы/отношения.
    --load_ID INTEGER --FOREIGN KEY REFERENCES D_product(log_id)  D_counteragent(log_id)
    --F_invoice(log_id)  F_invoice_line(log_id),
    -- Как сослаться внешним ключом на несколько таблиц?
    CONSTRAINT PK_log_table_load_A PRIMARY KEY (load_ID)

);

ALTER TABLE log_table_load_A ADD CONSTRAINT FK_log_dwh_load_A_load FOREIGN KEY (load_ID) REFERENCES log_dwh_load_A(load_ID); --почему выделяет load красным?
--ALTER TABLE A_log_table_load ADD CONSTRAINT log_log_fkey  FOREIGN KEY (log_id) REFERENCES A_D_product; --D_counteragent, F_invoice, F_invoice_line; ОШИБКА -- не ссылаться на логи, это не бизнес-ключ, нагрузка на сервер.
--ALTER TABLE log_table_log ADD CONSTRAINT log_log_fkey  FOREIGN KEY (log_id) REFERENCES D_counteragent; -- ОШИБКА
--ALTER TABLE A_log_table_load ADD CONSTRAINT logcounter_log_fkey  FOREIGN KEY (log_id) REFERENCES A_D_counteragent; -- не ссылаться на логи, это не бизнес-ключ, нагрузка на сервер.

CREATE TABLE E_log_table_load_err_A( -- E means error
    
    trg_table VARCHAR NOT NULL,
    proc_name VARCHAR NOT NULL, -- procedure_name - is like reserved word
    e_row_count BIGINT,
    error_code INTEGER NOT NULL, -- PRIMARY KEY, -- можно это сделать первичным ключом?
    err_type VARCHAR NOT NULL,
    err_text VARCHAR,
    key_value VARCHAR,
    --file_row_num BIGINT NOT NULL, --нужно ли это поле?
    
    sa_load_ID BIGINT,
    load_ID BIGINT, --FOREIGN KEY REFERENCES log_load_dwh(load_id),
    load_procedure_name VARCHAR,-- зарезервированно
    seans_load_ID BIGINT,
    load_name VARCHAR NOT NULL,
    load_period BIGINT NOT NULL,
    load_status VARCHAR,

    --error_priority INTEGER NOT NULL, --вместо этого err_type
    --text_msg VARCHAR(50) NOT NULL, -- вместо этого err_text
    
    
    --PROCEDURE_NAME --ГДЕ ПРОИЗОШЛО


    -- Нужен ли error_code?  

    --client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация
    
    
    --insert_date DATE NOT NULL,
    --last_update DATE NOT NULL,

    CONSTRAINT PK_E_log_table_load_err_A PRIMARY KEY (error_code) 
);
ALTER TABLE E_log_table_load_err_A ADD CONSTRAINT FK_log_dwh_load_A_load FOREIGN KEY (load_ID) REFERENCES log_dwh_load_A(load_ID); --почему выделяет load красным?
-- Можно ли связать таблицы логов?















