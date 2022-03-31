-- realization in Oracle / реализация в Оракл
-- CREATE OR REPLACE PROCEDURE filling_D_product( -- проверить название процедуры в соответствии с внутренним стандартом организации
--     pr_id IN D_product.product_id%TYPE -- можно ли называть одинаковым именем, как и в столбце таблицы?
--     pr_name IN D_product.p_name%TYPE
--     pr IN D_product.price%TYPE
--     prlog_id IN D_product.log_id%TYPE
-- )
-- IS 
-- BEGIN
--     INSERT INTO D_product VALUES (pr_id, pr_name, pr, prlog_id);
--     dbms_output.put_line('Product added.')
-- END;

-- EXECUTE filling_D_product(1, 'Apple', 20, 00001); --cmd command

-- SELECT * FROM D_product WHERE product_id = 1; --cmd command


-- не работает переименовать
-- CREATE OR REPLACE PROCEDURE filling_D_product(
--     product_id IN D_product.product_id%TYPE 
--     p_name IN D_product.p_name%TYPE
--     price IN D_product.price%TYPE
--     log_id IN D_product.log_id%TYPE
-- )
-- IS 
-- BEGIN
--     INSER INTO D_product VALUES (product_id, p_name, price, log_id);
--     dbms_output.put_line('Product added.')
-- END;

-- realization in Microsoft / реализация в Майкрософт
-- CREATE PROCEDURE filling_D_product 
--     @product_id BIGSERIAL, 
--     @p_name VARCHAR(50), 
--     @price MONEY, 
--     @log_id INTEGER
--    AS  
--    BEGIN  
--       -- The print statement returns text to the user  
--       --PRINT 'Enter the data ' + CAST(@price AS varchar(50));  
--       -- A second statement starts here  
--       --SELECT ProductName, Price FROM vw_Names    
--       SET CAST(@price AS varchar(50));
--       SET @p_name = LTRIM(RTRIM(@p_name));
    
--       INSERT INTO D_product (product_id, p_name, price, log_id)
--         VALUES (@product_id, @p_name, @price, @log_id)

--       SELECT * FROM D_product
--       WHERE product_id = @product_id

--    END  
-- GO   
-- не понятно, сохранилась ли процедура.


-----------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE filling_D_product() --triple(INOUT * int)
    ---
    LANGUAGE pl/sql
    AS $$
    BEGIN
        --x := x*3;
    END;
    $$;

    DO $$
    DECLARE --myvar int := 5;
    BEGIN
        CALL --triple(myvar);
        RAISE NOTICE --'myvar = %', myvar;
    END;
    $$;




CREATE OR REPLACE FUNCTION test.fill_table_A_D_product (
    product_ID BIGSERIAL NOT NULL, --PRIMARY KEY,
    client_code VARCHAR NOT NULL, -- UNIQUE KEY, идентификатор клиента, откуда пришла информация 
    source_system_ID BIGINT NOT NULL, -- источник информации, откуда пришла информация. Это тоже UNIQUE KEY?
    p_name VARCHAR(50) NOT NULL, -- можно ли называть столбец name, если высвечивается, что это
                                 -- зарезервированное слово?
    price REAL NOT NULL, --Can be amount in real type?
    log_ID INTEGER NOT NULL,
  
  

    --p_comm varchar = NULL::character varying -- Нужен ли этот столбец?
)
RETURNS void AS'
  BEGIN
    IF product_ID IS NOT NULL --p_status = ''Started'' 
      THEN
      INSERT INTO A_D_product
      VALUES
        (
        p_product_ID, -- столбцы должны содержать префикс p? - только в теле процедуры?
        p_client_code, 
        p_source_system_ID, 
        p_name,
        p_price;


    ELSE
      UPDATE A_D_product 
         SET 
        product_ID = p_product_ID, 
        client_code = p_client_code, 
        source_system_ID = p_source_system_ID,
        name = p_name,
        price = p_price;

        -- так или так??? скорее верхнее. но как именно из SA загрузить?
        -- p_product_ID  = sa_product_ID,
        -- p_client_code = sa_client_code,
        -- p_source_system_ID = sa_source_system_ID,
        -- p_name = sa_name,
        -- p_price = sa_price;


         

       WHERE product_id = p_product_id;
    END IF;
   /* COMMIT;
  EXCEPTION
  when others then
  raise;*/
  END;
'LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;


-----------------------------------------------------------------------------------------------------------------------------------------------
-- На основе данных процедуры для логов я разработала собственную процедуру для загрузки данных в эту таблицу. 
-- Можно ли так делать и правильно ли это? Будет ли процедура работать?

-- Можно ли использовать код/конструкцию/процедуру для логов для создания процедуры по заполнению данными других таблиц?
-- Или для создания процедуры по загрузке данных в таблицы нужно использовать скрипт package LOAD_FROM_SA_20181012?



CREATE OR REPLACE FUNCTION test.fill_table_load_log (
  p_load_id bigint,
  p_seans_load_id bigint,
  p_period bigint,
  p_trg_table varchar,
  p_group_name varchar,
  p_load_name varchar,
  p_status varchar,
  p_t_row_count bigint = 0,
  p_e_row_count bigint = 0,
  p_s_row_count bigint = 0,
  p_comm varchar = NULL::character varying
)
RETURNS void AS'
  BEGIN
    IF p_status = ''Started'' THEN
      INSERT INTO LOG$TABLE_LOAD_LOG
      VALUES
        (p_seans_load_id,
         p_load_id,
         p_status,
         p_load_name,
         p_group_name,
         p_t_row_count,
         p_e_row_count,
         null,
         p_period,
         p_trg_table,
         now(),
         null,
         p_s_row_count);
    ELSE
      UPDATE LOG$TABLE_LOAD_LOG
         SET status      = p_status,
             T_ROW_COUNT = p_t_row_count,
             E_ROW_COUNT = p_e_row_count,
             date_end    = now(),
             comm        =  substr(substr(comm || p_comm, -length(comm ||p_comm)),1,500),
             s_row_count = p_s_row_count
            --comm        = p_comm
       WHERE load_id = p_load_id;
    END IF;
   /* COMMIT;
  EXCEPTION
  when others then
  raise;*/
  END;
'LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;

CREATE OR REPLACE FUNCTION fill_table_load_log_err (
  p_load_id bigint,
  p_seans_load_id bigint,
  p_load_period bigint,
  p_trg_table varchar,
  p_group_name varchar,
  p_source_system_id bigint,
  p_sa_load_id bigint,
  p_file_row_num bigint = 0,
  p_key_value varchar = ''::character varying,
  p_err_type varchar = ''::character varying,
  p_err_text varchar = ''::character varying
)
RETURNS void AS'
  BEGIN

    INSERT INTO LOG$TABLE_LOAD_ERR
    VALUES
      (p_load_id, -- Id загрузки
       p_seans_load_id, -- Id сеанса
       p_sa_load_id, -- Id загрузки из стейджинга
       p_group_name, -- Имя группы загружаемого объекта объекта
       p_trg_table, -- в какую таблицу происходит загрузка
       p_source_system_id, -- источник
       p_load_period, -- загружаемый период
       p_file_row_num, -- номер строки в файле
       p_key_value,
       p_err_type,
       substr(substr(p_err_text, -length(p_err_text)),1,500));

  END ;
'LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;


















