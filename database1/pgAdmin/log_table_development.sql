-- -- Образец --
-- CREATE OR REPLACE FUNCTION test.fill_log_table_log (
--   load_id bigint,
--   p_seans_load_id bigint,
--   p_period bigint,
--   p_trg_table varchar,
--   p_group_name varchar,
--   p_load_name varchar,
--   p_status varchar,
--   p_t_row_count bigint = 0,
--   p_e_row_count bigint = 0,
--   p_s_row_count bigint = 0,
--   p_comm varchar = NULL::character varying
-- )
-- RETURNS void AS'
--   BEGIN
--     IF p_status = ''Started'' THEN
--       INSERT INTO LOG$TABLE_LOAD_LOG
--       VALUES
--         (p_seans_load_id,
--          p_load_id,
--          p_status,
--          p_load_name,
--          p_group_name,
--          p_t_row_count,
--          p_e_row_count,
--          null,
--          p_period,
--          p_trg_table,
--          now(),
--          null,
--          p_s_row_count);
--     ELSE
--       UPDATE LOG$log_table_log
--          SET status      = p_status,
--              T_ROW_COUNT = p_t_row_count,
--              E_ROW_COUNT = p_e_row_count,
--              date_end    = now(),
--              comm        =  substr(substr(comm || p_comm, -length(comm ||p_comm)),1,500),
--              s_row_count = p_s_row_count
--             --comm        = p_comm
--        WHERE load_id = p_load_id;
--     END IF;
--    /* COMMIT;
--   EXCEPTION
--   when others then
--   raise;*/
--   END;
-- 'LANGUAGE 'plpgsql'
-- VOLATILE
-- CALLED ON NULL INPUT
-- SECURITY INVOKER
-- COST 100;

--------------------------------------------------------------------------------------------------------
-- Логирование ошибок --



CREATE OR REPLACE FUNCTION public.fill_table_load_err (
  p_load_id bigint, --префикс p нужен? Он означает периодические таблицы же?
  p_seans_load_id bigint, -- в скрипте создания БД тоже надо с префиксом p именовать столбцы? Нет, только в процедурах.
  p_sa_load_id bigint,
  p_load_name varchar, -- ВМЕСТО GROUP_NAME LOAD_NAME
  p_trg_table varchar,
  p_source_system_id bigint,
  p_load_period bigint,
  --p_e_row_count bigint = 0, -- кол-во errors, строк в таргете
  --p_load_status varchar,
  --p_error_code integer, -- нужно ли это поле?
  --p_file_row_num bigint = 0, -- должен быть?
  p_key_value varchar = ''::character varying,
  p_err_type varchar = ''::character varying,
  p_err_text varchar = ''::character varying
  

)


RETURNS void AS' 

  BEGIN

    INSERT INTO log_table_load_err_A
    --ОБЪЯВЛЕНИЕ ПОЛЕЙ СДЕЛАТЬ!
      (load_id, -- Id загрузки
       seans_load_id, -- Id сеанса
       sa_load_id, -- Id загрузки из стейджинга
       load_name,
       trg_table, -- в какую таблицу происходит загрузка
       source_system_id, -- источник
       load_period, -- загружаемый период
       -- error_code,
       key_value,
       err_type,
       err_text



       -- file_row_num, -- номер строки в файле
       -- e_row_count, 
       -- status,
       -- group_name, -- Имя группы загружаемого объекта объекта - не нужно.
      )

    VALUES
      (p_load_id, -- Id загрузки
       p_seans_load_id, -- Id сеанса
       p_sa_load_id, -- Id загрузки из стейджинга
       p_load_name,
       p_trg_table, -- в какую таблицу происходит загрузка
       p_source_system_id, -- источник
       p_load_period, -- загружаемый период
       -- p_error_code,
       p_key_value,
       p_err_type,
       p_err_text);

       
       -- p_e_row_count, 
       -- p_status,
       -- p_group_name, -- Имя группы загружаемого объекта объекта
       --p_file_row_num, -- номер строки в файле
       -- substr(substr(p_err_text, -length(p_err_text)),1,500)); -- Комментарий может не вместиться.
     
     
      EXCEPTION WHEN OTHERS THEN
      INSERT INTO log_table_load_err_A(err_type, err_text)
        VALUES (SQLSTATE, SQLERRM);




  END ;
'LANGUAGE 'plpgsql'
VOLATILE  -- нужен ли последующий код?
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;


--------------------------------------------------------------------------------------------------------
-- Логирование загрузки таблиц --


CREATE OR REPLACE FUNCTION public.fill_table_load_log (
  p_load_id bigint,
  p_seans_load_id bigint,
  p_load_status varchar,
  p_load_name varchar,
  p_load_period bigint,
  p_trg_table varchar,
  p_source_table varchar,
  p_t_row_count bigint = 0, -- КОЛ-ВО ПРОЦЕДУР или таблиц? НЕ НУЖНО ??
  p_e_row_count bigint = 0, 
  p_s_row_count bigint = 0 -- НЕ НУЖНО ??

  --p_group_name varchar,
  --p_comm varchar = NULL::character varying

)
RETURNS void AS'
  BEGIN

    --start_log --логирование старта загрузки процедуры. Как сделать?
    --START LOG

    IF UPPER(p_status) = ''STARTED'' THEN
    --IF p_t_row_count = 0 THEN -- тут дб "Строка из SA новая?" Если да, новая, тогда, 

      INSERT INTO log_table_load_A 
    -- ОБЪЯВЛЕНИЕ ПОЛЕЙ СДЕЛАТЬ -- like this or how? or in braces ()? Yes, in braces
        (seans_load_id, --тут всё без префикса p
         load_id,
         load_status,
         load_name,
         load_period,
         trg_table,
         source_table,
         date_begin --почему не задекларировано, не написано ранее? - Это необязательно, нужно в других процедурах, 
                    --конкурирующих за ресурсы операционной системы.
        
        
         --group_name,
         --t_row_count,
         --e_row_count,
         --null,
         --now(),
         --null,
         --s_row_count

         )


      VALUES
        (p_seans_load_id,
         p_load_id,
         p_load_status,
         p_load_name,
         p_period,
         p_trg_table,
         p_source_table,
         now()

         --p_group_name,
         --p_t_row_count,
         --p_e_row_count,
         --null,
         --null,
         --p_s_row_count
        );


    ELSE
      UPDATE log_table_load_A 

        -- ОБЪЯВЛЕНИЕ ПОЛЕЙ СДЕЛАТЬ -- the same declaration as before?
        -- (seans_load_id,
        --  load_id,
        --  load_status,
        --  load_name,
        --  group_name,
        --  t_row_count,
        --  e_row_count,
        --  --null,
        --  period,
        --  trg_table,
        --  now(),
        --  --null,
        --  s_row_count);


         SET load_status = p_load_status,
             t_row_count = p_t_row_count,
             e_row_count = p_e_row_count,
             date_end    = now(),
             s_row_count = p_s_row_count

             --comm      = p_comm
             --comm      =  substr(substr(comm || p_comm, -length(comm ||p_comm)),1,500),
 
       WHERE load_id = p_load_id;
    END IF;

    EXCEPTION WHEN OTHERS THEN --вызвать процедуру  public.fill_table_load_err()
      -- INSERT INTO log_table_load_err_A(err_type, err_text)
      --  VALUES (SQLSTATE, SQLERRM);

      SELECT public.fill_table_load_err()        

    END;


  --end_log --логирование конца загрузки процедуры, выход.


  --  /* COMMIT;
  -- EXCEPTION --  OR EXCEPTIONS SHOULD BE RAISED AFTER?
  -- when others then
  -- raise;*/
  END;
'LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;

--------------------------------------------------------------------------------------------------------
-- Логирование загрузки всего хранилища -- -- Плюс логика запуска, отработки процедур.

CREATE OR REPLACE FUNCTION public.dwh_load ( --процедура включает в себя процедуру логирования и запуска нужных функций по порядку. 
-- Какие столбцы нужны в log_dwh_load?

  --p_load_id bigint,
  p_seans_load_id bigint, --входной параметр
  p_load_period bigint,
  --p_load_result varchar, -- передавать в начале исходные параматры, которые потом дальше не будут использоваться, не нужно.
  --p_trg_table varchar,
  --p_group_name varchar,
  --p_load_name varchar,
  --p_load_status varchar,
  -- p_t_row_count bigint = 0,
  -- p_e_row_count bigint = 0,
  -- p_s_row_count bigint = 0,
  v_load_status VARCHAR = ''::character varying
	--bigint = 0 --задекларировать, дать значение

  --p_comm varchar = NULL::character varying

  --date_begin DATE,
  --date_end DATE,
)



RETURNS void AS'
  DECLARE
  BEGIN
   


    --- КУРСОР ТУТ НЕ НУЖЕН
    --start_log --логирование старта загрузки хранилища. Как сделать?
    SELECT public.fill_table_load_log();
	
	--IF UPPER(v_load_status) = 1 THEN
	UPPER(v_load_status) = ''STARTED'' THEN --без if
    	INSERT INTO log_dwh_load_A
			(seans_load_id, load_status, date_begin) -- без $ лучше?
      	VALUES (p_seans_load_id, v_load_status, now()); -- без параметров можно, без p_seans_load_id seans_load_ID - для главной процедуры
    
    --CALL имя ( [ аргумент ] [, ...] ) -- вызывает процедуру

    --CALL load_D_product_A(p_seans_load_id => p_seans_load_id,
	--		p_load_status => p_load_status);

    -- CALL fill_table_load_log 
    --   (p_seans_load_id => p_seans_load_id,
		-- 	 p_load_id => v_load_id,
		-- 	 p_load_status => p_load_status);
    
    CALL load_D_counteragent_A(p_seans_load_id => p_seans_load_id,
			p_load_status => p_load_status);

--     CALL load_F_invoice_A(p_seans_load_id => p_seans_load_id,
-- 			p_load_status => p_load_status);

--     CALL load_F_invoice_line_A(p_seans_load_id => p_seans_load_id,
-- 			p_load_status => p_load_status);


    -- дописать основную логику работы, КОД НА СЕРВЕР, ЗАПУСТИТЬ ПРОЦЕДУРУ, ОСНОВНУЮ ЛОГИКУ , посмотреть работает ли.
    -- по блокам выполнять, не перепрыгивать, комментарии удалить.
	ELSE
    UPDATE log_dwh_load_A --dwh_load
		SET load_status = UPPER(v_load_status) = ''FINISHED'',
			date_end = now()
      WHERE seans_load_id = p_seans_load_id;
  --END IF; -- не нужно
  
  EXCEPTION WHEN OTHERS THEN 
    SELECT public.fill_table_load_err();
  
  

	-- INSERT INTO log_table_load_err_A(err_type, err_text)  --вызвать процедуру  public.fill_table_load_err()
  --   VALUES (SQLSTATE, SQLERRM);

    -- EXCEPTION WHEN condition  -- обработка исключений
    --   THEN handle_exception;

    -- EXCEPTION WHEN p_e_row_count > 0  -- IF there are errors
    --   THEN RAISE EXCEPTION ''ERRORS found'', p_e_row_count;

    
    -- IF p_e_row_count = 0 --Есть ли ошибки? Если нет ошибок 
    --   THEN -- выход из условия 
    --   ELSE -- логирование ошибки 
    -- END IF;

    -- test.fill_log_table_load_A(table, seans_ID, load_ID, date_begin); --запуск процедуры по загрузке таблиц -- сюда список таблиц передавать?
    --load_ID, источник данных - src_system
    

    -- IF TABLE IS NOT LAST -- последняя ли таблица? --nj
    --   THEN --запуск процедуры по загрузке таблиц
    --   ELSE end_log --логирование окочания загрузки, выход.
    -- END IF;
SELECT public.fill_table_load_log() -- окончание логирования, как сделать?
END;
END;
'LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;
  



