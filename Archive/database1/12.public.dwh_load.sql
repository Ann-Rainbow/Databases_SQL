-- FUNCTION: public.dwh_load(bigint, bigint, character varying)

-- DROP FUNCTION IF EXISTS public.dwh_load(bigint, bigint, character varying);

CREATE OR REPLACE FUNCTION public.dwh_load(
	p_seans_load_id bigint, --входной параметр
	p_load_period bigint,
	
	v_load_status character varying DEFAULT ''::character varying)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
  DECLARE
  BEGIN
   

    --- КУРСОР ТУТ НЕ НУЖЕН
    --start_log --логирование старта загрузки хранилища. Как сделать?
    SELECT public.fill_table_load_log();
	
	--IF UPPER(v_load_status) = 1 THEN
	v_load_status = UPPER(v_load_status) = 'STARTED'; --без IF -- нужно ли 'STARTED'?
    	INSERT INTO public.log_dwh_load_A
			(seans_load_id, load_status, date_begin) -- без $ лучше?
      		VALUES (p_seans_load_id, v_load_status, now()); -- без параметров можно, без p_seans_load_id seans_load_ID - для главной процедуры
    
    --CALL имя ( [ аргумент ] [, ...] ) -- вызывает процедуру

--     CALL load_D_product_A(p_seans_load_id => p_seans_load_id,
-- 			p_load_status => p_load_status);

    -- CALL fill_table_load_log 
    --   (p_seans_load_id => p_seans_load_id,
		-- 	 p_load_id => v_load_id,
		-- 	 p_load_status => p_load_status);
    
--     CALL load_D_counteragent_A(p_seans_load_id => p_seans_load_id,
-- 			p_load_status => p_load_status);

--     CALL load_F_invoice_A(p_seans_load_id => p_seans_load_id,
-- 			p_load_status => p_load_status);

--     CALL load_F_invoice_line_A(p_seans_load_id => p_seans_load_id,
-- 			p_load_status => p_load_status); --call пока закомментировать.

    -- дописать основную логику работы, КОД НА СЕРВЕР, ЗАПУСТИТЬ ПРОЦЕДУРУ, ОСНОВНУЮ ЛОГИКУ , посмотреть работает ли.
    -- по блокам выполнять, не перепрыгивать, комментарии удалить.
	--ELSE
    UPDATE public.log_dwh_load_A --dwh_load
		SET load_status = UPPER(v_load_status) = 'FINISHED',
			date_end = now()
      WHERE seans_load_id = p_seans_load_id;
  --END IF; --IF быть не должно, иаче не войдет далее, не залогирует.
  
  EXCEPTION WHEN OTHERS THEN -- логирование в error_table
    SELECT public.fill_table_load_err (
		-- No function matches the given name and argument types. You might need to add explicit type casts.
		
		
		
		--p_load_id
		--p_load_period, -- bigint
		--v_load_status, -- character varying DEFAULT ''::character varying
 		
		
		
		--p_load_id, -- не задекларировано в именно этой процедуре, этого параметра не существует.

		--p_sa_load_id, -- => p_sa_load_id :: bigint,
		--p_trg_table,
		
-- 		p_load_id => p_load_id :: bigint DEFAULT -1,
-- 		p_sa_load_id => p_sa_load_id :: bigint DEFAULT -1,
-- 		p_trg_table => p_trg_table :: character varying DEFAULT ''::character varying,
		
		--p_trg_table => -1,
		p_seans_load_id => p_seans_load_id :: bigint, -- bigint
		p_load_name => 'LOAD_TABLE'::varchar, -- вручную в кавычках прописать.
		p_source_system_id => -1 :: bigint,  -- column "p_source_system_id" does not exist
		p_load_period => p_load_period :: bigint
		
		
-- 		CREATE CAST (p_source_system_id AS p_source_system_id)  -- явное приведение типов
-- 	     WITH FUNCTION public.fill_table_load_err (bigint)
--         [ AS ASSIGNMENT  ]
		
		

-- 		p_key_value, -- НЕуказывают, т.к. Default подряд идут
-- 		p_err_type,
-- 		p_err_text
		);
		
		--CREATE CAST (исходный_тип AS целевой_тип)  -- явное приведение типов
--     WITH FUNCTION (тип_аргумента [, ...]) (тип_аргумента [, ...])
--     [ AS ASSIGNMENT | AS IMPLICIT ]
		
		
		
-- 	CREATE CAST (исходный_тип AS целевой_тип) -- явное приведение типов
--     WITHOUT FUNCTION (тип_аргумента [, ...])
--     [ AS ASSIGNMENT | AS IMPLICIT ]

					-- без параметров в  запросе жаловалось на эту строку
					-- ошибка в параметрах
    --SELECT public.fill_table_load_err(SQLSTATE,SQLERRM);
  	

	-- INSERT INTO log_table_load_err_A(err_type, err_text)  --вызвать процедуру  public.fill_table_load_err()
  --   VALUES (SQLSTATE, SQLERRM);

    -- EXCEPTION WHEN condition  -- обработка исключений
    --   THEN handle_exception;

    -- EXCEPTION WHEN p_e_row_count > 0  -- IF there are errors
    --   THEN RAISE EXCEPTION 'ERRORS found', p_e_row_count;

    
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
$BODY$;

ALTER FUNCTION public.dwh_load(bigint, bigint, character varying)
    OWNER TO postgres;
