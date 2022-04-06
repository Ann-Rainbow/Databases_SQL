<<<<<<< HEAD
-- FUNCTION: public.dwh_load(bigint, bigint, character varying)

-- DROP FUNCTION IF EXISTS public.dwh_load(bigint, bigint, character varying);

CREATE OR REPLACE FUNCTION public.dwh_load(
	p_load_period bigint,
	p_source_system_id bigint,
	p_seans_load_id bigint DEFAULT nextval('seq_seans_load_id'))
     RETURNS void
     LANGUAGE 'plpgsql'
     COST 100
     VOLATILE PARALLEL UNSAFE
AS $BODY$
  DECLARE
  v_load_status character varying = 'STARTED';
  
  BEGIN

=======
-- FUNCTION: public.dwh_load(bigint, bigint, bigint)

-- DROP FUNCTION IF EXISTS public.dwh_load(bigint, bigint, bigint);

CREATE OR REPLACE FUNCTION public.dwh_load(
	p_load_period bigint,
	p_seans_load_id bigint, 
	p_source_system_id bigint DEFAULT nextval('seq_seans_load_id'::regclass))
	
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
  DECLARE
  v_load_status character varying = 'STARTED';
  --v_tbl character varying;
  --v_t_index_count integer; --= 0;
  
  	cur_load CURSOR IS
	SELECT load_status			--src_table --logt.
	FROM log_table_load_a logt
	WHERE seans_load_id = p_seans_load_id
	ORDER BY CASE WHEN  load_status = 'STARTED' THEN 1	  --v_load_status = 'STARTED'
				  WHEN	load_status = 'FINISHED*' THEN 2 --v_load_status = 'FINISHED*'
	              WHEN  load_status = 'FINISHED' THEN 3  --v_load_status = 'FINISHED'
				  ELSE 4 --результат выполнения CASE = 2, сортировка по load_status = 'FINISHED*'. Когда внутри CASE истина, остальное не проходит.
				  END; -- Далее FETCH извлекает строку 'FINISHED*' из курсора в цель, нужную переменную.
	-- сортировка: сначала 1, затем где 2, потом 3.			  
				  
				  
  -- The PG manual says the ORDER BY expression:
  --Each expression can be the name or ordinal number of an output column (SELECT list item)
  --, or it can be an arbitrary expression formed from input-column values.
  --Каждое выражение может быть именем или порядковым номером выходного столбца (элемент списка SELECT), 
  --или оно может быть произвольным выражением, сформированным из значений входного столбца.
  
  BEGIN
	--v_tbl = ('SA_product_A', 'SA_counteragent_A', 'SA_invoice_A', 'SA_invoice_line_A');
>>>>>>> new_branch
  
    INSERT INTO public.log_dwh_load_A -- логирование старта работы где? Тут
		(seans_load_id, load_status, date_begin, source_system_id) 
      	VALUES (p_seans_load_id, v_load_status, now(), p_source_system_id); -- без параметров можно, без p_seans_load_id seans_load_ID - для главной процедуры
<<<<<<< HEAD
    
    --CALL имя ( [ аргумент ] [, ...] ) -- вызывает процедуру


-- 			CALL fill_table_load_log 
-- 			 (p_seans_load_id => p_seans_load_id,
-- 			 p_load_id => v_load_id,
-- 			 p_load_status => p_load_status
			 

			 

CALL load_D_product_A(
	p_seans_load_id => p_seans_load_id::bigint
);	

CALL load_D_counteragent_A (
	p_seans_load_id => p_seans_load_id::bigint
);
			 
    
--     CALL load_D_counteragent_A(p_seans_load_id => p_seans_load_id,
-- 			p_load_status => p_load_status);

--     CALL load_F_invoice_A(p_seans_load_id => p_seans_load_id,
-- 			p_load_status => p_load_status);

--     CALL load_F_invoice_line_A(p_seans_load_id => p_seans_load_id,
-- 			p_load_status => p_load_status); --call пока закомментировать.

    -- дописать основную логику работы, КОД НА СЕРВЕР, ЗАПУСТИТЬ ПРОЦЕДУРУ, ОСНОВНУЮ ЛОГИКУ , посмотреть работает ли.
    -- по блокам выполнять, не перепрыгивать, комментарии удалить.
	--ELSE
	v_load_status = 'FINISHED';
    UPDATE public.log_dwh_load_A --dwh_load --здесь дописать что если закончилось с ошибками, то обновить, что
								 -- закончилось FINISHED* именно в dwh_log. Или просто FINISHED туда же.
		SET load_status = v_load_status,
			--IF e_row_count > 0 then
=======
			 

CALL load_D_product_A(
	p_seans_load_id => p_seans_load_id::bigint,
	p_load_period   => p_load_period::bigint
);	
	
CALL load_D_counteragent_A (
	p_seans_load_id => p_seans_load_id::bigint,
	p_load_period   => p_load_period::bigint
);
			 
CALL load_F_invoice_A(
	p_seans_load_id => p_seans_load_id::bigint,
	p_load_period   => p_load_period::bigint
);  
  
CALL load_F_invoice_line_A(
	p_seans_load_id => p_seans_load_id::bigint,
	p_load_period   => p_load_period::bigint
);    
  
-- 	v_load_status = 'FINISHED';
-- 	v_load_status = logt.load_status 
-- 	FROM log_table_load_a logt
-- 	WHERE seans_load_id = p_seans_load_id;

-- CREATE [ UNIQUE ] INDEX [ CONCURRENTLY ] [ [ IF NOT EXISTS ] имя ] ON имя_таблицы [ USING метод ]
--     ( { имя_столбца | ( выражение ) 
-- CREATE UNIQUE INDEX IF NOT EXISTS v_t_index_count ON log_table_load_a ( src_table ) 
-- 							WHERE log_table_load_a.seans_load_id = p_seans_load_id;

	
	--SELECT logt.src_table AS src_table FROM log_table_load_a logt;
	--FOR unnest(ARRAY['SA_product_A','SA_counteragent_A','SA_invoice_A','SA_invoice_line_A']) IN 
	--курсор FOR v_tbl IN
	
	

	
	
				  
-- 	FOR v_line IN cur_load LOOP
	OPEN cur_load;			  
	FETCH FROM cur_load INTO v_load_status;
-- 	END LOOP;
	--1-й строкой  
	-- сортировка от худшего к лучшему.
	--FETCH [направление { FROM | IN }] курсор INTO цель;	
-- 	IF  logt.load_status = 'FINISHED*'
-- 		FROM log_table_load_a logt
-- 		WHERE seans_load_id = p_seans_load_id --AND
			  --logt.src_table IN v_tbl[i]				--   query returned more than one row
			  --p(logt.src_table) = v_t_row_count::integer   -- нужно ли? как уточнить поле?
		      --if 

			  --v_t_index_count::integer = p(logt.src_table)
			  
-- 	THEN v_load_status = 'FINISHED*';
-- 	ELSE v_load_status = 'FINISHED';
-- 	END IF;
-- 	v_t_index_count = v_t_index_count::integer + 1;
	
	--RAISE NOTICE 'v_t_row_count';
	--END
-- 	END LOOP;
	
	
	
-- 	IF  logt.load_status = 'FINISHED*'
-- 		FROM log_table_load_a logt
-- 		WHERE seans_load_id = p_seans_load_id AND
-- 		src_table = 'SA_product_A' --OR src_table = 'SA_counteragent_A' OR src_table = 'SA_invoice_A' OR src_table = 'SA_invoice_line_A'
-- 	THEN v_load_status = 'FINISHED*';
	
-- 	ELSEIF logt.load_status = 'FINISHED*'
-- 	FROM log_table_load_a logt
-- 	WHERE seans_load_id = p_seans_load_id AND
-- 	src_table = 'SA_counteragent_A'
-- 	THEN v_load_status = 'FINISHED*';
	
-- 	ELSEIF 
	
-- 	ELSE v_load_status = 'FINISHED';
-- 	END IF;
	
	
	
-- 	LOOP
-- 	BEGIN
    UPDATE public.log_dwh_load_A --dwh_load --здесь дописать что если закончилось с ошибками, то обновить, что
								 -- закончилось FINISHED* именно в dwh_log. Или просто FINISHED туда же.
								 						 
		SET 
			load_status = v_load_status,
			--IF 
>>>>>>> new_branch
			--load_status = 'FINISHED', 
			date_end = now()
      WHERE seans_load_id = p_seans_load_id;

<<<<<<< HEAD

 


=======
 

>>>>>>> new_branch
 EXCEPTION WHEN OTHERS THEN -- логирование в error_table
	PERFORM public.fill_table_load_err(
	p_trg_table => 'log_dwh_load_A',
	p_err_type => SQLSTATE,
	p_err_text => SQLERRM,
<<<<<<< HEAD
	p_key_value => NULL,
=======
	p_key_value => 'Найдена ошибка', --NULL
>>>>>>> new_branch
	p_sa_load_id => NULL,
	p_load_id => NULL,
	p_seans_load_id => p_seans_load_id,
	p_load_name => 'dwh_load',
	p_source_system_id => p_source_system_id
	  );
<<<<<<< HEAD
	v_load_status = 'FINISHED*'; -- фиксирует ли в таблицу dwh_load?
		
=======
	v_load_status = 'FINISHED*';
>>>>>>> new_branch

END;

$BODY$;

<<<<<<< HEAD
=======
-- ALTER FUNCTION public.dwh_load(bigint, bigint, bigint)
--     OWNER TO postgres;
>>>>>>> new_branch
