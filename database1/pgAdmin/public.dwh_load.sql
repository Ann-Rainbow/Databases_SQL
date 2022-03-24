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
   
    	INSERT INTO public.log_dwh_load_A
			(seans_load_id, load_status, date_begin, source_system_id) -- без $ лучше?
      		VALUES (p_seans_load_id, v_load_status, now(), p_source_system_id); -- без параметров можно, без p_seans_load_id seans_load_ID - для главной процедуры
    
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
		SET load_status = 'FINISHED', 
			date_end = now()
      WHERE seans_load_id = p_seans_load_id;


 EXCEPTION WHEN OTHERS THEN -- логирование в error_table
	  SELECT public.fill_table_load_err(
	p_trg_table => 'log_dwh_load_A',
	p_err_type => SQLSTATE,
	p_err_text => SQLERRM,
	p_key_value => NULL,
	p_sa_load_id => NULL,
	p_load_id => NULL,
	p_seans_load_id => p_seans_load_id,
	p_load_name => 'dwh_load',
	p_source_system_id => p_source_system_id
	  
	  );
		

END;

$BODY$;

