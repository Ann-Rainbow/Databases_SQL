-- FUNCTION: public.dwh_load(bigint, bigint, bigint)

-- DROP FUNCTION IF EXISTS public.dwh_load(bigint, bigint, bigint);

CREATE OR REPLACE FUNCTION public.dwh_load(
	p_load_period bigint,
	p_source_system_id bigint,
	p_seans_load_id bigint DEFAULT nextval('seq_seans_load_id'::regclass))
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
  DECLARE
  v_load_status character varying = 'STARTED';
  
  BEGIN

  
    INSERT INTO public.log_dwh_load_A -- логирование старта работы где? Тут
		(seans_load_id, load_status, date_begin, source_system_id) 
      	VALUES (p_seans_load_id, v_load_status, now(), p_source_system_id); -- без параметров можно, без p_seans_load_id seans_load_ID - для главной процедуры
    
    --CALL имя ( [ аргумент ] [, ...] ) -- вызывает процедуру

-- 			CALL fill_table_load_log 
-- 			 (p_seans_load_id => p_seans_load_id,
-- 			 p_load_id => v_load_id,
-- 			 p_load_status => p_load_status
			 

			 

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
  

    -- дописать основную логику работы, КОД НА СЕРВЕР, ЗАПУСТИТЬ ПРОЦЕДУРУ, ОСНОВНУЮ ЛОГИКУ , посмотреть работает ли.
    -- по блокам выполнять, не перепрыгивать, комментарии удалить.
	--ELSE
	v_load_status = 'FINISHED';
    UPDATE public.log_dwh_load_A --dwh_load --здесь дописать что если закончилось с ошибками, то обновить, что
								 -- закончилось FINISHED* именно в dwh_log. Или просто FINISHED туда же.
								 
								 
								 
		SET load_status = v_load_status,
			--IF 
			--load_status = 'FINISHED', 
			date_end = now()
      WHERE seans_load_id = p_seans_load_id;

 

 EXCEPTION WHEN OTHERS THEN -- логирование в error_table
	PERFORM public.fill_table_load_err(
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
	v_load_status = 'FINISHED*';


END;

$BODY$;

-- ALTER FUNCTION public.dwh_load(bigint, bigint, bigint)
--     OWNER TO postgres;
