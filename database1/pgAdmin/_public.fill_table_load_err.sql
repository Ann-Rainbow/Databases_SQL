<<<<<<< HEAD
-- FUNCTION: public.fill_table_load_err(bigint, bigint, bigint, character varying, character varying, bigint, bigint, character varying, character varying, character varying)

-- DROP FUNCTION IF EXISTS public.fill_table_load_err(bigint, bigint, bigint, character varying, character varying, bigint, bigint, character varying, character varying, character varying);
=======
-- FUNCTION: public.fill_table_load_err(character varying, character varying, character varying, character varying, bigint, bigint, bigint, character varying, bigint)

-- DROP FUNCTION IF EXISTS public.fill_table_load_err(character varying, character varying, character varying, character varying, bigint, bigint, bigint, character varying, bigint);
>>>>>>> new_branch

CREATE OR REPLACE FUNCTION public.fill_table_load_err(
	p_trg_table character varying,
	p_err_type character varying,
	p_err_text character varying,
	p_key_value character varying,
	p_sa_load_id bigint,
	p_load_id bigint,
	p_seans_load_id bigint,
	p_load_name character varying,
<<<<<<< HEAD
	p_source_system_id bigint
	)
-- 	p_seans_load_id bigint,
-- 	p_load_name character varying, -- не может быть DEFAULT
-- 	p_source_system_id bigint, -- не может быть DEFAULT
-- 	p_load_period bigint, -- не может быть DEFAULT
	
-- 	p_load_id bigint DEFAULT -1, -- input parameters after one with a default value must also have defaults
-- 	p_sa_load_id bigint DEFAULT -1, 
-- 	p_trg_table character varying DEFAULT '-1'::character varying,
-- 	p_key_value character varying DEFAULT ''::character varying,
-- 	p_err_type character varying DEFAULT ''::character varying,
-- 	p_err_text character varying DEFAULT ''::character varying)
=======
	p_source_system_id bigint)
>>>>>>> new_branch
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
 

  BEGIN
<<<<<<< HEAD
RAISE NOTICE 'fill_table_load_err - after Begin';

=======
--RAISE NOTICE 'fill_table_load_err - after Begin';
>>>>>>> new_branch

    INSERT INTO E_log_table_load_err_A
    --ОБЪЯВЛЕНИЕ ПОЛЕЙ СДЕЛАТЬ!
      (load_id, -- Id загрузки
       seans_load_id, -- Id сеанса
       sa_load_id, -- Id загрузки из стейджинга
       load_name,
       trg_table, -- в какую таблицу происходит загрузка
       source_system_id, -- источник
       --load_period, -- загружаемый период
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
       --p_load_period, -- загружаемый период
       -- p_error_code,
       p_key_value,
       p_err_type,
       p_err_text);
--RAISE NOTICE 'fill_table_load_err - after Insert into - values';
       
       -- p_e_row_count, 
       -- p_status,
       -- p_group_name, -- Имя группы загружаемого объекта объекта
       --p_file_row_num, -- номер строки в файле
       -- substr(substr(p_err_text, -length(p_err_text)),1,500)); -- Комментарий может не вместиться.
     
     
      EXCEPTION WHEN OTHERS THEN
	  SELECT public.fill_table_load_err(
	p_trg_table => 'E_log_table_load_err_A',
	p_err_type => SQLSTATE,
	p_err_text => SQLERRM,
	p_key_value => NULL,
	p_sa_load_id => NULL,
	p_load_id => p_load_id,
	p_seans_load_id => p_seans_load_id,
	p_load_name => 'fill_table_load_err',
	p_source_system_id => p_source_system_id
	  
	  );
	  
--       INSERT INTO E_log_table_load_err_A
-- 	  		(
-- 		  	seans_load_id, -- => p_seans_load_id :: bigint,
-- 			load_name, -- => p_load_name :: character varying, -- не может быть DEFAULT
-- 			source_system_id ,-- p_source_system_id :: bigint, -- не может быть DEFAULT
-- 			load_period, --bigint, 
-- 			load_id, --bigint DEFAULT -1, -- input parameters after one with a default value must also have defaults
-- 			sa_load_id, --bigint DEFAULT -1, 
-- 			trg_table, --character varying DEFAULT '-1'::character varying,
-- 			key_value,
-- 		  	err_type, -- имеет DEFAULT
-- 			err_text -- имеет DEFAULT
			
-- 			)
--         VALUES (p_seans_load_id, p_load_name, p_source_system_id, p_load_period, p_load_id, 
-- 				p_sa_load_id, p_trg_table, p_key_value, SQLSTATE, SQLERRM);
--RAISE NOTICE 'fill_table_load_err - after Exception - values';

  END ;
$BODY$;

<<<<<<< HEAD
-- ALTER FUNCTION public.fill_table_load_err(bigint, character varying, bigint, bigint, bigint, bigint, character varying, character varying, character varying, character varying)
--     OWNER TO postgres;


=======
-- ALTER FUNCTION public.fill_table_load_err(character varying, character varying, character varying, character varying, bigint, bigint, bigint, character varying, bigint)
--     OWNER TO postgres;
>>>>>>> new_branch
