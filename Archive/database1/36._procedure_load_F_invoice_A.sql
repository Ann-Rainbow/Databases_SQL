<<<<<<< HEAD
CREATE OR REPLACE PROCEDURE load_F_invoice_A (   
    p_seans_load_id bigint
    ) AS $$
=======
-- PROCEDURE: public.load_f_invoice_a(bigint)

-- DROP PROCEDURE IF EXISTS public.load_f_invoice_a(bigint);

CREATE OR REPLACE PROCEDURE public.load_f_invoice_a(
	p_seans_load_id bigint,  -- IN 
	p_load_period bigint
	) AS $$
	
--LANGUAGE 'plpgsql'
--AS $BODY$
>>>>>>> new_branch
DECLARE
    v_load_id bigint;
	v_sa_load_id bigint DEFAULT nextval('seq_sa_load_id'); 
	v_t_row_count bigint = 0;
	v_e_row_count bigint = 0;
	v_load_status character varying; -- DEFAULT 'STARTED';

-- из таблицы D_counteragent вставить в SA_invoice counteragent_ID, counteragent_code.
    cur_inv CURSOR IS  -- проверить строки на ошибки, грязь.
<<<<<<< HEAD
	SELECT  finv.invoice_ID, COALESCE(dagent.counteragent_ID, '-1'),  sainv.f_date, sainv.operation_type,
		    sainv.sa_load_id, sainv.load_period, sainv.is_last, sainv.invoice_code,
			sainv.invoice_code, dagent.counteragent_src_code,
			ROW_NUMBER() OVER(PARTITION BY sainv.invoice_code)
			--, sainv.load_period, sainv.load_status --  inv.load_id, --, COUNT(*) OVER (PARTITION BY saproduct.product_code) AS num_rows
		   --использование оконной функции over partition by.
	FROM SA_invoice_A AS sainv 
    LEFT JOIN F_invoice_A finv 
        ON sainv.counteragent_code::bigint = finv.counteragent_src_code::bigint
		
	LEFT JOIN D_counteragent_A dagent
		ON finv.counteragent_src_code::bigint = dagent.counteragent_src_code::bigint
    --FROM F_invoice_A AS finv 
	WHERE  sainv.is_last = 'true';

		
	--GROUP BY product_code;
	--IF COUNT(*) 
=======
	SELECT  COALESCE(dagent.counteragent_ID, '-1') AS counteragent_ID, COALESCE(dprod.product_ID, '-1') AS product_ID,
		    sainv.invoice_code, sainv.counteragent_code,   sainv.product_code,
			sainv.f_date, sainv.operation_type, sainv.sa_load_id, sainv.is_last, sainv.load_period
			--finv.invoice_ID,
			--ROW_NUMBER() OVER(PARTITION BY sainv.invoice_code) AS over_inv --убрать проверку на дубликаты.
	FROM SA_invoice_A AS sainv 
	LEFT JOIN D_counteragent_A dagent
	ON  dagent.counteragent_src_code::varchar = sainv.counteragent_code
	
	LEFT JOIN D_product_A dprod
	ON dprod.product_src_code = sainv.product_code
	
-- 	LEFT JOIN F_invoice_line_A
	
 	WHERE sainv.is_last = 'true'  AND
 	sainv.load_period = p_load_period;
>>>>>>> new_branch
    
BEGIN
    v_load_id = nextval('seq_load_id'); 
	v_load_status = 'STARTED'::character varying;
<<<<<<< HEAD
=======
	
>>>>>>> new_branch
PERFORM public.fill_table_load_log ( 
	p_seans_load_id => p_seans_load_id::bigint,
	p_load_id => v_load_id::bigint,
	p_load_status => v_load_status::character varying, --v_load_status =>'STARTED'::character varying, 
	p_load_name => 'load_F_invoice_A'::character varying, 
	p_trg_table => 'F_invoice_A'::character varying,
	p_date_begin => now()::date,
    p_date_end =>  NULL::date, 
	p_source_system_id => 0::bigint,
	p_src_table =>'SA_invoice_A'::character varying,
	p_t_row_count => v_t_row_count::bigint,
	p_e_row_count => v_e_row_count::bigint,
	p_s_row_count => v_t_row_count + v_e_row_count::bigint
	);	
<<<<<<< HEAD
	
	DELETE FROM F_invoice_A --в этом случае ставится -1 куда-то.
	WHERE load_period = cur_inv.load_period; --p_load_period; --где должны сохраняться старые данные?
	
=======

-- update or delete on table "f_invoice_a" violates 
-- foreign key constraint "fk_f_invoice_line_a_invoice" on table "f_invoice_line_a"

	DELETE FROM F_invoice_A 			--в этом случае ставится -1 куда-то.
	WHERE load_period = p_load_period;
	--load_period = p_load_period; --p_load_period; --где должны сохраняться старые данные?
						
>>>>>>> new_branch
BEGIN
	v_load_status = 'FINISHED'::character varying; 
    FOR v_inv IN cur_inv LOOP 
    BEGIN 
<<<<<<< HEAD
	
=======
	v_sa_load_id = v_inv.sa_load_id;

>>>>>>> new_branch
	IF v_inv.counteragent_ID = '-1' THEN 
		PERFORM public.fill_table_load_err (
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_F_invoice_A'::character varying,
			p_trg_table => 'F_invoice_A'::character varying, 
			p_source_system_id => 0::bigint,
<<<<<<< HEAD
			p_key_value => 'Найдена ошибка',
			p_err_type => 'Нулевое значение',
			p_err_text => 'Найден NULL'
	    	);
			v_e_row_count = v_e_row_count + 1;
	END IF;
	
	
-- IF v_inv.product_name = 'XXX' THEN --v_prod.product_name 
-- 		PERFORM public.fill_table_load_err (
-- 			p_load_id => v_load_id::bigint,
-- 			p_seans_load_id => p_seans_load_id::bigint,
-- 			p_sa_load_id => v_sa_load_id::bigint,
-- 			p_load_name => 'load_D_product_A'::character varying,
-- 			p_trg_table => 'D_product_A'::character varying, 
-- 			p_source_system_id => 0::bigint,
-- 			p_key_value => 'Найдена ошибка',
-- 			p_err_type => 'Нулевое значение',
-- 			p_err_text => 'Найден NULL'
-- 	    	);
-- 			v_e_row_count = v_e_row_count + 1;
-- 	END IF;


	IF v_inv.over_inv > 1 THEN
		 PERFORM public.fill_table_load_err (
=======
			p_key_value => 'Контрагент не найден.', --v_debug1,
																	--v_inv.counteragent_src_code, --чётко, подробно везде, чтобы идетифицировать, где ошибка.
			p_err_type => 'map',                    --SQLSTATE::character varying,
			p_err_text => 'Ошибка в строке. Код контрагента: '||v_inv.counteragent_code||'. Ошибка в селекте, в начале процедуры.'  --SQLERRM::character varying 
	    	);
			v_e_row_count = v_e_row_count + 1;
			v_load_status = 'FINISHED*'::character varying; 
	--CONTINUE;
	END IF;
		
    IF v_inv.product_ID = '-1' THEN --v_prod.product_name 
		PERFORM public.fill_table_load_err (
>>>>>>> new_branch
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_F_invoice_A'::character varying,
			p_trg_table => 'F_invoice_A'::character varying, 
			p_source_system_id => 0::bigint,
<<<<<<< HEAD
			p_key_value => 'Найдена ошибка',
			p_err_type => 'Дубликат',
			p_err_text => 'Cтрока уже существует'
	    	);
			v_e_row_count = v_e_row_count + 1;
	CONTINUE;
=======
			p_key_value => 'Продукт не найден.',
			p_err_type => 'map',
			p_err_text => 'Ошибка в строке. Код продукта: '||v_inv.product_code||'. Ошибка в селекте, в начале процедуры.'
	    	);
			v_e_row_count = v_e_row_count + 1;
			v_load_status = 'FINISHED*'::character varying; 
	--CONTINUE;
>>>>>>> new_branch
	END IF;

	
	--IF v_inv.invoice_ID IS NULL THEN 
		INSERT INTO public.F_invoice_A (invoice_id,               counteragent_id,          invoice_src_code,   counteragent_src_code,       f_date, 	      operation_type, 
<<<<<<< HEAD
												 load_id,   load_period) 
 			VALUES                     (nextval('seq_F_invoice'), v_inv.counteragent_id,    v_inv.invoice_code,  v_inv.counteragent_src_code, v_inv.f_date::date, v_inv.operation_type,
									           v_load_id,  v_inv.load_period);	
			v_t_row_count = v_t_row_count + 1;
--     --ELSE
--         UPDATE public.F_invoice_A  --updat'a нет?
--         SET --counteragent_code = v_inv.counteragent_code,
-- 			load_id = v_load_id
-- 			--load_status = v_load_status
-- 			-- как занести FINISHED* в load_dwh?
-- 		WHERE v_inv.counteragent_code = F_invoice_A.counteragent_src_code; 
--         v_t_row_count = v_t_row_count + 1;
-- 	--END IF;
=======
												 load_id,   load_period, seans_load_id, product_id,         product_src_code) 
 			VALUES                     (nextval('seq_F_invoice'), v_inv.counteragent_id,    v_inv.invoice_code,  v_inv.counteragent_code, v_inv.f_date::date, v_inv.operation_type::integer,
									           v_load_id, p_load_period, p_seans_load_id, v_inv.product_id, v_inv.product_code); 
			v_t_row_count = v_t_row_count + 1; --кол-во отработанных строк
			

>>>>>>> new_branch
	
	EXCEPTION
	WHEN OTHERS THEN
        PERFORM public.fill_table_load_err ( 
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_F_invoice_A'::character varying,
			p_trg_table => 'F_invoice_A'::character varying, 
<<<<<<< HEAD
			p_source_system_id => v_prod.source_system_id::bigint,  
=======
			p_source_system_id => v_prod.source_system_id::bigint,  --v_prod неверно, переделать.
>>>>>>> new_branch
			p_key_value => 'Найдена ошибка',
			p_err_type => SQLSTATE::character varying,
			p_err_text => SQLERRM::character varying
			);
			v_e_row_count = v_e_row_count + 1;
			v_load_status = 'FINISHED*'::character varying; 
		END;
		END LOOP;	
		END; 

PERFORM fill_table_load_log ( 
	p_seans_load_id => p_seans_load_id::bigint,
	p_load_id => v_load_id::bigint,
	p_load_status => v_load_status::character varying, 
	p_load_name => 'load_F_invoice_A'::character varying, 
	p_trg_table => 'F_invoice_A'::character varying, 
	p_date_begin => NULL::date,
    p_date_end => now()::date, 
	p_source_system_id => 0::bigint, 
	p_src_table => 'SA_invoice_A'::character varying, 
	p_t_row_count => v_t_row_count::bigint,
	p_e_row_count => v_e_row_count::bigint,
	p_s_row_count => v_t_row_count + v_e_row_count::bigint
	);
	
    EXCEPTION 
	WHEN OTHERS THEN 
    PERFORM public.fill_table_load_err (
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_F_invoice_A'::character varying,
			p_trg_table => 'F_invoice_A'::character varying, 
			p_source_system_id => 0::bigint,
			p_key_value => 'Найдена ошибка',
			p_err_type => SQLSTATE::character varying,
			p_err_text => SQLERRM::character varying
	    	);
			v_e_row_count = v_e_row_count + 1;	
			v_load_status = 'FINISHED*'::character varying; 
			
END;
$$ LANGUAGE 'plpgsql'
<<<<<<< HEAD





=======
-- ALTER PROCEDURE public.load_f_invoice_a(bigint)
--     OWNER TO postgres;
>>>>>>> new_branch
