CREATE OR REPLACE PROCEDURE load_F_invoice_line_A (   
    p_seans_load_id bigint
    ) AS $$
DECLARE
    v_load_id bigint;
	v_sa_load_id bigint DEFAULT nextval('seq_sa_load_id'); 
	v_t_row_count bigint = 0;
	v_e_row_count bigint = 0;
	v_load_status character varying; -- DEFAULT 'STARTED';
-- т.к. новых зачений не было, ничего не меняется, проверка на ноль, заменяет значение на ноль. 
--load_status для всей таблицы, а не каждой строки. Не нужна колонка.


-- из таблицы D_counteragent вставить в SA_invoice counteragent_ID, counteragent_code.
    cur_line CURSOR IS  -- проверить строки а ошибки, грязь.
	SELECT  fline.invoice_ID, saline.counteragent_code, saline.product_id, saline.product_code, saline.amount,
		    saline.operation_type, saline.source_system_id, saline.load_id, saline.load_period --, sainv.load_status --  inv.load_id, --, COUNT(*) OVER (PARTITION BY saproduct.product_code) AS num_rows
		   --использование оконной функции over partition by.
	FROM SA_invoice_line_A AS saline 
    LEFT JOIN F_invoice_line_A fline 
        ON sainv.counteragent_code = dinv.counteragent_code 
    WHERE sainv.operation_type != COALESCE (finv.operation_type, '000') AND --что здесь должно быть? operation_type? 
        sainvoice.is_last = 'true';
		--GROUP BY saproduct.product_code
		--HAVING COUNT(*) = 1;
		
	--GROUP BY product_code;
	--IF COUNT(*) 
    
BEGIN
    v_load_id = nextval('seq_load_id'); 
	v_load_status = 'STARTED'::character varying;
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
	
BEGIN
	v_load_status = 'FINISHED'::character varying; 
    FOR v_invoice IN cur_invoice LOOP 
    BEGIN 
	
	
	-- Проверка на дубликаты. Тут ли должна быть?

--       
-- { { [ GLOBAL ] cursor_name } | @cursor_variable_name }   
-- [ INTO @variable_name [ ,...n ] ] 

-- 	 IF row > 1 THEN
-- 		RAISE NOTICE 'Duplicate rows'
-- 	 ELSE
-- 		RAISE NOTICE 'No duplicate rows';	
-- 	END IF;
	
	IF v_invoice.invoice_ID IS NULL THEN 
		INSERT INTO public.F_invoice_A (invoice_id,               counteragent_id,          counteragent_code,       f_date, 	 operation_type, 
												source_system_id, 			     load_id, 			 seans_load_id,        load_period,          load_status) 
 			VALUES                     (nextval('seq_F_invoice'), v_invoice.counteragent_id, v_invoice.counteragent_code, v_invoice.f_date::date, v_invoice.operation_type,
									v_invoice.source_system_id::bigint, 		v_load_id,          p_seans_load_id,     v_prod.load_period, 	v_load_status);	
			v_t_row_count = v_t_row_count + 1;
    ELSE
        UPDATE public.F_invoice_A 
        SET counteragent_code = v_invoice.counteragent_code,
			load_id = v_load_id
			--load_status = v_load_status
			-- как занести FINISHED* в load_dwh?
		WHERE v_invoice.counteragent_code = F_invoice_A.counteragent_code; 
        v_t_row_count = v_t_row_count + 1;
	END IF;
	
	EXCEPTION
	WHEN OTHERS THEN
        PERFORM public.fill_table_load_err ( 
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_F_invoice_A'::character varying,
			p_trg_table => 'F_invoice_A'::character varying, 
			p_source_system_id => v_prod.source_system_id::bigint,  
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





