-- PROCEDURE: public.load_f_invoice_a(bigint)

-- DROP PROCEDURE IF EXISTS public.load_f_invoice_a(bigint);

CREATE OR REPLACE PROCEDURE public.load_f_invoice_line_a(
	p_seans_load_id bigint,  -- IN 
	p_load_period bigint
	) AS $$
	
--LANGUAGE 'plpgsql'
--AS $BODY$
DECLARE
    v_load_id bigint;
	v_sa_load_id bigint DEFAULT nextval('seq_sa_load_id'); 
	v_t_row_count bigint = 0;
	v_e_row_count bigint = 0;
	v_load_status character varying; -- DEFAULT 'STARTED';

-- из таблицы D_counteragent вставить в SA_invoice counteragent_ID, counteragent_code.
    cur_line CURSOR IS  -- проверить строки на ошибки, грязь.
	SELECT  finv.invoice_ID, saline.invoice_code,
		    COALESCE(saline.amount, '-1') AS amount, saline.sa_load_id, saline.load_period,  saline.is_last
			--finv.invoice_ID,   --finv.load_period added.
			--ROW_NUMBER() OVER(PARTITION BY sainv.invoice_code) AS over_inv --убрать проверку на дубликаты.
	FROM SA_invoice_line_A AS saline 
	LEFT JOIN F_invoice_line_A fline
	ON  fline.invoice_src_code = saline.invoice_code --::varchar
	
	LEFT JOIN F_invoice_A finv
	ON finv.invoice_src_code = saline.invoice_code
	WHERE saline.is_last = 'true' AND
	saline.load_period = p_load_period;
	--p_load_period надо использовать в запросе тут p_load_period одной таблицы равен или как-то 
	--p_load_period другой. 
	--saline.load_period = fline.load_period;

	--saline.load_period = finv.load_period; --не работает
	--saline.load_period = finv.load_period; --AND
	
	--fline.load_period = finv.load_period;
	--saline.load_period = fline.load_period;--saline.load_period => p_load_period::bigint;
	--AND
	
    
BEGIN
    v_load_id = nextval('seq_load_id'); 
	v_load_status = 'STARTED'::character varying;
	
PERFORM public.fill_table_load_log ( 
	p_seans_load_id => p_seans_load_id::bigint,
	p_load_id => v_load_id::bigint,
	p_load_status => v_load_status::character varying, --v_load_status =>'STARTED'::character varying, 
	p_load_name => 'load_F_invoice_line_A'::character varying, 
	p_trg_table => 'F_invoice_line_A'::character varying,
	p_date_begin => now()::date,
    p_date_end =>  NULL::date, 
	p_source_system_id => 0::bigint,
	p_src_table =>'SA_invoice_line_A'::character varying,
	p_t_row_count => v_t_row_count::bigint,
	p_e_row_count => v_e_row_count::bigint,
	p_s_row_count => v_t_row_count + v_e_row_count::bigint
	);	

	DELETE FROM F_invoice_line_A 			--в этом случае ставится -1 куда-то.
	WHERE load_period = p_load_period;
	--load_period = p_load_period; --p_load_period; --где должны сохраняться старые данные?
						
BEGIN
	v_load_status = 'FINISHED'::character varying; 
    FOR v_line IN cur_line LOOP 
    BEGIN 
	v_sa_load_id = v_line.sa_load_id;
	
--RAISE NOTICE 'before fill_table_load_err 1';	
--duplicate key value violates unique constraint "pk_f_invoice_line_a"
	IF v_line.amount = '-1' THEN 
		PERFORM public.fill_table_load_err (
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_F_invoice_line_A'::character varying,
			p_trg_table => 'F_invoice_line_A'::character varying, 
			p_source_system_id => 0::bigint,
			p_key_value => 'Запись стоимости не найдена', --v_debug1,
																	--v_inv.counteragent_src_code, --чётко, подробно везде, чтобы идетифицировать, где ошибка.
			p_err_type => 'map',                    --SQLSTATE::character varying,
			p_err_text => 'Ошибка в строке. Код накладной: '||v_line.invoice_code||'. Ошибка в селекте, в начале процедуры.'    --SQLERRM::character varying 
	    	);
			v_e_row_count = v_e_row_count + 1;
			v_load_status = 'FINISHED*'::character varying; 
	--CONTINUE;
	END IF;
--RAISE NOTICE 'after fill_table_load_err 1';
	
-- insert or update on table "f_invoice_line_a" violates foreign key constraint "fk_f_invoice_line_a_invoice"
-- DETAIL:  Key (invoice_id)=(14) is not present in table "f_invoice_a".

--insert or update on table "f_invoice_line_a" violates foreign key constraint "fk_f_invoice_line_a_invoice"
--DETAIL:  Key (invoice_id)=(-1) is not present in table "f_invoice_a".	
	--IF v_inv.invoice_ID IS NULL THEN 
		INSERT INTO public.F_invoice_line_A (invoice_id,                 invoice_src_code, 			amount,
												 load_id,     load_period) 
 			VALUES                     (v_line.invoice_id,       v_line.invoice_code,        v_line.amount::numeric,
									           v_load_id,    p_load_period); --nextval('seq_F_invoice')
			v_t_row_count = v_t_row_count + 1;
			

	
	EXCEPTION
	WHEN OTHERS THEN
        PERFORM public.fill_table_load_err ( 
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_F_invoice_line_A'::character varying,
			p_trg_table => 'F_invoice_line_A'::character varying, 
			p_source_system_id => 0::bigint, --v_line.source_system_id::bigint,  
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
	p_load_name => 'load_F_invoice_line_A'::character varying, 
	p_trg_table => 'F_invoice_line_A'::character varying, 
	p_date_begin => NULL::date,
    p_date_end => now()::date, 
	p_source_system_id => 0::bigint, 
	p_src_table => 'SA_invoice_line_A'::character varying, 
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
			p_load_name => 'load_F_invoice_line_A'::character varying,
			p_trg_table => 'F_invoice_line_A'::character varying, 
			p_source_system_id => 0::bigint,
			p_key_value => 'Найдена ошибка',
			p_err_type => SQLSTATE::character varying,
			p_err_text => SQLERRM::character varying
	    	);
			v_e_row_count = v_e_row_count + 1;	
			v_load_status = 'FINISHED*'::character varying; 
			
END;
$$ LANGUAGE 'plpgsql'
-- ALTER PROCEDURE public.load_f_invoice_a(bigint)
--     OWNER TO postgres;
