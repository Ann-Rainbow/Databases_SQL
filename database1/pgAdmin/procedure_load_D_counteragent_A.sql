-- переделать процедуру.
CREATE OR REPLACE PROCEDURE load_D_counteragent_A (   
    p_seans_load_id bigint
    ) AS $$
DECLARE
    v_load_id bigint;
	v_sa_load_id bigint DEFAULT nextval('seq_sa_load_id'); 
	v_t_row_count bigint = 0;
	v_e_row_count bigint = 0;
	v_load_status character varying; --DEFAULT 'STARTED'; --добавлено

    cur_agent CURSOR IS 
	SELECT  dagent.counteragent_ID, saagent.counteragent_code, saagent.sa_load_id, 
	        saagent.load_period, saagent.is_last, COALESCE(saagent.counteragent_name, 'XXX') AS agent_name,
			ROW_NUMBER() OVER(PARTITION BY saagent.counteragent_code) AS over_agent
	FROM SA_counteragent_A AS saagent
    LEFT JOIN D_counteragent_A dagent 
        ON saagent.counteragent_code = dagent.counteragent_src_code
    WHERE  saagent.is_last = 'true'; 
    
BEGIN
    v_load_id = nextval('seq_load_id'); 
	v_load_status = 'STARTED'::character varying;
	p_seans_load_id = nextval('seq_seans_load_id'); 
	
PERFORM public.fill_table_load_log ( 
	p_seans_load_id => p_seans_load_id::bigint,
	p_load_id => v_load_id::bigint,
	p_load_status => v_load_status::character varying, --v_load_status =>'STARTED'::character varying, 
	p_load_name => 'load_D_counteragent_A'::character varying, 
	p_trg_table => 'D_counteragent_A'::character varying,
	p_date_begin => now()::date,
    p_date_end =>  NULL::date, 
	p_source_system_id => 0::bigint,
	p_src_table =>'SA_counteragent_A'::character varying,
	p_t_row_count => v_t_row_count::bigint,
	p_e_row_count => v_e_row_count::bigint,
	p_s_row_count => v_t_row_count + v_e_row_count::bigint
	);	
	
BEGIN
v_load_status = 'FINISHED'::character varying; 
    FOR v_agent IN cur_agent LOOP 
    BEGIN 
	IF v_agent.counteragent_name = 'XXX' THEN --v_prod.product_name 
		PERFORM public.fill_table_load_err (
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_D_counteragent_A'::character varying,
			p_trg_table => 'D_counteragent_A'::character varying, 
			p_source_system_id => 0::bigint,
			p_key_value => 'Найдена ошибка',
			p_err_type => 'Нулевое значение',
			p_err_text => 'Найден NULL'
	    	);
			v_e_row_count = v_e_row_count + 1;
	END IF;
	
	IF v_agent.over_agent > 1 THEN
		 PERFORM public.fill_table_load_err (
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_D_counteragent_A'::character varying,
			p_trg_table => 'D_counteragent_A'::character varying, 
			p_source_system_id => 0::bigint,
			p_key_value => 'Найдена ошибка',
			p_err_type => 'Дубликат',
			p_err_text => 'Cтрока уже существует'
	    	);
			v_e_row_count = v_e_row_count + 1;
	CONTINUE;
	END IF;	
	
	IF v_agent.counteragent_ID IS NULL THEN 
		INSERT INTO public.D_counteragent_A (counteragent_id,              counteragent_name,          counteragent_src_code,       load_id, 
														  load_period) --seans_load_id не вставляют
 			VALUES                     (nextval('seq_D_counteragent'),v_agent.counteragent_name,   v_agent.counteragent_code, v_load_id,  --v_prod.price::numeric, 
											             v_agent.load_period);	
			v_t_row_count = v_t_row_count + 1;
    ELSE
        UPDATE public.D_counteragent_A 
        SET counteragent_name = v_agent.counteragent_name,
			load_id = v_load_id
			--load_status = v_load_status
		WHERE v_agent.counteragent_code = D_counteragent_A.counteragent_src_code; 
        v_t_row_count = v_t_row_count + 1;
	END IF;
	
	EXCEPTION
	WHEN OTHERS THEN
        PERFORM public.fill_table_load_err ( 
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_D_counteragent_A'::character varying,
			p_trg_table => 'D_counteragent_A'::character varying, 
			p_source_system_id => 0::bigint,--v_agent.source_system_id::bigint,  p_source_system_id
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
	p_load_name => 'load_D_counteragent_A'::character varying, 
	p_trg_table => 'D_counteragent_A'::character varying, 
	p_date_begin => NULL::date,
    p_date_end => now()::date, 
	p_source_system_id => 0::bigint, 
	p_src_table => 'SA_counteragent_A'::character varying, 
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
			p_load_name => 'load_D_counteragent_A'::character varying,
			p_trg_table => 'D_counteragent_A'::character varying, 
			p_source_system_id => 0::bigint,
			p_key_value => 'Найдена ошибка',
			p_err_type => SQLSTATE::character varying,
			p_err_text => SQLERRM::character varying
	    	);
			v_e_row_count = v_e_row_count + 1;	
			v_load_status = 'FINISHED*'::character varying; 
			
END;
$$ LANGUAGE 'plpgsql'
