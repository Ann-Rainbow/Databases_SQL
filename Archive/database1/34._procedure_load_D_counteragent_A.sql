<<<<<<< HEAD
-- переделать процедуру.
CREATE OR REPLACE PROCEDURE load_D_counteragent_A (   
    p_seans_load_id bigint
    ) AS $$
=======
-- PROCEDURE: public.load_d_counteragent_a(bigint)

-- DROP PROCEDURE IF EXISTS public.load_d_counteragent_a(bigint);

CREATE OR REPLACE PROCEDURE public.load_d_counteragent_a(
	p_seans_load_id bigint,
	p_load_period bigint
) AS $$ --IN

>>>>>>> new_branch
DECLARE
    v_load_id bigint;
	v_sa_load_id bigint DEFAULT nextval('seq_sa_load_id'); 
	v_t_row_count bigint = 0;
	v_e_row_count bigint = 0;
	v_load_status character varying; --DEFAULT 'STARTED'; --добавлено

    cur_agent CURSOR IS 
	SELECT  dagent.counteragent_ID, saagent.counteragent_code, saagent.sa_load_id, 
<<<<<<< HEAD
	        saagent.load_period, saagent.is_last, COALESCE(saagent.counteragent_name, 'XXX') AS agent_name,
=======
	        saagent.load_period, saagent.is_last, COALESCE(saagent.counteragent_name, 'XXX') AS counteragent_name,
>>>>>>> new_branch
			ROW_NUMBER() OVER(PARTITION BY saagent.counteragent_code) AS over_agent
	FROM SA_counteragent_A AS saagent
    LEFT JOIN D_counteragent_A dagent 
        ON saagent.counteragent_code = dagent.counteragent_src_code
<<<<<<< HEAD
    WHERE  saagent.is_last = 'true'; 
=======
    WHERE  saagent.is_last = 'true' AND
		   saagent.load_period::integer =  p_load_period;   --p_load_period;	 
>>>>>>> new_branch
    
BEGIN
    v_load_id = nextval('seq_load_id'); 
	v_load_status = 'STARTED'::character varying;
<<<<<<< HEAD
	p_seans_load_id = nextval('seq_seans_load_id'); 
=======
>>>>>>> new_branch
	
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
<<<<<<< HEAD
	IF v_agent.counteragent_name = 'XXX' THEN --v_prod.product_name 
=======
	v_sa_load_id = v_agent.sa_load_id;
	
	IF v_agent.counteragent_name = 'XXX' THEN 
>>>>>>> new_branch
		PERFORM public.fill_table_load_err (
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_D_counteragent_A'::character varying,
			p_trg_table => 'D_counteragent_A'::character varying, 
			p_source_system_id => 0::bigint,
			p_key_value => 'Найдена ошибка',
<<<<<<< HEAD
			p_err_type => 'Нулевое значение',
			p_err_text => 'Найден NULL'
	    	);
			v_e_row_count = v_e_row_count + 1;
=======
			p_err_type => 'map',
			p_err_text => 'Название контрагента не найдено. Код контрагента: '||v_agent.counteragent_code||'. Ошибка в селекте, в начале процедуры.'
	    	);
			v_e_row_count = v_e_row_count + 1;
			v_load_status = 'FINISHED*'::character varying; 
	--CONTINUE;
>>>>>>> new_branch
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
<<<<<<< HEAD
			p_err_text => 'Cтрока уже существует'
	    	);
			v_e_row_count = v_e_row_count + 1;
	CONTINUE;
=======
			p_err_text => 'Cтрока уже существует. Код контрагента найден несколько раз. Код контрагента: '||v_agent.counteragent_code||'. Ошибка в селекте, в начале процедуры.'
	    	);
			v_e_row_count = v_e_row_count + 1;
			v_load_status = 'FINISHED*'::character varying; 
	--CONTINUE;
>>>>>>> new_branch
	END IF;	
	
	IF v_agent.counteragent_ID IS NULL THEN 
		INSERT INTO public.D_counteragent_A (counteragent_id,              counteragent_name,          counteragent_src_code,       load_id, 
<<<<<<< HEAD
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
=======
														  load_period) --seans_load_id не вставляют. почему?
 			VALUES                     (nextval('seq_D_counteragent'),v_agent.counteragent_name,   v_agent.counteragent_code, v_load_id,  --v_prod.price::numeric, 
											             v_agent.load_period);	
			v_t_row_count = v_t_row_count + 1;
			--RAISE NOTICE 'AFTER INSERT';
    ELSE
        UPDATE public.D_counteragent_A 
        SET counteragent_name = v_agent.counteragent_name,
			load_id = v_load_id,
			load_period = p_load_period
			--counteragent_src_code = v_agent.counteragent_code
			--load_status = v_load_status
		WHERE v_agent.counteragent_code = D_counteragent_A.counteragent_src_code; 
        v_t_row_count = v_t_row_count + 1; --кол-во обработанных строк
		--RAISE NOTICE 'AFTER UPDATE';
>>>>>>> new_branch
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
<<<<<<< HEAD
=======
-- ALTER PROCEDURE public.load_d_counteragent_a(bigint)
--     OWNER TO postgres;
>>>>>>> new_branch
