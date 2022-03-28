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

    cur_counteragent CURSOR IS 
	SELECT  saagent.counteragent_name, saagent.counteragent_code, dagent.counteragent_ID, saagent.is_last, --saagent.price, 
		   saagent.source_system_id, saagent.load_period, dagent.load_status--::character varying
	FROM SA_counteragent_A AS saagent
    LEFT JOIN D_counteragent_A dagent 
        ON saagent.counteragent_code = dagent.counteragent_code
    WHERE saagent.counteragent_name != COALESCE (dagent.counteragent_name, '000') AND 
        saagent.is_last = 'true'; 
    
BEGIN
    v_load_id = nextval('seq_load_id'); 
	v_load_status = 'STARTED'::character varying;
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
    FOR v_counteragent IN cur_counteragent LOOP 
    BEGIN 
	--v_load_status = 'FINISHED'::character varying; 
	
	IF v_counteragent.counteragent_ID IS NULL THEN 
		INSERT INTO public.D_counteragent_A (counteragent_id,              counteragent_name,          counteragent_code,       load_id, 
												source_system_id, 			  load_period, 			seans_load_id, 				 load_status) --insert has more expressions than target table
 			VALUES                     (nextval('seq_D_counteragent'),v_counteragent.counteragent_name,   v_counteragent.counteragent_code, v_load_id,  --v_prod.price::numeric, 
									v_counteragent.source_system_id::bigint, 		  v_counteragent.load_period, 	p_seans_load_id, 			 v_load_status);	
			v_t_row_count = v_t_row_count + 1;
    ELSE
        UPDATE public.D_counteragent_A 
        SET counteragent_name = v_counteragent.counteragent_name,
			load_id = v_load_id,
			load_status = v_load_status
		WHERE v_counteragent.counteragent_code = D_counteragent_A.counteragent_code; 
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
			p_source_system_id => v_counteragent.source_system_id::bigint,  
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



















-- CREATE OR REPLACE PROCEDURE load_D_counteragent_A (
-- 	p_seans_load_id bigint,
-- 	p_load_status varchar) AS $$
-- DECLARE
-- 	v_load_id bigint;
-- 	v_sa_load_id bigint;
-- 	v_t_row_count bigint = 0;
-- 	v_e_row_count bigint = 0;

-- 	cur_counteragent CURSOR IS SELECT saagent.counteragent_code, saagent.counteragent_name, 
-- 									  dagent.counteragent_id, saagent.is_last 
--     FROM SA_counteragent_A saagent
-- 	LEFT JOIN D_counteragent_A dagent ON 
-- 	saagent.counteragent_code = dagent.counteragent_code
-- 	WHERE saagent.counteragent_name != dagent.counteragent_name 
-- 	AND saagent.is_last = true;
-- BEGIN
-- 	v_load_id = nextval('seq_load_id');
-- 	SELECT fill_table_load_log (
--         p_seans_load_id => p_seans_load_id, -- параметр => значение
-- 		p_load_id => v_load_id,
-- 		p_load_status => p_load_status,
-- 	    p_load_name => 'Load_D_counteragent_A',
-- 		p_load_period => p_load_period,  ---???  now()
-- 	  	p_trg_table => 'D_counteragent_A',
-- 		p_source_table => 'SA_counteragent_A',
-- 		p_t_row_count => v_t_row_count,
-- 		p_e_row_count => v_e_row_count,
-- 		p_s_row_count => v_t_row_count + v_e_row_count
--         );
-- 		BEGIN
-- 			FOR v_counteragent in cur_counteragent LOOP 
-- 				BEGIN
-- 				IF v_counteragent.counteragent_id IS NULL THEN
-- 				INSERT INTO public.D_counteragent_A (counteragent_id, counteragent_name, counteragent_code, load_id)
--  					VALUES (nextval('seq_counteragent'), v_counteragent.counteragent_name, 
-- 							v_counteragent.counteragent_code, v_load_id);
-- 					v_t_row_count = v_t_row_count + 1;
-- 				ELSE
-- 				UPDATE public.D_counteragent_A 
--                     SET counteragent_name = v_counteragent.counteragent_name,load_id = v_load_id
-- 				WHERE v_counteragent.counteragent_code = D_counteragent_A.counteragent_code;
-- 				COMMIT;
-- 				v_t_row_count = v_t_row_count + 1;
-- 				END IF;
-- 				EXCEPTION
-- 				WHEN OTHERS THEN
-- 					v_sa_load_id = nextval('seq_sa_load_id');

-- 					SELECT fill_table_load_err (
--                         p_load_id => v_load_id::bigint,
-- 						p_seans_load_id => p_seans_load_id::bigint,
-- 						p_sa_load_id => v_sa_load_id::bigint,
-- 						p_load_name => 'Load_D_counteragent_A'::varchar,
-- 						p_trg_table => 'D_counteragent_A'::varchar,
-- 						p_source_system_id => '123'::bigint, --??
-- 						p_load_period => p_load_period,
-- 						p_key_value => 'Найдена ошибка',
-- 						p_err_type => SQLSTATE::varchar,
-- 						p_err_text => SQLERRM::varchar
-- 			            );
-- 						v_e_row_count = v_e_row_count + 1;
-- 				END;
-- 				END LOOP;	
-- 				END;
-- 	p_load_status = 'FINISHED';
-- 	SELECT fill_table_load_log (
--         p_seans_load_id => p_seans_load_id,
-- 		p_load_id => v_load_id,
-- 		p_load_status => p_load_status,
-- 	    p_load_name => 'Load_D_counteragent_A',
-- 		p_load_period => p_load_period, --??
-- 	  	p_trg_table => 'D_counteragent_A',
-- 		p_source_table => 'SA_counteragent_A',
-- 		p_t_row_count => v_t_row_count,
-- 		p_e_row_count => v_e_row_count,
-- 		p_s_row_count => v_t_row_count + v_e_row_count
--         );

-- 		EXCEPTION WHEN OTHERS THEN
-- 		v_sa_load_id = nextval('seq_sa_load_id');

-- 		SELECT fill_table_load_err (
--             p_load_id => v_load_id::bigint,
-- 			p_seans_load_id => p_seans_load_id::bigint,
-- 			p_sa_load_id => v_sa_load_id::bigint,
-- 			p_load_name => 'Load_D_counteragent_A'::varchar,
-- 			p_trg_table => 'D_counteragent_A'::varchar,
-- 			p_source_system_id => '111'::bigint, --?
-- 			p_load_period => p_load_period,
-- 			p_key_value => 'Найдена ошибка',
-- 			p_err_type => SQLSTATE::varchar,
-- 			p_err_text => SQLERRM::varchar
-- 	        );
-- 			v_e_row_count = v_e_row_count + 1;
	
-- END;

-- $$ LANGUAGE 'plpgsql'
