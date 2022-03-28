CREATE OR REPLACE PROCEDURE load_D_product_A (   
    p_seans_load_id bigint --DEFAULT nextval('seq_seans_load_id')
    ) AS $$
DECLARE
    v_load_id bigint;
	v_sa_load_id bigint DEFAULT nextval('seq_sa_load_id'); 
	v_t_row_count bigint = 0;
	v_e_row_count bigint = 0;
	v_load_status character varying; -- DEFAULT 'STARTED';

    cur_product CURSOR IS  -- проверить строки а ошибки, грязь.
   SELECT dproduct.product_ID, saproduct.product_code, saproduct.price, 
	saproduct.sa_load_id, saproduct.load_period, saproduct.is_last,
	COALESCE(saproduct.product_name,'XXX') AS product_name, --dproduct.product_src_code,
	ROW_NUMBER() OVER(PARTITION BY saproduct.product_code) AS over_prod

	FROM SA_product_A AS saproduct 
    LEFT JOIN D_product_A dproduct 
        ON saproduct.product_code = dproduct.product_src_code 
    WHERE  
        saproduct.is_last = 'true';
		
    
BEGIN
    v_load_id = nextval('seq_load_id'); 
	v_load_status = 'STARTED'::character varying;
	p_seans_load_id = nextval('seq_seans_load_id'); 
	
PERFORM public.fill_table_load_log ( 
	p_seans_load_id => p_seans_load_id::bigint,
	p_load_id => v_load_id::bigint,
	p_load_status => v_load_status::character varying, --v_load_status =>'STARTED'::character varying, 
	p_load_name => 'load_D_product_A'::character varying, 
	p_trg_table => 'D_product_A'::character varying,
	p_date_begin => now()::date,
    p_date_end =>  NULL::date, 
	p_source_system_id => 0::bigint,
	p_src_table =>'SA_product_A'::character varying,
	p_t_row_count => v_t_row_count::bigint,
	p_e_row_count => v_e_row_count::bigint,
	p_s_row_count => v_t_row_count + v_e_row_count::bigint
	);	
	
BEGIN
	v_load_status = 'FINISHED'::character varying; 
	--p_seans_load_id = nextval('seq_seans_load_id'); 
	
    FOR v_prod IN cur_product LOOP 
    BEGIN
	IF v_prod.product_name = 'XXX' THEN --v_prod.product_name 
		PERFORM public.fill_table_load_err (
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_D_product_A'::character varying,
			p_trg_table => 'D_product_A'::character varying, 
			p_source_system_id => 0::bigint,
			p_key_value => 'Найдена ошибка',
			p_err_type => 'Нулевое значение',
			p_err_text => 'Найден NULL'
	    	);
			v_e_row_count = v_e_row_count + 1;
	END IF;
		
	IF v_prod.over_prod > 1 THEN
		 PERFORM public.fill_table_load_err (
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_D_product_A'::character varying,
			p_trg_table => 'D_product_A'::character varying, 
			p_source_system_id => 0::bigint,
			p_key_value => 'Найдена ошибка',
			p_err_type => 'Дубликат',
			p_err_text => 'Cтрока уже существует'
	    	);
			v_e_row_count = v_e_row_count + 1;
	CONTINUE;
	END IF;
		
	--v_load_status = 'FINISHED'::character varying; 
	
	-- Проверка на дубликаты. Тут ли должна быть?


-- 	 IF row > 1 THEN
-- 		RAISE NOTICE 'Duplicate rows'
-- 	 ELSE
-- 		RAISE NOTICE 'No duplicate rows';	
-- 	END IF;
	
	IF v_prod.product_ID IS NULL THEN 
		INSERT INTO public.D_product_A (product_id,              product_name,          product_src_code,       load_id, 	price, 
												 load_period) --insert has more expressions than target table
 			VALUES                     (nextval('seq_D_product'),v_prod.product_name,   v_prod.product_code, v_load_id,  v_prod.price::numeric, 
											  v_prod.load_period);	
			v_t_row_count = v_t_row_count + 1;
    ELSE
        UPDATE public.D_product_A 
        SET product_name = v_prod.product_name,
			load_id = v_load_id
			--load_status = v_load_status
		WHERE v_prod.product_code = D_product_A.product_src_code; 
        	  v_t_row_count = v_t_row_count + 1;
	END IF;

	
	EXCEPTION 
	WHEN OTHERS THEN
        PERFORM public.fill_table_load_err ( 
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_D_product_A'::character varying,
			p_trg_table => 'D_product_A'::character varying, 
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
	p_load_name => 'load_D_product_A'::character varying, 
	p_trg_table => 'D_product_A'::character varying, 
	p_date_begin => NULL::date,
    p_date_end => now()::date, 
	p_source_system_id => 0::bigint, 
	p_src_table => 'SA_product_A'::character varying, 
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
			p_load_name => 'load_D_product_A'::character varying,
			p_trg_table => 'D_product_A'::character varying, 
			p_source_system_id => 0::bigint,
			p_key_value => 'Найдена ошибка',
			p_err_type => SQLSTATE::character varying,
			p_err_text => SQLERRM::character varying
	    	);
			v_e_row_count = v_e_row_count + 1;	
			v_load_status = 'FINISHED*'::character varying; 
			
END;
$$ LANGUAGE 'plpgsql'





