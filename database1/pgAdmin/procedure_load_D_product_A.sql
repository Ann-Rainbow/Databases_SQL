CREATE OR REPLACE PROCEDURE load_D_product_A (   
    p_seans_load_id bigint
    ) AS $$
DECLARE
    v_load_id bigint;
	v_sa_load_id bigint DEFAULT nextval('seq_sa_load_id'); 
	v_t_row_count bigint = 0;
	v_e_row_count bigint = 0;
	v_load_status character varying;

    cur_product CURSOR IS 
	SELECT  saproduct.product_name, saproduct.product_code, dproduct.product_ID, saproduct.is_last, saproduct.price, 
		   saproduct.source_system_id, saproduct.load_period, dproduct.load_status::character varying
	FROM SA_product_A AS saproduct
    LEFT JOIN D_product_A dproduct 
        ON saproduct.product_code = dproduct.product_code 
    WHERE saproduct.product_name != COALESCE (dproduct.product_name, '000') AND 
        saproduct.is_last = 'true'; 
    
BEGIN
    v_load_id = nextval('seq_load_id'); 
PERFORM public.fill_table_load_log ( 
	p_seans_load_id => p_seans_load_id::bigint,
	p_load_id => v_load_id::bigint,
	p_load_status => 'STARTED'::character varying, 
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
    FOR v_prod IN cur_product LOOP 
    BEGIN 
	IF v_prod.product_ID IS NULL THEN 
		INSERT INTO public.D_product_A (product_id,              product_name,          product_code,       load_id, 	price, 
												source_system_id, 			  load_period, 			seans_load_id, 				 load_status) --insert has more expressions than target table
 			VALUES                     (nextval('seq_D_product'),v_prod.product_name,   v_prod.product_code, v_load_id,  v_prod.price::numeric, 
									v_prod.source_system_id::bigint, 		  v_prod.load_period, 	p_seans_load_id, 			 v_load_status);	
			v_t_row_count = v_t_row_count + 1;
    ELSE
        UPDATE public.D_product_A 
        SET product_name = v_prod.product_name,
			load_id = v_load_id,
			load_status = v_load_status
		WHERE v_prod.product_code = D_product_A.product_code; 
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
	p_load_status => 'FINISHED'::character varying, 
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





