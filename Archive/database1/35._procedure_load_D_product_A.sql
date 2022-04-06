<<<<<<< HEAD
CREATE OR REPLACE PROCEDURE load_D_product_A (   
    p_seans_load_id bigint --DEFAULT nextval('seq_seans_load_id')
    ) AS $$
DECLARE
    v_load_id bigint;
	v_sa_load_id bigint DEFAULT nextval('seq_sa_load_id'); 
	v_t_row_count bigint = 0;
	v_e_row_count bigint = 0;
	v_load_status character varying; -- DEFAULT 'STARTED';
=======
-- PROCEDURE: public.load_d_product_a(bigint)
-- DROP PROCEDURE IF EXISTS public.load_d_product_a(bigint);

CREATE OR REPLACE PROCEDURE public.load_d_product_a(
	 p_seans_load_id bigint, --IN
	 p_load_period bigint
) AS $$
--LANGUAGE 'plpgsql'

DECLARE
    v_load_id bigint;
	v_sa_load_id bigint; --DEFAULT nextval('seq_sa_load_id'); --load_id из sa_таблицы. 
	v_t_row_count bigint = 0;
	v_e_row_count bigint = 0;
	v_load_status character varying; -- DEFAULT 'STARTED';
-- 	v_sqlerrm character varying;
	
-- т.к. новых зачений не было, ничего не меняется, проверка на ноль, заменяет значение на ноль. 
--load_status для всей таблицы, а не каждой строки. Не нужна колонка.
>>>>>>> new_branch

    cur_product CURSOR IS  -- проверить строки а ошибки, грязь.
   SELECT dproduct.product_ID, saproduct.product_code, saproduct.price, 
	saproduct.sa_load_id, saproduct.load_period, saproduct.is_last,
<<<<<<< HEAD
	COALESCE(saproduct.product_name,'XXX') AS product_name, --dproduct.product_src_code,
=======
	COALESCE(saproduct.product_name::character varying,'XXX') AS product_name, --dproduct.product_src_code,
>>>>>>> new_branch
	ROW_NUMBER() OVER(PARTITION BY saproduct.product_code) AS over_prod

	FROM SA_product_A AS saproduct 
    LEFT JOIN D_product_A dproduct 
        ON saproduct.product_code = dproduct.product_src_code 
    WHERE  
<<<<<<< HEAD
        saproduct.is_last = 'true';
=======
        saproduct.is_last = 'true' AND
		saproduct.load_period = p_load_period;
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
	
<<<<<<< HEAD
BEGIN
	v_load_status = 'FINISHED'::character varying; 
	--p_seans_load_id = nextval('seq_seans_load_id'); 
	
    FOR v_prod IN cur_product LOOP 
    BEGIN
=======

	
BEGIN
	v_load_status = 'FINISHED'::character varying; 
	
	--p_seans_load_id = nextval('seq_seans_load_id'); 
-- 	EXCEPTION 
-- 	WHEN OTHERS THEN
	
    FOR v_prod IN cur_product LOOP 
    BEGIN
	v_sa_load_id = v_prod.sa_load_id;
	
>>>>>>> new_branch
	IF v_prod.product_name = 'XXX' THEN --v_prod.product_name 
		PERFORM public.fill_table_load_err (
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_D_product_A'::character varying,
			p_trg_table => 'D_product_A'::character varying, 
			p_source_system_id => 0::bigint,
			p_key_value => 'Найдена ошибка',
<<<<<<< HEAD
			p_err_type => 'Нулевое значение',
			p_err_text => 'Найден NULL'
	    	);
			v_e_row_count = v_e_row_count + 1;
=======
			p_err_type => 'map', --суперконкретно, неинформативно сейчас, 
			p_err_text => 'Название продукта не найдено. Код продукта: ' || v_prod.product_code || '. Ошибка в селекте, в начале процедуры.'
	    	);
			v_e_row_count = v_e_row_count + 1;
			v_load_status = 'FINISHED*'::character varying; 
>>>>>>> new_branch
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
<<<<<<< HEAD
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
	
=======
			p_err_type => 'Дубликат',   --SQLSTATE::character varying, --'Дубликат', -- какой код, как найти, где найти ошибку
			p_err_text => 'Cтрока уже существует. Код продукта найден несколько раз. Код продукта: '||v_prod.product_code||'. Ошибка в селекте, в начале процедуры.'
			 --SQLERRM::character varying --'Cтрока уже существует'
	    	);
			v_e_row_count = v_e_row_count + 1;
			v_load_status = 'FINISHED*'::character varying; 
	--CONTINUE;
	END IF;

	 
>>>>>>> new_branch
	IF v_prod.product_ID IS NULL THEN 
		INSERT INTO public.D_product_A (product_id,              product_name,          product_src_code,       load_id, 	price, 
												 load_period) --insert has more expressions than target table
 			VALUES                     (nextval('seq_D_product'),v_prod.product_name,   v_prod.product_code, v_load_id,  v_prod.price::numeric, 
<<<<<<< HEAD
											  v_prod.load_period);	
=======
											     p_load_period);	
>>>>>>> new_branch
			v_t_row_count = v_t_row_count + 1;
    ELSE
        UPDATE public.D_product_A 
        SET product_name = v_prod.product_name,
<<<<<<< HEAD
			load_id = v_load_id
=======
			price = v_prod.price::numeric,
			load_id = v_load_id,
			load_period = p_load_period
			--product_src_code = v_prod.product_code
			--seans_load_id = p_seans_load_id, --надо ли update полей с переданными параметрами? т.е. 
			--load_period = p_load_period      --якобы, что загрузка проходила.
			
>>>>>>> new_branch
			--load_status = v_load_status
		WHERE v_prod.product_code = D_product_A.product_src_code; 
        	  v_t_row_count = v_t_row_count + 1;
	END IF;

<<<<<<< HEAD
	
=======
	-- 	v_sqlerrm = SQLERRM;
   --     v_sqlstate = SQLSTATE;   
>>>>>>> new_branch
	EXCEPTION 
	WHEN OTHERS THEN
        PERFORM public.fill_table_load_err ( 
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_D_product_A'::character varying,
			p_trg_table => 'D_product_A'::character varying, 
<<<<<<< HEAD
			p_source_system_id => v_prod.source_system_id::bigint,  
=======
			p_source_system_id => 0::bigint,  --v_prod.source_system_id::bigint
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
<<<<<<< HEAD





=======
-- ALTER PROCEDURE public.load_d_product_a(bigint)
--     OWNER TO postgres;
>>>>>>> new_branch
