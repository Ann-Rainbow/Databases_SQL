
--------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE load_D_counteragent_A (
	p_seans_load_id bigint,
	p_load_status varchar) AS $$
DECLARE
	v_load_id bigint;
	v_sa_load_id bigint;
	v_debug_mark varchar;
	v_debug_mark1 varchar;
	v_t_row_count bigint = 0;
	v_e_row_count bigint = 0;

	cur_counteragent CURSOR IS SELECT saagent.counteragent_code, saagent.counteragent_name, dagent.counteragent_id, saagent.is_last 
    FROM SA_counteragent_A saagent
	LEFT JOIN D_counteragent_A dagent on saagent.counteragent_code = dagent.counteragent_code
	WHERE saagent.counteragent_name != COALESCE(dagent.counteragent_name, 'XXX') AND saagent.is_last = true;
BEGIN
	v_load_id = nextval('seq_load_id');
	SELECT fill_table_load_log (
        p_seans_load_id => p_seans_load_id, -- параметр => значение
		p_load_id => v_load_id,
		p_load_status => p_load__status,
	    p_load_name => 'Load_D_counteragent_A',
		p_load_period => 111111,  ---???
	  	p_trg_table => 'D_counteragent_A',
		p_source_table => 'SA_counteragent_A',
		p_t_row_count => v_t_row_count,
		p_e_row_count => v_e_row_count,
		p_s_row_count => v_t_row_count + v_e_row_count
        );
		BEGIN
			FOR v_counteragent in cur_counteragent LOOP 
				BEGIN
				IF v_counteragent.counteragent_id IS NULL THEN
				INSERT INTO public.D_counteragent_A (counteragent_id, counteragent_name, counteragent_code, load_id)
 					VALUES (nextval('seq_counteragent'), v_counteragent.counteragent_name, v_counteragent.counteragent_code, v_load_id);
					v_t_row_count = v_t_row_count + 1;
				ELSE
				UPDATE public.D_counteragent_A 
                    SET counteragent_name = v_counteragent.counteragent_name,load_id = v_load_id
				WHERE v_counteragent.counteragent_code = D_counteragent_A.counteragent_code;
				COMMIT;
				v_t_row_count = v_t_row_count + 1;
				END IF;
				EXCEPTION
				WHEN OTHERS THEN
					v_sa_load_id = nextval('seq_sa_load_id');
					v_debug_mark = v_counteragent.counteragent_code;
					v_debug_mark1 = v_counteragent.counteragent_name;
					SELECT fill_table_load_err (
                        p_load_id => v_load_id::bigint,
						p_seans_load_id => p_seans_load_id::bigint,
						p_sa_load_id => v_sa_load_id::bigint,
						p_load_name => 'Load_D_counteragent_A'::varchar,
						p_trg_table => 'D_counteragent_A'::varchar,
						p_source_system_id => '123'::bigint, --??
						p_load_period => 11111::bigint,
						p_key_value => 'Произошла ошибка в строке '|| v_debug_mark || ', '||v_debug_mark1,
						p_err_type => SQLSTATE::varchar,
						p_err_text => SQLERRM::varchar
			            );
						v_e_row_count = v_e_row_count + 1;
				END;
				END LOOP;	
				END;
	p_load_status = 'FINISHED';
	SELECT fill_table_load_log (
        p_seans_load_id => p_seans_load_id,
		p_load_id => v_load_id,
		p_load_status => p_load_status,
	    p_load_name => 'Load_D_counteragent_A',
		p_load_period => 111111, --??
	  	p_trg_table => 'D_counteragent_A',
		p_source_table => 'SA_counteragent_A',
		p_t_row_count => v_t_row_count,
		p_e_row_count => v_e_row_count,
		p_s_row_count => v_t_row_count + v_e_row_count
        );

		EXCEPTION WHEN OTHERS THEN
		v_sa_load_id = nextval('seq_sa_load_id');
		v_debug_mark = v_counteragent.counteragent_code;
		v_debug_mark1 = v_counteragent.counteragent_name;
		SELECT fill_table_load_err (
            p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'Load_D_counteragent_A'::varchar,
			p_trg_table => 'D_counteragent_A'::varchar,
			p_source_system_id => '123'::bigint,
			p_load_period => 11111::bigint,
			p_key_value => 'Произошла ошибка в строке '|| v_debug_mark || ', '||v_debug_mark1,
			p_err_type => SQLSTATE::varchar,
			p_err_text => SQLERRM::varchar
	        );
			v_e_row_count = v_e_row_count + 1;
	
END;

$$ LANGUAGE 'plpgsql'

--------------------------------------------------------------------------------------------------------
-- Дописать ещё 3 процедуры так.
-- fill_D_product

-- fill_D_product


CREATE OR REPLACE PROCEDURE load_D_product_A (   
    p_seans_load_id bigint,
	p_load_status varchar -- это p или v load_status лучше сделать?
    -- какие еще поля лучше написать?
    
    ) AS $$
DECLARE
    v_load_id bigint;
	v_sa_load_id bigint;
	v_t_row_count bigint = 0;
	v_e_row_count bigint = 0;
    
    -- DECLARE curs1 refcursor;
    -- curs2 CURSOR FOR SELECT * FROM tenk1;
    -- curs3 CURSOR (key integer) FOR SELECT * FROM tenk1 WHERE unique1 = key;

    -- OPEN имя_курсора
    -- OPEN имя_курсора USING значение [, ... ]

    -- КУРСОР

    --OPEN curs FOR SELECT * FROM A_D_product -- table -- как написать что из любой, каждой таблицы? 
                                                 -- Тут же не каждая таблица

--FOR КУРСОР 
-- SELECT COLUMN/ROW(?) FROM TABLE - this is LOOKUP

    cur_product CURSOR IS SELECT saproduct.product_code, saproduct.product_name, 
                                 dproduct.product_ID, saproduct.is_last, saproduct.price 
								 FROM SA_product_A AS saproduct

                -- ЭТО В КУРСОРЕ В СЕЛЕКТЕ. 
                -- SELECT COUNT(product_code) AS count_product
                -- FROM SA_product_A saproduct
                -- GROUP BY product_code 
                -- HAVING COUNT(product_code) > 1
                --     THEN CALL public.fill_table_load_err ()
    
    LEFT JOIN D_product_A dproduct 
        ON saproduct.product_code = dproduct.product_code AND
        saproduct.product_ID = dproduct.product_ID --?

    WHERE saproduct.product_name != dproduct.product_name AND 
        saproduct.is_last = true; --в is_last хранится булево значение?
    -- COALESCE – функция T-SQL, которая возвращает первое выражение из списка параметров, неравное NULL.
    
BEGIN
    v_load_id = nextval(); ---- чего? 

--START_LOG
--START log_table_load() -- начало процедуры логирования


SELECT public.fill_table_load_log (
	p_seans_load_id => p_seans_load_id,
	p_load_id => v_load_id,
	p_load_status => p_load_status,
	p_load_name => 'load_D_product_A',
	p_load_period => p_load_period, --? now() ?
	p_trg_table => 'D_product_A',
	p_source_table => 'SA_product_A',
	p_t_row_count => v_t_row_count,
	p_e_row_count => v_e_row_count,
	p_s_row_count => v_t_row_count + v_e_row_count
	);



BEGIN

    FOR v_prod IN cur_product LOOP -- must be here or not? --yes

    BEGIN
	IF v_prod.product_id IS NULL THEN
		INSERT INTO public.D_product (product_id, product_name, product_code, load_id)
 			VALUES (nextval('seq_D_product'), v_prod.product_name, v_prod.product_code, v_load_id);
			v_t_row_count = v_t_row_count + 1;



        --     EXCEPTION WHEN p_e_row_count > 0  -- IF there are errors
--         THEN RAISE EXCEPTION 'ERRORS found', p_e_row_count; -- логирование ошибки 

    -- IF p_e_row_count = 0 -- Произошла ли ошибка в строке? Если не произошла, то 
    --     THEN -- выход из условия
    --     ELSE -- логирование ошибки 
    -- END IF;

    

    -- IF  A_SA_product.is_last IS EMPTY / IS NULL... -- если строка пустая
    --     then INSERT INTO A_D_product
    -- ( product_ID, --without p
    --   product_code,
    --   source_system_ID,
    --   product_name,
    --   price,
    --   load_ID,
    --   seans_load_ID,
    --   load_period
    --   e_row_count); -- может ли быть такое поле в таблице?


    --   VALUES
    -- ( p_product_ID, --without p
    --   p_product_code,
    --   p_source_system_ID,
    --   p_product_name,
    --   p_price,
    --   p_load_ID,
    --   p_seans_load_ID,
    --   p_load_period
    --   p_e_row_count);

    -- ELSE
    --   UPDATE A_D_product
    --      SET 
    --      product_ID = p_product_ID, -- все остальные поля обновлять? -- column1 = value1,
    --      product_code = p_product_code,
    --      source_system_ID = p_source_system_ID,
    --      product_name = p_product_name,
    --      price = p_price,
    --      e_row_count = p_e_row_count
    --   WHERE load_id = p_load_id;

    ELSE
        UPDATE public.D_product_A 
        SET product_name = v_prod.product_name,
			load_id = v_load_id
		WHERE v_prod.product_code = D_product_A.product_code;
		COMMIT;
        v_t_row_count = v_t_row_count + 1;
	END IF;

                --END IF;

            -- EXCEPTION WHEN condition  -- обработка исключений
            --   THEN handle_exception;

            -- EXCEPTION ...
            -- e_log


	EXCEPTION
	WHEN OTHERS THEN
		v_sa_load_id = nextval('seq_SA_load_ID');
        SELECT public.fill_table_load_err (
			p_load_id => v_load_id::bigint,
			p_seans_load_id => p_seans_load_id::bigint,
			p_sa_load_id => v_sa_load_id::bigint,
			p_load_name => 'load_D_product_A'::varchar,
			p_trg_table => 'D_product_A'::varchar,
			p_source_system_id => '123'::bigint,
			p_load_period => p_load_period, --?
			p_key_value => 'Найдена ошибка',
			p_err_type => SQLSTATE::varchar,
			p_err_text => SQLERRM::varchar
			);
			v_e_row_count = v_e_row_count + 1;
		
		
		
		
	
            
            
            
                ---

		END;
		END LOOP;	
		END;



	p_load_status = 'FINISHED';  

-- вызов процедуры логирования, окончание логирования.



SELECT fill_table_load_log (
	p_seans_load_id => p_seans_load_id,
	p_load_id => v_load_id,
	p_status => p_status,
	p_load_name => 'load_D_product_A',
	p_load_period => p_load_period, ---?..
	p_trg_table => 'D_product_A',
	p_source_table => 'SA_product_A',
	p_t_row_count => v_t_row_count,
	p_e_row_count => v_e_row_count,
	p_s_row_count => v_t_row_count + v_e_row_count



); 

    EXCEPTION WHEN OTHERS THEN -- Стоит ли 2 раза EXCEPTION делать?
		v_sa_load_id = nextval('seq_sa_load_id');
    SELECT public.fill_table_load_err (
		p_load_id => v_load_id::bigint,
		p_seans_load_id => p_seans_load_id::bigint,
		p_sa_load_id => v_sa_load_id::bigint,
		p_load_name => 'load_D_product_A'::varchar,
		p_trg_table => 'D_product_A'::varchar,
		p_source_system_id => '123'::bigint,
		p_load_period => p_load_period,
		p_key_value => 'Найдена ошибка',
		p_err_type => SQLSTATE::varchar,
		p_err_text => SQLERRM::varchar
	    );
		v_e_row_count = v_e_row_count + 1;
		

    
END;

$$ LANGUAGE 'plpgsql'



-- END LOOP;
-- CLOSE curs; -- тут же закрывать?





    

    --СДЕЛАТЬ ИЗ IF - EXCEPTION
    -- IF p_t_row_count IS NOT LAST -- Последняя ли строка обработана? Если нет, то
    --     THEN --загрузка строк
    --     ELSE --логирование окончания процедуры
    -- END IF;




    -- IF p_e_row_count = 0 --Есть ли ошибка в процедуре? Если нет ошибки в процедуре 
    --   THEN -- загрузка строк
    --   ELSE -- логирование ошибки 
    -- END IF;  -- instead of if make exception 


    --EXCEPTION WHEN TABLE.ПОЛЕ/КОЛОНКА != SA_TABLE.IS_LAST THEN UPDATE   -- IF there are errors

    --END;

    -- EXCEPTION обпеrs (обработана?)
    --     e_log;
    --     end_log;



    -- END LOOP; -- позже, дальше закрыла
    -- CLOSE curs;
    --CLOSE имя_курсора; - курсор закрывается после update?



--END LOOP;
--CLOSE curs;

--end_log --логирование конца загрузки процедуры, выход.

--END;




    ----INSERT INTO films (code, title, did, date_prod, kind)
    ----VALUES ('T_601', 'Yojimbo', 106, DEFAULT, 'Drama');




