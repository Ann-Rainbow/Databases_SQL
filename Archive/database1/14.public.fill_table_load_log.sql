-- FUNCTION: public.fill_table_load_log(bigint, bigint, character varying, character varying, bigint, character varying, character varying, bigint, bigint, bigint)

-- DROP FUNCTION IF EXISTS public.fill_table_load_log(bigint, bigint, character varying, character varying, bigint, character varying, character varying, bigint, bigint, bigint);

CREATE OR REPLACE FUNCTION public.fill_table_load_log(
	p_load_id bigint,
	p_seans_load_id bigint,
	p_load_status character varying,
	p_load_name character varying,
	p_load_period bigint,
	p_trg_table character varying,
	p_source_table character varying,
	p_t_row_count bigint DEFAULT 0,
	p_e_row_count bigint DEFAULT 0,
	p_s_row_count bigint DEFAULT 0)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
  BEGIN

    --start_log --логирование старта загрузки процедуры. Как сделать?
    --START LOG

    IF UPPER(p_status) = 'STARTED' THEN
    --IF p_t_row_count = 0 THEN -- тут дб "Строка из SA новая?" Если да, новая, тогда, 

      INSERT INTO log_table_load_A 
    -- ОБЪЯВЛЕНИЕ ПОЛЕЙ СДЕЛАТЬ -- like this or how? or in braces ()? Yes, in braces
        (seans_load_id, --тут всё без префикса p
         load_id,
         load_status,
         load_name,
         load_period,
         trg_table,
         source_table,
         date_begin --почему не задекларировано, не написано ранее? - Это необязательно, нужно в других процедурах, 
                    --конкурирующих за ресурсы операционной системы.
        
        
         --group_name,
         --t_row_count,
         --e_row_count,
         --null,
         --now(),
         --null,
         --s_row_count

         )

      VALUES
        (p_seans_load_id,
         p_load_id,
         p_load_status,
         p_load_name,
         p_period,
         p_trg_table,
         p_source_table,
         now()

         --p_group_name,
         --p_t_row_count,
         --p_e_row_count,
         --null,
         --null,
         --p_s_row_count
        );

    ELSE
      UPDATE log_table_load_A 

        -- ОБЪЯВЛЕНИЕ ПОЛЕЙ СДЕЛАТЬ -- the same declaration as before?
        -- (seans_load_id,
        --  load_id,
        --  load_status,
        --  load_name,
        --  group_name,
        --  t_row_count,
        --  e_row_count,
        --  --null,
        --  period,
        --  trg_table,
        --  now(),
        --  --null,
        --  s_row_count);

         SET load_status = p_load_status,
             t_row_count = p_t_row_count,
             e_row_count = p_e_row_count,
             date_end    = now(),
             s_row_count = p_s_row_count

             --comm      = p_comm
             --comm      =  substr(substr(comm || p_comm, -length(comm ||p_comm)),1,500),
 
       WHERE load_id = p_load_id;
    END IF;

    EXCEPTION WHEN OTHERS THEN --вызвать процедуру  public.fill_table_load_err()
      -- INSERT INTO log_table_load_err_A(err_type, err_text)
      --  VALUES (SQLSTATE, SQLERRM);

      SELECT public.fill_table_load_err()        

    END;

  --end_log --логирование конца загрузки процедуры, выход.

  --  /* COMMIT;
  -- EXCEPTION --  OR EXCEPTIONS SHOULD BE RAISED AFTER?
  -- when others then
  -- raise;*/
  END;
$BODY$;

ALTER FUNCTION public.fill_table_load_log(bigint, bigint, character varying, character varying, bigint, character varying, character varying, bigint, bigint, bigint)
    OWNER TO postgres;
