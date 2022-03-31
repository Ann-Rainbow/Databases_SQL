-- FUNCTION: public.fill_table_load_log(bigint, character varying, character varying, character varying, date, date, bigint, character varying, bigint, bigint, bigint, bigint)

-- DROP FUNCTION IF EXISTS public.fill_table_load_log(bigint, character varying, character varying, character varying, date, date, bigint, character varying, bigint, bigint, bigint, bigint);

CREATE OR REPLACE FUNCTION public.fill_table_load_log(
	p_seans_load_id bigint,
	p_load_status character varying,
	p_load_name character varying,
	p_trg_table character varying,
	p_date_begin date,
	p_date_end date,
	p_source_system_id bigint,
	p_src_table character varying,
	p_load_id bigint DEFAULT nextval('seq_seans_load_id'::regclass),
	p_t_row_count bigint DEFAULT 0,
	p_e_row_count bigint DEFAULT 0,
	p_s_row_count bigint DEFAULT 0)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
  BEGIN
    IF UPPER(p_load_status) = 'STARTED' THEN
      INSERT INTO log_table_load_A 
    (trg_table,
    date_begin,
    --date_end,
    source_system_id,
    load_id,
    seans_load_id,
    load_name,
    load_status,
    src_table
         )
      VALUES
        (		
	p_trg_table,
    now(),
    --date_end,
    p_source_system_id,
    p_load_id,
    p_seans_load_id,
    p_load_name,
    p_load_status,
    p_src_table
        );
    ELSE
      UPDATE log_table_load_A 
         SET load_status = p_load_status,
             t_row_count = p_t_row_count,
             e_row_count = p_e_row_count,
             date_end    = p_date_end, 
             s_row_count = p_s_row_count
       WHERE load_id = p_load_id;
    END IF;
     EXCEPTION WHEN OTHERS THEN 
      SELECT public.fill_table_load_err(
	p_trg_table => 'log_table_load_A',
	p_err_type => SQLSTATE,
	p_err_text => SQLERRM,
	p_key_value => NULL,
	p_sa_load_id => NULL,
	p_load_id => p_load_id,
	p_seans_load_id => p_seans_load_id,
	p_load_name => 'fill_table_load_log',
	p_source_system_id => p_source_system_id   
	  );        
    END;
$BODY$;

ALTER FUNCTION public.fill_table_load_log(bigint, character varying, character varying, character varying, date, date, bigint, character varying, bigint, bigint, bigint, bigint)
    OWNER TO postgres;
