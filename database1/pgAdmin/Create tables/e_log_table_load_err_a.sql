-- Table: public.e_log_table_load_err_a

-- DROP TABLE IF EXISTS public.e_log_table_load_err_a;

CREATE TABLE IF NOT EXISTS public.e_log_table_load_err_a
(
    trg_table character varying COLLATE pg_catalog."default",
    err_type character varying COLLATE pg_catalog."default",
    err_text character varying COLLATE pg_catalog."default",
    key_value character varying COLLATE pg_catalog."default",
    sa_load_id bigint,
    load_id bigint,
    seans_load_id bigint,
    load_name character varying COLLATE pg_catalog."default",
    source_system_id bigint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.e_log_table_load_err_a
    OWNER to postgres;