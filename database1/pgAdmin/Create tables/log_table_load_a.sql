-- Table: public.log_table_load_a

-- DROP TABLE IF EXISTS public.log_table_load_a;

CREATE TABLE IF NOT EXISTS public.log_table_load_a
(
    trg_table character varying COLLATE pg_catalog."default",
    date_begin date NOT NULL,
    date_end date,
    t_row_count bigint DEFAULT 0,
    e_row_count bigint DEFAULT 0,
    s_row_count bigint,
    source_system_id bigint NOT NULL,
    load_id bigint NOT NULL,
    seans_load_id bigint NOT NULL,
    load_name character varying COLLATE pg_catalog."default",
    load_status character varying COLLATE pg_catalog."default",
    src_table character varying COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.log_table_load_a
    OWNER to postgres;