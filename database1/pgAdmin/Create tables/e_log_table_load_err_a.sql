-- Table: public.e_log_table_load_err_a

-- DROP TABLE IF EXISTS public.e_log_table_load_err_a;

CREATE TABLE IF NOT EXISTS public.e_log_table_load_err_a
(
    trg_table character varying COLLATE pg_catalog."default" NOT NULL,
    proc_name character varying COLLATE pg_catalog."default",
    e_row_count bigint,
    error_code integer NOT NULL,
    err_type character varying COLLATE pg_catalog."default" NOT NULL,
    err_text character varying COLLATE pg_catalog."default",
    key_value character varying COLLATE pg_catalog."default",
    sa_load_id bigint,
    load_id bigint,
    load_procedure_name character varying COLLATE pg_catalog."default",
    seans_load_id bigint,
    load_name character varying COLLATE pg_catalog."default" NOT NULL,
    load_period bigint NOT NULL,
    load_status character varying COLLATE pg_catalog."default",
    source_system_id bigint NOT NULL,
    CONSTRAINT pk_e_log_table_load_err_a PRIMARY KEY (error_code),
    CONSTRAINT fk_log_dwh_load_a_load FOREIGN KEY (load_id)
        REFERENCES public.log_dwh_load_a (load_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.e_log_table_load_err_a
    OWNER to postgres;