-- Table: public.log_dwh_load_a

-- DROP TABLE IF EXISTS public.log_dwh_load_a;

CREATE TABLE IF NOT EXISTS public.log_dwh_load_a
(
    date_begin date NOT NULL,
    date_end date,
    source_system_id bigint,
    seans_load_id bigint NOT NULL,
    load_status character varying COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.log_dwh_load_a
    OWNER to postgres;