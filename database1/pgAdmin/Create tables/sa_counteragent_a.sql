-- Table: public.sa_counteragent_a

-- DROP TABLE IF EXISTS public.sa_counteragent_a;

CREATE TABLE IF NOT EXISTS public.sa_counteragent_a
(
    counteragent_code character varying COLLATE pg_catalog."default",
    counteragent_name character varying COLLATE pg_catalog."default",
    sa_load_id bigint,
    load_period bigint NOT NULL,
    is_last character varying COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sa_counteragent_a
    OWNER to postgres;