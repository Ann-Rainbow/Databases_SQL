-- Table: public.d_counteragent_a

-- DROP TABLE IF EXISTS public.d_counteragent_a;

CREATE TABLE IF NOT EXISTS public.d_counteragent_a
(
    counteragent_id bigint NOT NULL DEFAULT nextval('d_counteragent_a_counteragent_id_seq'::regclass),
    counteragent_src_code character varying COLLATE pg_catalog."default",
    counteragent_name character varying COLLATE pg_catalog."default" NOT NULL,
    load_id integer,
    load_period bigint NOT NULL,
    CONSTRAINT pk_d_counteragent_a PRIMARY KEY (counteragent_id),
    CONSTRAINT u_d_counteragent_a_client UNIQUE (counteragent_src_code)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.d_counteragent_a
    OWNER to postgres;