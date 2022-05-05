-- Table: public.sa_product_a

-- DROP TABLE IF EXISTS public.sa_product_a;

CREATE TABLE IF NOT EXISTS public.sa_product_a
(
    product_code character varying COLLATE pg_catalog."default",
    product_name character varying(50) COLLATE pg_catalog."default",
    price character varying COLLATE pg_catalog."default" NOT NULL,
    sa_load_id bigint,
    load_period bigint NOT NULL,
    is_last character varying COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sa_product_a
    OWNER to postgres;