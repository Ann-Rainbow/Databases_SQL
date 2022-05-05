-- Table: public.d_product_a

-- DROP TABLE IF EXISTS public.d_product_a;

CREATE TABLE IF NOT EXISTS public.d_product_a
(
    product_id bigint NOT NULL DEFAULT nextval('seq_d_product'::regclass),
    product_src_code character varying COLLATE pg_catalog."default",
    product_name character varying COLLATE pg_catalog."default" NOT NULL,
    price numeric,
    load_id integer,
    load_period bigint,
    CONSTRAINT pk_d_product_a PRIMARY KEY (product_id),
    CONSTRAINT u_d_product_a_client UNIQUE (product_src_code)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.d_product_a
    OWNER to postgres;