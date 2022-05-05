-- Table: public.sa_invoice_a

-- DROP TABLE IF EXISTS public.sa_invoice_a;

CREATE TABLE IF NOT EXISTS public.sa_invoice_a
(
    counteragent_code character varying COLLATE pg_catalog."default",
    f_date date NOT NULL,
    operation_type character varying COLLATE pg_catalog."default" NOT NULL,
    sa_load_id bigint,
    load_period bigint NOT NULL,
    is_last character varying COLLATE pg_catalog."default",
    invoice_code character varying COLLATE pg_catalog."default",
    product_code character varying COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sa_invoice_a
    OWNER to postgres;