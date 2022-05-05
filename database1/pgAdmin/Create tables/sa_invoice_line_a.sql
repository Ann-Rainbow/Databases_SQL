-- Table: public.sa_invoice_line_a

-- DROP TABLE IF EXISTS public.sa_invoice_line_a;

CREATE TABLE IF NOT EXISTS public.sa_invoice_line_a
(
    amount character varying COLLATE pg_catalog."default",
    sa_load_id bigint,
    load_period bigint NOT NULL,
    is_last character varying COLLATE pg_catalog."default",
    invoice_code character varying COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sa_invoice_line_a
    OWNER to postgres;