-- Table: public.f_invoice_a

-- DROP TABLE IF EXISTS public.f_invoice_a;

CREATE TABLE IF NOT EXISTS public.f_invoice_a
(
    invoice_id bigint NOT NULL DEFAULT nextval('f_invoice_a_invoice_id_seq'::regclass),
    counteragent_id integer NOT NULL,
    f_date date,
    operation_type integer NOT NULL,
    load_id integer,
    seans_load_id integer,
    load_period bigint NOT NULL,
    invoice_src_code character varying COLLATE pg_catalog."default",
    counteragent_src_code character varying COLLATE pg_catalog."default",
    product_src_code character varying COLLATE pg_catalog."default",
    product_id integer,
    CONSTRAINT pk_f_invoice_a PRIMARY KEY (invoice_id),
    CONSTRAINT fk_f_invoice_a_counteragent FOREIGN KEY (counteragent_id)
        REFERENCES public.d_counteragent_a (counteragent_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_f_invoice_a_product FOREIGN KEY (product_id)
        REFERENCES public.d_product_a (product_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.f_invoice_a
    OWNER to postgres;
-- Index: fki_fk_f_invoice_a_product

-- DROP INDEX IF EXISTS public.fki_fk_f_invoice_a_product;

CREATE INDEX IF NOT EXISTS fki_fk_f_invoice_a_product
    ON public.f_invoice_a USING btree
    (product_id ASC NULLS LAST)
    TABLESPACE pg_default;