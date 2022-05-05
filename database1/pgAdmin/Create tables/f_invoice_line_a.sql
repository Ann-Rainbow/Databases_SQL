-- Table: public.f_invoice_line_a

-- DROP TABLE IF EXISTS public.f_invoice_line_a;

CREATE TABLE IF NOT EXISTS public.f_invoice_line_a
(
    invoice_id bigint NOT NULL,
    amount numeric,
    load_id integer,
    load_period bigint NOT NULL,
    invoice_src_code character varying COLLATE pg_catalog."default",
    CONSTRAINT pk_f_invoice_line_a PRIMARY KEY (invoice_id),
    CONSTRAINT fk_f_invoice_line_a_invoice FOREIGN KEY (invoice_id)
        REFERENCES public.f_invoice_a (invoice_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.f_invoice_line_a
    OWNER to postgres;