CREATE DATABASE test_proj;

-- Table: public.session_vals

-- DROP TABLE public.session_vals;

CREATE TABLE public.test_ds
(
    id serial NOT NULL,
    ts timestamp with time zone DEFAULT now(),
    CONSTRAINT test_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
);

