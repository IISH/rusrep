--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: russianrepo_en; Type: TABLE; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE TABLE russianrepo_en (
    id character varying(1024),
    territory character varying(1024),
    ter_code character varying(1024),
    town character varying(1024),
    district character varying(1024),
    year character varying(1024),
    month character varying(1024),
    value character varying(1024),
    value_unit character varying(1024),
    value_label character varying(1024),
    datatype character varying(1024),
    histclass1 character varying(1024),
    histclass2 character varying(1024),
    histclass3 character varying(1024),
    histclass4 character varying(1024),
    histclass5 character varying(1024),
    histclass6 character varying(1024),
    histclass7 character varying(1024),
    histclass8 character varying(1024) DEFAULT ''::character varying,
    histclass9 character varying(1024) DEFAULT ''::character varying,
    histclass10 character varying(1024) DEFAULT ''::character varying,
    class1 character varying(1024),
    class2 character varying(1024),
    class3 character varying(1024),
    class4 character varying(1024),
    class5 character varying(1024),
    class6 character varying(1024),
    class7 character varying(1024),
    class8 character varying(1024) DEFAULT ''::character varying,
    class9 character varying(1024) DEFAULT ''::character varying,
    class10 character varying(1024) DEFAULT ''::character varying,
    comment_source character varying(4096),
    source character varying(1024),
    volume character varying(1024),
    page character varying(1024),
    naborshik_id character varying(1024),
    comment_naborshik character varying(1024),
    base_year integer,
    indicator character varying(1024),
    valuemark boolean DEFAULT false,
    "timestamp" timestamp without time zone,
    pk integer NOT NULL
);


ALTER TABLE public.russianrepo_en OWNER TO rusrep;

--
-- Name: russianrepo_en_pk_seq; Type: SEQUENCE; Schema: public; Owner: rusrep
--

CREATE SEQUENCE russianrepo_en_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.russianrepo_en_pk_seq OWNER TO rusrep;

--
-- Name: russianrepo_en_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: rusrep
--

ALTER SEQUENCE russianrepo_en_pk_seq OWNED BY russianrepo_en.pk;


--
-- Name: pk; Type: DEFAULT; Schema: public; Owner: rusrep
--

ALTER TABLE ONLY russianrepo_en ALTER COLUMN pk SET DEFAULT nextval('russianrepo_en_pk_seq'::regclass);


--
-- Name: russianrepo_en_pkey; Type: CONSTRAINT; Schema: public; Owner: rusrep; Tablespace: 
--

ALTER TABLE ONLY russianrepo_en
    ADD CONSTRAINT russianrepo_en_pkey PRIMARY KEY (pk);

CREATE INDEX russianrepo_en_territory_idx ON russianrepo_en USING btree (territory);
CREATE INDEX russianrepo_en_tercode_idx ON russianrepo_en USING btree (ter_code);

CREATE INDEX russianrepo_en_valueunit_idx ON russianrepo_en USING btree (value_unit);
CREATE INDEX russianrepo_en_datatype_idx ON russianrepo_en USING btree (datatype);
CREATE INDEX russianrepo_en_baseyear_idx ON russianrepo_en USING btree (base_year);

CREATE INDEX russianrepo_en_datatypebaseyear_idx ON russianrepo_en USING btree (datatype, base_year);
CREATE INDEX russianrepo_en_tercodebaseyear_idx ON russianrepo_en USING btree (ter_code, base_year);
CREATE INDEX russianrepo_en_tercodedatatype_idx ON russianrepo_en USING btree (ter_code, datatype);
CREATE INDEX russianrepo_en_tercodedatatypebaseyear_idx ON russianrepo_en USING btree (ter_code, datatype, base_year);

CREATE INDEX russianrepo_en_class1_idx ON russianrepo_en USING btree (class1);
CREATE INDEX russianrepo_en_class2_idx ON russianrepo_en USING btree (class2);
CREATE INDEX russianrepo_en_class3_idx ON russianrepo_en USING btree (class3);
CREATE INDEX russianrepo_en_class4_idx ON russianrepo_en USING btree (class4);
CREATE INDEX russianrepo_en_class5_idx ON russianrepo_en USING btree (class5);
CREATE INDEX russianrepo_en_class6_idx ON russianrepo_en USING btree (class6);
CREATE INDEX russianrepo_en_class7_idx ON russianrepo_en USING btree (class7);
CREATE INDEX russianrepo_en_class8_idx ON russianrepo_en USING btree (class8);
CREATE INDEX russianrepo_en_class9_idx ON russianrepo_en USING btree (class9);
CREATE INDEX russianrepo_en_class10_idx ON russianrepo_en USING btree (class10);

CREATE INDEX russianrepo_en_histclass1_idx ON russianrepo_en USING btree (histclass1);
CREATE INDEX russianrepo_en_histclass2_idx ON russianrepo_en USING btree (histclass2);
CREATE INDEX russianrepo_en_histclass3_idx ON russianrepo_en USING btree (histclass3);
CREATE INDEX russianrepo_en_histclass4_idx ON russianrepo_en USING btree (histclass4);
CREATE INDEX russianrepo_en_histclass5_idx ON russianrepo_en USING btree (histclass5);
CREATE INDEX russianrepo_en_histclass6_idx ON russianrepo_en USING btree (histclass6);
CREATE INDEX russianrepo_en_histclass7_idx ON russianrepo_en USING btree (histclass7);
CREATE INDEX russianrepo_en_histclass8_idx ON russianrepo_en USING btree (histclass8);
CREATE INDEX russianrepo_en_histclass9_idx ON russianrepo_en USING btree (histclass9);
CREATE INDEX russianrepo_en_histclass10_idx ON russianrepo_en USING btree (histclass10);

--
-- PostgreSQL database dump complete
--

