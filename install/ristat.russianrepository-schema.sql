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
-- Name: russianrepository; Type: TABLE; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE TABLE russianrepository (
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


ALTER TABLE public.russianrepository OWNER TO rusrep;

--
-- Name: russianrepository_pk_seq; Type: SEQUENCE; Schema: public; Owner: rusrep
--

CREATE SEQUENCE russianrepository_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.russianrepository_pk_seq OWNER TO rusrep;

--
-- Name: russianrepository_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: rusrep
--

ALTER SEQUENCE russianrepository_pk_seq OWNED BY russianrepository.pk;


--
-- Name: pk; Type: DEFAULT; Schema: public; Owner: rusrep
--

ALTER TABLE ONLY russianrepository ALTER COLUMN pk SET DEFAULT nextval('russianrepository_pk_seq'::regclass);


--
-- Name: russianrepository_pkey; Type: CONSTRAINT; Schema: public; Owner: rusrep; Tablespace: 
--

ALTER TABLE ONLY russianrepository
    ADD CONSTRAINT russianrepository_pkey PRIMARY KEY (pk);


--
-- Name: basecodeidx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX basecodeidx ON russianrepository USING btree (ter_code, base_year);


--
-- Name: baseyidx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX baseyidx ON russianrepository USING btree (base_year);


--
-- Name: c1r; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX c1r ON russianrepository USING btree (class1);


--
-- Name: c2r; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX c2r ON russianrepository USING btree (class2);


--
-- Name: c3r; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX c3r ON russianrepository USING btree (class3);


--
-- Name: c4r; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX c4r ON russianrepository USING btree (class4);


--
-- Name: c5r; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX c5r ON russianrepository USING btree (class5);


--
-- Name: dataidx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX dataidx ON russianrepository USING btree (datatype);


--
-- Name: dataidxyear; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX dataidxyear ON russianrepository USING btree (datatype, base_year);


--
-- Name: h1r; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX h1r ON russianrepository USING btree (histclass1);


--
-- Name: h2r; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX h2r ON russianrepository USING btree (histclass2);


--
-- Name: h3r; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX h3r ON russianrepository USING btree (histclass3);


--
-- Name: h4r; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX h4r ON russianrepository USING btree (histclass4);


--
-- Name: h5r; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX h5r ON russianrepository USING btree (histclass5);


--
-- Name: hc2index; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX hc2index ON russianrepository USING btree (histclass2);


--
-- Name: hc3index; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX hc3index ON russianrepository USING btree (histclass3);


--
-- Name: hcindex; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX hcindex ON russianrepository USING btree (histclass1);


--
-- Name: histclass1idx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX histclass1idx ON russianrepository USING btree (histclass1);


--
-- Name: histclass2idx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX histclass2idx ON russianrepository USING btree (histclass2);


--
-- Name: histclass3idx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX histclass3idx ON russianrepository USING btree (histclass3);


--
-- Name: terrdataidx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX terrdataidx ON russianrepository USING btree (ter_code, datatype);


--
-- Name: terrdatayearidx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX terrdatayearidx ON russianrepository USING btree (ter_code, datatype, base_year);


--
-- Name: terridx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX terridx ON russianrepository USING btree (ter_code);


--
-- Name: territoryidx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX territoryidx ON russianrepository USING btree (territory);


--
-- Name: valueunitidx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX valueunitidx ON russianrepository USING btree (value_unit);


--
-- Name: yearidx; Type: INDEX; Schema: public; Owner: rusrep; Tablespace: 
--

CREATE INDEX yearidx ON russianrepository USING btree (base_year);


--
-- PostgreSQL database dump complete
--

