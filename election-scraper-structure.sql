--
-- PostgreSQL database dump
--

-- Dumped from database version 14.4
-- Dumped by pg_dump version 14.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: areas; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.areas (
    id character varying(255) NOT NULL,
    areas_group character varying(255),
    county_id character varying(255),
    county_name character varying(255),
    ward_id character varying(255),
    precinct_id character varying(255),
    precinct_name character varying(255),
    state_senate_id character varying(255),
    state_house_id character varying(255),
    county_commissioner_id character varying(255),
    district_court_id character varying(255),
    soil_water_id character varying(255),
    school_district_id character varying(255),
    school_district_name character varying(255),
    mcd_id character varying(255),
    precincts character varying(255),
    name character varying(255),
    updated timestamp without time zone
);


--
-- Name: contests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.contests (
    id character varying(255) NOT NULL,
    office_id character varying(255),
    results_group character varying(255),
    office_name character varying(255),
    district_code character varying(255),
    state character varying(255),
    county_id character varying(255),
    precinct_id character varying(255),
    precincts_reporting bigint,
    total_effected_precincts bigint,
    total_votes_for_office bigint,
    seats bigint,
    scope character varying(255),
    title character varying(255),
    boundary character varying(255),
    question_body text,
    sub_title character varying(255),
    incumbent_party character varying(255),
    updated timestamp without time zone,
    ranked_choice boolean,
    "primary" boolean,
    partisan boolean,
    called boolean
);


--
-- Name: meta; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.meta (
    key character varying(255) NOT NULL,
    value text,
    type character varying(255),
    updated timestamp without time zone
);


--
-- Name: questions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.questions (
    id character varying(255) NOT NULL,
    contest_id character varying(255),
    title character varying(255),
    sub_title character varying(255),
    question_body text,
    updated timestamp without time zone
);


--
-- Name: results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.results (
    id character varying(255) NOT NULL,
    contest_id character varying(255) NOT NULL,
    office_name character varying(255),
    candidate_id character varying(255),
    candidate character varying(255),
    suffix character varying(255),
    incumbent_code character varying(255),
    party_id character varying(255),
    votes_candidate bigint,
    percentage double precision,
    ranked_choice_place bigint,
    updated timestamp without time zone,
    results_group character varying(255)
);


--
-- Name: areas area_id_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.areas
    ADD CONSTRAINT area_id_pkey PRIMARY KEY (id);


--
-- Name: contests contest_id_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contests
    ADD CONSTRAINT contest_id_pkey PRIMARY KEY (id);


--
-- Name: meta meta_key_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.meta
    ADD CONSTRAINT meta_key_pkey PRIMARY KEY (key);


--
-- Name: questions question_id_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.questions
    ADD CONSTRAINT question_id_pkey PRIMARY KEY (id);


--
-- Name: results result_id_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.results
    ADD CONSTRAINT result_id_pkey PRIMARY KEY (id);


--
-- Name: results results_contest_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.results
    ADD CONSTRAINT results_contest_id_fkey FOREIGN KEY (contest_id) REFERENCES public.contests(id);


--
-- PostgreSQL database dump complete
--

