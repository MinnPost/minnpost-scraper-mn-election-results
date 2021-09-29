--
-- PostgreSQL database dump
--

-- Dumped from database version 13.4
-- Dumped by pg_dump version 13.4

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
-- Name: areas; Type: TABLE; Schema: public;
--

CREATE TABLE public.areas (
    id text,
    areas_group text,
    county_id text,
    county_name text,
    ward_id text,
    precinct_id text,
    precinct_name text,
    state_senate_id text,
    state_house_id text,
    county_commissioner_id text,
    district_court_id text,
    soil_water_id text,
    school_district_id text,
    school_district_name text,
    mcd_id text,
    precincts text,
    name text,
    updated bigint
);

--
-- Name: contests; Type: TABLE; Schema: public;
--

CREATE TABLE public.contests (
    id text,
    office_id text,
    results_group text,
    office_name text,
    district_code text,
    state text,
    county_id text,
    precinct_id text,
    precincts_reporting bigint,
    total_effected_precincts bigint,
    total_votes_for_office bigint,
    seats bigint,
    ranked_choice boolean,
    "primary" boolean,
    scope text,
    updated bigint,
    title text,
    boundary text,
    partisan boolean,
    question_body text,
    sub_title text
);

--
-- Name: meta; Type: TABLE; Schema: public;
--

CREATE TABLE public.meta (
    key text,
    value text,
    type text
);

--
-- Name: questions; Type: TABLE; Schema: public;
--

CREATE TABLE public.questions (
    id text,
    contest_id text,
    title text,
    sub_title text,
    question_body text,
    updated bigint
);


--
-- Name: results; Type: TABLE; Schema: public;
--

CREATE TABLE public.results (
    id text,
    results_group text,
    office_name text,
    candidate_id text,
    candidate text,
    suffix text,
    incumbent_code text,
    party_id text,
    votes_candidate bigint,
    percentage double precision,
    ranked_choice_place bigint,
    contest_id text,
    updated bigint
);


--
-- Name: areas_id_unique; Type: INDEX; Schema: public;
--

CREATE UNIQUE INDEX areas_id_unique ON public.areas USING btree (id);


--
-- Name: candidate; Type: INDEX; Schema: public;
--

CREATE INDEX candidate ON public.results USING btree (candidate);


--
-- Name: contest_id; Type: INDEX; Schema: public;
--

CREATE INDEX contest_id ON public.results USING btree (contest_id);


--
-- Name: contests_id_unique; Type: INDEX; Schema: public;
--

CREATE UNIQUE INDEX contests_id_unique ON public.contests USING btree (id);


--
-- Name: meta_key_unique; Type: INDEX; Schema: public;
--

CREATE UNIQUE INDEX meta_key_unique ON public.meta USING btree (key);


--
-- Name: office_name; Type: INDEX; Schema: public;
--

CREATE INDEX office_name ON public.results USING btree (office_name);


--
-- Name: questions_id_unique; Type: INDEX; Schema: public;
--

CREATE UNIQUE INDEX questions_id_unique ON public.questions USING btree (id);


--
-- Name: results_id_unique; Type: INDEX; Schema: public;
--

CREATE UNIQUE INDEX results_id_unique ON public.results USING btree (id);


--
-- PostgreSQL database dump complete
--

