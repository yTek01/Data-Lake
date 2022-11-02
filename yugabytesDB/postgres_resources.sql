--
--

SET default_transaction_read_only = off;

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

--
-- Roles
--

CREATE ROLE "dsr";
ALTER ROLE "dsr" WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION PASSWORD 'md56fed66d541f59729032c7a4b6afca9fa';
CREATE ROLE "postgres";
ALTER ROLE "postgres" WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION;

--
-- Database creation
--

CREATE DATABASE "Postgres" WITH TEMPLATE = template0 OWNER = "postgres";
REVOKE ALL ON DATABASE "template1" FROM PUBLIC;
REVOKE ALL ON DATABASE "template1" FROM "postgres";
GRANT ALL ON DATABASE "template1" TO "postgres";
GRANT CONNECT ON DATABASE "template1" TO PUBLIC;

\connect "Postgres"
SET default_transaction_read_only = off;

--
--
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

COMMENT ON SCHEMA "public" IS 'standard public schema';

-- DROP SCHEMA IF EXISTS "1841_staging";
CREATE SCHEMA IF NOT EXISTS "staging";
-- USE 1841_staging;

CREATE TABLE IF NOT EXISTS "staging"."reviews" (
    "product_id" integer NOT NULL,
    "review" integer NOT NULL
);

CREATE TABLE IF NOT EXISTS "staging"."orders" (
    "order_id" integer NOT NULL,
    "customer_id" integer NOT NULL,
    "order_date" date NOT NULL,
    "product_id" varchar(40) NOT NULL,
    "unit_price" integer NOT NULL,
    "quantity" integer NOT NULL,
    "amount" integer NOT NULL
);

CREATE TABLE IF NOT EXISTS "staging"."shipments_deliveries" (
    "shipment_id" integer NOT NULL,
    "order_id" integer NOT NULL,
    "shipment_date" date NULL,
    "delivery_date" date NULL
);

-- DROP SCHEMA IF EXISTS "if_common" ;
CREATE SCHEMA IF NOT EXISTS "ifcommon" ;
-- USE if_common;

CREATE TABLE IF NOT EXISTS "ifcommon"."dim_addresses" (
    "postal_code" integer NOT NULL PRIMARY KEY,
    "country" varchar(40) NOT NULL,
    "region" varchar(40) NOT NULL, 
    "state" varchar(40) NOT NULL,
    "address" varchar(50) NOT NULL
);


CREATE TABLE IF NOT EXISTS "ifcommon"."dim_products" (
    "product_id" integer NOT NULL PRIMARY KEY,
    "product_category" varchar(40) NOT NULL,
    "product_name" varchar(40) NOT NULL
);


CREATE TABLE IF NOT EXISTS "ifcommon"."dim_dates" (
    "calendar_dt" date NOT NULL PRIMARY KEY,
    "year_num" integer NOT NULL,
    "month_of_the_year_num" integer NOT NULL,
    "day_of_the_month_num" integer NOT NULL,
    "day_of_the_week_num" integer NOT NULL,
    "Working_day" boolean NOT NULL
);


CREATE TABLE IF NOT EXISTS "ifcommon"."dim_customers" (
  "customer_id" integer NOT NULL PRIMARY KEY,
  "customer_name" varchar(40) NOT NULL,
  "postal_code" integer NOT NULL REFERENCES "ifcommon"."dim_addresses"(postal_code)
);


CREATE SCHEMA IF NOT EXISTS "analytics";
-- USE 1841_analytics ;

CREATE TABLE IF NOT EXISTS "analytics"."agg_public_holiday" (
    "ingestion_date" date NOT NULL PRIMARY KEY,
    "tt_order_hol_jan" integer NOT NULL,
    "tt_order_hol_feb" integer NOT NULL,
    "tt_order_hol_mar" integer NOT NULL,
    "tt_order_hol_apr" integer NOT NULL,
    "tt_order_hol_may" integer NOT NULL,
    "tt_order_hol_jun" integer NOT NULL,
    "tt_order_hol_jul" integer NOT NULL,
    "tt_order_hol_aug" integer NOT NULL,
    "tt_order_hol_sep" integer NOT NULL,
    "tt_order_hol_oct" integer NOT NULL,
    "tt_order_hol_nov" integer NOT NULL,
    "tt_order_hol_dec" integer NOT NULL
);

CREATE TABLE IF NOT EXISTS "analytics"."agg_shipments" (
    "ingestion_date" date NOT NULL PRIMARY KEY, 
    "tt_late_shipments" integer NOT NULL, 
    "tt_undelivered_items" integer NOT NULL
);

CREATE TABLE IF NOT EXISTS "analytics"."best_performing_product" (
    "ingestion_date" date NOT NULL PRIMARY KEY, 
    "product_id" integer NOT NULL, 
    "most_ordered_day" date NOT NULL, 
    "is_public_holiday" boolean NOT NULL, 
    "tt_review_points" integer NOT NULL,
    "pct_one_star_review" decimal NOT NULL,
    "pct_two_star_review" decimal NOT NULL,
    "pct_three_star_review" decimal NOT NULL,
    "pct_four_star_review" decimal NOT NULL,
    "pct_five_star_review" decimal NOT NULL,
    "pct_early_shipments" decimal NOT NULL,
    "pct_late_shipments" decimal NOT NULL
);

