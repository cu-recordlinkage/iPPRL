
/*

This script generates the base data tables for iPPRL Aim 4
It create two sets of tables: 
  "cohort" tables contain patients/clinical data just for one time interval
  "cummulative" tables contains patients/clinical data for a time interval plus all earlier intervals.

WARNING: Destroys the aim4 schema so ALL tables are destroyed. 
         This ensures later processing steps use a uniform data sets.

Created by: Michael Kahn (michael.kahn@cuanschutz.edu)
Version date: 2021-04-21

*/


-- Step 0: DROP aim4 schema to start over.
-- Step 1: Create a version of chd_overall table that casts strings/dates to integers/dates
-- Step 2: Fill in new chd_overall_cast table
-- Step 3: Create/fill the tables for cohorts and cummulative data


-- Step 0: DROP aim4 schema to start over


DROP SCHEMA IF EXISTS aim4 CASCADE;
COMMIT;
CREATE SCHEMA aim4;



-- Step 1: Create a version of chd_overall table that casts strings/dates to integers/dates

DROP TABLE IF EXISTS aim4.chd_overall_cast;

CREATE TABLE aim4.chd_overall_cast
(
    indexcaseid character varying(50) COLLATE pg_catalog."default",
    recnum character varying(50) COLLATE pg_catalog."default",
    seqnum character varying(50) COLLATE pg_catalog."default",
    encnum character varying(50) COLLATE pg_catalog."default",
    reportingsite character varying(50) COLLATE pg_catalog."default",
    ageatencounter integer,
    fqe2distance integer,
    datasource character varying(50) COLLATE pg_catalog."default",
    datasourcetype character varying(50) COLLATE pg_catalog."default",
    startdateday integer,
    startdatemonth integer,
    startdatequarter integer,
    startdateyear integer,
    startdate date,
    enddateday integer,
    enddatemonth integer,
    enddatequarter integer,
    enddateyear integer,
    enddate date,
    encountertype character varying(50) COLLATE pg_catalog."default",
    encountertypeother character varying(50) COLLATE pg_catalog."default",
    lengthofstay integer,
    insselfpay character varying(50) COLLATE pg_catalog."default",
    insprivate character varying(50) COLLATE pg_catalog."default",
    insmedicaid character varying(50) COLLATE pg_catalog."default",
    insmedicare character varying(50) COLLATE pg_catalog."default",
    insothergovt character varying(50) COLLATE pg_catalog."default",
    insother character varying(50) COLLATE pg_catalog."default",
    insotherdesc character varying(50) COLLATE pg_catalog."default",
    insunavailable character varying(50) COLLATE pg_catalog."default",
    insunknown character varying(50) COLLATE pg_catalog."default",
    providertype character varying(50) COLLATE pg_catalog."default",
    providertypeother character varying(50) COLLATE pg_catalog."default",
    heightinches integer,
    weightpounds integer,
    uniqueid character varying(50) COLLATE pg_catalog."default",
    study_id bigint,
    mrn_chd_overall character varying(50) COLLATE pg_catalog."default",
    siteid character varying(50) COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE aim4.chd_overall_cast
    OWNER to postgres;


-- Truncte merged_source and cast id into an INT
DROP TABLE IF EXISTS aim4.merged_source;

CREATE TABLE aim4.merged_source
(
    uid integer,
    run_id integer,
    source_id integer,
    id integer,
    CONSTRAINT merged_source_pkey PRIMARY KEY (uid)
)
TABLESPACE pg_default;

ALTER TABLE aim4.merged_source
    OWNER to honestbroker;


--
-- Step 2a: Fill in merged_source while casting id
-- 

INSERT INTO aim4.merged_source
   SELECT
      uid ,
      run_id ,
      source_id ,
      cast(id as integer)
      FROM job_22092.merged_source;

--
-- Step 2: Fill in new chd_overall_cast table
--

-- CREATE OR REPLACE FUNCTION last_day(date)
CREATE OR REPLACE FUNCTION last_day(date)
RETURNS DATE AS
$$
	SELECT (DATE_TRUNC('MONTH',$1) + INTERVAl '1 month - 1 day')::DATE
$$ LANGUAGE 'sql' IMMUTABLE STRICT;

-- Integer values for dates
-- new columns: startdate, enddate
-- when endate fields are null, fills in with startdate fields to eliminate null enddates

INSERT INTO aim4.chd_overall_cast
  SELECT
    indexcaseid ,
    recnum ,
    seqnum ,
    encnum ,
    reportingsite ,
    cast(ageatencounter as integer) ,
    cast(fqe2distance as integer),
    datasource ,
    datasourcetype ,
    cast(startdateday as integer),
    cast(startdatemonth as integer),
    cast(startdatequarter as integer),
    cast(startdateyear as integer),
    make_date(startdateyear::int, startdatemonth::int, 1),
    -- If no values for enddates, ust values in startdate
    coalesce(cast(enddateday as integer),cast(startdateday as integer)),
    coalesce(cast(enddatemonth as integer),cast(startdatemonth as integer)),
    coalesce(cast(enddatequarter as integer), cast(startdatequarter as integer)),
    coalesce(cast(enddateyear as integer), cast(startdateyear as integer)),
    coalesce(last_day(make_date(enddateyear::int, enddatemonth::int,1)), last_day(make_date(startdateyear::int,startdatemonth::int,1))),
    encountertype ,
    encountertypeother , 
    cast(lengthofstay as integer),
    insselfpay ,
    insprivate ,
    insmedicaid ,
    insmedicare ,
    insothergovt ,
    insother ,
    insotherdesc , 
    insunavailable ,
    insunknown , 
    providertype ,
    providertypeother ,
    cast(heightinches as integer),
    cast(weightpounds as integer),
    uniqueid ,
    study_id,
    mrn_chd_overall ,
    siteid
  FROM tz.chd_overall

;


-- Step 3: Create/fill the tables for cohorts and cummulative data

-- Replicates cohort tables from tz partitions

DROP TABLE IF EXISTS aim4.year_2011_cohort;
DROP TABLE IF EXISTS aim4.quarter1_2012_cohort;
DROP TABLE IF EXISTS aim4.quarter2_2012_cohort;
DROP TABLE IF EXISTS aim4.quarter3_2012_cohort;
DROP TABLE IF EXISTS aim4.quarter4_2012_cohort;
DROP TABLE IF EXISTS aim4.month1_2013_cohort;
DROP TABLE IF EXISTS aim4.month2_2013_cohort;
DROP TABLE IF EXISTS aim4.month3_2013_cohort;
DROP TABLE IF EXISTS aim4.month4_2013_cohort;
DROP TABLE IF EXISTS aim4.month5_2013_cohort;
DROP TABLE IF EXISTS aim4.month6_2013_cohort;
DROP TABLE IF EXISTS aim4.month7_2013_cohort;
DROP TABLE IF EXISTS aim4.month8_2013_cohort;
DROP TABLE IF EXISTS aim4.month9_2013_cohort;
DROP TABLE IF EXISTS aim4.month10_2013_cohort;
DROP TABLE IF EXISTS aim4.month11_2013_cohort;
DROP TABLE IF EXISTS aim4.month12_2013_cohort;

/* These tables contain patients seen in the specific time period */

CREATE TABLE IF NOT EXISTS aim4.year_2011_cohort AS SELECT * FROM tz.year_2011;
CREATE TABLE IF NOT EXISTS aim4.quarter1_2012_cohort AS SELECT * FROM tz.quarter1_2012;
CREATE TABLE IF NOT EXISTS aim4.quarter2_2012_cohort AS SELECT * FROM tz.quarter2_2012;
CREATE TABLE IF NOT EXISTS aim4.quarter3_2012_cohort AS SELECT * FROM tz.quarter3_2012;
CREATE TABLE IF NOT EXISTS aim4.quarter4_2012_cohort AS SELECT * FROM tz.quarter4_2012;
CREATE TABLE IF NOT EXISTS aim4.month1_2013_cohort AS SELECT * FROM tz.month1_2013;
CREATE TABLE IF NOT EXISTS aim4.month2_2013_cohort AS SELECT * FROM tz.month2_2013;
CREATE TABLE IF NOT EXISTS aim4.month3_2013_cohort AS SELECT * FROM tz.month3_2013;
CREATE TABLE IF NOT EXISTS aim4.month4_2013_cohort AS SELECT * FROM tz.month4_2013;
CREATE TABLE IF NOT EXISTS aim4.month5_2013_cohort AS SELECT * FROM tz.month5_2013;
CREATE TABLE IF NOT EXISTS aim4.month6_2013_cohort AS SELECT * FROM tz.month6_2013;
CREATE TABLE IF NOT EXISTS aim4.month7_2013_cohort AS SELECT * FROM tz.month7_2013;
CREATE TABLE IF NOT EXISTS aim4.month8_2013_cohort AS SELECT * FROM tz.month8_2013;
CREATE TABLE IF NOT EXISTS aim4.month9_2013_cohort AS SELECT * FROM tz.month9_2013;
CREATE TABLE IF NOT EXISTS aim4.month10_2013_cohort AS SELECT * FROM tz.month10_2013;
CREATE TABLE IF NOT EXISTS aim4.month11_2013_cohort AS SELECT * FROM tz.month11_2013;
CREATE TABLE IF NOT EXISTS aim4.month12_2013_cohort AS SELECT * FROM tz.month12_2013;


-- _chd_overall_cohort contains encounter-level chd clinical data during just cohort time period
-- This would be the delta load -- the data available during that time period

DROP TABLE IF EXISTS aim4.year_2011_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.quarter1_2012_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.quarter2_2012_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.quarter3_2012_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.quarter4_2012_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month1_2013_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month2_2013_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month3_2013_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month4_2013_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month5_2013_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month6_2013_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month7_2013_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month8_2013_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month9_2013_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month10_2013_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month11_2013_chd_overall_cohort;
DROP TABLE IF EXISTS aim4.month12_2013_chd_overall_cohort;

create table aim4.year_2011_chd_overall_cohort as select * from aim4.year_2011_cohort join aim4.chd_overall_cast on aim4.year_2011_cohort.id = aim4.chd_overall_cast.study_id where enddate between '01-01-2011' and '12-31-2011';
create table aim4.quarter1_2012_chd_overall_cohort as select * from aim4.quarter1_2012_cohort join aim4.chd_overall_cast on aim4.quarter1_2012_cohort.id = aim4.chd_overall_cast.study_id where enddate between '01-01-2012' and '3-31-2012';
create table aim4.quarter2_2012_chd_overall_cohort as select * from aim4.quarter2_2012_cohort join aim4.chd_overall_cast on aim4.quarter2_2012_cohort.id = aim4.chd_overall_cast.study_id where enddate between  '04-01-2012' and '06-30-2012';
create table aim4.quarter3_2012_chd_overall_cohort as select * from aim4.quarter3_2012_cohort join aim4.chd_overall_cast on aim4.quarter3_2012_cohort.id = aim4.chd_overall_cast.study_id where enddate between '07-01-2012' and '09-30-2012';
create table aim4.quarter4_2012_chd_overall_cohort as select * from aim4.quarter4_2012_cohort join aim4.chd_overall_cast on aim4.quarter4_2012_cohort.id = aim4.chd_overall_cast.study_id where enddate between '09-01-2012' and '12-31-2012';
create table aim4.month1_2013_chd_overall_cohort as select * from aim4.month1_2013_cohort join aim4.chd_overall_cast on aim4.month1_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '01-01-2013' and  '01-31-2013';
create table aim4.month2_2013_chd_overall_cohort as select * from aim4.month2_2013_cohort join aim4.chd_overall_cast on aim4.month2_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '02-01-2013' and '02-28-2013';
create table aim4.month3_2013_chd_overall_cohort as select * from aim4.month3_2013_cohort join aim4.chd_overall_cast on aim4.month3_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '03-01-2013' and '03-31-2013';
create table aim4.month4_2013_chd_overall_cohort as select * from aim4.month4_2013_cohort join aim4.chd_overall_cast on aim4.month4_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '04-01-2013' and '04-30-2013';
create table aim4.month5_2013_chd_overall_cohort as select * from aim4.month5_2013_cohort join aim4.chd_overall_cast on aim4.month5_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '05-01-2013' and '05-31-2013';
create table aim4.month6_2013_chd_overall_cohort as select * from aim4.month6_2013_cohort join aim4.chd_overall_cast on aim4.month6_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '06-01-2013' and '06-30-2013';
create table aim4.month7_2013_chd_overall_cohort as select * from aim4.month7_2013_cohort join aim4.chd_overall_cast on aim4.month7_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '07-01-2013' and '07-31-2013';
create table aim4.month8_2013_chd_overall_cohort as select * from aim4.month8_2013_cohort join aim4.chd_overall_cast on aim4.month8_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '08-01-2013' and '08-30-2013';
create table aim4.month9_2013_chd_overall_cohort as select * from aim4.month9_2013_cohort join aim4.chd_overall_cast on aim4.month9_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '09-01-2013' and '09-30-2013';
create table aim4.month10_2013_chd_overall_cohort as select * from aim4.month10_2013_cohort join aim4.chd_overall_cast on aim4.month10_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '10-01-2013' and '10-31-2013';
create table aim4.month11_2013_chd_overall_cohort as select * from aim4.month11_2013_cohort join aim4.chd_overall_cast on aim4.month11_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '11-01-2013' and '11-30-2013';
create table aim4.month12_2013_chd_overall_cohort as select * from aim4.month12_2013_cohort join aim4.chd_overall_cast on aim4.month12_2013_cohort.id = aim4.chd_overall_cast.study_id where enddate between '12-01-2013' and '12-31-2013';



-- _chd_overall_cummulative contains encounter-level chd clinical data during the cohort time period **AND ALL EARLIER PERIODS**
-- This would be all available data at the time of linkage (old and incremental).
-- If you need old data separate from incremental data, use _cummulative from previous time period for old data
--       and _cohort table for this time period for incremental data

DROP TABLE IF EXISTS aim4.year_2011_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.quarter1_2012_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.quarter2_2012_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.quarter3_2012_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.quarter4_2012_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month1_2013_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month2_2013_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month3_2013_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month4_2013_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month5_2013_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month6_2013_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month7_2013_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month8_2013_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month9_2013_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month10_2013_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month11_2013_chd_overall_cummulative;
DROP TABLE IF EXISTS aim4.month12_2013_chd_overall_cummulative;

create table aim4.year_2011_chd_overall_cummulative as select * from aim4.year_2011_chd_overall_cohort;
create table aim4.quarter1_2012_chd_overall_cummulative as 
   (select distinct * from (select * from aim4.year_2011_chd_overall_cummulative UNION select * from aim4.quarter1_2012_chd_overall_cohort) a);
create table aim4.quarter2_2012_chd_overall_cummulative as
   (select distinct * from (select * from aim4.quarter1_2012_chd_overall_cummulative UNION select * from aim4.quarter2_2012_chd_overall_cohort) a);   
create table aim4.quarter3_2012_chd_overall_cummulative as
   (select distinct * from (select * from aim4.quarter2_2012_chd_overall_cummulative UNION select * from aim4.quarter3_2012_chd_overall_cohort) a);


create table aim4.quarter4_2012_chd_overall_cummulative as
   (select distinct * from (select * from aim4.quarter3_2012_chd_overall_cummulative UNION select * from aim4.quarter4_2012_chd_overall_cohort) a);
create table aim4.month1_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.quarter4_2012_chd_overall_cummulative UNION select * from aim4.month1_2013_chd_overall_cohort) a);
create table aim4.month2_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.month1_2013_chd_overall_cummulative UNION select * from aim4.month2_2013_chd_overall_cohort) a);
create table aim4.month3_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.month2_2013_chd_overall_cummulative UNION select * from aim4.month3_2013_chd_overall_cohort) a);
create table aim4.month4_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.month3_2013_chd_overall_cummulative UNION select * from aim4.month4_2013_chd_overall_cohort) a);
create table aim4.month5_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.month4_2013_chd_overall_cummulative UNION select * from aim4.month5_2013_chd_overall_cohort) a);
create table aim4.month6_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.month5_2013_chd_overall_cummulative UNION select * from aim4.month6_2013_chd_overall_cohort) a);
create table aim4.month7_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.month6_2013_chd_overall_cummulative UNION select * from aim4.month7_2013_chd_overall_cohort) a);
create table aim4.month8_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.month7_2013_chd_overall_cummulative UNION select * from aim4.month8_2013_chd_overall_cohort) a);
create table aim4.month9_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.month8_2013_chd_overall_cummulative UNION select * from aim4.month9_2013_chd_overall_cohort) a);
create table aim4.month10_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.month9_2013_chd_overall_cummulative UNION select * from aim4.month10_2013_chd_overall_cohort) a);
create table aim4.month11_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.month10_2013_chd_overall_cummulative UNION select * from aim4.month11_2013_chd_overall_cohort) a);
create table aim4.month12_2013_chd_overall_cummulative as
   (select distinct * from (select * from aim4.month11_2013_chd_overall_cummulative UNION select * from aim4.month12_2013_chd_overall_cohort) a);
  


-- Provides counts of all tables in a schema
/*
select table_schema, 
       table_name, 
       (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
from (
  select table_name, table_schema, 
         query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
  from information_schema.tables
  where table_schema = 'aim4' --<< change here for the schema you want
) t order by 3 DESC;

*/


