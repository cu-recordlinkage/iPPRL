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


-- _chd_overall_cohort contains chd clinical data during just cohort time period
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

create table aim4.year_2011_chd_overall_cohort as select * from aim4.year_2011_cohort join aim4.chd_overall_cast on aim4.year_2011_cohort.id = aim4.chd_overall_cast.study_id where startdate between '01-01-2011' and '12-31-2011';
create table aim4.quarter1_2012_chd_overall_cohort as select * from aim4.quarter1_2012_cohort join aim4.chd_overall_cast on aim4.quarter1_2012_cohort.id = aim4.chd_overall_cast.study_id where startdate between '01-01-2012' and '3-31-2012';
create table aim4.quarter2_2012_chd_overall_cohort as select * from aim4.quarter2_2012_cohort join aim4.chd_overall_cast on aim4.quarter2_2012_cohort.id = aim4.chd_overall_cast.study_id where startdate between  '04-01-2012' and '06-30-2012';
create table aim4.quarter3_2012_chd_overall_cohort as select * from aim4.quarter3_2012_cohort join aim4.chd_overall_cast on aim4.quarter3_2012_cohort.id = aim4.chd_overall_cast.study_id where startdate between '07-01-2012' and '09-30-2012';
create table aim4.quarter4_2012_chd_overall_cohort as select * from aim4.quarter4_2012_cohort join aim4.chd_overall_cast on aim4.quarter4_2012_cohort.id = aim4.chd_overall_cast.study_id where startdate between '09-01-2012' and '12-31-2012';
create table aim4.month1_2013_chd_overall_cohort as select * from aim4.month1_2013_cohort join aim4.chd_overall_cast on aim4.month1_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '01-01-2013' and  '01-31-2013';
create table aim4.month2_2013_chd_overall_cohort as select * from aim4.month2_2013_cohort join aim4.chd_overall_cast on aim4.month2_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '02-01-2013' and '02-28-2013';
create table aim4.month3_2013_chd_overall_cohort as select * from aim4.month3_2013_cohort join aim4.chd_overall_cast on aim4.month3_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '03-01-2013' and '03-31-2013';
create table aim4.month4_2013_chd_overall_cohort as select * from aim4.month4_2013_cohort join aim4.chd_overall_cast on aim4.month4_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '04-01-2013' and '04-30-2013';
create table aim4.month5_2013_chd_overall_cohort as select * from aim4.month5_2013_cohort join aim4.chd_overall_cast on aim4.month5_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '05-01-2013' and '05-31-2013';
create table aim4.month6_2013_chd_overall_cohort as select * from aim4.month6_2013_cohort join aim4.chd_overall_cast on aim4.month6_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '06-01-2013' and '06-30-2013';
create table aim4.month7_2013_chd_overall_cohort as select * from aim4.month7_2013_cohort join aim4.chd_overall_cast on aim4.month7_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '07-01-2013' and '07-31-2013';
create table aim4.month8_2013_chd_overall_cohort as select * from aim4.month8_2013_cohort join aim4.chd_overall_cast on aim4.month8_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '08-01-2013' and '08-30-2013';
create table aim4.month9_2013_chd_overall_cohort as select * from aim4.month9_2013_cohort join aim4.chd_overall_cast on aim4.month9_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '09-01-2013' and '09-30-2013';
create table aim4.month10_2013_chd_overall_cohort as select * from aim4.month10_2013_cohort join aim4.chd_overall_cast on aim4.month10_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '10-01-2013' and '10-31-2013';
create table aim4.month11_2013_chd_overall_cohort as select * from aim4.month11_2013_cohort join aim4.chd_overall_cast on aim4.month11_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '11-01-2013' and '11-30-2013';
create table aim4.month12_2013_chd_overall_cohort as select * from aim4.month12_2013_cohort join aim4.chd_overall_cast on aim4.month12_2013_cohort.id = aim4.chd_overall_cast.study_id where startdate between '12-01-2013' and '12-31-2013';



-- _chd_overall_cummulative contains chd clinical data during the cohort time period **AND ALL EARLIER PERIODS**
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
