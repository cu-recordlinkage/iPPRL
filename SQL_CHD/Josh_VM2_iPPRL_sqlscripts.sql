drop table if exists tz.year_2011;

create table if not exists tz.year_2011(
id bigint primary key,
mrn varchar(20),
	d_source varchar(50),
	first_name varchar(50),
	last_name varchar(50),
	middle_name varchar(50),
	suffix varchar(25),
	sex char(10),
	dob date,
	ssn	varchar(20),
	address varchar(250),
	address_ln2 varchar(250),
	city char(50),
	state char(25),
	zip varchar(15),
	phone varchar(20),
	phone2 varchar(20),
	ssn4 varchar(10)
);


DROP TABLE IF EXISTS tz.quarter1_2012;
DROP TABLE IF EXISTS tz.quarter2_2012;
DROP TABLE IF EXISTS tz.quarter3_2012;
DROP TABLE IF EXISTS tz.quarter4_2012;
DROP TABLE IF EXISTS tz.month1_2013;
DROP TABLE IF EXISTS tz.month2_2013;
DROP TABLE IF EXISTS tz.month3_2013;
DROP TABLE IF EXISTS tz.month4_2013;
DROP TABLE IF EXISTS tz.month5_2013;
DROP TABLE IF EXISTS tz.month6_2013;
DROP TABLE IF EXISTS tz.month7_2013;
DROP TABLE IF EXISTS tz.month8_2013;
DROP TABLE IF EXISTS tz.month9_2013;
DROP TABLE IF EXISTS tz.month10_2013;
DROP TABLE IF EXISTS tz.month11_2013;
DROP TABLE IF EXISTS tz.month12_2013;




CREATE TABLE IF NOT EXISTS tz.quarter1_2012 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.quarter2_2012 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.quarter3_2012 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.quarter4_2012 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month1_2013 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month2_2013 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month3_2013 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month4_2013 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month5_2013 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month6_2013 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month7_2013 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month8_2013 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month9_2013 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month10_2013 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month11_2013 AS TABLE tz.year_2011 WITH NO DATA;
CREATE TABLE IF NOT EXISTS tz.month12_2013 AS TABLE tz.year_2011 WITH NO DATA;

ALTER TABLE tz.quarter1_2012 ADD PRIMARY KEY(id);
ALTER TABLE tz.quarter2_2012 ADD PRIMARY KEY(id);
ALTER TABLE tz.quarter3_2012 ADD PRIMARY KEY(id);
ALTER TABLE tz.quarter4_2012 ADD PRIMARY KEY(id);
ALTER TABLE tz.month1_2013 ADD PRIMARY KEY(id);
ALTER TABLE tz.month2_2013 ADD PRIMARY KEY(id);
ALTER TABLE tz.month3_2013 ADD PRIMARY KEY(id);
ALTER TABLE tz.month4_2013 ADD PRIMARY KEY(id);
ALTER TABLE tz.month5_2013 ADD PRIMARY KEY(id);
ALTER TABLE tz.month6_2013 ADD PRIMARY KEY(id);
ALTER TABLE tz.month7_2013 ADD PRIMARY KEY(id);
ALTER TABLE tz.month8_2013 ADD PRIMARY KEY(id);
ALTER TABLE tz.month9_2013 ADD PRIMARY KEY(id);
ALTER TABLE tz.month10_2013 ADD PRIMARY KEY(id);
ALTER TABLE tz.month11_2013 ADD PRIMARY KEY(id);
ALTER TABLE tz.month12_2013 ADD PRIMARY KEY(id);

ALTER TABLE tz.chd_overall RENAME COLUMN mrn TO mrn_chd_overall;



select table_schema, 
       table_name, 
       (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
from (
  select table_name, table_schema, 
         query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
  from information_schema.tables
  where table_schema = 'tz' --<< change here for the schema you want
) t order by 3 DESC;

create table tz.year_2011_chd_overall_merge as select * from tz.year_2011 join tz.chd_overall on tz.year_2011.id = tz.chd_overall.study_id;
create table tz.quarter1_2012_chd_overall_merge as select * from tz.quarter1_2012 join tz.chd_overall on tz.quarter1_2012.id = tz.chd_overall.study_id;
create table tz.quarter2_2012_chd_overall_merge as select * from tz.quarter2_2012 join tz.chd_overall on tz.quarter2_2012.id = tz.chd_overall.study_id;
create table tz.quarter3_2012_chd_overall_merge as select * from tz.quarter3_2012 join tz.chd_overall on tz.quarter3_2012.id = tz.chd_overall.study_id;
create table tz.quarter4_2012_chd_overall_merge as select * from tz.quarter4_2012 join tz.chd_overall on tz.quarter4_2012.id = tz.chd_overall.study_id;
create table tz.month1_2013_chd_overall_merge as select * from tz.month1_2013 join tz.chd_overall on tz.month1_2013.id = tz.chd_overall.study_id;
create table tz.month2_2013_chd_overall_merge as select * from tz.month2_2013 join tz.chd_overall on tz.month2_2013.id = tz.chd_overall.study_id;
create table tz.month3_2013_chd_overall_merge as select * from tz.month3_2013 join tz.chd_overall on tz.month3_2013.id = tz.chd_overall.study_id;
create table tz.month4_2013_chd_overall_merge as select * from tz.month4_2013 join tz.chd_overall on tz.month4_2013.id = tz.chd_overall.study_id;
create table tz.month5_2013_chd_overall_merge as select * from tz.month5_2013 join tz.chd_overall on tz.month5_2013.id = tz.chd_overall.study_id;
create table tz.month6_2013_chd_overall_merge as select * from tz.month6_2013 join tz.chd_overall on tz.month6_2013.id = tz.chd_overall.study_id;
create table tz.month7_2013_chd_overall_merge as select * from tz.month7_2013 join tz.chd_overall on tz.month7_2013.id = tz.chd_overall.study_id;
create table tz.month8_2013_chd_overall_merge as select * from tz.month8_2013 join tz.chd_overall on tz.month8_2013.id = tz.chd_overall.study_id;
create table tz.month9_2013_chd_overall_merge as select * from tz.month9_2013 join tz.chd_overall on tz.month9_2013.id = tz.chd_overall.study_id;
create table tz.month10_2013_chd_overall_merge as select * from tz.month10_2013 join tz.chd_overall on tz.month10_2013.id = tz.chd_overall.study_id;
create table tz.month11_2013_chd_overall_merge as select * from tz.month11_2013 join tz.chd_overall on tz.month11_2013.id = tz.chd_overall.study_id;
create table tz.month12_2013_chd_overall_merge as select * from tz.month12_2013 join tz.chd_overall on tz.month12_2013.id = tz.chd_overall.study_id;


select table_schema, 
       table_name, 
       (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
from (
  select table_name, table_schema, 
         query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
  from information_schema.tables
  where table_schema = 'tz' --<< change here for the schema you want
) t order by 3 DESC;

