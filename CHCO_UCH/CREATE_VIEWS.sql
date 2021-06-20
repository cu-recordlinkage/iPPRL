create or replace view aim4.quarter3_2017_person as
select * from aim4.chco_year_2016
union distinct
select * from aim4.chco_q1_2017
union distinct
select * from aim4.chco_q2_2017
union distinct
select * from aim4.chco_q3_2017
union distinct
select * from aim4.uch_year_2016 
union distinct
select * from aim4.uch_q1_2017
union distinct
select * from aim4.uch_q2_2017
union distinct
select * from aim4.uch_q3_2017
;

create or replace view aim4.quarter2_2017_person as
select * from aim4.chco_year_2016
union distinct
select * from aim4.chco_q1_2017
union distinct
select * from aim4.chco_q2_2017
union distinct
select * from aim4.uch_year_2016
union distinct
select * from aim4.uch_q1_2017
union distinct
select * from aim4.uch_q2_2017
;

create or replace view aim4.quarter1_2017_person as
select * from aim4.chco_year_2016
union distinct
select * from aim4.chco_q1_2017
union distinct
select * from aim4.uch_year_2016
union distinct
select * from aim4.uch_q1_2017
;

create or replace view aim4.year_2016_person as
select * from aim4.chco_year_2016
union distinct
select * from aim4.uch_year_2016
;

-- LVs from chd; first_name, last_name, gender, dob, ssn, address_line1 as address, city, state, zip, prim_phone as phone, ssn4
drop view if exists aim4.raw_person cascade;
create or replace view aim4.raw_person as
select firstname as first_name
       , lastname as last_name
       , sex as gender
       , birthdate as dob
       , ssn
       , address as address_line1
       , city
       , stateorprovince as state
       , postalcode as zip
       , homephonenumber as prim_phone
       , null as ssn4
       , id as study_id from aim4.quarter3_2017_person
;