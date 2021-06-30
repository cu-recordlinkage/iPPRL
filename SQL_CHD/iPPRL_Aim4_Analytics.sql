/* Move the desired network_id (record linkage results) table into AIM4 schema. 
   All other scripts work using AIM4 schema

METHOD     Job_ID Schema
CTRL		job_21095
iCTRL		job_20733
iPPRL		job_22092
PPRL		job_22563

Change the "FROM" clause below 
Update metadata values
*/


DROP TABLE IF EXISTS aim4.network_id;
CREATE TABLE IF NOT EXISTS aim4.network_id AS SELECT * FROM job_22092.network_id;
DROP TABLE IF EXISTS aim4.merged_source;
CREATE TABLE IF NOT EXISTS aim4.merged_source AS SELECT * from job_22092.merged_source;
DROP TABLE IF EXISTS aim4.metadata;
CREATE TABLE IF NOT EXISTS aim4.metadata as
  SELECT 'network_id.schema' as Attribute, 'job_22092' as Val
  UNION ALL
  SELECT 'RL method','iPPRL'
;


/* for each network_id in each run_id, how many uids are linked to that network_id
Counts only
*/

with uid_groups as (
	select run_id, network_id, count(uid) as n_uid
	from aim4.network_id
	group by run_id, network_id
)
select * from uid_groups
order by run_id asc, n_uid

/* Previous query but modified to include the uids in each group
*/
with uid_groups as (
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as n_uids
	from aim4.network_id
)
select * from uid_groups
order by run_id asc, network_id asc, n_uids asc


/* Previous query but only those with at least one link = n_uid > 1
*/
with uid_groups as (
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as n_uids
	from aim4.network_id
)
select * from uid_groups
where n_uids > 1
order by run_id asc, network_id asc, n_uids asc

/* Using previous query, aggregate by the number of network_ids in each #uids for each run
*/

with uid_groups as (
	select run_id, network_id, count(uid) as n_uids
	from aim4.network_id
	group by run_id, network_id
),
nid_groups as (
	select run_id, n_uids, count(n_uids) as group_count
	, n_uids * count(n_uids) as group_size
	, sum(n_uids*count(n_uids)) over (partition by run_id) as run_size
	from uid_groups
	group by run_id,n_uids
)
select * from nid_groups
order by run_id asc, n_uids asc


/* Network_IDs that have at least one link (count uids > 1) separated by run_id
*/

select network_id
from aim4.network_id
group by network_id
having count(uid) > 1



/* parms: which run_ids to use and how many uids within selected nid
   uid_cohort: nids specified in run_ids grouped by the number of uids in them
   nid_cohort: nids that have the desired number of uids specified in uid_num
*/

/* Example Parms: uids from run 1 only. nids with 4 uids
   Uncomment incldue nids with 3 uids.
*/

with 
parms as (
  select 1 as run_id, 4 as uid_num
--   union all
-- select null as run_id, 3 as uid_num
),
uid_cohort as (
	select network_id, count(distinct uid) as n_uid
	from aim4.network_id
	where run_id in (select run_id from parms)
	group by network_id
),
nid_cohort as (
select network_id, n_uid
from uid_cohort
group by network_id, n_uid
having n_uid in (select uid_num from parms)
)
select * from nid_cohort


-- Look at the clear text linkage variables in raw_person for one of the network_id with four UIDs.
-- To get back to raw_person: 
--        Link network_id with merged_source using uid and then join with raw_person using study_id=id
-- Keeps NID
select nid.network_id, rp.*
from job_13482.merged_source ms join job_13482.network_id nid on ms.uid=nid.uid join tz.raw_person rp on (rp.study_id = ms.id::int)
where nid.network_id = 993370601



-- Better version to note if LVs are different: IGNORES NULLs
-- If ALL NULL --> unknown
with test_cases as
( select 993370435 as test_network_id UNION ALL select 993370436 UNION ALL select 993370437 UNION ALL select 993370438
)
, within_run_id as
(select nid.run_id,nid.network_id, count(nid.uid) as n_uids
    , case when count (distinct rp.first_name) = 1 then 'same' when count (distinct rp.first_name) = 0 then 'unknown' else 'different' end as firstnameFlag
    , case when count (distinct rp.last_name) = 1 then 'same' when count(distinct rp.last_name) = 0 then 'unknown' else 'different' end as lastnameFlag
    , case when count (distinct rp.middle_initial) = 1 then 'same' when count(distinct rp.middle_initial) = 0 then 'unknown' else 'different' end as middleinitialFlag
	, case when count (distinct rp.gender) = 1 then 'same' when count(distinct rp.gender) =0 then 'unknown' else 'different' end as genderFlag
    , case when count (distinct rp.dob) = 1 then 'same' when count(distinct rp.dob) = 0 then 'unknown' else 'different' end as dobFlag
    , case when count (distinct rp.address_line1) = 1 then 'same' when count(distinct rp.address_line1) = 0 then 'unknown' else 'different' end as addressFlag
    , case when count (distinct rp.zip) = 1 then 'same' when count(distinct rp.zip) = 0 then 'unknown' else 'different' end as zipFlag
from
    aim4.network_id nid join aim4.merged_source ms on (nid.uid = ms.uid)
    join tz.raw_person rp on (ms.id=rp.study_id)
where network_id in (select test_network_id from test_cases)
group by 
    nid.run_id,nid.network_id
order by
    nid.run_id,nid.network_id
),
adjacent_runs as (
select r1.network_id, r1.run_id as run1, r1.n_uids as r1_nuids
	   , r2.run_id as run2, r2.n_uids as r2_nuids
       , r1.firstnameflag as r1_fn, r2.firstnameflag as r2_fn
	   , r1.lastnameflag as r1_ln, r2.lastnameflag as r2_ln
	   , r1.middleinitialflag as r1_mi, r2.middleinitialflag as r2_mi
	   , r1.genderflag as r1_gender, r2.genderflag as r2_gender
	   , r1.dobflag as r1_dob, r2.dobflag as r2_dob
	   , r1.addressflag as r1_address, r2.addressflag as r2_address
	   , r1.zipflag as r1_zip, r2.zipflag as r2_zip
from within_run_id r1 left join within_run_id r2 on  (r1.network_id = r2.network_id and r2.run_id = r1.run_id + 1)
where r1.run_id < 17
order by network_id asc, run1 asc, run2 asc
),
across_runs as (
select network_id, run1, r1_nuids,run2, r2_nuids
	, case when r1_fn != r2_fn then 'CHANGED' else 'no change' END as fn
	, case when r1_ln != r2_ln then 'CHANGED' else 'no change' END as ln
	, case when r1_mi != r2_ln then 'CHANGED' else 'no change' END as mi
	, case when r1_gender != r2_gender then 'CHANGED' else 'no change' END as gender
	, case when r1_dob != r2_dob then 'CHANGED' else 'no change' END as dob
	, case when r1_address != r2_address then 'CHANGED' else 'no change' END as address
	, case when r1_zip != r2_zip then 'CHANGED' else 'no change' END as zip
from adjacent_runs
)
select * from adjacent_runs
--select * from across_runs




/* Returns current and next_run_id for each network_id
   Returns null with last run_id
   
*/

with tmp_networkid as
( select network_id, run_id, rank() over (partition by network_id order by run_id) as run_order
from (select distinct network_id, run_id from aim4.network_id) a
)
select a.network_id, a.run_id, b.run_id as next_run_id
from tmp_networkid a  left join tmp_networkid b on a.network_id = b.network_id
and a.run_order = b.run_order-1
order by a.network_id asc, run_id asc;



/*************************************************************************************************

See May 3, 2021 notes

*****************************************************************************************************/

/* COMPLETENESS */
/* At least one value. See slides for definition linked versus unlinked */

/* Cohort for all DQ measures are UIDs that are linked == network_ids with count(uids) > 1 */


/*********************************************************************
/* Test code for determining unlinked versus linked completeness
**********************************************************************/

with cteDummy as (
    select
        1 as network_id, 100 as uid, 'a' as Lv1, null as Lv2, 'a' as Lv3
    union all
    select 1, 200, 'a', null, 'b'
    union all
    select 1, 300, 'a',null, 'c'
    union all
    select 1, 400, null,null,'d'
    union all
    select 2, 500, null,null,null
    union all
    select 2, 600, 'z',null, null
    union all
    select 3, 700, null, null, 'z'
    union all
    select 3, 800, null , 'b' , null
)
-- Unlinked counts
(select 'Unlinked', count(uid) as denom, count(lv1),count(lv2), count(lv3)
from cteDummy)
union all
(select 'By network_id', network_id, case when count(lv1)=0 then 0 else 1 end 
                  , case when count(lv2) = 0 then 0 else 1 end a
 				, case when count(lv3)=0 then 0 else 1 end 
from cteDummy 
group by network_id
order by network_id asc)
union all
-- Linked counts
(select 'Linked', count(network_id) as denom, sum(lv1_flag), sum(lv2_flag), sum(lv3_flag)
	from (select network_id, case when count(lv1)=0 then 0 else 1 end as lv1_flag
                  , case when count(lv2) = 0 then 0 else 1 end as lv2_flag
 				, case when count(lv3)=0 then 0 else 1 end as lv3_flag
		  from cteDummy
		  group by network_id) zzz
)
;


/* cohort <run_id, network_id, uid> for Run 1 only */
with uid_groups as (
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as n_uids
	from aim4.network_id )
, linked_uids as (
	select run_id, network_id, uid, n_uids from uid_groups
	where n_uids > 1  -- must have at least one link to be in linked group
)
, linked_uids_run1 as (
	select run_id, network_id, uid, n_uids from linked_uids
	where run_id = 1 -- focus on run 1 linked group
)
select distinct(uid) from linked_uids_run1




/* For linked run 1, pull encounter level data
*/

with uid_groups as (
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as n_uids
	from aim4.network_id )
, linked_uid_groups as (
	select run_id, network_id, uid, n_uids from uid_groups
	where n_uids > 1  -- must have at least one link to be in linked group
)
, linked_uid_groups_run1 as (
	select run_id, network_id, uid, n_uids from linked_uid_groups
	where run_id = 1 -- focus on run 1 linked group
)
select lr1.*,c.*
from linked_uid_groups_run1 lr1 join aim4.merged_source ms on lr1.uid = ms.uid
     join aim4.year_2011_chd_overall_cummulative c on (ms.id = c.study_id)
order by network_id asc, uid asc




/* For linked run 1, count linkage vars as unlinked rows
*/
with uid_groups as (
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as n_uids
	from aim4.network_id )
, linked_uid_groups as (
	select run_id, network_id, uid, n_uids from uid_groups
	where n_uids > 1  -- must have at least one link to be in linked group
)
, linked_uid_groups_run1 as (
	select run_id, network_id, uid, n_uids from linked_uid_groups
	where run_id = 1 -- focus on run 1 linked group
)
, linkage_vars as (
	select distinct network_id, ms.uid, mrn, d_source, first_name, last_name,sex, dob,ssn,address, city, state,zip, phone,ssn4
	from linked_uid_groups_run1 lr1 join aim4.merged_source ms on lr1.uid = ms.uid
	join aim4.year_2011_chd_overall_cummulative c on (ms.id = c.study_id)
)
select count(uid) as unlinked_denom
       , count(first_name) as unlinked_fn
	   , count(last_name) as unlinked_ln
	   , count(sex) as unlinked_ln
	   , count(dob) as unlinked_dob
	   , count(ssn) as unlinked_ssn
	   , count(address) as unlinked_address
	   , count(city) as unlinked_city
	   , count(state) as unlinked_state
	   , count(zip) as unlinked_zip
	   , count(phone) as unlinked_phone
	   , count(ssn4) as unlinked_ssn4
from linkage_vars



with uid_groups as (
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as n_uids
	from aim4.network_id )
, linked_uid_groups as (
	select run_id, network_id, uid, n_uids from uid_groups
	where n_uids > 1  -- must have at least one link to be in linked group
)
, linked_uid_groups_run1 as (
	select run_id, network_id, uid, n_uids from linked_uid_groups
	where run_id = 1 -- focus on run 1 linked group
)
, linkage_vars as (
	select distinct network_id, ms.uid as uid, mrn, d_source, first_name, last_name,sex, dob,ssn,address, city, state,zip, phone,ssn4
	from linked_uid_groups_run1 lr1 join aim4.merged_source ms on lr1.uid = ms.uid
	join aim4.year_2011_chd_overall_cummulative c on (ms.id = c.study_id)
)
, linkage_vars_by_nid as (
	select network_id
		, case when count(first_name)=0 then 0 else 1 end as fn_linkage_val
		, case when count(last_name)=0 then 0 else 1 end as ln_linkage_val
		, case when count(sex)=0 then 0 else 1 end as sex_linkage_val
		, case when count(dob)=0 then 0 else 1 end as dob_linkage_val
		, case when count(ssn) =0 then 0 else 1 end as ssn_linkage_val
		, case when count(address)=0 then 0 else 1 end as address_linkage_val
		, case when count(city)=0 then 0 else 1 end as city_linkage_val
		, case when count(state)=0 then 0 else 1 end as state_linkage_val
		, case when count(zip)= 0 then 0 else 1 end as zip_linkage_val
		, case when count(phone)=0 then 0 else 1 end as phone_linkage_val
		, case when count(ssn4)=0 then 0 else 1 end as ssn4_linkage_val
	from linkage_vars
	group by network_id
)
select 'unlinked' as type
	   , count(uid) as denominator
       , count(first_name) as n_fn
	   , count(last_name) as n_ln
	   , count(sex) as n_sex
	   , count(dob) as n_dob
	   , count(ssn) as n_ssn
	   , count(address) as n_address
	   , count(city) as n_city
	   , count(state) as n_state
	   , count(zip) as n_zip
	   , count(phone) as n_phone
	   , count(ssn4) as n_ssn4
from linkage_vars
union all
select 'linked'
	   , count(network_id)
	   , sum(fn_linkage_val)
       , sum(ln_linkage_val)
       , sum(sex_linkage_val)
       , sum(dob_linkage_val)
       , sum(ssn_linkage_val)
	   , sum(address_linkage_val)
	   , sum(city_linkage_val)
	   , sum(state_linkage_val)
	   , sum(zip_linkage_val)
	   , sum(phone_linkage_val)
	   , sum(ssn4_linkage_val)
from linkage_vars_by_nid
;




/* 
Calculated completeness on unlinked and linked results for Run1
Calculates both counts and percentage present in each linkage variable
*/

with uid_groups as (
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as n_uids
	from aim4.network_id )
, linked_uid_groups as (
	select run_id, network_id, uid, n_uids from uid_groups
	where n_uids > 1  -- must have at least one link to be in linked group
)
, linked_uid_groups_run1 as (
	select run_id, network_id, uid, n_uids from linked_uid_groups
	where run_id = 1 -- focus on run 1 linked group
)
, linkage_vars as (
	select distinct network_id, ms.uid as uid, mrn, d_source, first_name, last_name,sex, dob,ssn,address, city, state,zip, phone,ssn4
	from linked_uid_groups_run1 lr1 join aim4.merged_source ms on lr1.uid = ms.uid
	join aim4.year_2011_chd_overall_cummulative c on (ms.id = c.study_id)
)
, linkage_vars_by_nid as (
	select network_id
		, case when count(first_name)=0 then 0 else 1 end as fn_linkage_val
		, case when count(last_name)=0 then 0 else 1 end as ln_linkage_val
		, case when count(sex)=0 then 0 else 1 end as sex_linkage_val
		, case when count(dob)=0 then 0 else 1 end as dob_linkage_val
		, case when count(ssn) =0 then 0 else 1 end as ssn_linkage_val
		, case when count(address)=0 then 0 else 1 end as address_linkage_val
		, case when count(city)=0 then 0 else 1 end as city_linkage_val
		, case when count(state)=0 then 0 else 1 end as state_linkage_val
		, case when count(zip)= 0 then 0 else 1 end as zip_linkage_val
		, case when count(phone)=0 then 0 else 1 end as phone_linkage_val
		, case when count(ssn4)=0 then 0 else 1 end as ssn4_linkage_val
	from linkage_vars
	group by network_id
), 
lv_counts as (
	select 'unlinked' as type
	   , count(uid) as denominator
       , count(first_name) as n_fn
	   , count(last_name) as n_ln
	   , count(sex) as n_sex
	   , count(dob) as n_dob
	   , count(ssn) as n_ssn
	   , count(address) as n_address
	   , count(city) as n_city
	   , count(state) as n_state
	   , count(zip) as n_zip
	   , count(phone) as n_phone
	   , count(ssn4) as n_ssn4
	from linkage_vars
	union all
	select 'linked'
	   , count(network_id)
	   , sum(fn_linkage_val)
       , sum(ln_linkage_val)
       , sum(sex_linkage_val)
       , sum(dob_linkage_val)
       , sum(ssn_linkage_val)
	   , sum(address_linkage_val)
	   , sum(city_linkage_val)
	   , sum(state_linkage_val)
	   , sum(zip_linkage_val)
	   , sum(phone_linkage_val)
	   , sum(ssn4_linkage_val)
	from linkage_vars_by_nid 
),
lv_percents as (
	select cast(n_fn*100.0/denominator as numeric(7,1)) as fn_complete
       , cast(n_ln*100.0/denominator as numeric(7,1)) as ln_complete
	   , cast(n_sex*100.0/denominator as numeric(7,1)) as sex_complete
	   , cast(n_dob*100.0/denominator as numeric(7,1)) as dob_complete
	   , cast(n_ssn*100.0/denominator as numeric(7,1)) as ssn_complete
	   , cast(n_address*100.0/denominator as numeric(7,1)) as address_complete
	   , cast(n_city*100.0/denominator as numeric(7,1)) as city_complete
	   , cast(n_state*100.0/denominator as numeric(7,1)) as state_complete
	   , cast(n_zip*100.0/denominator as numeric(7,1)) as zip_complete
	   , cast(n_phone*100.0/denominator as numeric(7,1)) as phone_complete
	   , cast(n_ssn4*100.0/denominator as numeric(7,1)) as ssn4_complete
	from lv_counts
)
select * from lv_percents;




/* Observation periods by UID (unlinked) and NID (linked)
*/

with uid_groups as (
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as n_uids
	from aim4.network_id )
, linked_uid_groups as (
	select run_id, network_id, uid, n_uids from uid_groups
	where n_uids > 1  -- must have at least one link to be in linked group
)
, linked_uid_groups_run1 as (
	select run_id, network_id, uid, n_uids from linked_uid_groups
	where run_id = 1 -- focus on run 1 linked group
)
, linked_clinical_run1 as (
	select lr1.*,c.*
	from linked_uid_groups_run1 lr1 join aim4.merged_source ms on lr1.uid = ms.uid
	join aim4.year_2011_chd_overall_cummulative c on (ms.id = c.study_id)
	order by network_id asc, uid asc
)
, obs_period_mrn_uid as (
	select uid, min(startdate) as min_startdate, max(enddate) as max_enddate, max(enddate)-min(startdate) as obs_days
	from linked_clinical_run1
	group by uid)
, obs_period_mrn_nid as (
	select network_id, min(startdate) as min_startdate, max(enddate) as max_enddate, max(enddate)-min(startdate) as obs_days
	from linked_clinical_run1
	where mrn is not null
	group by network_id
)
select 'unlinked' as type
	, min(min_startdate)
	, max(max_enddate)
	, min(obs_days) as min_obs_days
	, max(obs_days) as max_obs_days
	, avg(max_enddate-min_startdate)::numeric(7,1) as avg_obs_days
		  from obs_period_mrn_uid
UNION ALL
select 'linked'
	, min(min_startdate)
		  , max(max_enddate)
		  , min(obs_days) as min_obs_days
		  , max(obs_days) as max_obs_days
		  , avg(max_enddate-min_startdate)::numeric(7,1)
	  from obs_period_mrn_nid



/* Distribution of unlinked obs_days
*/

with uid_groups as (
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as n_uids
	from aim4.network_id )
, linked_uid_groups as (
	select run_id, network_id, uid, n_uids from uid_groups
	where n_uids > 1  -- must have at least one link to be in linked group
)
, linked_uid_groups_run1 as (
	select run_id, network_id, uid, n_uids from linked_uid_groups
	where run_id = 1 -- focus on run 1 linked group
)
, linked_clinical_run1 as (
	select lr1.*,c.*
	from linked_uid_groups_run1 lr1 join aim4.merged_source ms on lr1.uid = ms.uid
	join aim4.year_2011_chd_overall_cummulative c on (ms.id = c.study_id)
	order by network_id asc, uid asc
)
, obs_period_mrn_uid as (
	select uid, min(startdate) as min_startdate, max(enddate) as max_enddate, max(enddate)-min(startdate) as obs_days
	from linked_clinical_run1
	group by uid)
, obs_period_mrn_nid as (
	select network_id, min(startdate) as min_startdate, max(enddate) as max_enddate, max(enddate)-min(startdate) as obs_days
	from linked_clinical_run1
	where mrn is not null
	group by network_id
)
, obs_period_mrn_stats as (
	select 'unlinked' as type
	, min(min_startdate)
	, max(max_enddate)
	, min(obs_days) as min_obs_days
	, max(obs_days) as max_obs_days
	, avg(max_enddate-min_startdate)::numeric(7,1) as avg_obs_days
		  from obs_period_mrn_uid
UNION ALL
select 'linked'
	, min(min_startdate)
		  , max(max_enddate)
		  , min(obs_days) as min_obs_days
		  , max(obs_days) as max_obs_days
		  , avg(max_enddate-min_startdate)::numeric(7,1)
	  from obs_period_mrn_nid 
)
select * FROM
(select 'Unlinked' as type
	, obs_days, count(uid)
	from obs_period_mrn_uid  -- THIS IS UNLINKED data
	group by obs_days
UNION ALL
select 'Linked'
	, obs_days, count(network_id)
	from obs_period_mrn_nid -- THIS IS LINKED data
	group by obs_days) a
Order by type asc, obs_days asc