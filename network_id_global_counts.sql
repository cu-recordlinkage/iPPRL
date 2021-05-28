with uid_groups as (
-- counts of UIDS per NID, partion() needed to keep UID
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as cardinality
	from aim4.network_id )
-- Filter out UIDs that do not participate in at least one record linakage (Count(uid>1) per NID)
, linked_uids as (
    select run_id, network_id, uid, cardinality
    from uid_groups
    where cardinality > 1
)
-- All UIDs seen from runs <= :run_to_analyze
, linked_uids_run as (
    select :run_to_analyze as run_to_analyze, run_id, network_id, uid, cardinality 
    from linked_uids
    where run_id <= :run_to_analyze
)
-- Last run for every UID to local last NID
, lastrun as (
   select uid, max(run_id) as last_run
   from linked_uids_run
   group by uid
)
-- Use the network_id from the last run (based on conversation with Toan about using last NID NID)
, linked_uids_lastrun as (
   select lur.run_to_analyze as run_to_analyze, last_run, lur.uid, lur.network_id as last_network_id, cardinality 
   from linked_uids_run lur join lastrun lr on (lur.uid = lr.uid and lur.run_id = lr.last_run)
)
-- add linkage vars to UIDs in this run
, linkage_vars_by_uid as (
	select distinct run_to_analyze, last_network_id, ms.uid as uid, mrn, d_source, first_name, last_name,sex, dob,ssn,address, city, state,zip, phone,ssn4
	from linked_uids_lastrun lur join aim4.merged_source ms on lur.uid = ms.uid
	join :cummulative_to_use c on (ms.id = c.study_id)
)
-- Data count by uid (unlinked)  This is a count of missingness
-- Distinct won't do anything here because either singleton or NULL at UID level but included for symmetry
, linkage_vars_counts_by_uid as (
	select run_to_analyze, uid
		, count(distinct first_name) as fn_linkage_count
		, count(distinct last_name) as ln_linkage_count
		, count(distinct sex) as sex_linkage_count
		, count(distinct dob) as dob_linkage_count
		, count(distinct ssn) as ssn_linkage_count
		, count(distinct address) as address_linkage_count
		, count(distinct city) as city_linkage_count
		, count(distinct state) as state_linkage_count
		, count(distinct zip) as zip_linkage_count
		, count(distinct phone) as phone_linkage_count
		, count(distinct ssn4) as ssn4_linkage_count
	from linkage_vars_by_uid
	group by run_to_analyze, uid
)
-- Data Counts by nid (linked)
-- Distinct tells us how many different vaules we are seeing with linkage
, linkage_vars_counts_by_nid as (
	select run_to_analyze, last_network_id
		, count(distinct first_name) as fn_linkage_count
		, count(distinct last_name) as ln_linkage_count
		, count(distinct sex) as sex_linkage_count
		, count(distinct dob) as dob_linkage_count
		, count(distinct ssn) as ssn_linkage_count
		, count(distinct address) as address_linkage_count
		, count(distinct city) as city_linkage_count
		, count(distinct state) as state_linkage_count
		, count(distinct zip) as zip_linkage_count
		, count(distinct phone) as phone_linkage_count
		, count(distinct ssn4) as ssn4_linkage_count
	from linkage_vars_by_uid
	group by run_to_analyze, last_network_id
)
-- Value Density (vd)
, value_density as (
select 'Unlinked' as type
	, run_to_analyze
	, round(avg(fn_linkage_count),2) as fn_vd
    , round(avg(ln_linkage_count),2) as ln_vd
	, round(avg(sex_linkage_count),2) as sex_vd
	, round(avg(dob_linkage_count),2) as dob_vd
	, round(avg(ssn_linkage_count),2) as ssn_vd
	, round(avg(address_linkage_count),2) as address_vd
	, round(avg(city_linkage_count),2) as city_vd
	, round(avg(state_linkage_count),2) as state_vd
	, round(avg(zip_linkage_count),2) as zip_vd
	, round(avg(phone_linkage_count),2) as phone_vd
	, round(avg(ssn4_linkage_count),2) as ssn4_vd
from linkage_vars_counts_by_uid 
group by type, run_to_analyze
union 
select 'Linked' as type
	, run_to_analyze
	, round(avg(fn_linkage_count),2)
    , round(avg(ln_linkage_count),2)
	, round(avg(sex_linkage_count),2)
	, round(avg(dob_linkage_count),2)
	, round(avg(ssn_linkage_count),2)
	, round(avg(address_linkage_count),2)
	, round(avg(city_linkage_count),2)
	, round(avg(state_linkage_count),2)
	, round(avg(zip_linkage_count),2)
	, round(avg(phone_linkage_count),2)
	, round(avg(ssn4_linkage_count),2)
from linkage_vars_counts_by_nid 
group by type, run_to_analyze
)
select type
      , run_to_analyze
      , fn_vd
      , ln_vd
      , sex_vd
      , dob_vd
      , ssn_vd
      , address_vd
      , city_vd
      , state_vd
      , zip_vd
      , phone_vd
      , ssn4_vd
from value_density;


