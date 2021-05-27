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
-- Add linkage vars to UIDs in this run
, linkage_vars_by_uid as (
	select distinct run_to_analyze, last_network_id, ms.uid as uid, mrn, d_source, first_name, last_name,sex, dob,ssn,address, city, state,zip, phone,ssn4
	from linked_uids_lastrun lur join aim4.merged_source ms on lur.uid = ms.uid
	join :cummulative_to_use c on (ms.id = c.study_id)
)
-- Data count by uid (unlinked)  This is a count of missingness
, linkage_vars_counts_by_uid as (
	select run_to_analyze, uid
		, count(first_name) as fn_linkage_count
		, count(last_name) as ln_linkage_count
		, count(sex) as sex_linkage_count
		, count(dob) as dob_linkage_count
		, count(ssn) as ssn_linkage_count
		, count(address) as address_linkage_count
		, count(city) as city_linkage_count
		, count(state) as state_linkage_count
		, count(zip) as zip_linkage_count
		, count(phone) as phone_linkage_count
		, count(ssn4) as ssn4_linkage_count
	from linkage_vars_by_uid
	group by run_to_analyze, uid
)
-- Data Counts by nid (linked)
, linkage_vars_counts_by_nid as (
	select run_to_analyze, last_network_id
		, count(first_name) as fn_linkage_count
		, count(last_name) as ln_linkage_count
		, count(sex) as sex_linkage_count
		, count(dob) as dob_linkage_count
		, count(ssn) as ssn_linkage_count
		, count(address) as address_linkage_count
		, count(city) as city_linkage_count
		, count(state) as state_linkage_count
		, count(zip) as zip_linkage_count
		, count(phone) as phone_linkage_count
		, count(ssn4) as ssn4_linkage_count
	from linkage_vars_by_uid
	group by run_to_analyze, last_network_id
)
-- Data Density (dd)
, data_density as (
select 'Unlinked' as type
	, run_to_analyze
	, round(avg(fn_linkage_count),2) as fn_dd
    , round(avg(ln_linkage_count),2) as ln_dd
	, round(avg(sex_linkage_count),2) as sex_dd
	, round(avg(dob_linkage_count),2) as dob_dd
	, round(avg(ssn_linkage_count),2) as ssn_dd
	, round(avg(address_linkage_count),2) as address_dd
	, round(avg(city_linkage_count),2) as city_dd
	, round(avg(state_linkage_count),2) as state_dd
	, round(avg(zip_linkage_count),2) as zip_dd
	, round(avg(phone_linkage_count),2) as phone_dd
	, round(avg(ssn4_linkage_count),2) as ssn4_dd
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
      , fn_dd
      , ln_dd
      , sex_dd
      , dob_dd
      , ssn_dd
      , address_dd
      , city_dd
      , state_dd
      , zip_dd
      , phone_dd
      , ssn4_dd
from data_density;


