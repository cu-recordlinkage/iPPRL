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
    -- Use the network_id from the last run (based on technical note)
    , linked_uids_lastrun as (
       select lur.run_to_analyze as run_to_analyze, last_run, lur.uid, lur.network_id as last_network_id, cardinality 
       from linked_uids_run lur join lastrun lr on (lur.uid = lr.uid and lur.run_id = lr.last_run)
    -- Add linkage and clinical vars to UIDs in this run using run cummulative data set
    )
    , lvs_lastrun as (
        select distinct run_to_analyze, last_network_id, lur.uid, study_id
        ,first_name, last_name, gender, dob, ssn, address_line1 as address, city, state, zip, prim_phone as phone, ssn4
        from linked_uids_lastrun lur join aim4.merged_source ms on (lur.uid=ms.uid)
        join aim4.raw_person rp on ms.id=rp.study_id
     )
    , lvs_by_nid_counts as (
        select run_to_analyze, last_network_id
            , count(last_network_id) as n_denom
            , count(first_name) as n_fn
            , count(last_name) as n_ln
            , count(gender) as n_gender
            , count(dob) as n_dob
            , count(ssn) as n_ssn
            , count(address) as n_address
            , count(city) as n_city
            , count(state) as n_state
            , count(zip) as n_zip
            , count(phone) as n_phone
            , count(ssn4) as n_ssn4
        from lvs_lastrun
        group by run_to_analyze,last_network_id
    )
    , lvs_by_uid_counts as (
    select run_to_analyze, uid
           , count(uid) as n_denom
           , count(first_name) as n_fn
           , count(last_name) as n_ln
           , count(gender) as n_gender
           , count(dob) as n_dob
           , count(ssn) as n_ssn
           , count(address) as n_address
           , count(city) as n_city
           , count(state) as n_state
           , count(zip) as n_zip
           , count(phone) as n_phone
           , count(ssn4) as n_ssn4
    from lvs_lastrun
    group by run_to_analyze,uid
) 
, lv_data_density as (
select 'Unlinked' as type
	, run_to_analyze
	, round(avg(n_denom),2) as denom_dd
	, round(avg(n_fn),2) as fn_dd
    , round(avg(n_ln),2) as ln_dd
	, round(avg(n_gender),2) as sex_dd
	, round(avg(n_dob),2) as dob_dd
	, round(avg(n_ssn),2) as ssn_dd
	, round(avg(n_address),2) as address_dd
	, round(avg(n_city),2) as city_dd
	, round(avg(n_state),2) as state_dd
	, round(avg(n_zip),2) as zip_dd
	, round(avg(n_phone),2) as phone_dd
	, round(avg(n_ssn4),2) as ssn4_dd
from lvs_by_uid_counts
group by type, run_to_analyze
union 
select 'Linked' as type
	, run_to_analyze
	, round(avg(n_denom),2)
	, round(avg(n_fn),2)
    , round(avg(n_ln),2)
	, round(avg(n_gender),2)
	, round(avg(n_dob),2)
	, round(avg(n_ssn),2)
	, round(avg(n_address),2)
	, round(avg(n_city),2)
	, round(avg(n_state),2)
	, round(avg(n_zip),2)
	, round(avg(n_phone),2)
	, round(avg(n_ssn4),2)
from lvs_by_nid_counts
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
from lv_data_density;



)
