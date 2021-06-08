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
    -- Link UIDs to ClinVs
    , clinvs_by_sid_lastrun as (
        select distinct run_to_analyze, last_network_id, ms.uid as uid, study_id, startdate, enddate, encountertype as enc_type
            , providertype as prov_type, heightinches as hgt, weightpounds as wgt, siteid
        from linked_uids_lastrun lur join aim4.merged_source ms on (lur.uid = ms.uid)
        join :cumulative_to_use c on (ms.id = c.study_id)
    )
    , clinvs_by_nid_counts as (
        select run_to_analyze, last_network_id
            , count(last_network_id) as n_denom
            , count(startdate) as n_startdt
            , count(enddate) as n_enddt
            , count(enc_type) as n_enc_type
            , count(prov_type) as n_prov_type
            , count(hgt) as n_hgt
            , count(wgt) as n_wgt
            , count(siteid) as n_siteid
        from clinvs_by_sid_lastrun
        group by run_to_analyze,last_network_id
    )
    , clinvs_by_uid_counts as (
    select run_to_analyze, uid
           , count(uid) as n_denom
           , count(startdate) as n_startdt
           , count(enddate) as n_enddt
           , count(enc_type) as n_enc_type
           , count(prov_type) as n_prov_type
           , count(hgt) as n_hgt
           , count(wgt) as n_wgt
           , count(siteid) as n_siteid
    from clinvs_by_sid_lastrun
    group by run_to_analyze,uid
) 
, clinv_data_density as (
select 'Unlinked' as type
	, run_to_analyze
	, round(avg(n_denom),2) as denom_dd
	, round(avg(n_startdt),2) as startdt_dd
    , round(avg(n_enddt),2) as enddt_dd
	, round(avg(n_enc_type),2) as enc_type_dd
	, round(avg(n_prov_type),2) as prov_type_dd
	, round(avg(n_hgt),2) as hgt_dd
	, round(avg(n_wgt),2) as wgt_dd
	, round(avg(n_siteid),2) as siteid_dd
from clinvs_by_uid_counts
group by type, run_to_analyze
union 
select 'Linked' as type
	, run_to_analyze
	, round(avg(n_denom),2)
	, round(avg(n_startdt),2)
    , round(avg(n_enddt),2)
	, round(avg(n_enc_type),2)
	, round(avg(n_prov_type),2)
	, round(avg(n_hgt),2)
	, round(avg(n_wgt),2)
	, round(avg(n_siteid),2)
from clinvs_by_nid_counts
group by type, run_to_analyze
)
select type
      , run_to_analyze
      , startdt_dd
      , enddt_dd
      , enc_type_dd
      , prov_type_dd
      , hgt_dd
      , wgt_dd
from clinv_data_density;
