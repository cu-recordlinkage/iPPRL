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
    , clinvs_by_nid_rows as (
        select last_network_id
            , case when count(startdate)=0 then 0 else 1 end as startdt_by_nid_present_flag
            , case when count(enddate)=0 then 0 else 1 end as enddt_by_nid_present_flag
            , case when count(enc_type)=0 then 0 else 1 end as enc_type_by_nid_present_flag
            , case when count(prov_type)=0 then 0 else 1 end as prov_type_by_nid_present_flag
            , case when count(hgt) =0 then 0 else 1 end as hgt_by_nid_present_flag
            , case when count(wgt)=0 then 0 else 1 end as wgt_by_nid_present_flag
            , case when count(siteid)=0 then 0 else 1 end as siteid_by_nid_present_flag
        from clinvs_by_sid_lastrun
        group by last_network_id
    )
    , clinvs_by_nid_counts as (
       select 'linked' as type,
           count(last_network_id) as denominator
           , sum(startdt_by_nid_present_flag) as n_startdt
           , sum(enddt_by_nid_present_flag) as n_enddt
           , sum(enc_type_by_nid_present_flag) as n_enc_type
           , sum(prov_type_by_nid_present_flag) as n_prov_type
           , sum(hgt_by_nid_present_flag) as n_hgt
           , sum(wgt_by_nid_present_flag) as n_wgt
           , sum(siteid_by_nid_present_flag) as n_siteid
           from clinvs_by_nid_rows
    )
    , clinvs_by_sid_counts as (
        select 'unlinked' as type
           , count(uid) as denominator
           , count(startdate) as n_startdt
           , count(enddate) as n_enddt
           , count(enc_type) as n_enc_type
           , count(prov_type) as n_prov_type
           , count(hgt) as n_hgt
           , count(wgt) as n_wgt
           , count(siteid) as n_siteid
    from clinvs_by_sid_lastrun
    )
    , clinvcounts as (
        select type, denominator, n_startdt, n_enddt, n_enc_type, n_prov_type, n_hgt, n_wgt, n_siteid 
        from clinvs_by_sid_counts
        union all
        select type, denominator, n_startdt, n_enddt, n_enc_type, n_prov_type, n_hgt, n_wgt, n_siteid 
        from clinvs_by_nid_counts
    )
    select type, denominator, n_startdt, n_enddt, n_enc_type, n_prov_type, n_hgt, n_wgt, n_siteid 
    from clinvcounts;
    