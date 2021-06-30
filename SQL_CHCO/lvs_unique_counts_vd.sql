  with nid_sid as (
     select distinct ni.run_id, ni.network_id as nid, rp.study_id as sid
        from aim4.network_id ni join aim4.merged_source ms on ni.uid=ms.uid
        join tz.raw_person rp on ms.id::int = rp.study_id 
)
, sid_groups as (
    -- counts of studyids per NID, partion() needed to keep SID
        select run_id, nid, sid, count(sid) over (partition by run_id,nid) as cardinality
        from nid_sid
)    
    -- Filter out SIDs that do not participate in at least one record linakage (Count(SID>1) per NID)
, linked_sids as (
        select run_id, nid, sid, cardinality
        from sid_groups
        where cardinality > 1
) 
    -- All SIDs seen from runs <= {run_to_analyze}
    , linked_sids_run as (
        select :run_to_analyze as run_to_analyze, run_id, nid, sid, cardinality 
        from linked_sids
        where run_id <= :run_to_analyze
    )
    -- Last run for every SID to local last NID
    , lastrun as (
       select sid, max(run_id) as last_run
       from linked_sids_run
       group by sid 
    )
    -- Use the nid from the last run (based on technical note)
    , linked_sids_lastrun as (
       select lsr.run_to_analyze as run_to_analyze, lr.last_run, lsr.sid, lsr.nid as last_nid, cardinality 
       from linked_sids_run lsr join lastrun lr on (lsr.sid = lr.sid and lsr.run_id = lr.last_run)
    -- Add linkage vars using RAW_PERSON
    )
    , lvs_lastrun as (
        select distinct run_to_analyze, last_nid, study_id as sid
        ,first_name, last_name, gender, dob, ssn, address_line1 as address, city, state, zip, prim_phone as phone, ssn4
        from linked_sids_lastrun lsr join tz.raw_person rp on lsr.sid=rp.study_id
     )
    -- Data count by sid (unlinked)  This is a count of missingness
    , lvs_counts_by_sid as (
        select run_to_analyze, sid
            , count(distinct first_name) as fn_linkage_count
            , count(distinct last_name) as ln_linkage_count
            , count(distinct gender) as gender_linkage_count
            , count(distinct dob) as dob_linkage_count
            , count(distinct ssn) as ssn_linkage_count
            , count(distinct address) as address_linkage_count
            , count(distinct city) as city_linkage_count
            , count(distinct state) as state_linkage_count
            , count(distinct zip) as zip_linkage_count
            , count(distinct phone) as phone_linkage_count
            , count(distinct ssn4) as ssn4_linkage_count
        from lvs_lastrun
        group by run_to_analyze, sid
    )
    -- Data Counts by nid (linked)
    , lvs_counts_by_nid as (
        select run_to_analyze, last_nid
            , count(distinct first_name) as fn_linkage_count
            , count(distinct last_name) as ln_linkage_count
            , count(distinct gender) as gender_linkage_count
            , count(distinct dob) as dob_linkage_count
            , count(distinct ssn) as ssn_linkage_count
            , count(distinct address) as address_linkage_count
            , count(distinct city) as city_linkage_count
            , count(distinct state) as state_linkage_count
            , count(distinct zip) as zip_linkage_count
            , count(distinct phone) as phone_linkage_count
            , count(distinct ssn4) as ssn4_linkage_count
        from lvs_lastrun
        group by run_to_analyze, last_nid
    )
    , lvs_value_density as (
    select 'Unlinked' as type
        , run_to_analyze
        , round(avg(fn_linkage_count),2) as fn_vd
        , round(avg(ln_linkage_count),2) as ln_vd
        , round(avg(gender_linkage_count),2) as gender_vd
        , round(avg(dob_linkage_count),2) as dob_vd
        , round(avg(ssn_linkage_count),2) as ssn_vd
        , round(avg(address_linkage_count),2) as address_vd
        , round(avg(city_linkage_count),2) as city_vd
        , round(avg(state_linkage_count),2) as state_vd
        , round(avg(zip_linkage_count),2) as zip_vd
        , round(avg(phone_linkage_count),2) as phone_vd
        , round(avg(ssn4_linkage_count),2) as ssn4_vd
    from lvs_counts_by_sid 
    group by type, run_to_analyze
    union 
    select 'Linked' as type
        , run_to_analyze
        , round(avg(fn_linkage_count),2)
        , round(avg(ln_linkage_count),2)
        , round(avg(gender_linkage_count),2)
        , round(avg(dob_linkage_count),2)
        , round(avg(ssn_linkage_count),2)
        , round(avg(address_linkage_count),2)
        , round(avg(city_linkage_count),2)
        , round(avg(state_linkage_count),2)
        , round(avg(zip_linkage_count),2)
        , round(avg(phone_linkage_count),2)
        , round(avg(ssn4_linkage_count),2)
    from lvs_counts_by_nid 
    group by type, run_to_analyze
    )
    select type
          , fn_vd
          , ln_vd
          , gender_vd
          , dob_vd
          , ssn_vd
          , address_vd
          , city_vd
          , state_vd
          , zip_vd
          , phone_vd
          , ssn4_vd
    from lvs_value_density
    order by type desc;



