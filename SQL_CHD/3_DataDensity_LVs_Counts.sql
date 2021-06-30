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
    -- All SIDs seen from runs <= {run_to_analyze}
    , linked_sids_run as (
        select :run_to_analyze as run_to_analyze, run_id, nid, sid, cardinality 
        from sid_groups
        where CARDINALITY > 1 AND run_id <= :run_to_analyze
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
            , count(first_name) as fn_linkage_count
            , count(last_name) as ln_linkage_count
            , count(gender) as gender_linkage_count
            , count(dob) as dob_linkage_count
            , count(ssn) as ssn_linkage_count
            , count(address) as address_linkage_count
            , count(city) as city_linkage_count
            , count(state) as state_linkage_count
            , count(zip) as zip_linkage_count
            , count(phone) as phone_linkage_count
            , count(ssn4) as ssn4_linkage_count
        from lvs_lastrun
        group by run_to_analyze, sid
    )
    -- Data Counts by nid (linked)
    , lvs_counts_by_nid as (
        select run_to_analyze, last_nid
            , count(first_name) as fn_linkage_count
            , count(last_name) as ln_linkage_count
            , count(gender) as gender_linkage_count
            , count(dob) as dob_linkage_count
            , count(ssn) as ssn_linkage_count
            , count(address) as address_linkage_count
            , count(city) as city_linkage_count
            , count(state) as state_linkage_count
            , count(zip) as zip_linkage_count
            , count(phone) as phone_linkage_count
            , count(ssn4) as ssn4_linkage_count
        from lvs_lastrun
        group by run_to_analyze, last_nid
    )
    -- LV Data Density (lv_dd)
    , lvs_data_density as (
    select 'Patient' as type
        , run_to_analyze
        , round(avg(fn_linkage_count),2) as fn_dd
        , round(avg(ln_linkage_count),2) as ln_dd
        , round(avg(gender_linkage_count),2) as gender_dd
        , round(avg(dob_linkage_count),2) as dob_dd
        , round(avg(ssn_linkage_count),2) as ssn_dd
        , round(avg(address_linkage_count),2) as address_dd
        , round(avg(city_linkage_count),2) as city_dd
        , round(avg(state_linkage_count),2) as state_dd
        , round(avg(zip_linkage_count),2) as zip_dd
        , round(avg(phone_linkage_count),2) as phone_dd
        , round(avg(ssn4_linkage_count),2) as ssn4_dd
    from lvs_counts_by_sid 
    group by type, run_to_analyze
    union 
    select 'Network/Patient' as type
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
          , fn_dd
          , ln_dd
          , gender_dd
          , dob_dd
          , ssn_dd
          , address_dd
          , city_dd
          , state_dd
          , zip_dd
          , phone_dd
          , ssn4_dd
    from lvs_data_density
    order by type desc;



