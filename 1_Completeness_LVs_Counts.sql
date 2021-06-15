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
     , lvs_by_nid_rows as (
        select 'Network/Patient' as type, last_nid
            , case when count(first_name)=0 then 0 else 1 end as fn_by_nid_present_flag
            , case when count(last_name)=0 then 0 else 1 end as ln_by_nid_present_flag
            , case when count(gender)=0 then 0 else 1 end as gender_by_nid_present_flag
            , case when count(dob)=0 then 0 else 1 end as dob_by_nid_present_flag
            , case when count(ssn) =0 then 0 else 1 end as ssn_by_nid_present_flag
            , case when count(address)=0 then 0 else 1 end as address_by_nid_present_flag
            , case when count(city)=0 then 0 else 1 end as city_by_nid_present_flag
            , case when count(state)=0 then 0 else 1 end as state_by_nid_present_flag
            , case when count(zip)= 0 then 0 else 1 end as zip_by_nid_present_flag
            , case when count(phone)=0 then 0 else 1 end as phone_by_nid_present_flag
            , case when count(ssn4)=0 then 0 else 1 end as ssn4_by_nid_present_flag
        from lvs_lastrun
        group by last_nid
    )
    , lvs_by_nid_counts as (
        select type
            , count(last_nid) as denominator
            , sum(fn_by_nid_present_flag) as n_fn
            , sum(ln_by_nid_present_flag) as n_ln
            , sum(gender_by_nid_present_flag) as n_gender
            , sum(dob_by_nid_present_flag) as n_dob
            , sum(ssn_by_nid_present_flag) as n_ssn
            , sum(address_by_nid_present_flag) as n_address
            , sum(city_by_nid_present_flag) as n_city
            , sum(state_by_nid_present_flag) as n_state
            , sum(zip_by_nid_present_flag) as n_zip
            , sum(phone_by_nid_present_flag) as n_phone
            , sum(ssn4_by_nid_present_flag) as n_ssn4
        from lvs_by_nid_rows
        group by type
    )
    , lvs_by_sid_counts as (
    select 'Patient' as type
           , count(sid) as denominator
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
    ) 
    ,lvcounts as (
        select type, denominator, n_fn, n_ln, n_gender, n_dob, n_ssn, n_address, n_city, n_state, n_zip, n_phone, n_ssn4 from lvs_by_sid_counts
        union all
        select type, denominator, n_fn, n_ln, n_gender, n_dob, n_ssn, n_address, n_city, n_state, n_zip, n_phone, n_ssn4  from lvs_by_nid_counts
    )
    select type, denominator, n_fn, n_ln, n_gender, n_dob, n_ssn, n_address, n_city, n_state, n_zip, n_phone, n_ssn4  from lvcounts;
